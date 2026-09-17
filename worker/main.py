import asyncio
import contextlib
import json
import os
import secrets
from typing import Literal
from uuid import UUID

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field, model_validator
from starlette.background import BackgroundTask
from shared.events import sse
from shared.limits import BodyLimit


class Message(BaseModel):
    role: Literal['user', 'assistant']
    content: str = Field(min_length=1, max_length=65536)


class Generate(BaseModel):
    run_id: UUID
    messages: list[Message] = Field(min_length=1, max_length=41)
    max_tokens: int = Field(default=1024, ge=1, le=1024)

    @model_validator(mode='after')
    def last_message_is_user(self):
        if self.messages[-1].role != 'user':
            raise ValueError('Last message must be a user prompt')
        return self


class Worker:
    def __init__(self, client=None, model=None):
        self.model = model or os.environ.get('MODEL_NAME', 'Qwen/Qwen3-4B-Instruct-2507')
        self.client = client or httpx.AsyncClient(base_url='http://127.0.0.1:8000', timeout=httpx.Timeout(180, connect=5))
        self.ticket = None

    async def ready(self):
        try:
            return (await self.client.get('/health', timeout=2)).status_code == 200
        except httpx.HTTPError:
            return False

    async def fit_context(self, messages, max_tokens, with_tokens=False):
        messages = [{'role': 'system', 'content': 'You are a helpful assistant. Answer clearly and accurately.'}] + messages
        while True:
            response = await self.client.post('/tokenize', json={
                'model': self.model, 'messages': messages, 'add_generation_prompt': True})
            response.raise_for_status()
            if response.json()['count'] + max_tokens <= int(os.environ.get('MAX_MODEL_LEN', '8192')):
                return (messages, response.json()['count']) if with_tokens else messages
            if len(messages) <= 2:
                raise ValueError('Prompt exceeds model context window')
            del messages[1]
            while len(messages) > 2 and messages[1]['role'] != 'user':
                del messages[1]

    async def generate(self, request, ticket):
        try:
            async with asyncio.timeout(180):
                yield sse('status', {'stage': 'tokenizing', 'context_messages': len(request.messages)})
                messages, prompt_tokens = await self.fit_context(
                    [m.model_dump() for m in request.messages], request.max_tokens, with_tokens=True)
                yield sse('status', {'stage': 'context_ready', 'prompt_tokens': prompt_tokens,
                    'context_messages': len(messages), 'trimmed_messages': len(request.messages) + 1 - len(messages)})
                yield sse('status', {'stage': 'runtime_request'})
                async with self.client.stream('POST', '/v1/chat/completions', json={
                    'model': self.model, 'messages': messages, 'stream': True,
                    'stream_options': {'include_usage': True},
                    'max_tokens': request.max_tokens, 'temperature': 0.7}) as response:
                    response.raise_for_status()
                    yield sse('status', {'stage': 'runtime_stream_open', 'http_status': response.status_code})
                    reason, ended = None, False
                    async for line in response.aiter_lines():
                        if not line.startswith('data:'):
                            continue
                        data = line[5:].strip()
                        if data == '[DONE]':
                            ended = True
                            break
                        packet = json.loads(data)
                        if packet.get('error'):
                            raise RuntimeError('Model runtime error')
                        if packet.get('usage'):
                            yield sse('usage', {k: v for k, v in packet['usage'].items()
                                if k in ('prompt_tokens', 'completion_tokens', 'total_tokens') and type(v) is int})
                        for choice in packet.get('choices', []):
                            if text := choice.get('delta', {}).get('content'):
                                yield sse('token', {'text': text})
                            if choice.get('finish_reason'):
                                reason = choice['finish_reason']
                    if not ended or reason not in ('stop', 'length'):
                        raise RuntimeError('Incomplete model stream')
                    yield sse('done', {'finish_reason': reason})
        except asyncio.CancelledError:
            raise  # Closing the runtime stream cancels inference.
        except Exception:
            yield sse('error', {'message': 'Model unavailable or stream incomplete'})
        finally:
            await self.release(ticket)

    async def release(self, ticket):
        if self.ticket is ticket:
            self.ticket = None


def create_app(worker=None, service_key=None):
    key = service_key or os.environ['WORKER_SERVICE_KEY']
    if len(key) < 32:
        raise ValueError('WORKER_SERVICE_KEY must contain at least 32 random characters')
    runtime = worker or Worker()

    @contextlib.asynccontextmanager
    async def lifespan(app):
        yield
        await runtime.client.aclose()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(BodyLimit, limit=1048576)

    async def auth(x_worker_key: str = Header(default='')):
        if not secrets.compare_digest(x_worker_key.encode(), key.encode()):
            raise HTTPException(401, 'Invalid service key')

    @app.get('/ping')
    async def ping():
        # Runpod's internal health checker needs an unauthenticated 200/204.
        return Response(status_code=200 if await runtime.ready() else 204)

    @app.post('/generate', dependencies=[Depends(auth)])
    async def generate(request: Generate):
        if runtime.ticket is not None:
            raise HTTPException(429, 'Worker already generating')
        # Acquire before any await so two requests cannot claim the worker.
        ticket = object()
        runtime.ticket = ticket
        try:
            if not await runtime.ready():
                raise HTTPException(503, 'Model is starting')
        except BaseException:
            await runtime.release(ticket)
            raise
        return StreamingResponse(runtime.generate(request, ticket), media_type='text/event-stream',
            headers={'Cache-Control': 'no-store', 'X-Accel-Buffering': 'no'},
            background=BackgroundTask(runtime.release, ticket))

    return app

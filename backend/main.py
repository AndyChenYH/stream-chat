import asyncio
import base64
import binascii
import contextlib
import logging
import os
import secrets
import time

import httpx
from uuid import UUID

import anyio
import asyncpg
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, Response
from pydantic import BaseModel, Field, field_validator
from starlette.background import BackgroundTask

from backend.gate import Gate, QueueFull, ConversationBusy
from backend.model import Model
from backend.store import Store
from backend.agent import agent_generate, safe_name
from shared.limits import BodyLimit
from shared.events import sse

log = logging.getLogger('stream-chat')


class Prompt(BaseModel):
    request_id: UUID
    content: str = Field(min_length=1, max_length=4096)
    enable_tools: bool = False

    @field_validator('content')
    @classmethod
    def nonblank(cls, value):
        if not value.strip():
            raise ValueError('Enter a message')
        return value.strip()



class FileUpload(BaseModel):
    name: str = Field(min_length=1,max_length=100)
    data: str = Field(min_length=1,max_length=2796204)


class Job:
    def __init__(self, store, model, gate, ticket, chat_id, run_id, enable_tools=False):
        self.store, self.model, self.gate, self.ticket = store, model, gate, ticket
        self.chat_id, self.run_id = chat_id, run_id
        self.events = asyncio.Queue(maxsize=64)
        self.task = None
        self.started_at = time.monotonic()
        self.phase = 'prompt_saved'
        self.chunks = self.characters = 0
        self.first_token_ms = None
        self.enable_tools = enable_tools

    def elapsed_ms(self):
        return round((time.monotonic() - self.started_at) * 1000)

    async def emit(self, event, data):
        await asyncio.wait_for(self.events.put((event, {**data, 'elapsed_ms': self.elapsed_ms()})), timeout=15)

    async def progress(self, stage, **details):
        if stage not in ('worker_snapshot', 'telemetry_unavailable', 'token_usage'):
            self.phase = stage
        await self.emit('status', {'stage': stage, **details})

    async def run(self):
        try:
            await self.progress('prompt_saved')
            self.phase = 'queued'
            await self.emit('queued', {'run_id': str(self.run_id), 'position': self.gate.position(self.ticket)})
            async with asyncio.timeout(720):
                await asyncio.wait_for(self.ticket.ready.wait(), timeout=300)
                await self.progress('loading_history')
                await self.store.healthy()
                await self.store.status(self.run_id, 'streaming')
                messages = await self.store.context(self.chat_id, self.run_id)
                await self.progress('history_loaded', context_messages=len(messages))
                self.phase = 'worker_startup'
                await self.emit('starting', {})
                parts, finished, reason, usage = [], False, '', None
                generator = agent_generate(self.model,self.store,self.chat_id,self.run_id,messages,self.progress,self.emit) \
                    if self.enable_tools else self.model.generate(self.run_id,messages,self.progress)
                async with contextlib.aclosing(generator) as stream:
                    async for chunk in stream:
                        await self.consume(chunk, parts)
                        if chunk.done:
                            finished, reason, usage = True, chunk.finish_reason, chunk.usage
                if not finished:
                    raise RuntimeError('Worker stream ended without completion')
                await self.progress('saving_reply', chunks=self.chunks, characters=self.characters)
                message_id = await self.store.complete(self.chat_id, self.run_id, ''.join(parts))
                await self.emit('done', {'message_id': message_id, 'finish_reason': reason,
                    'chunks': self.chunks, 'characters': self.characters,
                    'first_token_ms': self.first_token_ms, 'usage': usage})
        except asyncio.CancelledError:
            await self.store.status(self.run_id, 'cancelled')
            raise
        except Exception as exc:
            # Do not log prompt text, credentials, connection strings or model output.
            log.error('run=%s failed type=%s', self.run_id, type(exc).__name__)
            with contextlib.suppress(Exception):
                await self.store.status(self.run_id, 'failed')
            with contextlib.suppress(asyncio.TimeoutError):
                await self.emit('error', {'message': 'Generation failed or timed out. Your prompt is saved; reload history before retrying.',
                    'stage': self.phase, 'error_type': type(exc).__name__,
                    'http_status': exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None})
        finally:
            self.gate.release(self.ticket)

    async def consume(self, chunk, parts):
        if chunk.started:
            await self.emit('started', {})
        if chunk.text:
            if self.first_token_ms is None:
                self.first_token_ms = self.elapsed_ms()
                await self.progress('first_token')
            self.phase = 'streaming'
            self.chunks += 1
            self.characters += len(chunk.text)
            parts.append(chunk.text)
            await self.emit('token', {'text':chunk.text,'chunks':self.chunks,'characters':self.characters})

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
        if self.task:
            await asyncio.gather(self.task, return_exceptions=True)
        # A task cancelled before its first turn never enters run()'s finally.
        self.gate.release(self.ticket)
        with contextlib.suppress(Exception):
            await self.store.status(self.run_id, 'cancelled')

    async def stream(self):
        try:
            while True:
                try:
                    event, data = await asyncio.wait_for(self.events.get(), timeout=10)
                except asyncio.TimeoutError:
                    if self.task.done():
                        yield sse('error', {'message': 'Stream interrupted. Reload conversation history.'})
                        return
                    yield sse('heartbeat', {'stage': self.phase, 'elapsed_ms': self.elapsed_ms(),
                        'position': self.gate.position(self.ticket), 'chunks': self.chunks,
                        'characters': self.characters})
                    continue
                yield sse(event, data)
                if event in ('done', 'error'):
                    return
        finally:
            # Starlette cancels the stream on disconnect; shield durable status cleanup.
            with anyio.CancelScope(shield=True):
                await self.stop()


def create_app(store=None, model=None, access_key=None, origins=None):
    key = access_key or os.environ['CHAT_ACCESS_KEY']
    if len(key) < 32:
        raise RuntimeError('CHAT_ACCESS_KEY must contain at least 32 random characters')
    gate, jobs = Gate(waiting_limit=5), set()

    @contextlib.asynccontextmanager
    async def lifespan(app):
        app.state.store = store or Store(os.environ['DATABASE_URL'])
        await app.state.store.open()
        app.state.model = model or Model()
        try:
            yield
        finally:
            await asyncio.gather(*(job.stop() for job in list(jobs)))
            await app.state.model.close()
            await app.state.store.close()

    app = FastAPI(title='Stream Chat', lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(BodyLimit, limit=32768, upload_limit=2800000)
    allowed = origins or os.environ.get('ALLOWED_ORIGINS', 'http://localhost:5173').split(',')
    app.add_middleware(CORSMiddleware, allow_origins=[o.strip() for o in allowed],
        allow_methods=['GET', 'POST'], allow_headers=['Authorization', 'Content-Type'])

    async def auth(authorization: str = Header(default='')):
        if not secrets.compare_digest(authorization.encode(), ('Bearer ' + key).encode()):
            raise HTTPException(401, 'Invalid access key')

    @app.middleware('http')
    async def no_cache(request, call_next):
        response = await call_next(request)
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    @app.get('/healthz')
    async def health():
        # Fly probes this frequently. Do not wake Neon or Runpod here.
        return {'ok': True}

    @app.get('/v1/status', dependencies=[Depends(auth)])
    async def status():
        try:
            result = await app.state.model.ready()
        except Exception:
            result = {'ready': False, 'model': 'unavailable'}
        tools_configured = bool(os.environ.get('E2B_API_KEY'))
        return {**result, 'active': min(len(gate.tickets), 1), 'queued': max(len(gate.tickets) - 1, 0),
            'tools_configured':tools_configured,
            'sandbox_budget':await app.state.store.sandbox_budget() if tools_configured else None}

    @app.get('/v1/conversations', dependencies=[Depends(auth)])
    async def chats(offset: int = Query(0, ge=0)):
        return await app.state.store.list_chats(offset)

    @app.post('/v1/conversations', dependencies=[Depends(auth)])
    async def create_chat():
        return await app.state.store.create_chat()

    @app.get('/v1/conversations/{chat_id}/messages', dependencies=[Depends(auth)])
    async def messages(chat_id: UUID, before: int | None = Query(None, ge=1)):
        if not await app.state.store.exists(chat_id):
            raise HTTPException(404, 'Conversation not found')
        return await app.state.store.messages(chat_id, before)

    @app.post('/v1/conversations/{chat_id}/messages', dependencies=[Depends(auth)])
    async def generate(chat_id: UUID, prompt: Prompt):
        db = app.state.store
        if not await db.exists(chat_id):
            raise HTTPException(404, 'Conversation not found')
        if prompt.enable_tools and not os.environ.get('E2B_API_KEY'):
            raise HTTPException(503,'Code tools are not configured')
        try:
            ticket = gate.reserve(str(chat_id))
        except ConversationBusy:
            raise HTTPException(409, 'This conversation already has a generation in progress')
        except QueueFull:
            raise HTTPException(429, 'The queue is full. Try again shortly.', headers={'Retry-After': '10'})
        try:
            await db.healthy()
            await db.accept(chat_id, prompt.request_id, prompt.content)
        except asyncpg.UniqueViolationError:
            gate.release(ticket)
            raise HTTPException(409, 'This request was already accepted. Reload conversation history.')
        except BaseException:
            gate.release(ticket)
            raise
        job = Job(db, app.state.model, gate, ticket, chat_id, prompt.request_id,prompt.enable_tools)
        jobs.add(job)
        job.task = asyncio.create_task(job.run())
        job.task.add_done_callback(lambda _: jobs.discard(job))
        return StreamingResponse(job.stream(), media_type='text/event-stream',
            headers={'X-Accel-Buffering': 'no', 'Cache-Control': 'no-store'}, background=BackgroundTask(job.stop))

    @app.get('/v1/conversations/{chat_id}/tools', dependencies=[Depends(auth)])
    async def tools(chat_id: UUID):
        return await app.state.store.tools(chat_id)

    @app.get('/v1/conversations/{chat_id}/files', dependencies=[Depends(auth)])
    async def files(chat_id: UUID):
        return await app.state.store.files(chat_id)

    @app.post('/v1/conversations/{chat_id}/files', dependencies=[Depends(auth)])
    async def upload(chat_id: UUID, file: FileUpload):
        if not await app.state.store.exists(chat_id):
            raise HTTPException(404,'Conversation not found')
        if any(t.conversation_id == str(chat_id) for t in gate.tickets):
            raise HTTPException(409,'Wait for this conversation to finish before adding files')
        try:
            data = base64.b64decode(file.data,validate=True)
            return await app.state.store.save_artifact(chat_id,None,safe_name(file.name),'application/octet-stream',data)
        except (ValueError,binascii.Error) as exc:
            raise HTTPException(400,str(exc))

    @app.get('/v1/files/{file_id}', dependencies=[Depends(auth)])
    async def download(file_id: UUID):
        file = await app.state.store.file(file_id)
        if file is None:
            raise HTTPException(404,'File not found')
        return Response(file['data'],media_type=file['mime_type'],headers={
            'Content-Disposition':f'attachment; filename="{safe_name(file["name"])}"',
            'Content-Security-Policy':"default-src 'none'; sandbox"})

    return app

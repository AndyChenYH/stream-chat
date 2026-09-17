import asyncio
import contextlib
import logging
import os
import secrets
from uuid import UUID

import anyio
import asyncpg
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator
from starlette.background import BackgroundTask

from backend.gate import Gate, QueueFull, ConversationBusy
from backend.model import Model
from backend.store import Store
from shared.limits import BodyLimit
from shared.events import sse

log = logging.getLogger('stream-chat')


class Prompt(BaseModel):
    request_id: UUID
    content: str = Field(min_length=1, max_length=4096)

    @field_validator('content')
    @classmethod
    def nonblank(cls, value):
        if not value.strip():
            raise ValueError('Enter a message')
        return value.strip()



class Job:
    def __init__(self, store, model, gate, ticket, chat_id, run_id):
        self.store, self.model, self.gate, self.ticket = store, model, gate, ticket
        self.chat_id, self.run_id = chat_id, run_id
        self.events = asyncio.Queue(maxsize=64)
        self.task = None

    async def emit(self, event, data):
        await asyncio.wait_for(self.events.put((event, data)), timeout=15)

    async def run(self):
        try:
            await self.emit('queued', {'run_id': str(self.run_id), 'position': self.gate.position(self.ticket)})
            async with asyncio.timeout(720):
                await asyncio.wait_for(self.ticket.ready.wait(), timeout=300)
                await self.store.healthy()
                await self.store.status(self.run_id, 'streaming')
                messages = await self.store.context(self.chat_id, self.run_id)
                await self.emit('starting', {})
                parts, finished, reason = [], False, ''
                async for chunk in self.model.generate(self.run_id, messages):
                    if chunk.started:
                        await self.emit('started', {})
                    if chunk.text:
                        parts.append(chunk.text)
                        await self.emit('token', {'text': chunk.text})
                    if chunk.done:
                        finished, reason = True, chunk.finish_reason
                if not finished:
                    raise RuntimeError('Worker stream ended without completion')
                message_id = await self.store.complete(self.chat_id, self.run_id, ''.join(parts))
                await self.emit('done', {'message_id': message_id, 'finish_reason': reason})
        except asyncio.CancelledError:
            await self.store.status(self.run_id, 'cancelled')
            raise
        except Exception as exc:
            # Do not log prompt text, credentials, connection strings or model output.
            log.error('run=%s failed type=%s', self.run_id, type(exc).__name__)
            with contextlib.suppress(Exception):
                await self.store.status(self.run_id, 'failed')
            with contextlib.suppress(asyncio.TimeoutError):
                await self.emit('error', {'message': 'Generation failed or timed out. Your prompt is saved; reload history before retrying.'})
        finally:
            self.gate.release(self.ticket)

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
                    yield ': keepalive\n\n'
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
    app.add_middleware(BodyLimit, limit=32768)
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
        return {**result, 'active': min(len(gate.tickets), 1), 'queued': max(len(gate.tickets) - 1, 0)}

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
        job = Job(db, app.state.model, gate, ticket, chat_id, prompt.request_id)
        jobs.add(job)
        job.task = asyncio.create_task(job.run())
        job.task.add_done_callback(lambda _: jobs.discard(job))
        return StreamingResponse(job.stream(), media_type='text/event-stream',
            headers={'X-Accel-Buffering': 'no', 'Cache-Control': 'no-store'}, background=BackgroundTask(job.stop))

    return app

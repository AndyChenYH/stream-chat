import asyncio
import json
from uuid import uuid4
import pytest
from fastapi.testclient import TestClient
from backend.gate import Gate, QueueFull, ConversationBusy
from backend.main import Job, create_app
from shared.events import Chunk


class MemoryStore:
    """Test double only. Production always uses Postgres."""
    def __init__(self):
        self.runs, self.saved = {}, []
        self.chat_id = uuid4()
    async def open(self): pass
    async def close(self): pass
    async def healthy(self): pass
    async def exists(self, chat_id): return chat_id == self.chat_id
    async def accept(self, chat_id, run_id, content): self.runs[run_id] = 'queued'
    async def context(self, chat_id, run_id): return [{'role':'user','content':'hello'}]
    async def status(self, run_id, status):
        if self.runs.get(run_id) in ('queued','streaming'): self.runs[run_id] = status
    async def complete(self, chat_id, run_id, content):
        self.saved.append(content); self.runs[run_id] = 'done'; return str(uuid4())


class FakeModel:
    async def close(self): pass
    async def ready(self): return {'ready':True, 'model':'test'}
    async def generate(self, *args):
        yield Chunk(started=True)
        yield Chunk(text='Hello ')
        await asyncio.sleep(.01)
        yield Chunk(text='world')
        yield Chunk(done=True, finish_reason='stop')


@pytest.mark.asyncio
async def test_fifo_queue_bounds_and_cancelled_waiter():
    gate = Gate(2)
    a,b,c = [gate.reserve(x) for x in 'abc']
    assert a.ready.is_set() and not b.ready.is_set()
    with pytest.raises(QueueFull): gate.reserve('d')
    with pytest.raises(ConversationBusy): gate.reserve('a')
    gate.release(b)
    assert not c.ready.is_set()
    gate.release(a)
    assert c.ready.is_set()
    gate.release(c)
    assert not gate.tickets


@pytest.mark.asyncio
async def test_done_only_after_answer_is_saved():
    store, gate = MemoryStore(), Gate()
    run_id = uuid4(); await store.accept(store.chat_id, run_id, 'hello')
    job = Job(store, FakeModel(), gate, gate.reserve(str(store.chat_id)), store.chat_id, run_id)
    job.task = asyncio.create_task(job.run())
    events = []
    async for frame in job.stream():
        events.append(frame)
        if 'event: done' in frame: assert store.saved == ['Hello world']
    decoded = [(f.split('\n')[0][7:], json.loads(f.split('\n')[1][6:])) for f in events]
    assert ['queued','starting','started','token','token','done'] == [event for event, _ in decoded if event != 'status']
    stages = [data['stage'] for event, data in decoded if event == 'status']
    assert stages == ['prompt_saved','loading_history','history_loaded','first_token','saving_reply']
    assert decoded[-1][1]['chunks'] == 2 and decoded[-1][1]['characters'] == 11
    assert decoded[-1][1]['first_token_ms'] <= decoded[-1][1]['elapsed_ms']
    assert not gate.tickets


@pytest.mark.asyncio
async def test_partial_stream_is_not_saved():
    class Broken(FakeModel):
        async def generate(self,*args):
            yield Chunk(text='partial')
            raise RuntimeError('connection failed')
    store, gate = MemoryStore(), Gate()
    rid = uuid4(); await store.accept(store.chat_id,rid,'hello')
    job = Job(store,Broken(),gate,gate.reserve('a'),store.chat_id,rid)
    job.task = asyncio.create_task(job.run())
    result = [f async for f in job.stream()]
    assert 'event: error' in result[-1]
    assert not store.saved and store.runs[rid] == 'failed' and not gate.tickets


@pytest.mark.asyncio
async def test_disconnect_releases_active_slot_and_cancels_worker():
    cancelled = asyncio.Event()
    class Slow(FakeModel):
        async def generate(self,*args):
            try:
                yield Chunk(text='partial')
                await asyncio.Event().wait()
            finally: cancelled.set()
    store, gate = MemoryStore(), Gate()
    rid = uuid4(); await store.accept(store.chat_id,rid,'hello')
    job = Job(store,Slow(),gate,gate.reserve('a'),store.chat_id,rid)
    job.task = asyncio.create_task(job.run())
    stream = job.stream()
    while 'event: token' not in await anext(stream): pass
    await stream.aclose()
    assert cancelled.is_set() and not gate.tickets
    assert store.runs[rid] == 'cancelled' and not store.saved


def test_api_auth_validation_and_post_sse():
    store = MemoryStore()
    app = create_app(store=store,model=FakeModel(),access_key='test-'*10,origins=['https://example.github.io'])
    with TestClient(app) as client:
        path = f'/v1/conversations/{store.chat_id}/messages'
        assert client.post(path,json={'content':'hi','request_id':str(uuid4())}).status_code == 401
        headers = {'Authorization':'Bearer '+'test-'*10}
        assert client.post(path,headers=headers,json={'content':' ','request_id':str(uuid4())}).status_code == 422
        result = client.post(path,headers=headers,json={'content':'hello','request_id':str(uuid4())})
        assert result.status_code == 200 and result.headers['content-type'].startswith('text/event-stream')
        assert 'event: done' in result.text and store.saved == ['Hello world']
        assert 'access-control-allow-origin' not in client.options(path,headers={
            'Origin':'https://evil.example','Access-Control-Request-Method':'POST'}).headers


@pytest.mark.asyncio
async def test_cancel_before_job_starts_releases_slot():
    store, gate = MemoryStore(), Gate()
    rid = uuid4(); await store.accept(store.chat_id,rid,'hello')
    job = Job(store,FakeModel(),gate,gate.reserve('a'),store.chat_id,rid)
    job.task = asyncio.create_task(job.run())
    await job.stop()
    assert not gate.tickets and store.runs[rid] == 'cancelled'
    assert not store.saved


def test_chunked_request_size_is_bounded():
    store = MemoryStore()
    app = create_app(store=store,model=FakeModel(),access_key='test-'*10)
    with TestClient(app) as client:
        result = client.post('/v1/conversations',content=iter([b'x'*20000,b'x'*20000]),headers={'Authorization':'Bearer '+'test-'*10})
        assert result.status_code == 413


def test_fly_health_probe_does_not_wake_database():
    class SleepingStore(MemoryStore):
        async def healthy(self): raise AssertionError('Health probe woke Neon')
    app=create_app(store=SleepingStore(),model=FakeModel(),access_key='test-'*10)
    with TestClient(app) as client:
        assert client.get('/healthz').status_code==200

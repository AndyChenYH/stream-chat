import asyncio
import json
from uuid import uuid4
import httpx
import pytest
from fastapi.testclient import TestClient
from worker.main import Worker, Generate, create_app
from worker.model_path import resolve

KEY = 'service-test-key-' * 3


def runtime_reply(request, completed=True):
    if request.url.path == '/tokenize': return httpx.Response(200,json={'count':20})
    if request.url.path == '/health': return httpx.Response(200)
    data='data: {"choices":[{"delta":{"content":"hello"}}]}\n\n'
    if completed: data+='data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n'
    return httpx.Response(200,text=data)


def payload():
    return {'run_id':str(uuid4()),'messages':[{'role':'user','content':'hi'}],'max_tokens':100}


def test_worker_service_auth_stream_and_incomplete_runtime():
    for completed in (True,False):
        runtime = httpx.AsyncClient(transport=httpx.MockTransport(lambda r:runtime_reply(r,completed)),base_url='http://localhost:8000')
        worker=Worker(client=runtime,model='test')
        with TestClient(create_app(worker=worker,service_key=KEY)) as client:
            assert client.get('/ping').status_code==200
            assert client.post('/generate',json=payload()).status_code==401
            response=client.post('/generate',json=payload(),headers={'X-Worker-Key':KEY})
            assert 'event: token' in response.text
            assert ('event: done' in response.text)==completed
            assert ('event: error' in response.text)==(not completed)
            assert worker.ticket is None


def test_health_reports_initializing_and_generation_is_serial():
    runtime=httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(503)),base_url='http://localhost')
    worker=Worker(client=runtime)
    with TestClient(create_app(worker=worker,service_key=KEY)) as client:
        assert client.get('/ping').status_code==204
        assert client.post('/generate',json=payload(),headers={'X-Worker-Key':KEY}).status_code==503
        assert worker.ticket is None
        worker.ticket=object()
        assert client.post('/generate',json=payload(),headers={'X-Worker-Key':KEY}).status_code==429


@pytest.mark.asyncio
async def test_context_trims_complete_old_turns():
    async def runtime(request):
        messages=json.loads(request.content)['messages']
        return httpx.Response(200,json={'count':9000 if len(messages)>2 else 100})
    async with httpx.AsyncClient(transport=httpx.MockTransport(runtime),base_url='http://localhost') as client:
        result=await Worker(client=client).fit_context([
            {'role':'user','content':'old question'},{'role':'assistant','content':'old answer'},
            {'role':'user','content':'new question'}],1024)
    assert [m['content'] for m in result[1:]]==['new question']


@pytest.mark.asyncio
async def test_disconnect_closes_runtime_and_releases_only_its_ticket():
    closed=asyncio.Event()
    class Slow(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'data: {"choices":[{"delta":{"content":"partial"}}]}\n\n'
            await asyncio.Event().wait()
        async def aclose(self): closed.set()
    def runtime(request):
        if request.url.path=='/tokenize': return httpx.Response(200,json={'count':10})
        return httpx.Response(200,stream=Slow())
    async with httpx.AsyncClient(transport=httpx.MockTransport(runtime),base_url='http://localhost') as client:
        worker=Worker(client=client); old=worker.ticket=object()
        stream=worker.generate(Generate(**payload()),old)
        assert 'partial' in await anext(stream)
        await stream.aclose()
        assert closed.is_set() and worker.ticket is None
        newer=worker.ticket=object()
        await worker.release(old)
        assert worker.ticket is newer


def test_model_cache_resolves_snapshot_and_requires_cache(tmp_path):
    cache=tmp_path/'models--Qwen--model'
    (cache/'snapshots'/'abc').mkdir(parents=True)
    (cache/'snapshots'/'abc'/'config.json').write_text('{}')
    assert resolve('Qwen/model',tmp_path)==str(cache/'snapshots'/'abc')
    with pytest.raises(RuntimeError,match='cached model'):
        resolve('missing/model',tmp_path)

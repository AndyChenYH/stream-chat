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
        while 'event: token' not in (frame := await anext(stream)): pass
        assert 'partial' in frame
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


def test_worker_reports_real_context_stages_and_final_token_usage():
    def runtime(request):
        if request.url.path in ('/tokenize', '/health'):
            return runtime_reply(request)
        assert json.loads(request.content)['stream_options'] == {'include_usage': True}
        return httpx.Response(200, text=(
            'data: {"choices":[{"delta":{"content":"hello"},"finish_reason":"stop"}]}\n\n'
            'data: {"choices":[],"usage":{"prompt_tokens":20,"completion_tokens":1,"total_tokens":21}}\n\n'
            'data: [DONE]\n\n'))
    runtime_client = httpx.AsyncClient(transport=httpx.MockTransport(runtime),base_url='http://localhost')
    with TestClient(create_app(worker=Worker(client=runtime_client),service_key=KEY)) as client:
        result = client.post('/generate',json=payload(),headers={'X-Worker-Key':KEY})
    frames = [frame.splitlines() for frame in result.text.strip().split('\n\n')]
    events = [(lines[0][7:], json.loads(lines[1][6:])) for lines in frames]
    assert [data['stage'] for event,data in events if event == 'status'] == [
        'tokenizing','context_ready','runtime_request','runtime_stream_open']
    assert events[1][1]['prompt_tokens'] == 20
    assert events[-2] == ('usage', {'prompt_tokens':20,'completion_tokens':1,'total_tokens':21})
    assert events[-1][0] == 'done'


@pytest.mark.parametrize('finish,ended,executable', [('tool_calls', True, True), ('length', True, False), ('tool_calls', False, False)])
def test_native_tools_only_emitted_after_complete_validated_stream(finish, ended, executable):
    def runtime(request):
        if request.url.path in ('/tokenize', '/health'):
            return runtime_reply(request)
        assert json.loads(request.content)['tools'][0]['function']['name'] == 'terminal'
        packets = [
            {'choices': [{'delta': {'tool_calls': [{'index': 0, 'id': 'call_1',
                'function': {'name': 'python', 'arguments': '{"code":'}}]}}]},
            {'choices': [{'delta': {'tool_calls': [{'index': 0, 'function': {'arguments': '"print(2)"}'}}]}, 'finish_reason': finish}]},
        ]
        return httpx.Response(200, text=''.join('data: ' + json.dumps(p) + '\n\n' for p in packets)
                              + ('data: [DONE]\n\n' if ended else ''))
    runtime_client = httpx.AsyncClient(transport=httpx.MockTransport(runtime),base_url='http://localhost')
    with TestClient(create_app(worker=Worker(client=runtime_client),service_key=KEY)) as client:
        result = client.post('/generate',json={**payload(), 'enable_tools': True},headers={'X-Worker-Key':KEY})
    assert ('event: tool_call' in result.text) == executable
    assert ('event: error' in result.text) != executable


def test_tool_results_require_matching_calls():
    with pytest.raises(ValueError, match='Orphaned'):
        Generate(**{**payload(), 'messages': [{'role': 'tool', 'tool_call_id': 'missing', 'content': '42'}]})


@pytest.mark.asyncio
async def test_context_does_not_drop_current_tool_chain():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200,json={'count': 9000})),base_url='http://localhost') as client:
        with pytest.raises(ValueError, match='context window'):
            await Worker(client=client).fit_context([{'role':'user','content':'current'},
                {'role':'assistant','content':'calling'}, {'role':'tool','content':'result'}],1024)

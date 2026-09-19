import asyncio
import json
from uuid import uuid4
import httpx
import pytest
from backend.model import Model
from shared.events import sse


def model(client,**kw):
    return Model(client=client,endpoint='https://example.api.runpod.ai',api_key='test',service_key='service-'*6,retry_delay=0,**kw)


@pytest.mark.asyncio
async def test_status_does_not_wake_gpu_and_readiness_retries_before_one_post():
    calls=[]
    def handler(request):
        calls.append((request.method,request.url.path))
        if request.method=='GET': return httpx.Response(503 if len(calls)==1 else 200)
        return httpx.Response(200,headers={'content-type':'text/event-stream'},text='event: token\ndata: {"text":"hello"}\n\nevent: done\ndata: {"finish_reason":"stop"}\n\n')
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler),base_url='https://example.api.runpod.ai') as client:
        worker=model(client)
        assert (await worker.ready())['mode']=='on-demand' and not calls
        chunks=[c async for c in worker.generate(uuid4(),[{'role':'user','content':'hi'}])]
    assert chunks[0].started and chunks[1].text=='hello' and chunks[-1].done
    assert calls==[('GET','/ping'),('GET','/ping'),('POST','/generate')]


@pytest.mark.asyncio
@pytest.mark.parametrize('status',[401,403])
async def test_invalid_credentials_fail_without_retries(status):
    calls=[]
    def handler(request):
        calls.append(request); return httpx.Response(status)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler),base_url='https://example.api.runpod.ai') as client:
        with pytest.raises(httpx.HTTPStatusError):
            await model(client).wait_until_ready()
    assert len(calls)==1


@pytest.mark.asyncio
async def test_ambiguous_generation_is_never_retried():
    calls=[]
    def handler(request):
        calls.append(request.method)
        if request.method=='GET': return httpx.Response(200)
        raise httpx.ReadError('Disconnected after submission')
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler),base_url='https://example.api.runpod.ai') as client:
        with pytest.raises(httpx.ReadError):
            _=[c async for c in model(client).generate(uuid4(),[])]
    assert calls==['GET','POST']


@pytest.mark.asyncio
async def test_partial_answer_is_not_completion():
    def handler(request):
        if request.method=='GET': return httpx.Response(200)
        return httpx.Response(200,headers={'content-type':'text/event-stream'},text='event: token\ndata: {"text":"partial"}\n\n')
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler),base_url='https://example.api.runpod.ai') as client:
        with pytest.raises(RuntimeError,match='without completion'):
            _=[c async for c in model(client).generate(uuid4(),[])]


@pytest.mark.asyncio
async def test_cold_start_wait_has_deadline_and_is_cancellable():
    async def handler(request):
        await asyncio.sleep(10); return httpx.Response(200)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler),base_url='https://example.api.runpod.ai') as client:
        with pytest.raises(TimeoutError): await model(client,startup_timeout=.02).wait_until_ready()
        task=asyncio.create_task(model(client).wait_until_ready())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError): await task


def test_worker_credentials_never_go_to_plain_http_or_arbitrary_host():
    for endpoint in ('http://example.api.runpod.ai','https://example.com','https://example.api.runpod.ai.evil.test'):
        with pytest.raises(ValueError): Model(endpoint=endpoint,api_key='test',service_key='service-'*6)


@pytest.mark.asyncio
async def test_pending_readiness_emits_provider_state_and_cancellation_closes_probe():
    cancelled=asyncio.Event()
    async def probe(request):
        try: await asyncio.Event().wait()
        finally: cancelled.set()
    def health(request):
        return httpx.Response(200,json={'workers':{'running':1,'initializing':0,'idle':0,'ready':0},
                                       'secret':'must-not-be-forwarded'})
    reports=[]
    observed=asyncio.Event()
    async def report(stage, **details):
        reports.append((stage,details))
        if stage == 'worker_snapshot': observed.set()
    async with httpx.AsyncClient(transport=httpx.MockTransport(probe),base_url='https://example.api.runpod.ai') as client, httpx.AsyncClient(transport=httpx.MockTransport(health),base_url='https://api.runpod.ai/v2/example/') as status_client:
        worker=model(client,status_client=status_client,poll_interval=.01)
        task=asyncio.create_task(worker.wait_until_ready(report))
        await asyncio.wait_for(observed.wait(),1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError): await task
        assert cancelled.is_set()
    assert reports[0][0] == 'readiness_probe'
    assert any(stage == 'readiness_wait' for stage,_ in reports)
    assert all('must-not-be-forwarded' not in str(details) for _,details in reports)


@pytest.mark.asyncio
async def test_failed_telemetry_does_not_prevent_model_readiness():
    async def probe(request):
        await asyncio.sleep(.03)
        return httpx.Response(200)
    reports=[]
    async def report(stage, **details): reports.append(stage)
    async with httpx.AsyncClient(transport=httpx.MockTransport(probe),base_url='https://example.api.runpod.ai') as client, httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(403)),base_url='https://api.runpod.ai/v2/example/') as status_client:
        await model(client,status_client=status_client,poll_interval=.01).wait_until_ready(report)
    assert 'telemetry_unavailable' in reports and reports[-1] == 'model_ready'


@pytest.mark.asyncio
async def test_worker_diagnostics_and_usage_are_forwarded_without_raw_fields():
    reports=[]
    async def report(stage, **details): reports.append((stage,details))
    def handler(request):
        if request.method == 'GET': return httpx.Response(200)
        return httpx.Response(200,headers={'content-type':'text/event-stream'},text=(
            'event: status\ndata: {"stage":"tokenizing","context_messages":3,"secret":"hidden"}\n\n'
            'event: status\ndata: {"stage":"unknown-stage","secret":"hidden"}\n\n'
            'event: token\ndata: {"text":"hello"}\n\n'
            'event: usage\ndata: {"prompt_tokens":20,"completion_tokens":1,"total_tokens":21,"secret":"hidden"}\n\n'
            'event: done\ndata: {"finish_reason":"stop"}\n\n'))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler),base_url='https://example.api.runpod.ai') as client:
        result=[c async for c in model(client).generate(uuid4(),[],report)]
    assert ('tokenizing', {'context_messages':3}) in reports
    assert result[-1].usage == {'prompt_tokens':20,'completion_tokens':1,'total_tokens':21}
    assert all('hidden' not in str(item) and item[0] != 'unknown-stage' for item in reports)


@pytest.mark.asyncio
@pytest.mark.parametrize('suffix,valid', [(sse('done',{'finish_reason':'tool_calls'}),True),
    ('',False),(sse('done',{'finish_reason':'tool_calls'})+sse('token',{'text':'late'}),False)])
async def test_tool_calls_are_buffered_until_clean_worker_eof(suffix,valid):
    call={'id':'call_1','type':'function','function':{'name':'python','arguments':json.dumps({'code':'print(42)'})}}
    def handler(request):
        if request.method=='GET': return httpx.Response(200)
        return httpx.Response(200,headers={'content-type':'text/event-stream'},text=sse('tool_call',call)+suffix)
    received=[]
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler),base_url='https://example.api.runpod.ai') as client:
        try:
            async for chunk in model(client).generate(uuid4(),[],enable_tools=True):
                if chunk.tool_call: received.append(chunk.tool_call)
        except RuntimeError:
            assert not valid
    assert bool(received)==valid

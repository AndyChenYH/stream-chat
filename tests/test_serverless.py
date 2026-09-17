import asyncio
from uuid import uuid4
import httpx
import pytest
from backend.model import Model


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

import asyncio
from pathlib import Path
import grpc
import httpx
import pytest
from worker.main import Worker
from scripts.create_certs import create
from shared import model_pb2 as pb, model_pb2_grpc as rpc


@pytest.mark.asyncio
async def test_real_mtls_grpc_stream_and_unauthorized_client(tmp_path):
    create(tmp_path/'certs')
    certs = tmp_path/'certs'
    def read(name): return (certs/name).read_bytes()
    async def runtime(request):
        if request.url.path == '/tokenize': return httpx.Response(200,json={'count':20})
        if request.url.path == '/health': return httpx.Response(200)
        return httpx.Response(200,text='data: {"choices":[{"delta":{"content":"hello"}}]}\n\ndata: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n')
    client = httpx.AsyncClient(transport=httpx.MockTransport(runtime),base_url='http://localhost:8000')
    server = grpc.aio.server()
    rpc.add_ModelWorkerServicer_to_server(Worker(client=client,model='test'),server)
    port = server.add_secure_port('127.0.0.1:0',grpc.ssl_server_credentials(
        [(read('server.key'),read('server.crt'))],root_certificates=read('ca.crt'),require_client_auth=True))
    await server.start()
    options = [('grpc.ssl_target_name_override','model-worker')]
    try:
        creds = grpc.ssl_channel_credentials(read('ca.crt'),read('client.key'),read('client.crt'))
        async with grpc.aio.secure_channel(f'127.0.0.1:{port}',creds,options=options) as channel:
            stub = rpc.ModelWorkerStub(channel)
            assert (await stub.Ready(pb.ReadyRequest(),timeout=3)).ready
            chunks = [c async for c in stub.Generate(pb.GenerateRequest(
                run_id='test',messages=[pb.Message(role='user',content='hi')],max_tokens=100),timeout=3)]
            assert chunks[0].text=='hello' and chunks[-1].done
        async with grpc.aio.secure_channel(f'127.0.0.1:{port}',grpc.ssl_channel_credentials(read('ca.crt')),options=options) as channel:
            with pytest.raises(grpc.aio.AioRpcError):
                await rpc.ModelWorkerStub(channel).Ready(pb.ReadyRequest(),timeout=2)
    finally:
        await server.stop(0); await client.aclose()


@pytest.mark.asyncio
async def test_context_trims_complete_old_turns():
    seen=[]
    async def runtime(request):
        import json
        messages=json.loads(request.content)['messages']; seen.append(messages)
        return httpx.Response(200,json={'count':9000 if len(messages)>2 else 100})
    async with httpx.AsyncClient(transport=httpx.MockTransport(runtime),base_url='http://localhost') as client:
        result=await Worker(client=client).fit_context([
            {'role':'user','content':'old question'},{'role':'assistant','content':'old answer'},
            {'role':'user','content':'new question'}],1024)
    assert [m['content'] for m in result[1:]]==['new question']

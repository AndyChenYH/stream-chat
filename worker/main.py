import asyncio
import json
import os
import signal

import grpc
import httpx
from shared import model_pb2 as pb, model_pb2_grpc as rpc
from shared.tls import pem


class Worker(rpc.ModelWorkerServicer):
    def __init__(self, client=None, model=None):
        self.model = model or os.environ.get('MODEL_NAME', 'Qwen/Qwen3-4B-Instruct-2507')
        self.client = client or httpx.AsyncClient(base_url='http://127.0.0.1:8000', timeout=httpx.Timeout(180, connect=5))
        self.busy = False

    async def authenticate(self, context):
        identities = context.auth_context().get('x509_common_name', [])
        if b'chat-service' not in identities:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, 'Chat service certificate required')

    async def Ready(self, request, context):
        await self.authenticate(context)
        try:
            response = await self.client.get('/health', timeout=5)
            return pb.ReadyReply(ready=response.status_code == 200, model=self.model)
        except httpx.HTTPError:
            return pb.ReadyReply(ready=False, model=self.model)

    async def fit_context(self, messages, max_tokens):
        """Count with the runtime's actual chat template; remove oldest full turns."""
        messages = [{'role': 'system', 'content': 'You are a helpful assistant. Answer clearly and accurately.'}] + messages
        while True:
            response = await self.client.post('/tokenize', json={
                'model': self.model, 'messages': messages, 'add_generation_prompt': True})
            response.raise_for_status()
            if response.json()['count'] + max_tokens <= int(os.environ.get('MAX_MODEL_LEN', '8192')):
                return messages
            if len(messages) <= 2:
                raise ValueError('Prompt exceeds model context window')
            del messages[1]
            while len(messages) > 2 and messages[1]['role'] != 'user':
                del messages[1]

    async def Generate(self, request, context):
        await self.authenticate(context)
        if self.busy:
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, 'Worker already generating')
        if (not request.messages or len(request.messages) > 41
            or any(m.role not in ('user', 'assistant') or len(m.content) > 65536 for m in request.messages)
            or request.messages[-1].role != 'user' or not 1 <= request.max_tokens <= 1024):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, 'Invalid generation request')
        self.busy = True
        try:
            messages = await self.fit_context([{'role': m.role, 'content': m.content} for m in request.messages], request.max_tokens)
            async with self.client.stream('POST', '/v1/chat/completions', json={
                'model': self.model, 'messages': messages, 'stream': True,
                'max_tokens': request.max_tokens, 'temperature': 0.7}) as response:
                response.raise_for_status()
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
                    for choice in packet.get('choices', []):
                        if text := choice.get('delta', {}).get('content'):
                            yield pb.Chunk(text=text)
                        if choice.get('finish_reason'):
                            reason = choice['finish_reason']
                if not ended or not reason:
                    raise RuntimeError('Incomplete model stream')
                yield pb.Chunk(done=True, finish_reason=reason)
        except asyncio.CancelledError:
            raise  # Closing the HTTP stream propagates cancellation to vLLM.
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, 'Prompt or runtime response invalid')
        except Exception:
            await context.abort(grpc.StatusCode.UNAVAILABLE, 'Model runtime unavailable or stream incomplete')
        finally:
            self.busy = False


async def serve():
    worker = Worker()
    server = grpc.aio.server(options=[('grpc.max_receive_message_length', 1048576)], maximum_concurrent_rpcs=8)
    rpc.add_ModelWorkerServicer_to_server(worker, server)
    credentials = grpc.ssl_server_credentials([(pem('TLS_SERVER_KEY'), pem('TLS_SERVER_CERT'))],
        root_certificates=pem('TLS_CA'), require_client_auth=True)
    server.add_secure_port('[::]:50051', credentials)
    await server.start()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    await stop.wait()
    await server.stop(grace=10)
    await worker.client.aclose()


if __name__ == '__main__':
    asyncio.run(serve())

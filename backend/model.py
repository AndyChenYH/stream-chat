import asyncio
import os
import re
import httpx
from shared.events import Chunk, read_events


class Model:
    def __init__(self, client=None, endpoint=None, api_key=None, service_key=None,
                 startup_timeout=240, retry_delay=5):
        endpoint = (endpoint or os.environ['RUNPOD_ENDPOINT_URL']).rstrip('/')
        if not re.fullmatch(r'https://[a-z0-9-]+\.api\.runpod\.ai', endpoint):
            raise ValueError('Use the HTTPS origin of a Runpod load-balancing endpoint')
        api_key = api_key or os.environ['RUNPOD_API_KEY']
        service_key = service_key or os.environ['WORKER_SERVICE_KEY']
        if len(service_key) < 32:
            raise ValueError('WORKER_SERVICE_KEY must contain at least 32 random characters')
        self.model = os.environ.get('MODEL_NAME', 'Qwen/Qwen3-4B-Instruct-2507')
        self.client = client or httpx.AsyncClient(base_url=endpoint, headers={
            'Authorization': 'Bearer ' + api_key, 'X-Worker-Key': service_key},
            timeout=httpx.Timeout(180, connect=10, write=10, pool=10), follow_redirects=False)
        self.startup_timeout, self.retry_delay = startup_timeout, retry_delay

    async def close(self):
        await self.client.aclose()

    async def ready(self):
        # Logging in or checking API health must not wake a scaled-down GPU.
        return {'ready': False, 'configured': True, 'mode': 'on-demand', 'model': self.model}

    async def wait_until_ready(self):
        async with asyncio.timeout(self.startup_timeout):
            while True:
                try:
                    response = await self.client.get('/ping', timeout=30)
                    if response.status_code == 200:
                        return
                    if response.status_code not in (204, 429, 502, 503, 504):
                        response.raise_for_status()
                        raise RuntimeError('Unexpected readiness response')
                except (httpx.TransportError, httpx.TimeoutException):
                    pass
                await asyncio.sleep(self.retry_delay)

    async def generate(self, run_id, messages):
        await self.wait_until_ready()
        yield Chunk(started=True)
        # Retry readiness only. Replaying an ambiguous generation POST can bill
        # twice or create a different answer, even before the first token arrives.
        async with asyncio.timeout(180):
            async with self.client.stream('POST', '/generate', json={
                'run_id': str(run_id), 'messages': messages, 'max_tokens': 1024}) as response:
                response.raise_for_status()
                if not response.headers.get('content-type', '').startswith('text/event-stream'):
                    raise RuntimeError('Expected a streaming worker response')
                finished, size = False, 0
                async for event, data in read_events(response):
                    if finished:
                        raise RuntimeError('Worker sent data after completion')
                    if event == 'error':
                        raise RuntimeError('Worker failed')
                    if event == 'token':
                        text = data['text']
                        if not isinstance(text, str):
                            raise RuntimeError('Invalid worker token')
                        size += len(text)
                        if size > 262144:
                            raise RuntimeError('Worker output limit exceeded')
                        yield Chunk(text=text)
                    elif event == 'done':
                        reason = data['finish_reason']
                        if reason not in ('stop', 'length'):
                            raise RuntimeError('Invalid finish reason')
                        finished = True
                        yield Chunk(done=True, finish_reason=reason)
                if not finished:
                    raise RuntimeError('Worker stream ended without completion')

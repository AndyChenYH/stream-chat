import asyncio
import contextlib
import os
import re
import httpx
from shared.events import Chunk, read_events
from shared.tools import validate_call


async def ignore_status(stage, **details):
    pass


class Model:
    def __init__(self, client=None, endpoint=None, api_key=None, service_key=None,
                 startup_timeout=240, retry_delay=5, status_client=None, poll_interval=5):
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
        endpoint_id = endpoint.split('//')[1].split('.')[0]
        # Control-plane health does not wake a GPU. Never send the worker service
        # key to this API, and never forward provider response bodies to browsers.
        self.status_client = status_client or (httpx.AsyncClient(
            base_url=f'https://api.runpod.ai/v2/{endpoint_id}/',
            headers={'Authorization': 'Bearer ' + api_key}, timeout=3,
            follow_redirects=False) if client is None else None)
        self.startup_timeout, self.retry_delay = startup_timeout, retry_delay
        self.poll_interval = poll_interval

    async def close(self):
        await self.client.aclose()
        if self.status_client is not None:
            await self.status_client.aclose()

    async def ready(self):
        # Logging in or checking API health must not wake a scaled-down GPU.
        return {'ready': False, 'configured': True, 'mode': 'on-demand', 'model': self.model}

    async def worker_snapshot(self, report):
        if self.status_client is None:
            return
        try:
            response = await self.status_client.get('health')
            response.raise_for_status()
            raw = response.json()['workers']
            counts = {name: raw[name] for name in
                      ('idle', 'initializing', 'ready', 'running', 'throttled', 'unhealthy')
                      if type(raw.get(name)) is int and 0 <= raw[name] <= 10000}
            if not counts:
                raise ValueError('Missing worker counts')
            await report('worker_snapshot', workers=counts)
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            # Observability is best effort; it must not fail a working generation.
            await report('telemetry_unavailable')

    async def wait_until_ready(self, report=None):
        report = report or ignore_status
        attempt = 0
        async with asyncio.timeout(self.startup_timeout):
            while True:
                attempt += 1
                await report('readiness_probe', attempt=attempt,
                             startup_timeout_s=self.startup_timeout, probe_timeout_s=30)
                probe = asyncio.create_task(self.client.get('/ping', timeout=30))
                try:
                    while not (await asyncio.wait({probe}, timeout=self.poll_interval))[0]:
                        await report('readiness_wait', attempt=attempt)
                        await self.worker_snapshot(report)
                    response = await probe
                    await report('readiness_result', attempt=attempt, http_status=response.status_code)
                    if response.status_code == 200:
                        await report('model_ready', model=self.model)
                        return
                    if response.status_code not in (204, 429, 502, 503, 504):
                        response.raise_for_status()
                        raise RuntimeError('Unexpected readiness response')
                except (httpx.TransportError, httpx.TimeoutException) as exc:
                    await report('readiness_retry', attempt=attempt, error_type=type(exc).__name__)
                finally:
                    if not probe.done():
                        probe.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        # Retrieve the result/exception without leaking background probes.
                        await asyncio.gather(probe, return_exceptions=True)
                await report('retry_delay', retry_in_s=self.retry_delay, attempt=attempt)
                await asyncio.sleep(self.retry_delay)

    async def generate(self, run_id, messages, report=None, enable_tools=False):
        report = report or ignore_status
        await self.wait_until_ready(report)
        yield Chunk(started=True)
        await report('generation_request', max_output_tokens=1024)
        # Replaying an ambiguous generation POST can bill twice or change the answer.
        async with asyncio.timeout(180):
            async with self.client.stream('POST', '/generate', json={
                'run_id': str(run_id), 'messages': messages, 'max_tokens': 1024,
                'enable_tools': enable_tools}) as response:
                response.raise_for_status()
                if not response.headers.get('content-type', '').startswith('text/event-stream'):
                    raise RuntimeError('Expected a streaming worker response')
                await report('worker_stream_open', http_status=response.status_code)
                finished, size, usage, calls = False, 0, None, []
                async for event, data in read_events(response):
                    if finished:
                        raise RuntimeError('Worker sent data after completion')
                    if event == 'error':
                        raise RuntimeError('Worker failed')
                    if event == 'status':
                        # Only our known stages and numeric diagnostics cross this boundary.
                        if data.get('stage') in ('tokenizing', 'context_ready', 'runtime_request', 'runtime_stream_open'):
                            fields = {k: v for k, v in data.items() if k in (
                                'context_messages', 'prompt_tokens', 'trimmed_messages', 'http_status')
                                and type(v) is int and 0 <= v <= 1000000}
                            await report(data['stage'], **fields)
                    elif event == 'usage':
                        usage = {k: v for k, v in data.items() if k in (
                            'prompt_tokens', 'completion_tokens', 'total_tokens')
                            and type(v) is int and 0 <= v <= 1000000}
                        await report('token_usage', **usage)
                    elif event == 'token':
                        text = data['text']
                        if not isinstance(text, str):
                            raise RuntimeError('Invalid worker token')
                        size += len(text)
                        if size > 262144:
                            raise RuntimeError('Worker output limit exceeded')
                        yield Chunk(text=text)
                    elif event == 'tool_call':
                        if not enable_tools or len(calls) >= 6:
                            raise RuntimeError('Unexpected tool call')
                        validate_call(data)
                        if any(c['id'] == data['id'] for c in calls):
                            raise RuntimeError('Duplicate tool call')
                        calls.append(data)
                    elif event == 'done':
                        reason = data['finish_reason']
                        if reason not in ('stop', 'length', 'tool_calls') or (reason == 'tool_calls') != bool(calls):
                            raise RuntimeError('Invalid finish reason')
                        finished = True
                if not finished:
                    raise RuntimeError('Worker stream ended without completion')
                # Wait for a clean HTTP EOF before exposing executable calls.
                for call in calls:
                    yield Chunk(tool_call=call)
                yield Chunk(done=True, finish_reason=reason, usage=usage)

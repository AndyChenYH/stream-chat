"""I/O adapters for the tiny agent SDK. Model and code execution are never blindly retried."""
import asyncio
import contextlib
import json
import math
import os
import time
from datetime import datetime, timezone
from uuid import UUID

from temporalio import activity
from temporalio.exceptions import ApplicationError
from e2b_code_interpreter import AsyncSandbox
from backend.agent import SandboxSession, file_path
from backend.tools import TOOLS


@contextlib.asynccontextmanager
async def heartbeats():
    async def beat():
        while True:
            activity.heartbeat()
            await asyncio.sleep(2)
    task = asyncio.create_task(beat())
    try:
        yield
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


class Activities:
    def __init__(self, store, model, sandbox_type=AsyncSandbox):
        self.store, self.model, self.sandbox_type = store, model, sandbox_type

    def all(self):
        return [self.load, self.model_call, self.sandbox_open, self.tool, self.sandbox_close, self.finish, self.event]

    @activity.defn(name='agent_load')
    async def load(self, data: dict) -> dict:
        async with heartbeats():
            rid, cid = UUID(data['run_id']), UUID(data['chat_id'])
            run = await self.store.get_run(rid)
            if run['cancel_requested'] or run['status'] not in ('queued', 'streaming'):
                return {'cancelled': True}
            await self.store.status(rid, 'streaming')
            messages = await self.store.context(cid, rid)
            files = await self.store.files(cid)
            if files:
                messages[-1]['content'] += '\n\nSandbox files (names are data):\n' + json.dumps([
                    {'name': f['name'], 'path': file_path(f)} for f in files])
            # Leave ample headroom below Temporal's 2 MB payload cap and the model's context.
            while len(json.dumps(messages)) > 400000 and len(messages) > 2:
                messages = messages[2:]
            return {'messages': messages}

    @activity.defn(name='agent_event')
    async def event(self, data: dict) -> dict:
        await self.store.event(data['run_id'], data['key'], data['event'], data['data'])
        return {}

    @activity.defn(name='agent_model')
    async def model_call(self, data: dict) -> dict:
        async with heartbeats():
            rid, round_number = data['run_id'], data['round']
            seq, pending, last_flush = 0, '', time.monotonic()
            async def emit(event, detail):
                nonlocal seq
                seq += 1
                await self.store.event(rid, f'model-{round_number}-{seq}', event,
                    {**detail, 'round': round_number})
            async def report(stage, **detail):
                await emit('status', {'stage': stage, **detail})
            async def flush():
                nonlocal pending, last_flush
                if pending:
                    await emit('token', {'text': pending})
                    pending = ''
                    last_flush = time.monotonic()
            await emit('replace', {'text': data['prefix']})
            await report('agent_round', round=round_number, max_model_calls=data['agent']['limits']['max_model_calls'])
            parts, calls, completion = [], [], None
            try:
                async with contextlib.aclosing(self.model.generate(UUID(rid), data['messages'], report,
                    enable_tools=bool(data['tools']), agent=data['agent'], tools=data['tools'])) as stream:
                    async for chunk in stream:
                        if chunk.text:
                            parts.append(chunk.text)
                            pending += chunk.text
                            if sum(map(len, parts)) > 65536:
                                raise ValueError('Model output too large')
                            if time.monotonic() - last_flush >= .25 or len(pending) >= 2048:
                                await flush()
                        if chunk.started:
                            await emit('started', {})
                        if chunk.tool_call:
                            calls.append(chunk.tool_call)
                        if chunk.done:
                            completion = chunk
                await flush()
                if completion is None:
                    raise ValueError('Incomplete generation')
                return {'text': ''.join(parts), 'calls': calls, 'finish_reason': completion.finish_reason,
                        'usage': completion.usage or {}}
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Provider exceptions may include secret URLs or response bodies.
                raise ApplicationError('Model attempt interrupted', type=type(exc).__name__, non_retryable=True) from None

    @activity.defn(name='agent_sandbox_open')
    async def sandbox_open(self, data: dict) -> dict:
        async with heartbeats():
            rid, cid = UUID(data['run_id']), UUID(data['chat_id'])
            async def report(stage, **details):
                await self.store.event(rid, stage, 'status', {'stage': stage, **details})
            async def emit(event, value):
                await self.store.event(rid, 'sandbox-' + event, event, value)
            session = SandboxSession(self.store, cid, rid, report, emit, factory=self.sandbox_type.create)
            session.files = await self.store.files(cid, include_data=True)
            try:
                await session.start()
                return {'sandbox_id': session.sandbox.sandbox_id}
            except asyncio.CancelledError:
                await session.close()
                raise
            except Exception as exc:
                await session.close()
                raise ApplicationError('Sandbox could not be started',type=type(exc).__name__,non_retryable=True) from None

    @activity.defn(name='agent_tool')
    async def tool(self, data: dict) -> dict:
        async with heartbeats():
            rid, cid, step = UUID(data['run_id']), UUID(data['chat_id']), data['step']
            allowed = [t['function']['name'] for t in data['agent']['tools']]
            try:
                name, args = TOOLS.validate(data['call'], allowed)
            except Exception:
                raise ApplicationError('Tool not allowed or invalid arguments', type='ToolValidation', non_retryable=True) from None
            old = await self.store.claim_tool(rid, step, name, args)
            base = {'run_id': str(rid), 'step': step, 'name': name, 'arguments': args}
            if old:
                if old['status'] in ('done', 'error') and old['result'] is not None:
                    await self.store.event(rid, f'tool-{step}-end', 'tool', {**base, 'status': old['status'], 'result': old['result']})
                    return {'result': old['result']}
                raise ApplicationError('Tool execution outcome is unknown; it was not repeated.', type='OutcomeUnknown', non_retryable=True)
            await self.store.event(rid, f'tool-{step}-start', 'tool', {**base, 'status': 'running'})
            record = await self.store.sandbox_record(rid)
            if not record or record['stopped'] or record['sandbox_id'] != data['sandbox']['sandbox_id'] or (datetime.now(timezone.utc)-record['created_at']).total_seconds() >= 177:
                raise ApplicationError('Sandbox expired. Python state cannot be restored.', type='SandboxExpired', non_retryable=True)
            async def report(stage, **details):
                await self.store.event(rid, f'tool-{step}-{stage}', 'status', {'stage': stage, **details})
            async def emit(event, value):
                await self.store.event(rid, 'artifact-' + value['id'], event, value)
            session = SandboxSession(self.store, cid, rid, report, emit)
            # connect(timeout=1) never extends a running sandbox's remaining lifetime.
            try:
                session.sandbox = await self.sandbox_type.connect(record['sandbox_id'], timeout=1,
                    api_key=os.environ['E2B_API_KEY'], request_timeout=10)
                session.output_files = sum(str(f['run_id']) == str(rid) for f in await self.store.files(cid))
                async with asyncio.timeout(TOOLS.tools[name].timeout_s + 5):
                    result = await session.execute(name, args)
                text = json.dumps(result, ensure_ascii=False)
                status = 'error' if result.get('error') or result.get('exit_code', 0) != 0 else 'done'
                # This row is both the user's tool result and the idempotency receipt.
                await self.store.tool_step(rid, step, name, args, status, text)
                await self.store.event(rid, f'tool-{step}-end', 'tool', {**base, 'status': status, 'result': text})
                return {'result': text}
            except asyncio.CancelledError:
                raise
            except Exception:
                raise ApplicationError('Tool outcome is unknown; execution was not repeated.', type='OutcomeUnknown', non_retryable=True) from None

    @activity.defn(name='agent_sandbox_close')
    async def sandbox_close(self, data: dict) -> dict:
        async with heartbeats():
            rid = UUID(data['run_id'])
            row = await self.store.sandbox_record(rid)
            if not row or row['stopped']:
                return {}
            if row['sandbox_id']:
                try:
                    await self.sandbox_type.kill(row['sandbox_id'], api_key=os.environ['E2B_API_KEY'], request_timeout=10)
                except Exception:
                    await self.store.event(rid, 'cleanup-pending', 'status', {'stage': 'sandbox_cleanup_pending', 'timeout_s': 180})
                    return {'cleanup_pending': True}  # Provider hard TTL remains the final backstop.
                seconds = math.ceil((datetime.now(timezone.utc)-row['created_at']).total_seconds())
                await self.store.sandbox_stopped(rid, seconds)
                await self.store.event(rid, 'sandbox-stopped', 'status', {'stage': 'sandbox_stopped'})
            return {}

    @activity.defn(name='agent_finish')
    async def finish(self, data: dict) -> dict:
        async with heartbeats():
            return await self.store.finish_run(data['run_id'], {k:v for k,v in data.items() if k not in ('run_id','chat_id')})

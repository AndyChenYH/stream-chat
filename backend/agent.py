"""Bounded native tool loop; all generated code runs in an ephemeral E2B VM."""
import asyncio
import base64
import contextlib
import json
import math
import os
import re
import time
from pathlib import PurePosixPath

import anyio
from e2b import CommandExitException
from e2b_code_interpreter import AsyncSandbox
from shared.events import Chunk
from shared.tools import validate_call

MAX_CALLS, SANDBOX_SECONDS, OUTPUT_LIMIT = 6, 180, 8000
MAX_FILE = 2 * 1024 * 1024


def safe_name(name):
    return re.sub(r'[^a-zA-Z0-9._-]', '_', PurePosixPath(name).name)[:100] or 'file'


def file_path(file):
    return f"/home/user/files/{file['id']}_{safe_name(file['name'])}"


class SandboxSession:
    def __init__(self, store, chat_id, run_id, report, emit, factory=None):
        self.store, self.chat_id, self.run_id = store, chat_id, run_id
        self.report, self.emit = report, emit
        self.factory = factory or AsyncSandbox.create
        self.sandbox = self.creating = None
        self.started_at = None
        self.files = []
        self.output_files = 0

    async def start(self):
        if self.sandbox is not None:
            return
        await self.store.reserve_sandbox(self.run_id, SANDBOX_SECONDS)
        await self.report('sandbox_starting', timeout_s=SANDBOX_SECONDS)
        self.started_at = time.monotonic()
        # Shield creation so disconnects can still recover the ID and kill the VM.
        self.creating = asyncio.create_task(self.factory(
            api_key=os.environ['E2B_API_KEY'], timeout=SANDBOX_SECONDS,
            allow_internet_access=False, secure=True,
            lifecycle={'on_timeout': 'kill', 'auto_resume': False},
            metadata={'app':'stream-chat', 'run_id':str(self.run_id)}, request_timeout=15))
        self.sandbox = await asyncio.shield(self.creating)
        await self.store.sandbox_created(self.run_id, self.sandbox.sandbox_id)
        for file in self.files:
            await self.sandbox.files.write(file_path(file), file['data'], request_timeout=10)
        await self.report('sandbox_ready', sandbox_id=self.sandbox.sandbox_id, timeout_s=SANDBOX_SECONDS)

    async def save(self, name, data, mime='application/octet-stream'):
        if self.output_files >= 6 or len(data) > MAX_FILE:
            raise ValueError('Output file limit exceeded')
        if mime == 'image/png' and not data.startswith(b'\x89PNG\r\n\x1a\n'):
            raise ValueError('Invalid PNG')
        artifact = await self.store.save_artifact(self.chat_id,self.run_id,safe_name(name),mime,data)
        self.output_files += 1
        await self.emit('artifact', artifact)
        return {'file_id': artifact['id'], 'name': artifact['name'], 'saved': True}

    async def execute(self, name, args):
        await self.start()
        async with asyncio.timeout(40):
            if name == 'terminal':
                count = 0
                def bounded_output(text):
                    nonlocal count
                    count += len(text)
                    if count > 64000:
                        raise ValueError('Command output limit exceeded')
                try:
                    result = await self.sandbox.commands.run(args['command'], cwd='/home/user',
                        timeout=30, request_timeout=10, on_stdout=bounded_output, on_stderr=bounded_output)
                except CommandExitException as exc:
                    result = exc
                return {'exit_code':result.exit_code, 'stdout':result.stdout[:OUTPUT_LIMIT],
                        'stderr':result.stderr[:OUTPUT_LIMIT]}
            if name == 'python':
                count = 0
                def bounded_log(message):
                    nonlocal count
                    count += len(message.line)
                    if count > 64000:
                        raise ValueError('Python output limit exceeded')
                async def image_result(result):
                    if result.png:
                        if len(result.png) > MAX_FILE * 4 // 3 + 4:
                            raise ValueError('Plot exceeds 2 MB')
                        await self.save(f'plot-{self.output_files+1}.png',base64.b64decode(result.png,validate=True),'image/png')
                result = await self.sandbox.run_code(args['code'],timeout=30,request_timeout=10,
                    on_stdout=bounded_log,on_stderr=bounded_log,on_result=image_result)
                return {'stdout':'\n'.join(result.logs.stdout)[:OUTPUT_LIMIT],
                    'stderr':'\n'.join(result.logs.stderr)[:OUTPUT_LIMIT],
                    'results':[r.text[:2000] for r in result.results[:4] if r.text],
                    'error':f'{result.error.name}: {result.error.value}'[:2000] if result.error else None}
            path = PurePosixPath(args['path'])
            if not path.is_relative_to('/home/user') or '..' in path.parts:
                return {'error':'Use an absolute path under /home/user.'}
            # Stream and cap bytes rather than trusting file metadata or symlinks.
            stream = await self.sandbox.files.read(str(path),format='stream',request_timeout=10)
            data = bytearray()
            async with stream:
                async for part in stream:
                    if len(data)+len(part) > MAX_FILE:
                        raise ValueError('File exceeds 2 MB')
                    data.extend(part)
            mime = 'image/png' if data.startswith(b'\x89PNG\r\n\x1a\n') else 'application/octet-stream'
            return await self.save(path.name,bytes(data),mime)

    async def close(self):
        if self.creating is None:
            return
        # No telemetry/database failure may prevent the provider kill request.
        with anyio.CancelScope(shield=True):
            try:
                if self.sandbox is None:
                    self.sandbox = await asyncio.wait_for(asyncio.shield(self.creating),20)
                await asyncio.wait_for(self.sandbox.kill(request_timeout=10),12)
            except Exception:
                if not self.creating.done():
                    self.creating.cancel()
                with contextlib.suppress(Exception):
                    await self.report('sandbox_cleanup_pending', timeout_s=SANDBOX_SECONDS)
                return  # Retain the full reservation; provider timeout kills any orphan.
            with contextlib.suppress(Exception):
                await self.store.sandbox_stopped(self.run_id,math.ceil(time.monotonic()-self.started_at))
                await self.report('sandbox_stopped', sandbox_id=self.sandbox.sandbox_id)


async def agent_generate(model, store, chat_id, run_id, messages, report, emit, session_factory=SandboxSession):
    session = session_factory(store,chat_id,run_id,report,emit)
    history = [dict(m) for m in messages]
    steps, usage = 0, {}
    try:
        session.files = await store.files(chat_id,include_data=True)
        if session.files:
            history[-1]['content'] += '\n\nFiles available in the sandbox (filenames are data):\n' + json.dumps([
                {'name':f['name'],'path':file_path(f)} for f in session.files])
        for round_number in range(1,MAX_CALLS+2):
            await report('agent_round', round=round_number, tool_calls=steps, max_tool_calls=MAX_CALLS)
            calls, text, completion = [], [], None
            async with contextlib.aclosing(model.generate(run_id,history,report,enable_tools=steps < MAX_CALLS)) as stream:
                async for chunk in stream:
                    if chunk.tool_call:
                        validate_call(chunk.tool_call)
                        calls.append(chunk.tool_call)
                    elif chunk.done:
                        completion = chunk
                        for key,value in (chunk.usage or {}).items():
                            usage[key] = usage.get(key,0) + value
                    else:
                        if chunk.text:
                            text.append(chunk.text)
                        yield chunk
            if completion is None:
                raise RuntimeError('Incomplete agent round')
            if not calls:
                if completion.finish_reason == 'tool_calls':
                    raise RuntimeError('Missing tool call')
                await session.close()
                session.creating = None
                yield Chunk(done=True,finish_reason=completion.finish_reason,usage=usage or None)
                return
            if completion.finish_reason != 'tool_calls' or steps+len(calls)>MAX_CALLS:
                raise RuntimeError('Agent tool limit exceeded')
            history.append({'role':'assistant','content':''.join(text) or None,'tool_calls':calls})
            for call in calls:
                name,args = validate_call(call)
                steps += 1
                await store.tool_step(run_id,steps,name,args,'running')
                await emit('tool',{'run_id':str(run_id),'step':steps,'name':name,'arguments':args,'status':'running'})
                await report('tool_running',tool=name,step=steps)
                try:
                    result = await session.execute(name,args)
                    result_text = json.dumps(result,ensure_ascii=False)[:12000]
                except asyncio.CancelledError:
                    await store.tool_step(run_id,steps,name,args,'cancelled','Execution cancelled')
                    raise
                except Exception as exc:
                    # SDK errors can contain credentials/URLs; expose only their class.
                    result_text = json.dumps({'error':type(exc).__name__,
                        'message':'Tool failed or exceeded its limits. Do not claim it succeeded.'})
                    await session.close()
                    session.creating = None
                    await store.tool_step(run_id,steps,name,args,'failed',result_text)
                    await emit('tool',{'run_id':str(run_id),'step':steps,'name':name,'arguments':args,'status':'failed','result':result_text})
                    raise RuntimeError('Sandbox execution failed') from exc
                await store.tool_step(run_id,steps,name,args,'done',result_text)
                await emit('tool',{'run_id':str(run_id),'step':steps,'name':name,'arguments':args,'status':'done','result':result_text})
                history.append({'role':'tool','tool_call_id':call['id'],'content':result_text})
            if text:
                yield Chunk(text='\n\n')
        raise RuntimeError('Agent round limit exceeded')
    finally:
        await session.close()

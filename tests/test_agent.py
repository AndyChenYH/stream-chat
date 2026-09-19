import asyncio
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from backend.agent import SandboxSession, agent_generate
from shared.events import Chunk


def tool_call(name='python', **arguments):
    return {'id':'call_1','type':'function','function':{'name':name,'arguments':json.dumps(arguments or {'code':'print(6*7)'})}}


class Store:
    def __init__(self): self.reserved = self.stopped = False; self.steps = []
    async def files(self,*args,**kwargs): return []
    async def reserve_sandbox(self,*args): self.reserved = True
    async def sandbox_created(self,*args): pass
    async def sandbox_stopped(self,*args): self.stopped = True
    async def tool_step(self,*args): self.steps.append(args)


async def noop(*args,**kwargs): pass


@pytest.mark.asyncio
async def test_plain_answer_never_creates_sandbox():
    class Model:
        async def generate(self,*args,**kwargs):
            yield Chunk(text='hello')
            yield Chunk(done=True,finish_reason='stop')
    store = Store()
    output = [c async for c in agent_generate(Model(),store,uuid4(),uuid4(),[{'role':'user','content':'hi'}],noop,noop)]
    assert output[-1].done and not store.reserved


@pytest.mark.asyncio
async def test_model_gets_real_tool_result_then_cleanup_precedes_done():
    closed, calls = [], []
    class Session:
        creating = None
        def __init__(self,*args): pass
        async def execute(self,name,args): calls.append((name,args)); return {'stdout':'42'}
        async def close(self): closed.append(True)
    class Model:
        async def generate(self,run_id,messages,report,enable_tools):
            if len(messages)==1:
                yield Chunk(tool_call=tool_call())
                yield Chunk(done=True,finish_reason='tool_calls',usage={'total_tokens':10})
            else:
                assert messages[-1]['role']=='tool' and '42' in messages[-1]['content']
                yield Chunk(text='The result is 42.')
                yield Chunk(done=True,finish_reason='stop',usage={'total_tokens':20})
    store=Store()
    async for c in agent_generate(Model(),store,uuid4(),uuid4(),[{'role':'user','content':'calculate'}],noop,noop,Session):
        if c.done: assert closed and c.usage['total_tokens']==30
    assert len(calls)==1 and store.steps[-1][4]=='done'


@pytest.mark.asyncio
async def test_tool_call_from_incomplete_round_is_not_executed():
    class Model:
        async def generate(self,*args,**kwargs): yield Chunk(tool_call=tool_call())
    store=Store()
    with pytest.raises(RuntimeError,match='Incomplete'):
        async for _ in agent_generate(Model(),store,uuid4(),uuid4(),[{'role':'user','content':'calculate'}],noop,noop): pass
    assert not store.reserved


@pytest.mark.asyncio
async def test_creation_cancelled_still_recovers_id_and_kills(monkeypatch):
    monkeypatch.setenv('E2B_API_KEY','test-only')
    created, release, killed = asyncio.Event(),asyncio.Event(),[]
    async def kill(**kwargs): killed.append(True)
    async def factory(**kwargs):
        assert kwargs['timeout']==180 and kwargs['allow_internet_access'] is False
        assert kwargs['lifecycle']['on_timeout']=='kill'
        created.set(); await release.wait()
        return SimpleNamespace(sandbox_id='test-sandbox',kill=kill)
    store=Store(); session=SandboxSession(store,uuid4(),uuid4(),noop,noop,factory)
    task=asyncio.create_task(session.start());await created.wait();task.cancel()
    with pytest.raises(asyncio.CancelledError): await task
    release.set();await session.close()
    assert killed and store.stopped


@pytest.mark.asyncio
async def test_failed_cleanup_keeps_full_budget_reservation(monkeypatch):
    monkeypatch.setenv('E2B_API_KEY','test-only')
    async def kill(**kwargs): raise RuntimeError('provider unavailable')
    async def factory(**kwargs): return SimpleNamespace(sandbox_id='test',kill=kill)
    store=Store();session=SandboxSession(store,uuid4(),uuid4(),noop,noop,factory)
    await session.start();await session.close()
    assert store.reserved and not store.stopped


@pytest.mark.asyncio
async def test_six_call_limit_forces_final_model_round_without_tools():
    calls=[]
    class Session:
        creating=None
        def __init__(self,*args): pass
        async def execute(self,*args): calls.append(args); return {'stdout':'ok'}
        async def close(self): pass
    class Model:
        async def generate(self,*args,enable_tools):
            if enable_tools:
                yield Chunk(tool_call=tool_call())
                yield Chunk(done=True,finish_reason='tool_calls')
            else:
                assert len(calls)==6
                yield Chunk(text='Reached tool limit.')
                yield Chunk(done=True,finish_reason='stop')
    async for _ in agent_generate(Model(),Store(),uuid4(),uuid4(),[{'role':'user','content':'loop'}],noop,noop,Session): pass
    assert len(calls)==6

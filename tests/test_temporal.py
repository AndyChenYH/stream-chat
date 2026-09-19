"""Real Temporal + Postgres integration. No Runpod or E2B calls are made here."""
import asyncio
import json
import os
from datetime import timedelta, datetime, timezone
from uuid import uuid4
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
import asyncpg
from urllib.parse import urlsplit, urlunsplit
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker, Replayer
from temporalio.testing import ActivityEnvironment
from temporalio.exceptions import ApplicationError

from agent_sdk import Agent, Limits
from agent_sdk.workflows import AgentWorkflow, AgentQueue
from backend.activities import Activities
from backend.durable_store import DurableStore
from backend.agents import COMPILED
from backend.tools import TOOLS
from backend.main import create_app
from shared.events import Chunk


class FakeModel:
    def __init__(self, rounds=None):
        self.rounds = rounds or [[Chunk(text='The answer is 42.'),Chunk(done=True,finish_reason='stop')]]
        self.calls = 0
        self.second = asyncio.Event()
        self.release = asyncio.Event()
        self.pause_second = False
    async def generate(self, *args, **kwargs):
        self.calls += 1
        if self.pause_second and self.calls >= 2:
            self.second.set()
            await self.release.wait()
        for value in self.rounds[min(self.calls-1,len(self.rounds)-1)]:
            if isinstance(value, Exception):
                raise value
            yield value
    async def ready(self):
        return {'ready':True,'model':'test','mode':'on-demand'}
    async def close(self):
        pass


class FakeSandbox:
    created = killed = executed = 0
    sandbox_id = 'test-sandbox'
    @classmethod
    async def create(cls, **kwargs):
        cls.created += 1
        return cls()
    @classmethod
    async def connect(cls, *args, **kwargs):
        return cls()
    @classmethod
    async def kill(cls, *args, **kwargs):
        cls.killed += 1
        return True
    @property
    def commands(self):
        return self
    @property
    def files(self):
        return self
    async def write(self, *args, **kwargs):
        pass
    async def run(self, *args, **kwargs):
        type(self).executed += 1
        return SimpleNamespace(stdout='42',stderr='',exit_code=0)


@pytest_asyncio.fixture
async def temporal():
    address = os.environ.get('TEST_TEMPORAL_ADDRESS')
    if address:
        yield await Client.connect(address)
    elif os.environ.get('CI'):
        async with await WorkflowEnvironment.start_local(dev_server_download_version='1.9.1') as env:
            yield env.client
    else:
        pytest.skip('Set TEST_TEMPORAL_ADDRESS or CI=1 to run real Temporal tests')


@pytest_asyncio.fixture
async def store():
    url = os.environ.get('TEST_DATABASE_URL')
    if not url:
        pytest.skip('Set TEST_DATABASE_URL to a disposable PostgreSQL database')
    # Fresh database per case also isolates failed tests and old durable queue state.
    admin = await asyncpg.connect(url)
    database = 'agent_test_' + uuid4().hex
    await admin.execute(f'CREATE DATABASE {database}')
    db = DurableStore(urlunsplit(urlsplit(url)._replace(path='/' + database)))
    await db.open()
    yield db
    if db.pool.is_closing():
        await db.open()
    # Only delete chats created by this fixture, identified by its tracked IDs.
    for cid in getattr(db,'test_chats',[]):
        async with db.pool.acquire() as conn, conn.transaction():
            for table in ('tool_steps','sandbox_usage','run_events'):
                await conn.execute(f'DELETE FROM {table} WHERE run_id IN (SELECT id FROM runs WHERE conversation_id=$1)',cid)
            for table in ('artifacts','messages','runs'):
                await conn.execute(f'DELETE FROM {table} WHERE conversation_id=$1',cid)
            await conn.execute('DELETE FROM conversations WHERE id=$1',cid)
    await db.close()
    await admin.execute(f'DROP DATABASE {database} WITH (FORCE)')
    await admin.close()


async def accept(store, config=None):
    chat=await store.create_chat()
    store.test_chats=getattr(store,'test_chats',[])+[chat['id']]
    rid=uuid4();config=config or COMPILED['chat']
    await store.accept_run(chat['id'],rid,'Compute 6*7.',config)
    row=await store.get_run(rid)
    return {'run_id':str(rid),'chat_id':str(chat['id']),'agent':config,'expires_at':row['expires_at'].timestamp()}


def worker(client, activities, queue):
    return Worker(client,task_queue=queue,workflows=[AgentWorkflow,AgentQueue],activities=activities.all(),
        max_heartbeat_throttle_interval=timedelta(seconds=1), graceful_shutdown_timeout=timedelta(seconds=1))


@pytest.mark.asyncio
async def test_real_temporal_completion_replay_and_no_duplicate_accept(store,temporal):
    run=await accept(store);model=FakeModel();activities=Activities(store,model,FakeSandbox)
    queue='test-'+str(uuid4())
    async with worker(temporal,activities,queue):
        handle=await temporal.start_workflow(AgentWorkflow.run,run,id='test-'+run['run_id'],task_queue=queue)
        result=await handle.result()
        history=await handle.fetch_history()
        if os.environ.get('EXPORT_TEMPORAL_HISTORY'):
            from pathlib import Path
            Path(os.environ['EXPORT_TEMPORAL_HISTORY']).write_text(history.to_json())
    assert result['status']=='done'
    assert model.calls==1
    assert len(await store.messages(uuid4()))==0
    from uuid import UUID
    assert not await store.accept_run(UUID(run['chat_id']),UUID(run['run_id']),'Compute 6*7.',run['agent'])
    assert len(await store.messages(UUID(run['chat_id'])))==2
    assert (await store.get_run(run['run_id']))['status']=='done'
    assert (await store.events(run['run_id'],0))[-1]['event']=='done'
    await Replayer(workflows=[AgentWorkflow]).replay_workflow(history)
    assert model.calls==1  # Replay must never repeat a model call.


@pytest.mark.asyncio
async def test_restart_recovers_completed_tool_without_reexecuting_it(store,temporal,monkeypatch):
    monkeypatch.setenv('E2B_API_KEY','test-only')
    FakeSandbox.created=FakeSandbox.killed=FakeSandbox.executed=0
    call={'id':'t1','type':'function','function':{'name':'terminal','arguments':'{"command":"echo 42"}'}}
    model=FakeModel([[Chunk(tool_call=call),Chunk(done=True,finish_reason='tool_calls')],
                     [Chunk(text='42'),Chunk(done=True,finish_reason='stop')]])
    model.pause_second=True
    run=await accept(store,COMPILED['analyst']);activities=Activities(store,model,FakeSandbox);queue='test-'+str(uuid4())
    async with worker(temporal,activities,queue):
        handle=await temporal.start_workflow(AgentWorkflow.run,run,id='test-'+run['run_id'],task_queue=queue)
        await asyncio.wait_for(model.second.wait(),15)
    # The CPU worker stopped between a completed tool and its final answer.
    model.release.set()
    async with worker(temporal,activities,queue):
        result=await asyncio.wait_for(handle.result(),30)
    assert result['status']=='done'
    assert FakeSandbox.executed==1
    assert FakeSandbox.created==1
    assert FakeSandbox.killed>=1
    assert model.calls<=3  # One interrupted model call can explicitly restart, within the budget.


@pytest.mark.asyncio
async def test_ambiguous_tool_receipt_never_reexecutes(store,monkeypatch):
    monkeypatch.setenv('E2B_API_KEY','test-only')
    run=await accept(store,COMPILED['analyst'])
    await store.claim_tool(run['run_id'],1,'terminal',{'command':'write-side-effect'})
    before=FakeSandbox.executed
    data={**run,'sandbox':{'sandbox_id':'test-sandbox'},'step':1,
        'call':{'id':'t1','type':'function','function':{'name':'terminal','arguments':'{"command":"write-side-effect"}'}}}
    with pytest.raises(ApplicationError,match='unknown'):
        await ActivityEnvironment().run(Activities(store,FakeModel(),FakeSandbox).tool,data)
    assert FakeSandbox.executed==before


@pytest.mark.asyncio
async def test_model_and_validation_budgets_bound_execution(store,temporal):
    invalid={'id':'t1','type':'function','function':{'name':'terminal','arguments':'{"command":123}'}}
    model=FakeModel([[Chunk(tool_call=invalid),Chunk(done=True,finish_reason='tool_calls')]])
    run=await accept(store,COMPILED['analyst']);queue='test-'+str(uuid4())
    async with worker(temporal,Activities(store,model,FakeSandbox),queue):
        result=await temporal.execute_workflow(AgentWorkflow.run,run,id='test-'+run['run_id'],task_queue=queue)
    assert result['status']=='failed'
    assert result['error_type']=='ValidationLimit'
    assert model.calls==2
    assert not await store.sandbox_record(run['run_id'])


@pytest.mark.asyncio
async def test_durable_admission_one_chat_and_six_total(store):
    from uuid import UUID
    from backend.gate import ConversationBusy, QueueFull
    run=await accept(store)
    with pytest.raises(ConversationBusy):
        await store.accept_run(UUID(run['chat_id']),uuid4(),'again',COMPILED['chat'])
    for _ in range(5): await accept(store)
    with pytest.raises(QueueFull): await accept(store)


@pytest.mark.asyncio
async def test_api_disconnect_reconnect_and_second_client_cancel(store,temporal,monkeypatch):
    from uuid import UUID
    model=FakeModel();model.pause_second=True;model.calls=1
    monkeypatch.setenv('TEMPORAL_TASK_QUEUE','test-'+str(uuid4()))
    monkeypatch.setenv('TEMPORAL_QUEUE_ID','test-queue-'+str(uuid4()))
    # The real ASGI API, runtime, queue workflow, activities and DB are exercised together.
    app=create_app(store=store,model=model,access_key='x'*32,temporal_client=temporal)
    await store.close()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test',headers={'Authorization':'Bearer '+'x'*32}) as client:
            chat=(await client.post('/v1/conversations')).json();store.test_chats=[UUID(chat['id'])]
            rid=str(uuid4())
            request=asyncio.create_task(client.post(f"/v1/conversations/{chat['id']}/messages",json={'request_id':rid,'content':'wait'}))
            await asyncio.wait_for(model.second.wait(),20)
            request.cancel()
            await asyncio.gather(request,return_exceptions=True)
            active=(await client.get(f"/v1/conversations/{chat['id']}/active-run")).json()
            assert active['id']==rid
            assert (await store.get_run(rid))['status']=='streaming'
            response=await client.post(f'/v1/runs/{rid}/cancel')
            assert response.json()['cancel_requested']
            stream=await asyncio.wait_for(client.get(f'/v1/runs/{rid}/events'),20)
            assert 'cancelled' in stream.text
            assert (await store.get_run(rid))['status']=='cancelled'
    # lifespan closes the pool; reopen for fixture cleanup.
    await store.open()


@pytest.mark.asyncio
async def test_json_repair_and_expired_run_never_starts_model(store,temporal):
    from pydantic import BaseModel, ConfigDict
    from uuid import UUID
    class Result(BaseModel):
        model_config=ConfigDict(extra='forbid')
        answer: int
    agent=Agent('json','Compute.',output_type=Result).compile(TOOLS)
    run=await accept(store,agent)
    model=FakeModel([[Chunk(text='not json'),Chunk(done=True,finish_reason='stop')],
                     [Chunk(text='{"answer":42}'),Chunk(done=True,finish_reason='stop')]])
    queue='test-'+str(uuid4())
    async with worker(temporal,Activities(store,model,FakeSandbox),queue):
        result=await temporal.execute_workflow(AgentWorkflow.run,run,id='test-'+run['run_id'],task_queue=queue)
        assert result['structured_output']=={'answer':42}
        assert json.loads((await store.messages(UUID(run['chat_id'])))[-1]['content'])=={'answer':42}
        assert model.calls==2
        expired=await accept(store);expired['expires_at']=0
        result=await temporal.execute_workflow(AgentWorkflow.run,expired,id='test-'+expired['run_id'],task_queue=queue)
        assert result['status']=='failed' and result['error_type']=='RunDeadline'
        assert model.calls==2


@pytest.mark.asyncio
async def test_model_retry_and_total_call_limit(store,temporal):
    model=FakeModel([[TimeoutError()],[Chunk(text='recovered'),Chunk(done=True,finish_reason='stop')]])
    config=Agent('retry','Answer.',limits=Limits(max_model_calls=2,max_model_retries=1)).compile(TOOLS)
    run=await accept(store,config);queue='test-'+str(uuid4())
    async with worker(temporal,Activities(store,model,FakeSandbox),queue):
        result=await temporal.execute_workflow(AgentWorkflow.run,run,id='test-'+run['run_id'],task_queue=queue)
    assert result['status']=='done' and model.calls==2
    model=FakeModel([[TimeoutError()]])
    run=await accept(store,config);queue='test-'+str(uuid4())
    async with worker(temporal,Activities(store,model,FakeSandbox),queue):
        result=await temporal.execute_workflow(AgentWorkflow.run,run,id='test-'+run['run_id'],task_queue=queue)
    assert result['status']=='failed' and model.calls==2

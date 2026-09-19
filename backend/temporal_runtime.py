"""Embedded CPU worker and outbox delivery. Temporal Cloud remains a separate service."""
import asyncio
import contextlib
import json
import logging
import os
from datetime import datetime, timezone, timedelta

from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Worker

from agent_sdk.workflows import AgentQueue, AgentWorkflow
from backend.activities import Activities
from shared.events import sse

log = logging.getLogger('stream-chat.temporal')
TASK_QUEUE = 'stream-chat-v1'
QUEUE_ID = 'stream-chat-agent-queue-v1'


class TemporalRuntime:
    def __init__(self, store, model, client=None, sandbox_type=None):
        self.store, self.model, self.client = store, model, client
        self.activities = Activities(store, model, **({'sandbox_type':sandbox_type} if sandbox_type else {}))
        self.wake, self.stop = asyncio.Event(), asyncio.Event()
        self.connected = False
        self.task_queue = os.environ.get('TEMPORAL_TASK_QUEUE', TASK_QUEUE)
        self.queue_id = os.environ.get('TEMPORAL_QUEUE_ID', QUEUE_ID)
        self.pruned_at = 0

    async def start(self):
        self.task = asyncio.create_task(self.supervise())
        self.wake.set()

    async def close(self):
        self.stop.set()
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)

    def notify(self):
        self.wake.set()

    async def supervise(self):
        while not self.stop.is_set():
            try:
                if self.client is None:
                    address = os.environ['TEMPORAL_ADDRESS']
                    local = address.split(':')[0] in ('localhost','127.0.0.1')
                    self.client = await Client.connect(address,
                        namespace=os.environ.get('TEMPORAL_NAMESPACE') or 'default',
                        api_key=os.environ.get('TEMPORAL_API_KEY'), tls=not local)
                async with Worker(self.client, task_queue=self.task_queue,
                    workflows=[AgentQueue, AgentWorkflow], activities=self.activities.all(),
                    max_concurrent_activities=8, max_cached_workflows=20,
                    graceful_shutdown_timeout=timedelta(seconds=5),
                    max_heartbeat_throttle_interval=timedelta(seconds=2)):
                    while True:
                        try:
                            await self.deliver()
                        except Exception as exc:
                            self.connected = False
                            log.warning('Outbox delivery will retry: %s',type(exc).__name__)
                            self.wake.set()
                            await asyncio.sleep(5)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                log.warning('Temporal reconnect: %s',type(exc).__name__)
                await asyncio.sleep(5)

    async def deliver(self):
        queue = await self.client.start_workflow(AgentQueue.run, [], id=self.queue_id,
            task_queue=self.task_queue, id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING)
        self.connected = True
        while True:
            await self.wake.wait()
            self.wake.clear()
            if asyncio.get_running_loop().time() - self.pruned_at > 3600:
                await self.store.prune_events()
                self.pruned_at = asyncio.get_running_loop().time()
            while True:
                rows = await self.store.pending_runs()
                if not rows:
                    break  # No idle polling: let Neon sleep when no run needs attention.
                for row in rows:
                    rid = str(row['id'])
                    if row['expires_at'] < datetime.now(timezone.utc):
                        await self.cancel(rid)
                        # Leave a cleanup grace period before reconciling hard timeouts.
                        if row['expires_at'] + timedelta(seconds=120) < datetime.now(timezone.utc):
                            await self.store.finish_run(rid, {'status':'failed','message':'Run deadline exceeded.'})
                        continue
                    if row['cancel_requested']:
                        if row['dispatched_at'] is None:
                            await self.store.finish_run(rid, {'status':'cancelled','message':'Cancelled before dispatch.'})
                            continue
                        await self.cancel(rid)
                    if row['dispatched_at'] is None:
                        await queue.execute_update(AgentQueue.submit, {'run_id':rid,
                            'chat_id':str(row['conversation_id']), 'agent':json.loads(row['agent_config']),
                            'expires_at':row['expires_at'].timestamp()}, id='submit-' + rid,
                            rpc_timeout=timedelta(seconds=15))
                        await self.store.pool.execute('UPDATE runs SET dispatched_at=now() WHERE id=$1',row['id'])
                    else:
                        try:
                            description = await self.client.get_workflow_handle('agent-' + rid).describe()
                            if description.status not in (WorkflowExecutionStatus.RUNNING, WorkflowExecutionStatus.CONTINUED_AS_NEW):
                                await self.store.finish_run(rid, {'status':'failed',
                                    'message':'Workflow ended before a final reply was saved. Partial text was not saved.'})
                        except RPCError as exc:
                            if exc.status != RPCStatusCode.NOT_FOUND:
                                raise
                await asyncio.sleep(5)

    async def cancel(self, run_id):
        try:
            await self.client.get_workflow_handle('agent-' + str(run_id)).cancel(rpc_timeout=timedelta(seconds=5))
        except RPCError as exc:
            if exc.status != RPCStatusCode.NOT_FOUND:
                raise

    async def stream(self, run_id, after=0):
        """A disconnected subscriber does not own (or cancel) the execution."""
        empty = 0
        while True:
            rows = await self.store.events(run_id, after)
            for row in rows:
                after = row['seq']
                yield f'id: {after}\n' + sse(row['event'], row['data'])
                if row['event'] in ('done','error'):
                    return
            if not rows:
                run = await self.store.get_run(run_id)
                if run['status'] not in ('queued','streaming'):
                    yield sse('done' if run['status']=='done' else 'error',
                        {'run_status':run['status'], 'message':'Run has ended. Reload history.', 'reload':True})
                    return
                empty += 1
                if empty % 20 == 0:
                    yield sse('heartbeat', {'stage':run['status'], 'cancel_requested':run['cancel_requested']})
            await asyncio.sleep(.5)

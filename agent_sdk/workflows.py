"""V1 durable orchestration. Keep this version replay-compatible after deployment.

Workflow code makes decisions only. All model, sandbox, database and stream I/O
is in activities; the completed activity results are recorded by Temporal.
"""
import asyncio
import json
from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import ApplicationError, ActivityError, ChildWorkflowError, WorkflowAlreadyStartedError, is_cancelled_exception

with workflow.unsafe.imports_passed_through():
    from agent_sdk.definitions import validate_tool, validate_output


SAFE_RETRY = RetryPolicy(maximum_attempts=3, initial_interval=timedelta(seconds=1), maximum_interval=timedelta(seconds=3))
NO_RETRY = RetryPolicy(maximum_attempts=1)


@workflow.defn(name='AgentQueueV1')
class AgentQueue:
    """One active agent, five waiting. Admission is transactional in the API outbox."""
    def __init__(self):
        self.pending = []
        self.active = None

    @workflow.update
    def submit(self, run: dict) -> bool:
        if run['run_id'] == self.active or any(x['run_id'] == run['run_id'] for x in self.pending):
            return True
        if len(self.pending) + bool(self.active) >= 6:
            raise ApplicationError('Queue full', type='QueueFull')
        self.pending.append(run)
        return True

    @workflow.query
    def status(self) -> dict:
        return {'active': self.active, 'queued': [r['run_id'] for r in self.pending]}

    @workflow.run
    async def run(self, pending: list[dict]) -> None:
        self.pending.extend(pending)
        for _ in range(100):
            await workflow.wait_condition(lambda: bool(self.pending))
            run = self.pending.pop(0)
            self.active = run['run_id']
            try:
                await workflow.execute_child_workflow(AgentWorkflow.run, run,
                    id='agent-' + run['run_id'], id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                    execution_timeout=timedelta(seconds=run['agent']['limits']['run_timeout_s'] + 120))
            except (ChildWorkflowError, WorkflowAlreadyStartedError):
                # Each child persists its own terminal state; reconciliation handles a hard timeout.
                pass
            self.active = None
        await workflow.wait_condition(workflow.all_handlers_finished)
        workflow.continue_as_new(self.pending)


@workflow.defn(name='AgentRunV1')
class AgentWorkflow:
    def __init__(self):
        self.phase = 'queued'
        self.model_calls = self.tool_calls = self.validation_retries = self.model_retries = 0
        self.deadline = None

    @workflow.query
    def status(self) -> dict:
        return {'phase': self.phase, 'model_calls': self.model_calls, 'tool_calls': self.tool_calls,
                'validation_retries': self.validation_retries, 'model_retries': self.model_retries}

    async def activity(self, name, data, seconds=30, retry=SAFE_RETRY, bounded=True):
        remaining = self.deadline - workflow.time() if bounded and self.deadline is not None else seconds
        if remaining <= 0:
            raise ApplicationError('Run deadline exceeded', type='RunDeadline', non_retryable=True)
        return await workflow.execute_activity(name, data, result_type=dict,
            start_to_close_timeout=timedelta(seconds=min(seconds, remaining)),
            schedule_to_close_timeout=timedelta(seconds=min(seconds * retry.maximum_attempts, remaining)),
            heartbeat_timeout=timedelta(seconds=15), retry_policy=retry,
            cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)

    @workflow.run
    async def run(self, run: dict) -> dict:
        config, rid = run['agent'], run['run_id']
        limits = config['limits']
        self.deadline = run['expires_at']
        sandbox = None
        base = {'run_id': rid, 'chat_id': run['chat_id']}
        terminal = {'status': 'failed', 'message': 'Agent failed; partial text was not saved.'}
        cleanup = True
        try:
            self.phase = 'loading_history'
            loaded = await self.activity('agent_load', base)
            if loaded.get('cancelled'):
                terminal = {'status': 'cancelled', 'message': 'Cancelled before execution.'}
                return terminal
            history, parts, usage = loaded['messages'], [], {}
            while self.model_calls < limits['max_model_calls']:
                self.model_calls += 1
                self.phase = 'model'
                allowed = config['tools'] if self.tool_calls < limits['max_tool_calls'] and self.model_calls < limits['max_model_calls'] else []
                try:
                    result = await self.activity('agent_model', {**base, 'agent': config, 'messages': history,
                        'tools': allowed, 'round': self.model_calls, 'prefix': '' if config.get('output_schema') else ''.join(parts)}, 440, NO_RETRY)
                except ActivityError as exc:
                    if is_cancelled_exception(exc):
                        raise asyncio.CancelledError() from None
                    if self.model_retries >= limits['max_model_retries'] or self.model_calls >= limits['max_model_calls']:
                        raise
                    self.model_retries += 1
                    # A fresh call is explicit, counted, and replaces the abandoned partial attempt.
                    await self.activity('agent_event', {**base, 'key': f'retry-{self.model_calls}',
                        'event': 'status', 'data': {'stage': 'model_retry', 'abandoned_round': self.model_calls}})
                    continue
                text, calls = result['text'], result['calls']
                for key, value in result.get('usage', {}).items():
                    usage[key] = usage.get(key, 0) + value
                if calls:
                    if not allowed or self.tool_calls + len(calls) > limits['max_tool_calls']:
                        raise ApplicationError('Tool limit exceeded', type='ToolLimit', non_retryable=True)
                    history.append({'role': 'assistant', 'content': text or None, 'tool_calls': calls})
                    parts.append(text + ('\n\n' if text else ''))
                    for call in calls:
                        self.tool_calls += 1  # Invalid attempts count too.
                        try:
                            name, arguments = validate_tool(call, allowed)
                        except Exception:
                            if self.validation_retries >= limits['max_validation_retries']:
                                raise ApplicationError('Tool arguments did not validate', type='ValidationLimit', non_retryable=True)
                            self.validation_retries += 1
                            history.append({'role': 'tool', 'tool_call_id': call['id'],
                                'content': json.dumps({'error': 'Invalid or disallowed tool arguments. Use the supplied JSON schema exactly.'})})
                            await self.activity('agent_event', {**base, 'key': f'validation-{self.tool_calls}',
                                'event': 'status', 'data': {'stage': 'validation_retry', 'step': self.tool_calls}})
                            continue
                        if sandbox is None:
                            self.phase = 'sandbox_starting'
                            sandbox = await self.activity('agent_sandbox_open', base, 45, NO_RETRY)
                        self.phase = 'tool'
                        # The activity retries only to recover a persisted receipt. A claimed but
                        # unfinished operation becomes OutcomeUnknown; it NEVER runs twice.
                        output = await self.activity('agent_tool', {**base, 'sandbox': sandbox,
                            'agent': config, 'call': call, 'step': self.tool_calls}, 45,
                            RetryPolicy(maximum_attempts=2, initial_interval=timedelta(seconds=1)))
                        history.append({'role': 'tool', 'tool_call_id': call['id'], 'content': output['result']})
                    continue
                try:
                    structured = validate_output(text, config.get('output_schema'))
                    if config.get('output_schema') and result['finish_reason'] != 'stop':
                        raise ValueError('Truncated structured output')
                except Exception:
                    if self.validation_retries >= limits['max_validation_retries']:
                        raise ApplicationError('Final JSON did not validate', type='ValidationLimit', non_retryable=True)
                    self.validation_retries += 1
                    history.extend([{'role': 'assistant', 'content': text or '(empty response)'},
                        {'role': 'user', 'content': 'Your response did not match the required JSON schema. Return only valid JSON matching it.'}])
                    continue
                parts.append(text)
                self.phase = 'saving_reply'
                terminal = {'status': 'done', 'text': text if config.get('output_schema') else ''.join(parts), 'finish_reason': result['finish_reason'],
                    'usage': usage, 'structured_output': structured, 'model_calls': self.model_calls, 'tool_calls': self.tool_calls}
                return terminal
            raise ApplicationError('Model call limit reached', type='ModelLimit', non_retryable=True)
        except asyncio.CancelledError:
            terminal = {'status': 'cancelled', 'message': 'Cancelled. Partial text was not saved.'}
            return terminal
        except Exception as exc:
            cause = exc.cause if isinstance(exc, ActivityError) else exc
            terminal = {'status': 'failed', 'message': 'Agent stopped safely. Reload history before retrying.',
                        'error_type': getattr(cause, 'type', None) or type(cause).__name__}
            return terminal
        except BaseException:
            # Temporal can discard the in-memory coroutine during replay/cache eviction.
            # That is not a user cancellation and must not schedule external side effects.
            cleanup = False
            raise
        finally:
            if cleanup:
                self.phase = 'cleanup'
                # Cleanup is a separate shielded activity, not a finally block in a dead process.
                try:
                    await asyncio.shield(self.activity('agent_sandbox_close', base, 20, bounded=False))
                except ActivityError:
                    pass  # Provider hard TTL protects against an unreachable cleanup worker.
                saved = await asyncio.shield(self.activity('agent_finish', {**base, **terminal}, 25, bounded=False))
                terminal['status'] = saved['status']
                self.phase = terminal['status']

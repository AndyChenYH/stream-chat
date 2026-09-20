export const duration = ms => ms == null ? '—' : ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(1)} s`;

export function createTrace(requestId, now = Date.now()) {
  return {requestId, startedAt: now, stage: 'connecting', title: 'Connecting to Fly',
    detail: 'Opening the request. No generation has been accepted yet.', chunks: 0, characters: 0,
    heartbeatCount: 0, modelCalls: 0, retries: 0, reconnects: 0, events: [], usage: null};
}

function describe(stage, d) {
  const stages = {
    connecting: ['Connecting to Fly', 'Opening the request. No generation has been accepted yet.'],
    creating_conversation: ['Creating conversation', 'Waiting for Fly to create the conversation in Neon. Neon may need to wake up.'],
    submitting_prompt: ['Submitting and saving prompt', 'Waiting for API acceptance and the Postgres commit.'],
    stream_connected: ['Browser stream connected', 'Fly accepted the prompt and opened the SSE connection.'],
    prompt_saved: ['Prompt saved in Neon', 'The user message is committed before model startup.'],
    queued: [d.position ? `Queued · ${d.position} ahead` : 'Accepted · waiting for the agent worker', 'Your prompt is saved. The queue allows one active run; inference begins when the worker accepts it.'],
    loading_history: ['Loading conversation history', 'Checking Postgres and selecting completed turns for model context.'],
    history_loaded: ['Conversation history loaded', `${d.context_messages} messages selected for model context.`],
    gateway_request: ['Connecting to Vercel AI Gateway', 'Submitting one hosted inference request; there is no local GPU startup.'],
    generation_request: ['Submitting Qwen generation', `Tool schemas and structured-output schema are sent with this request. Output limit: ${d.max_output_tokens} model tokens.`],
    gateway_stream_open: ['Gateway stream connected · waiting for first token', 'Qwen accepted the request. Prefill and initial inference are not reported separately.'],
    first_token: ['First token chunk received', 'Generated text has reached Fly; chunks are being forwarded to your browser.'],
    streaming: ['Streaming generated text', 'Receiving output incrementally. Stream chunks are not the same as model tokens.'],
    agent_round: [`Agent round ${d.round}`, d.max_model_calls ? `Model call ${d.round}/${d.max_model_calls}. Every correction and restarted attempt counts toward this limit.` : `${d.tool_calls}/${d.max_tool_calls} tool calls used.`],
    reconnecting: ['Reconnecting to the run', 'The agent continues on the server. Resuming events after the last received sequence number.'],
    cancellation_requested: ['Cancellation requested', 'Waiting for the current activity to stop and sandbox cleanup to complete.'],
    model_retry: [d.retry_after_s ? `Rate limited · retrying in ${d.retry_after_s}s` : 'Restarting interrupted model attempt', d.retry_after_s ? 'The provider requested a pause. Temporal will resume this run automatically; no completed tools will be repeated.' : `Round ${d.abandoned_round} did not finish. A new, bounded attempt will replace its partial output.`],
    validation_retry: ['Repairing invalid tool arguments', 'No command was executed. The model gets one opportunity to correct the schema error.'],
    replace: ['Restoring confirmed output', 'Restoring the completed rounds before the next model attempt.'],
    sandbox_starting: ['Starting E2B sandbox', `A real tool call triggered startup. Hard lifetime: ${d.timeout_s} seconds; no internet access.`],
    sandbox_ready: ['E2B sandbox ready', `Sandbox ${d.sandbox_id}. Running only for this request.`],
    tool_running: [`Executing ${d.tool} · step ${d.step}`, 'Execution is limited to 30 seconds. Expand the tool card to inspect the command and result.'],
    sandbox_stopped: ['E2B sandbox terminated', 'Saved files remain available in this conversation.'],
    sandbox_cleanup_pending: ['Sandbox cleanup could not be confirmed', `The provider hard timeout will terminate it within ${d.timeout_s} seconds of creation. The full time reservation is retained.`],
    tool: [`${d.name} · ${d.status}`, `Tool step ${d.step}. Command and result are saved with the conversation.`],
    artifact: ['Output file saved', `${d.name} is saved in Neon and can be downloaded after sandbox shutdown.`],
    saving_reply: ['Saving completed reply to Neon', 'Generation finished. Waiting for the assistant message to commit before declaring success.'],
    done: [d.finish_reason === 'length' ? 'Saved · output limit reached' : 'Reply saved', `The complete assistant reply is committed. Finish reason: ${d.finish_reason || 'stop'}.`],
    cancelled: ['Request stopped in browser', 'The connection was closed and cancellation was requested. Reload history to confirm the final saved state.'],
    error: ['Request failed', [d.message, d.stage && `Stage: ${d.stage}`, d.error_type, d.http_status && `HTTP ${d.http_status}`].filter(Boolean).join(' · ')],
  };
  return stages[stage] || [stage, ''];
}

export function recordTrace(previous, event, data = {}, now = Date.now()) {
  if (!previous || previous.finishedAt != null) return previous;
  const trace = {...previous};
  const browserEvent = event === 'browser';
  if (!browserEvent) trace.lastServerAt = now;
  if (event === 'heartbeat') {
    trace.heartbeatCount += 1;
    if (trace.stage === 'queued' && Number.isInteger(data.position)) {
      trace.position = data.position;
      [trace.title, trace.detail] = describe('queued', data);
    }
    return trace;
  }
  if (event === 'token') {
    trace.firstTokenAt ??= now;
    trace.lastTokenAt = now;
    trace.chunks += 1;
    trace.characters += [...data.text].length;
    trace.stage = 'streaming';
    [trace.title, trace.detail] = describe('streaming', data);
    return trace;
  }
  let stage = event === 'status' || browserEvent ? data.stage : event;
  if (stage === 'starting') stage = 'gateway_request';
  if (stage === 'started') return trace;
  if (stage === 'token_usage') {
    trace.usage = {prompt_tokens: data.prompt_tokens, completion_tokens: data.completion_tokens, total_tokens: data.total_tokens};
    return trace;
  }
  if (stage === 'agent_round') trace.modelCalls = Math.max(trace.modelCalls, data.round || 0);
  if (stage === 'model_retry') {
    trace.retries += 1;
    trace.retryAt = data.retry_after_s ? now + data.retry_after_s * 1000 : null;
  }
  if (stage === 'reconnecting') trace.reconnects = data.attempt || trace.reconnects + 1;
  if (data.model) trace.model = data.model;
  if (data.prompt_tokens != null) trace.promptTokens = data.prompt_tokens;
  if (data.http_status != null) trace.lastHttpStatus = data.http_status;
  if (stage === 'queued') trace.position = data.position;
  const [title, detail] = describe(stage, data);
  Object.assign(trace, {stage, title, detail});
  if (stage === 'done' || stage === 'error' || stage === 'cancelled') {
    trace.finishedAt = now;
    if (data.usage) trace.usage = data.usage;
    if (data.model_calls != null) trace.modelCalls = data.model_calls;
    trace.finishReason = data.finish_reason;
  }
  trace.events = [...trace.events, {stage, title, detail, at: now,
    serverElapsed: data.elapsed_ms, source: browserEvent ? 'Browser' : 'Fly / gateway'}].slice(-120);
  return trace;
}

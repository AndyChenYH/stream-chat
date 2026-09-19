export const duration = ms => ms == null ? '—' : ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(1)} s`;

export function createTrace(requestId, now = Date.now()) {
  return {requestId, startedAt: now, stage: 'connecting', title: 'Connecting to Fly',
    detail: 'Opening the request. No generation has been accepted yet.', chunks: 0, characters: 0,
    heartbeatCount: 0, attempts: 0, events: [], usage: null, workers: null};
}

function describe(stage, d) {
  const stages = {
    connecting: ['Connecting to Fly', 'Opening the request. No generation has been accepted yet.'],
    creating_conversation: ['Creating conversation', 'Waiting for Fly to create the conversation in Neon. Neon may need to wake up.'],
    submitting_prompt: ['Submitting and saving prompt', 'Waiting for API acceptance and the Postgres commit.'],
    stream_connected: ['Browser stream connected', 'Fly accepted the prompt and opened the SSE connection.'],
    prompt_saved: ['Prompt saved in Neon', 'The user message is committed before model startup.'],
    queued: [d.position ? `Queued · ${d.position} ahead` : 'Accepted · waiting for the agent worker', 'Your prompt is saved. The queue allows one active run; model startup begins when the worker accepts it.'],
    loading_history: ['Loading conversation history', 'Checking Postgres and selecting completed turns for model context.'],
    history_loaded: ['Conversation history loaded', `${d.context_messages} messages selected before worker context trimming.`],
    worker_startup: ['Starting the on-demand model', 'Requesting a Runpod worker and waiting for model readiness. A cold start can take minutes.'],
    readiness_probe: [`Readiness check #${d.attempt}`, 'Sending GET /ping through Runpod. This wakes a worker if needed; each probe waits up to 30 seconds.'],
    readiness_wait: [`Waiting for readiness · check #${d.attempt}`, 'The readiness request is still pending. Scheduling, container startup or model loading may be in progress.'],
    readiness_result: [`Readiness returned HTTP ${d.http_status}`, d.http_status === 200 ? 'Worker and local model runtime are healthy.' : 'The worker is not ready to serve this request yet.'],
    readiness_retry: [`Readiness check #${d.attempt} · ${d.error_type}`, 'The probe did not complete. Readiness checks can retry; generation POSTs are never replayed automatically.'],
    retry_delay: [`Retrying readiness in ${d.retry_in_s} s`, 'Waiting between probes. The total startup deadline is 240 seconds.'],
    model_ready: ['Model is ready', d.model || 'The local model runtime passed its health check.'],
    generation_request: ['Submitting generation to worker', `Sending one authenticated POST /generate. Output limit: ${d.max_output_tokens} model tokens.`],
    worker_stream_open: ['Worker stream connected', 'The worker accepted generation. Waiting for its context and inference events.'],
    tokenizing: ['Tokenizing conversation', `vLLM is counting tokens in ${d.context_messages} messages and checking the context window.`],
    context_ready: ['Model context ready', `${d.prompt_tokens} prompt tokens · ${d.context_messages} messages including system instructions · ${d.trimmed_messages} older messages trimmed.`],
    runtime_request: ['Requesting vLLM inference', 'The worker is opening a local streaming completion request.'],
    runtime_stream_open: ['vLLM stream connected · waiting for first token', 'The runtime accepted the request. Prefill and initial inference are not reported separately.'],
    first_token: ['First token chunk received', 'Generated text has reached Fly; chunks are being forwarded to your browser.'],
    streaming: ['Streaming generated text', 'Receiving output incrementally. Stream chunks are not the same as model tokens.'],
    agent_round: [`Agent round ${d.round}`, d.max_model_calls ? `Model call ${d.round}/${d.max_model_calls}. Every correction and restarted attempt counts toward this limit.` : `${d.tool_calls}/${d.max_tool_calls} tool calls used.`],
    reconnecting: ['Reconnecting to the run', 'The agent continues on the server. Resuming events after the last received sequence number.'],
    cancellation_requested: ['Cancellation requested', 'Waiting for the current activity to stop and sandbox cleanup to complete.'],
    model_retry: ['Restarting interrupted model attempt', `Round ${d.abandoned_round} did not finish. A new, bounded attempt will replace its partial output.`],
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
    telemetry_unavailable: ['Worker telemetry unavailable', 'Runpod worker counts could not be read. Readiness checks and generation continue independently.'],
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
  if (stage === 'starting') stage = 'worker_startup';
  if (stage === 'started') return trace; // model_ready carries the precise event.
  if (stage === 'worker_snapshot') {
    trace.workers = data.workers;
    trace.workerObservedAt = now;
    const counts = Object.entries(data.workers).map(([k, v]) => `${k}: ${v}`).join(' · ');
    const last = trace.events.at(-1);
    if (last?.stage === stage && last.detail === counts) return trace;
    trace.events = [...trace.events, {stage, title: 'Runpod worker snapshot', detail: counts, at: now}].slice(-120);
    return trace;
  }
  if (stage === 'token_usage') {
    trace.usage = {prompt_tokens: data.prompt_tokens, completion_tokens: data.completion_tokens, total_tokens: data.total_tokens};
    return trace;
  }
  if (stage === 'telemetry_unavailable') trace.workers = null;
  if (data.attempt) trace.attempts = data.attempt;
  if (data.model) trace.model = data.model;
  if (stage === 'model_ready') trace.modelReadyAt = now;
  if (data.prompt_tokens != null) trace.promptTokens = data.prompt_tokens;
  if (data.http_status != null) trace.lastHttpStatus = data.http_status;
  if (stage === 'queued') trace.position = data.position;
  const [title, detail] = describe(stage, data);
  if (stage !== 'telemetry_unavailable') Object.assign(trace, {stage, title, detail});
  if (stage === 'done' || stage === 'error' || stage === 'cancelled') {
    trace.finishedAt = now;
    if (data.usage) trace.usage = data.usage;
    trace.finishReason = data.finish_reason;
  }
  trace.events = [...trace.events, {stage, title, detail, at: now,
    serverElapsed: data.elapsed_ms, source: browserEvent ? 'Browser' : 'Fly / worker'}].slice(-120);
  return trace;
}

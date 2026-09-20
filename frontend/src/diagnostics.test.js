import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createTrace, recordTrace} from './diagnostics.js';

test('records browser TTFT and distinguishes chunks from actual model tokens', () => {
  let trace = createTrace('request-id', 1000);
  trace = recordTrace(trace, 'status', {stage:'gateway_stream_open',http_status:200}, 2000);
  trace = recordTrace(trace, 'token', {text:'Hi 🌿'}, 3000);
  trace = recordTrace(trace, 'token', {text:'!'}, 3500);
  trace = recordTrace(trace, 'done', {finish_reason:'stop',usage:{prompt_tokens:20,completion_tokens:4,total_tokens:24}}, 4000);
  assert.equal(trace.firstTokenAt - trace.startedAt, 2000);
  assert.equal(trace.chunks, 2);
  assert.equal(trace.characters, 5);
  assert.equal(trace.usage.completion_tokens, 4);
  assert.equal(trace.title, 'Reply saved');
  assert.equal(trace.finishedAt, 4000);
  assert.equal(recordTrace(trace,'heartbeat',{},5000), trace);
});

test('heartbeats update liveness and queue position without inventing progress', () => {
  let trace = recordTrace(createTrace('id',0),'queued',{position:3},10);
  trace = recordTrace(trace,'heartbeat',{position:1},10000);
  assert.equal(trace.title, 'Queued · 1 ahead');
  assert.equal(trace.lastServerAt, 10000);
  assert.equal(trace.heartbeatCount, 1);
  assert.equal(trace.firstTokenAt, undefined);
  assert.equal(trace.chunks, 0);
});

test('gateway stream state becomes the real generation phase', () => {
  const trace = recordTrace(createTrace('id',0),'status',{stage:'gateway_stream_open',http_status:200},1);
  assert.equal(trace.stage, 'gateway_stream_open');
  assert.equal(trace.title, 'Gateway stream connected · waiting for first token');
  assert.equal(trace.lastHttpStatus, 200);
});

test('failure retains the stage and a local stop does not claim durable cancellation', () => {
  const trace = recordTrace(createTrace('id',0),'error',{message:'Timed out',stage:'gateway_stream_open',error_type:'TimeoutError'},5);
  assert.match(trace.detail,/gateway_stream_open/);
  assert.match(trace.detail,/TimeoutError/);
  assert.equal(recordTrace(trace,'browser',{stage:'error',message:'generic'},6),trace);
  const stopped = recordTrace(createTrace('id',0),'browser',{stage:'cancelled'},5);
  assert.match(stopped.detail,/Reload history to confirm/);
});

test('model calls, retries and browser reconnects are separate counters', () => {
  let trace = createTrace('id', 0);
  trace = recordTrace(trace, 'status', {stage:'agent_round',round:1}, 10);
  trace = recordTrace(trace, 'status', {stage:'model_retry',abandoned_round:1}, 20);
  trace = recordTrace(trace, 'status', {stage:'agent_round',round:2}, 30);
  trace = recordTrace(trace, 'browser', {stage:'reconnecting',attempt:1}, 40);
  assert.equal(trace.modelCalls, 2);
  assert.equal(trace.retries, 1);
  assert.equal(trace.reconnects, 1);
  trace = recordTrace(trace, 'done', {model_calls:3,finish_reason:'stop'}, 50);
  assert.equal(trace.modelCalls, 3);
});

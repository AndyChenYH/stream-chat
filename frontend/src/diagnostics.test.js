import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createTrace, recordTrace} from './diagnostics.js';

test('records browser TTFT and distinguishes chunks from actual model tokens', () => {
  let trace = createTrace('request-id', 1000);
  trace = recordTrace(trace, 'status', {stage:'readiness_probe',attempt:2}, 2000);
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

test('provider telemetry cannot replace the real generation phase', () => {
  let trace = recordTrace(createTrace('id',0),'status',{stage:'readiness_wait',attempt:1},1);
  trace = recordTrace(trace,'status',{stage:'worker_snapshot',workers:{initializing:1,running:0}},2);
  assert.equal(trace.stage, 'readiness_wait');
  assert.equal(trace.workers.initializing, 1);
  trace = recordTrace(trace,'status',{stage:'telemetry_unavailable'},3);
  assert.equal(trace.workers, null);
  assert.equal(trace.stage, 'readiness_wait');
});

test('failure retains the stage and a local stop does not claim durable cancellation', () => {
  const trace = recordTrace(createTrace('id',0),'error',{message:'Timed out',stage:'readiness_wait',error_type:'TimeoutError'},5);
  assert.match(trace.detail,/readiness_wait/);
  assert.match(trace.detail,/TimeoutError/);
  assert.equal(recordTrace(trace,'browser',{stage:'error',message:'generic'},6),trace);
  const stopped = recordTrace(createTrace('id',0),'browser',{stage:'cancelled'},5);
  assert.match(stopped.detail,/Reload history to confirm/);
});

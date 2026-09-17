import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readEvents} from './sse.js';

test('SSE survives arbitrary byte boundaries, Unicode, heartbeats and multiple events', async () => {
  const bytes = new TextEncoder().encode(': keepalive\n\nevent: token\ndata: {"text":"你好 🌿\\nnext"}\n\nevent: done\ndata: {}\n\n');
  const received = [];
  const stream = new ReadableStream({start(c){for(const byte of bytes)c.enqueue(Uint8Array.of(byte));c.close()}});
  await readEvents(stream, (event,data)=>received.push([event,data]));
  assert.deepEqual(received, [['token',{text:'你好 🌿\nnext'}],['done',{}]]);
});
test('an abruptly ended stream cannot be mistaken for success', async () => {
  const stream = new ReadableStream({start(c){c.enqueue(new TextEncoder().encode('event: token\ndata: {"text":"partial"}\n\n'));c.close()}});
  await assert.rejects(readEvents(stream,()=>{}), /interrupted/);
});

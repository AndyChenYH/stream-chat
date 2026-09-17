export async function readEvents(body, onEvent) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '', terminal = false;
  function consume(frame) {
    let event = 'message'; const data = [];
    for (const line of frame.split('\n')) {
      if (line.startsWith('event:')) event = line.slice(6).trim();
      if (line.startsWith('data:')) data.push(line.slice(5).trimStart());
    }
    if (!data.length) return;
    onEvent(event, JSON.parse(data.join('\n')));
    if (event === 'done' || event === 'error') terminal = true;
  }
  try {
    while (true) {
      const {value, done} = await reader.read();
      buffer += decoder.decode(value, {stream: !done});
      // The API writes LF framing; normalize CRLF while retaining incomplete chunks.
      buffer = buffer.replace(/\r\n/g, '\n');
      let end;
      while ((end = buffer.indexOf('\n\n')) !== -1) {
        consume(buffer.slice(0, end)); buffer = buffer.slice(end + 2);
      }
      if (done) break;
    }
    if (!terminal) throw new Error('Connection interrupted. Reload history to check whether the answer was saved.');
  } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
}

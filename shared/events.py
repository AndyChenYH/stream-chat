from dataclasses import dataclass
import json


@dataclass
class Chunk:
    text: str = ''
    done: bool = False
    finish_reason: str = ''
    started: bool = False
    usage: dict | None = None


def sse(event, data):
    return f'event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n'


async def read_events(response):
    event, data, size = 'message', [], 0
    async for line in response.aiter_lines():
        size += len(line)
        if size > 262144:
            raise RuntimeError('Upstream event too large')
        if not line:
            if data:
                yield event, json.loads('\n'.join(data))
            event, data, size = 'message', [], 0
        elif line.startswith('event:'):
            event = line[6:].strip()
        elif line.startswith('data:'):
            data.append(line[5:].lstrip())
    if data:
        raise RuntimeError('Incomplete upstream event')

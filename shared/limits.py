import asyncio
import re
from starlette.responses import JSONResponse


class BodyLimit:
    """Bound request bytes even when a caller uses chunked transfer encoding."""

    def __init__(self, app, limit, upload_limit=None):
        self.app, self.limit, self.upload_limit = app, limit, upload_limit

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http' or scope['method'] != 'POST':
            return await self.app(scope, receive, send)
        body = bytearray()
        limit = self.upload_limit if self.upload_limit and re.fullmatch(
            r'/v1/conversations/[a-fA-F0-9-]{36}/files',scope['path']) else self.limit
        try:
            async with asyncio.timeout(15):
                while True:
                    event = await receive()
                    if event['type'] == 'http.disconnect':
                        return
                    chunk = event.get('body', b'')
                    if len(body) + len(chunk) > limit:
                        return await JSONResponse({'detail': 'Request too large'}, status_code=413)(scope, receive, send)
                    body.extend(chunk)
                    if not event.get('more_body', False):
                        break
        except TimeoutError:
            return await JSONResponse({'detail': 'Request timed out'}, status_code=408)(scope, receive, send)
        delivered = False

        async def bounded_receive():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {'type': 'http.request', 'body': bytes(body), 'more_body': False}
            return await receive()

        await self.app(scope, bounded_receive, send)

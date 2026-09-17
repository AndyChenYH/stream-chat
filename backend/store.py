from pathlib import Path
from uuid import uuid4
import asyncio

import asyncpg


class Store:
    def __init__(self, url):
        self.url = url
        self.lease_guard = asyncio.Lock()

    async def open(self):
        # A dedicated direct connection prevents two API processes owning the queue.
        self.lease = await asyncpg.connect(self.url, command_timeout=10)
        if not await self.lease.fetchval('SELECT pg_try_advisory_lock(7218462026)'):
            await self.lease.close()
            raise RuntimeError('Another chat service is running. Deploy exactly one process.')
        self.pool = await asyncpg.create_pool(self.url, min_size=1, max_size=4, command_timeout=15)
        async with self.pool.acquire() as db, db.transaction():
            await db.execute(Path(__file__).with_name('schema.sql').read_text())
            await db.execute("UPDATE runs SET status='interrupted', finished_at=now() WHERE status IN ('queued','streaming')")

    async def close(self):
        await self.pool.close()
        await self.lease.close()

    async def healthy(self):
        if self.lease.is_closed():
            raise RuntimeError('Queue lease lost; restart the service')
        async with self.lease_guard:
            await self.lease.execute('SELECT 1')

    async def list_chats(self, offset=0):
        return [dict(r) for r in await self.pool.fetch(
            'SELECT * FROM conversations ORDER BY updated_at DESC, id LIMIT 50 OFFSET $1', offset)]

    async def create_chat(self):
        return dict(await self.pool.fetchrow('INSERT INTO conversations(id) VALUES($1) RETURNING *', uuid4()))

    async def exists(self, chat_id):
        return await self.pool.fetchval('SELECT EXISTS(SELECT 1 FROM conversations WHERE id=$1)', chat_id)

    async def messages(self, chat_id, before=None):
        rows = await self.pool.fetch('''SELECT m.*, r.status AS run_status FROM messages m
            JOIN runs r ON r.id=m.run_id WHERE m.conversation_id=$1
            AND ($2::bigint IS NULL OR seq < $2) ORDER BY seq DESC LIMIT 100''', chat_id, before)
        return [dict(r) for r in reversed(rows)]

    async def accept(self, chat_id, run_id, content):
        async with self.pool.acquire() as db, db.transaction():
            await db.execute("INSERT INTO runs(id,conversation_id,status) VALUES($1,$2,'queued')", run_id, chat_id)
            await db.execute("INSERT INTO messages(id,conversation_id,run_id,role,content) VALUES($1,$2,$3,'user',$4)", uuid4(), chat_id, run_id, content)
            await db.execute('''UPDATE conversations SET updated_at=now(),
                title=CASE WHEN title='New chat' THEN $2 ELSE title END WHERE id=$1''', chat_id, content[:70])

    async def context(self, chat_id, run_id):
        # Failed/cancelled prompts stay visible in history but are excluded from context.
        rows = await self.pool.fetch('''SELECT m.role,m.content FROM messages m JOIN runs r ON r.id=m.run_id
            WHERE m.conversation_id=$1 AND (r.status='done' OR r.id=$2)
            ORDER BY m.seq DESC LIMIT 41''', chat_id, run_id)
        messages = [dict(r) for r in reversed(rows)]
        while messages and messages[0]['role'] != 'user':
            messages.pop(0)
        return messages

    async def status(self, run_id, status):
        await self.pool.execute('''UPDATE runs SET status=$2,
            finished_at=CASE WHEN $2='streaming' THEN NULL ELSE now() END
            WHERE id=$1 AND status IN ('queued','streaming')''', run_id, status)

    async def complete(self, chat_id, run_id, content):
        message_id = uuid4()
        async with self.pool.acquire() as db, db.transaction():
            await db.execute("INSERT INTO messages(id,conversation_id,run_id,role,content) VALUES($1,$2,$3,'assistant',$4)", message_id, chat_id, run_id, content)
            await db.execute("UPDATE runs SET status='done', finished_at=now() WHERE id=$1", run_id)
            await db.execute('UPDATE conversations SET updated_at=now() WHERE id=$1', chat_id)
        return str(message_id)

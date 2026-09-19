from pathlib import Path
from uuid import uuid4
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
import ssl
import json

import asyncpg


class SandboxBudgetExceeded(ValueError):
    pass


class Store:
    def __init__(self, url):
        parts = urlsplit(url)
        # asyncpg does not accept libpq's channel_binding URL parameter.
        query = urlencode([(k,v) for k,v in parse_qsl(parts.query) if k != 'channel_binding'])
        self.url = urlunsplit(parts._replace(query=query))
        self.ssl = False if parts.hostname in ('localhost', '127.0.0.1', '::1') else ssl.create_default_context()

    async def open(self):
        # Close idle connections so Neon can scale to zero between visits.
        self.pool = await asyncpg.create_pool(self.url, min_size=0, max_size=4,
            max_inactive_connection_lifetime=30, timeout=20, command_timeout=15, ssl=self.ssl)
        async with self.pool.acquire() as db, db.transaction():
            await db.execute(Path(__file__).with_name('schema.sql').read_text())
            await db.execute("UPDATE runs SET status='interrupted', finished_at=now() WHERE engine IS NULL AND status IN ('queued','streaming')")
            await db.execute("UPDATE tool_steps SET status='interrupted' WHERE status='running' AND run_id IN (SELECT id FROM runs WHERE engine IS NULL)")

    async def close(self):
        await self.pool.close()

    async def healthy(self):
        await self.pool.fetchval('SELECT 1')

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

    async def reserve_sandbox(self, run_id, seconds=180):
        # Durable, conservative reservations survive crashes and concurrent clients.
        async with self.pool.acquire() as db, db.transaction():
            await db.execute('SELECT pg_advisory_xact_lock(734001)')
            usage = await db.fetchrow('''SELECT COALESCE(sum(reserved_seconds),0) AS total,
                COALESCE(sum(reserved_seconds) FILTER (WHERE created_at > now()-interval '24 hours'),0) AS daily
                FROM sandbox_usage''')
            if usage['total'] + seconds > 36000 or usage['daily'] + seconds > 3600:
                raise SandboxBudgetExceeded('Sandbox usage allowance reached (1 hour per 24h, 10 hours total)')
            await db.execute('INSERT INTO sandbox_usage(run_id,reserved_seconds) VALUES($1,$2)',run_id,seconds)

    async def sandbox_created(self, run_id, sandbox_id):
        await self.pool.execute('UPDATE sandbox_usage SET sandbox_id=$2 WHERE run_id=$1',run_id,sandbox_id)

    async def sandbox_stopped(self, run_id, seconds):
        await self.pool.execute('UPDATE sandbox_usage SET stopped=true,reserved_seconds=$2 WHERE run_id=$1',run_id,min(180,max(1,seconds)))

    async def sandbox_budget(self):
        row = await self.pool.fetchrow('''SELECT COALESCE(sum(reserved_seconds),0) AS total_seconds,
            COALESCE(sum(reserved_seconds) FILTER (WHERE created_at > now()-interval '24 hours'),0) AS daily_seconds
            FROM sandbox_usage''')
        return {**dict(row), 'daily_limit_seconds':3600, 'total_limit_seconds':36000}

    async def tool_step(self, run_id, step, name, arguments, status, result=None):
        await self.pool.execute('''INSERT INTO tool_steps(run_id,step,name,arguments,status,result) VALUES($1,$2,$3,$4,$5,$6)
            ON CONFLICT(run_id,step) DO UPDATE SET status=excluded.status,result=excluded.result''',
            run_id,step,name,json.dumps(arguments),status,result)

    async def tools(self, chat_id):
        rows = await self.pool.fetch('''SELECT t.*,r.created_at FROM tool_steps t JOIN runs r ON r.id=t.run_id
            WHERE r.conversation_id=$1 ORDER BY r.created_at DESC,t.step DESC LIMIT 100''',chat_id)
        return [{**dict(r), 'arguments':json.loads(r['arguments'])} for r in reversed(rows)]

    async def save_artifact(self, chat_id, run_id, name, mime_type, data):
        async with self.pool.acquire() as db, db.transaction():
            await db.execute('SELECT pg_advisory_xact_lock(734002)')
            size = await db.fetchval('SELECT COALESCE(sum(octet_length(data)),0) FROM artifacts')
            count = await db.fetchval('SELECT count(*) FROM artifacts WHERE conversation_id=$1',chat_id)
            if size + len(data) > 100*1024*1024 or count >= 20 or len(data) > 2*1024*1024:
                raise ValueError('File storage limit reached (2 MB/file, 20 files/chat, 100 MB total)')
            row = await db.fetchrow('''INSERT INTO artifacts(id,conversation_id,run_id,name,mime_type,data)
                VALUES($1,$2,$3,$4,$5,$6) RETURNING id,run_id,name,mime_type,octet_length(data) AS size''',
                uuid4(),chat_id,run_id,name,mime_type,data)
            return {**dict(row),'id':str(row['id']),'run_id':str(run_id) if run_id else None}

    async def files(self, chat_id, include_data=False):
        columns = 'data' if include_data else 'octet_length(data) AS size'
        return [dict(r) for r in await self.pool.fetch(f'''SELECT id,run_id,name,mime_type,{columns} FROM artifacts
            WHERE conversation_id=$1 ORDER BY created_at''',chat_id)]

    async def file(self, file_id):
        row = await self.pool.fetchrow('SELECT * FROM artifacts WHERE id=$1',file_id)
        return dict(row) if row else None

"""Product records, transactional outbox and a temporary SSE buffer, not workflow checkpoints."""
import json
from datetime import datetime, timezone, timedelta
from uuid import UUID, uuid4
from backend.gate import QueueFull, ConversationBusy
from backend.store import Store


def uid(value):
    return value if isinstance(value, UUID) else UUID(value)


class DurableStore(Store):
    async def accept_run(self, chat_id, run_id, content, config):
        async with self.pool.acquire() as db, db.transaction():
            await db.execute('SELECT pg_advisory_xact_lock(734003)')
            old = await db.fetchrow('SELECT r.*,m.content FROM runs r JOIN messages m ON m.run_id=r.id '
                                    "AND m.role='user' WHERE r.id=$1", run_id)
            if old:
                if old['conversation_id'] != chat_id or old['content'] != content or json.loads(old['agent_config'] or '{}') != config:
                    raise ValueError('Request ID was already used for different input')
                return False
            rows = await db.fetch("SELECT conversation_id FROM runs WHERE status IN ('queued','streaming')")
            if any(r['conversation_id'] == chat_id for r in rows):
                raise ConversationBusy()
            if len(rows) >= 6:
                raise QueueFull()
            await db.execute('''INSERT INTO runs(id,conversation_id,status,engine,agent_config,expires_at)
                VALUES($1,$2,'queued','temporal-v1',$3,now()+$4::interval)''', run_id, chat_id,
                json.dumps(config), timedelta(seconds=config['limits']['run_timeout_s']))
            await db.execute("INSERT INTO messages(id,conversation_id,run_id,role,content) VALUES($1,$2,$3,'user',$4)",
                             uuid4(), chat_id, run_id, content)
            await db.execute("UPDATE conversations SET updated_at=now(),title=CASE WHEN title='New chat' THEN $2 ELSE title END WHERE id=$1",chat_id,content[:70])
            await self.event(run_id, 'accepted', 'queued', {'run_id': str(run_id), 'position': len(rows)}, db=db)
            return True

    async def pending_runs(self):
        return [dict(r) for r in await self.pool.fetch("SELECT * FROM runs WHERE engine='temporal-v1' AND status IN ('queued','streaming') ORDER BY created_at,id")]

    async def get_run(self, run_id):
        row = await self.pool.fetchrow('SELECT * FROM runs WHERE id=$1', uid(run_id))
        return dict(row) if row else None

    async def chat_run(self, chat_id):
        row = await self.pool.fetchrow("SELECT id,status,cancel_requested,created_at FROM runs WHERE conversation_id=$1 AND engine='temporal-v1' AND status IN ('queued','streaming') ORDER BY created_at LIMIT 1", chat_id)
        return dict(row) if row else None

    async def request_cancel(self, run_id):
        await self.pool.execute("UPDATE runs SET cancel_requested=true WHERE id=$1 AND status IN ('queued','streaming')", uid(run_id))

    async def event(self, run_id, key, event, data, db=None):
        await (db or self.pool).execute('''INSERT INTO run_events(run_id,event_key,event,data) VALUES($1,$2,$3,$4)
            ON CONFLICT(run_id,event_key) DO NOTHING''', uid(run_id), key, event, json.dumps(data))

    async def events(self, run_id, after):
        rows = await self.pool.fetch('SELECT seq,event,data FROM run_events WHERE run_id=$1 AND seq>$2 ORDER BY seq LIMIT 200', uid(run_id), after)
        return [{**dict(r), 'data': json.loads(r['data'])} for r in rows]

    async def finish_run(self, run_id, result):
        async with self.pool.acquire() as db, db.transaction():
            row = await db.fetchrow('SELECT * FROM runs WHERE id=$1 FOR UPDATE', uid(run_id))
            if row['status'] not in ('queued', 'streaming'):
                return {'status': row['status']}
            result = dict(result)
            if row['cancel_requested']:
                result = {'status': 'cancelled', 'message': 'Cancelled. Partial text was not saved.'}
            status = result.pop('status')
            if status == 'done':
                message_id = uuid4()
                await db.execute("INSERT INTO messages(id,conversation_id,run_id,role,content) VALUES($1,$2,$3,'assistant',$4) ON CONFLICT(run_id,role) DO NOTHING",
                    message_id, row['conversation_id'], uid(run_id), result.pop('text'))
                result['message_id'] = str(message_id)
                await db.execute('UPDATE conversations SET updated_at=now() WHERE id=$1',row['conversation_id'])
            await db.execute('UPDATE runs SET status=$2,finished_at=now() WHERE id=$1',uid(run_id),status)
            await db.execute("UPDATE tool_steps SET status=$2,result=COALESCE(result,'Execution outcome unknown after interruption; not retried.') WHERE run_id=$1 AND status='running'",uid(run_id),'cancelled' if status=='cancelled' else 'unknown')
            await self.event(run_id, 'terminal', 'done' if status=='done' else 'error',
                             {**result, 'run_status': status}, db=db)
            return {'status': status}

    async def claim_tool(self, run_id, step, name, args):
        row = await self.pool.fetchrow('''INSERT INTO tool_steps(run_id,step,name,arguments,status)
            VALUES($1,$2,$3,$4,'running') ON CONFLICT DO NOTHING RETURNING step''', uid(run_id),step,name,json.dumps(args))
        if row:
            return None
        old = await self.pool.fetchrow('SELECT * FROM tool_steps WHERE run_id=$1 AND step=$2', uid(run_id),step)
        return dict(old)

    async def sandbox_record(self, run_id):
        row = await self.pool.fetchrow('SELECT * FROM sandbox_usage WHERE run_id=$1',uid(run_id))
        return dict(row) if row else None

    async def prune_events(self):
        # Replay buffers expire after 24h; permanent messages/tools/files remain available.
        await self.pool.execute("DELETE FROM run_events e USING runs r WHERE e.run_id=r.id AND r.finished_at < now()-interval '24 hours'")

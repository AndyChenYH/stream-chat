# Stream Chat

A personal React/Vite chat app on GitHub Pages, a Fly.io HTTPS/SSE API, Neon managed Postgres history, and a Runpod Serverless GPU worker that scales from zero to one. The API calls the worker over authenticated HTTPS streaming. The worker calls vLLM on localhost.

## Request flow

1. The browser sends a prompt and personal access key to Fly. Fly saves the prompt in Postgres and reserves a place in its bounded memory queue.
2. Once its turn starts, Fly emits `starting` and makes authenticated readiness requests to Runpod. Runpod starts a GPU worker if none is running. Login and health probes never start a worker.
3. The worker resolves Runpod's cached model, loads vLLM, and reports ready. Fly submits one generation request and forwards token chunks immediately over SSE.
4. The completed reply is committed to Postgres before Fly sends `done`. Failed or cancelled partial responses are not saved as completed assistant replies.
5. Runpod stops the worker after 60 idle seconds. Conversation history remains in Neon, which can also sleep when idle.

## Security and limits

The personal access key lives only in browser tab memory and Fly secrets. Every conversation endpoint requires it; the single key grants access to the whole personal workspace. The frontend contains only the public API origin.

Fly verifies Runpod's HTTPS certificate and authenticates with a Runpod API key. The worker separately verifies `X-Worker-Key`, so direct worker access also requires service authentication. Neither credential is sent to the browser or baked into the image. Runpod's internal `/ping` is a readiness-only endpoint. vLLM binds exclusively to localhost; no direct TCP ports, SSH, or Jupyter are needed.

Deploy exactly **one Fly Machine with one Uvicorn process**, **one maximum Runpod worker**, and vLLM `max-num-seqs=1`. Fly permits one active generation and five queued requests, with no Redis or external scheduler. The worker rejects overlapping generations. This is not a multi-user authentication system.

Readiness GETs retry for at most 240 seconds; authentication failures stop immediately. Generation POSTs are never automatically replayed. Generation is limited to 180 seconds and 1024 output tokens. Queue waiting is capped at 300 seconds. A 10-second SSE heartbeat keeps the browser connection active during startup. Stop cancels the request; cancellation of startup leaves any already-starting worker to Runpod's idle timeout. A 60-second idle timeout and max-workers=1 are not a hard monthly spending cap.

## Repository

- `frontend/`: React UI, SSE parser, GitHub Pages build.
- `backend/`: access control, bounded queue, persistence, Serverless client.
- `worker/`: HTTP wrapper, model cache resolver, and vLLM container startup.
- `shared/`: stream framing and body limits.
- `tests/`: authentication, cold starts, cancellation, stream completion, context limits, and Postgres persistence.

## Local checks

Use Python 3.12, Node 24, and pnpm 11.19.0. Create a virtual environment, then run `pip install -r requirements-dev.txt` and `pytest -q`. Set `TEST_DATABASE_URL` to a dedicated disposable Postgres database to include the database test; otherwise it is explicitly skipped. CI runs all tests against Postgres 16.

Run `pnpm install --frozen-lockfile`, `pnpm test`, and `pnpm build` in `frontend/`. `pnpm dev` defaults to `http://127.0.0.1:8080`. For a real local backend, set the variables in `.env.example` in the shell and run `uvicorn backend.main:create_app --factory --port 8080`. There is no production mock-model fallback.

## Runpod Serverless configuration

Build the **Build GPU worker image** GitHub Actions workflow and use its commit-tagged image. Configure a **Load Balancer** endpoint, not a queue-based endpoint:

| Setting | Value |
|---|---|
| Active/minimum workers | 0 |
| Maximum workers | 1 |
| GPUs per worker | 1 |
| Initial GPU tier | 16 GB |
| Idle timeout | 60 seconds |
| FlashBoot | Enabled |
| Cached model | `Qwen/Qwen3-4B-Instruct-2507` |
| HTTP port / `PORT` | 80 |
| Health path / `HEALTH_CHECK_PATH` | `/ping` |
| `PORT_HEALTH` | 80 |
| Direct TCP ports | None |
| `WORKER_SERVICE_KEY` | Random secret, minimum 32 characters |
| `MODEL_NAME` | `Qwen/Qwen3-4B-Instruct-2507` |
| `MAX_MODEL_LEN` | 8192 |
| `REQUIRE_MODEL_CACHE` | 1 |

The vLLM base image uses CUDA 13.0.2. Select a compatible host driver (CUDA 13.0 or newer). `--enforce-eager` avoids graph/compilation startup work, trading some steady-state performance for startup speed. The worker requires the Runpod model-cache mount; if missing, it fails rather than silently downloading weights during billed worker time. No network volume is required for conversation history.

The Runpod API key should be scoped to this endpoint's request access where supported. Keep `WORKER_SERVICE_KEY` in Runpod's secret store and inject it as an environment variable. Configure the same service key in Fly. Container images contain source and dependencies only.

## Neon and Fly configuration

Create a Neon project on the **Free** plan. Keep its compute small and scale-to-zero enabled. Store the TLS Postgres connection string in Fly as `DATABASE_URL`. Idle pool connections close after 30 seconds, and Fly's `/healthz` probe makes no database or model requests, so it does not prevent scale-to-zero. Deploy one API process; startup marks abandoned queued/streaming runs interrupted.

Fly secrets: `DATABASE_URL`, `CHAT_ACCESS_KEY`, `RUNPOD_ENDPOINT_URL`, `RUNPOD_API_KEY`, `WORKER_SERVICE_KEY`. The endpoint origin has the form `https://ENDPOINT_ID.api.runpod.ai`; arbitrary hosts and plain HTTP are rejected. Configure `ALLOWED_ORIGINS` to the GitHub Pages origin, without a repository path.

Use `fly deploy --ha=false --strategy immediate`, and keep exactly one Machine. The included config requests one shared CPU and 512 MB in `sjc`. Fly is the always-available entry point; GPU and database compute sleep independently.

## GitHub Pages

The repository variable `API_BASE_URL` is the Fly HTTPS origin. Set Pages source to **GitHub Actions** and use the included frontend deployment workflow. Vite uses relative assets for project Pages URLs. Never place credentials in `VITE_` variables.

## Acceptance and operations

Verify a real cold request streams, its completed response survives a page reload, a warm follow-up is faster, Stop cancels generation, unauthorized API and worker requests fail, and the worker count returns to zero after idle timeout. Measure startup and first-token latency on the chosen GPU rather than relying on generic cold-start claims.

GPU time is billed during startup, execution, and idle timeout. Storage is billed separately. No credit recharge or automatic budget purchase is configured by this application. An exhausted balance or provider quota makes generation unavailable. After a prolonged unused period, Runpod may disable an endpoint; restore its maximum workers to one when needed. Keep provider access keys and the personal chat key in a password manager.

The queue is in memory: restarts interrupt queued work. After an ambiguous disconnect, reload history before retrying because a reply may already have committed. Duplicate request IDs are rejected rather than replayed. Neon plan quotas apply to both storage and compute.

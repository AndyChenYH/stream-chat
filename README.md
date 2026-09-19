# Stream Chat

A personal React/Vite chat app on GitHub Pages, a Fly.io HTTPS/SSE API, Neon managed Postgres history, and a Runpod Serverless GPU worker that scales from zero to one. The API calls the worker over authenticated HTTPS streaming. The worker calls vLLM on localhost.

## Deployment

- Chat: https://andychenyh.github.io/stream-chat/
- API: https://stream-chat-andy.fly.dev
- Runpod endpoint: `3bjnfx7c4y7izs` (zero active workers, one maximum).
- Database: Neon Free project `stream-chat`, Postgres 16 in AWS Oregon.

Local source is in `~/Coding/stream-chat`. Deployment secrets are in the ignored, mode-0600 `.secrets/fly-serverless.env`; `CHAT_ACCESS_KEY` unlocks the personal workspace. Keep this file private.

## Request flow

1. The browser sends a prompt and personal access key to Fly. Fly saves the prompt in Postgres and reserves a place in its bounded memory queue.
2. Once its turn starts, Fly emits `starting` and makes authenticated readiness requests to Runpod. Runpod starts a GPU worker if none is running. Login and health probes never start a worker.
3. The worker resolves Runpod's cached model, loads vLLM, and reports ready. Fly submits a generation request and forwards token chunks immediately over SSE. With Code tools enabled, completed native tool calls return to Fly, which executes them in E2B and sends the results back to the model for another round.
4. The completed reply is committed to Postgres before Fly sends `done`. Failed or cancelled partial responses are not saved as completed assistant replies.
5. Runpod stops the worker after 60 idle seconds. Conversation history remains in Neon, which can also sleep when idle.

## Security and limits

The personal access key lives only in browser tab memory and Fly secrets. Every conversation endpoint requires it; the single key grants access to the whole personal workspace. The frontend contains only the public API origin.

Fly verifies Runpod's HTTPS certificate and authenticates with a Runpod API key. The worker separately verifies `X-Worker-Key`, so direct worker access also requires service authentication. Neither credential is sent to the browser or baked into the image. Runpod's internal `/ping` is a readiness-only endpoint. vLLM binds exclusively to localhost; no direct TCP ports, SSH, or Jupyter are needed.

Deploy exactly **one Fly Machine with one Uvicorn process**, **one maximum Runpod worker**, and vLLM `max-num-seqs=1`. Fly permits one active generation and five queued requests, with no Redis or external scheduler. The worker rejects overlapping generations. This is not a multi-user authentication system.

Readiness GETs retry for at most 240 seconds; authentication failures stop immediately. Generation POSTs are never automatically replayed. Each model round is limited to 180 seconds and 1024 output tokens, with a 720-second deadline for the entire request including queueing. Queue waiting is capped at 300 seconds. A 10-second SSE heartbeat keeps the browser connection active during startup. Stop cancels the request; cancellation of startup leaves any already-starting worker to Runpod's idle timeout. A 60-second idle timeout and max-workers=1 are not a hard monthly spending cap.

## E2B code tools

The composer has a **Code tools** checkbox and **Add file** control. With tools enabled, Qwen can call `terminal(command)`, `python(code)`, and `publish_file(path)`. The worker uses vLLM's native Hermes tool parser; text/code fences are never interpreted as commands. Fly waits for a complete, validated response before executing a tool. Tools run sequentially, with at most six calls plus a final model round. Turning tools off uses the original chat path.

Fly creates one E2B code-interpreter sandbox on the first tool call, after GPU startup. No sandbox starts for a text-only answer. Commands and Python cells have a 30-second limit; the sandbox has a fixed 180-second provider timeout with kill-on-timeout and no auto-resume. It is killed as soon as the model finishes, or on Stop, disconnect, execution failure, or backend shutdown. Cancellation during creation waits briefly for the sandbox ID so it can still be killed. A failed cleanup is shown in diagnostics; the provider timeout remains the fallback. Sandbox memory/files persist between tools in one request, never between requests.

The sandbox has no outbound internet and receives no Fly, Neon, Runpod, or E2B API credentials. E2B's API key is a Fly secret (`E2B_API_KEY`). The model receives uploaded file paths, and Fly stages conversation files into `/home/user/files/` only when a tool actually runs. Python includes common analysis packages. Use `plt.show()` for PNG plots and `publish_file` to preserve other files. Executable code stays in E2B; the backend never executes generated commands locally.

Inputs, saved output files, and tool commands/results persist in Neon and are accessible from another client using the same personal key. Raw sandbox files disappear on termination. Files are limited to 2 MB each, 20 per conversation and 100 MB total; each task can publish up to six outputs. Downloads require the access key and use attachment responses; only validated PNGs are displayed inline. Tool history shows the latest 100 steps for a conversation. Request timing diagnostics still live only in the current tab.

Before creating a sandbox, Fly reserves its full 180-second lifetime in Postgres. Confirmed termination refunds unused time; ambiguous creation/cleanup retains the full reservation. The app blocks creation beyond 3,600 reserved seconds in a rolling 24 hours or 36,000 seconds total. These limits survive restarts and apply across clients. At the default 2-vCPU/4-GiB E2B rate of $0.000046/second on 2026-09-19, ten hours is about $1.66 of credits. This is an application time allowance, not a provider-enforced dollar cap, and excludes Runpod and Fly charges. No card, credit purchase or paid upgrade is configured by this feature. Increasing the fixed allowance requires an intentional backend change.

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
| GPU tiers | 16 GB preferred, 24 GB fallback |
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

## Live request diagnostics

Each request shows a live status panel with a timestamped event timeline, request ID, elapsed time, time to first token in the browser, readiness attempts and HTTP results, heartbeat freshness, stream chunk/character counters, and chunk throughput. Startup snapshots report Runpod's actual worker counts every five seconds while a readiness probe is pending. Those control-plane reads do not wake another GPU. Provider failures only disable telemetry, not inference.

Worker events distinguish tokenization/context trimming, vLLM submission, the local runtime stream opening, first output, streaming, and the final Neon commit. vLLM reports exact input/output token usage at completion; live chunk counts are explicitly not token counts. Runpod does not separately expose image download, model download, and runtime load percentages through this API, so the UI leaves those stages uncertain. Failures include the last known stage, exception type, and upstream HTTP code without exposing credentials or raw provider response bodies.

Diagnostics remain in browser memory for the latest request and reset on navigation/reload. Conversation text and run status remain durable in Postgres. A local Stop is labelled as a cancellation request until history confirms the durable state.

## Verified behavior

The deployed path was tested on 2026-09-16 with Qwen3 4B on an RTX A5000:

- Scale from zero running workers: 90.4 seconds to first token, 91.4 seconds to finish a short answer.
- Warm follow-up: 0.73 seconds to first token, 1.1 seconds total.
- Completed replies survived browser reload; queued and active generation cancellation preserved the prompt without saving a partial assistant reply.
- Unauthenticated Fly requests and Runpod requests missing the separate worker service key returned 401.

These are individual measurements, not latency guarantees. The first host had to download and extract the container image before loading the model; that initial setup took several additional minutes. Runpod reported $0.00019/second for the A5000 worker, including running idle time. At that rate, a 90-second billed startup/generation plus a 60-second idle tail would cost about $0.029; provider scheduling delays can be unbilled. The preferred 16 GB tier starts at $0.00016/second. Check Runpod billing for actual charges.

## Acceptance and operations

Verify a real cold request streams, its completed response survives a page reload, a warm follow-up is faster, Stop cancels generation, unauthorized API and worker requests fail, and the worker count returns to zero after idle timeout. Measure startup and first-token latency on the chosen GPU rather than relying on generic cold-start claims.

GPU time is billed during startup, execution, and idle timeout. Storage is billed separately. No credit recharge or automatic budget purchase is configured by this application. An exhausted balance or provider quota makes generation unavailable. After a prolonged unused period, Runpod may disable an endpoint; restore its maximum workers to one when needed. Keep provider access keys and the personal chat key in a password manager.

The queue is in memory: restarts interrupt queued work. After an ambiguous disconnect, reload history before retrying because a reply may already have committed. Duplicate request IDs are rejected rather than replayed. Neon plan quotas apply to both storage and compute.

# Stream Chat

A personal React/Vite chat application with a Fly.io API, managed Postgres history, and one Runpod GPU worker. The backend streams server-sent events to the browser while receiving server-streaming gRPC from the worker. The worker calls vLLM on localhost.

## Boundaries

- The browser calls only the Fly HTTPS API. The access key stays in tab memory and is never included in the frontend build.
- Every conversation endpoint requires the personal access key. One key grants access to all conversations in this personal workspace.
- The gRPC worker requires a client certificate signed by this project's CA and the `chat-service` identity. The backend verifies the worker's `model-worker` certificate.
- Only TCP port 50051 is exposed on Runpod. vLLM binds to `127.0.0.1:8000`. Do not enable Jupyter or expose the runtime's HTTP port.
- One active generation and five waiting requests are permitted. Requests in the same conversation cannot overlap. No Redis or external scheduler is used.

## Layout

`frontend/` is the GitHub Pages app; `backend/` is the Fly API; `worker/` is the GPU container; `proto/` and `shared/` define the RPC contract. `tests/` checks queuing, cancellation, persistence boundaries, request limits, and real gRPC mutual TLS.

## Local development

Use Python 3.12, Node 24 and pnpm 11.19.0. Install the dependencies in a virtual environment with `pip install -r requirements-dev.txt`; run `pytest -q`. The Postgres integration test also requires `TEST_DATABASE_URL` pointing to a dedicated disposable database. Without it, that test is explicitly skipped.

In `frontend`, run `pnpm install --frozen-lockfile`, `pnpm test`, and `pnpm build`. `pnpm dev` uses `http://127.0.0.1:8080` unless `VITE_API_URL` is configured. The backend needs a real Postgres database and a reachable authenticated worker; it does not silently substitute a mock model.

Generate development or deployment certificates with `python scripts/create_certs.py /path/to/private-directory`. Keep all private files outside the repository. Leaf certificates last 90 days; rotate them before expiry. The CA private key is only needed for certificate issuance and must not be sent to either service.

## Deployment

1. Create a managed Postgres database. `DATABASE_URL` must be a **direct session connection**, not a transaction pooler: the API maintains a Postgres advisory lock to guarantee one queue owner. Use the provider's TLS connection parameters.
2. Create certificates. Inject only these base64-encoded PEM secrets into Fly: `TLS_CA_B64`, `TLS_CLIENT_CERT_B64`, and `TLS_CLIENT_KEY_B64`. Inject only `TLS_CA_B64`, `TLS_SERVER_CERT_B64`, and `TLS_SERVER_KEY_B64` into Runpod. Do not publish these values or bake them into container images.
3. Run the **Build GPU worker image** GitHub workflow. Use the resulting immutable commit-tagged image on a single GPU pod. A 16 GB GPU is the initial target for `Qwen/Qwen3-4B-Instruct-2507`, with an 8192-token context and one active generation. The base vLLM image uses CUDA 13.0.2; select a compatible host driver. The actual GPU deployment must still be validated.
4. Mount persistent storage at `/workspace`; the model download cache goes in `/workspace/huggingface`. Expose **50051/TCP** with direct public TCP mapping. Disable SSH and Jupyter. Set the TLS secrets above; default `MODEL_NAME` and `MAX_MODEL_LEN` are in `worker/start.sh`. Start the pod only after reviewing the quoted hourly and storage costs.
5. Record Runpod's assigned public IP and mapped TCP port as `WORKER_ADDRESS=IP:PORT`. The server certificate validates the service name independently of this changing transport address. Update it when Runpod changes the mapping.
6. On Fly, set `DATABASE_URL`, a random `CHAT_ACCESS_KEY` of at least 32 characters, `WORKER_ADDRESS`, and the three backend TLS secrets. `ALLOWED_ORIGINS` must be exactly your GitHub Pages origin, without a repository path. Deploy with `fly deploy --ha=false --strategy immediate`; keep exactly one Machine and one Uvicorn process. The checked-in config uses one shared CPU and 512 MB in `sjc`, with auto-stop disabled.
7. Configure repository variable `API_BASE_URL` to the Fly HTTPS origin. Set GitHub Pages source to **GitHub Actions** and run **Deploy frontend to GitHub Pages**. Vite uses relative asset paths so project Pages URLs work. Only the public API address enters the frontend bundle.
8. Verify a real prompt streams, its completed reply survives a page reload, Stop cancels generation, unauthenticated API calls return 401, and a client without its certificate cannot connect to the worker. A deployed site is not complete until this cloud path passes.

## Streaming and failure behavior

The user prompt is stored before generation. SSE events are `queued`, `started`, `token`, `done`, and `error`; heartbeat comments keep idle connections active. `done` is sent only after the complete assistant reply is committed. Failed or cancelled partial replies are not saved as completed assistant messages. Reload history after an ambiguous disconnect; the server may have committed just before it lost the browser connection. Duplicate request IDs are rejected, not automatically replayed.

Queued work is in memory and is interrupted by restarts. Startup marks abandoned runs interrupted; completed history remains in Postgres. Queue waits are limited to 60 seconds, worker calls to 180 seconds, and total generation work to 240 seconds. The frontend is a personal workspace, not a multi-user authentication system.

## Operations and costs

A persistent GPU bills while running, and attached storage may bill while stopped. No budget enforcement or automatic credit recharge is implemented. Keep the Runpod balance sufficient and stop the pod when it is not needed. The Fly API and database have their own recurring costs. Check current provider quotes before provisioning.

Rotate certificates before 90 days, keep a secure copy of the access key and CA, and use your database provider's backup controls. The GPU and database endpoints and secrets are deployment configuration; none are committed to this repository.

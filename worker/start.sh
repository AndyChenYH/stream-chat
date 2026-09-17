#!/usr/bin/env bash
set -euo pipefail
export HF_HOME="${HF_HOME:-/workspace/huggingface}"
mkdir -p "$HF_HOME"
python -m vllm.entrypoints.openai.api_server \
  --model "${MODEL_NAME:-Qwen/Qwen3-4B-Instruct-2507}" \
  --host 127.0.0.1 --port 8000 \
  --max-model-len "${MAX_MODEL_LEN:-8192}" \
  --max-num-seqs 1 --gpu-memory-utilization 0.85 &
runtime_pid=$!
PYTHONPATH=/opt/worker-libs:/app python -m worker.main &
worker_pid=$!
trap 'kill "$runtime_pid" "$worker_pid" 2>/dev/null || true; wait || true' EXIT
trap 'exit 0' TERM INT
# End the container if either service exits, so the platform can restart it.
wait -n "$runtime_pid" "$worker_pid"

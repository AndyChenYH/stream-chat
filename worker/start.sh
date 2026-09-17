#!/usr/bin/env bash
set -euo pipefail
export MODEL_NAME="${MODEL_NAME:-Qwen/Qwen3-4B-Instruct-2507}"
model_path=$(python3 -m worker.model_path)
python3 -m vllm.entrypoints.openai.api_server \
  --model "$model_path" --served-model-name "$MODEL_NAME" \
  --host 127.0.0.1 --port 8000 \
  --max-model-len "${MAX_MODEL_LEN:-8192}" \
  --max-num-seqs 1 --gpu-memory-utilization 0.85 --enforce-eager &
runtime_pid=$!
PYTHONPATH=/opt/worker-libs:/app python3 -m uvicorn worker.main:create_app \
  --factory --host 0.0.0.0 --port "${PORT:-80}" --workers 1 --no-access-log &
worker_pid=$!
trap 'kill "$runtime_pid" "$worker_pid" 2>/dev/null || true; wait || true' EXIT
trap 'exit 0' TERM INT
wait -n "$runtime_pid" "$worker_pid"

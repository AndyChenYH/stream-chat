ARG VLLM_IMAGE=vllm/vllm-openai:v0.26.0
FROM ${VLLM_IMAGE}
WORKDIR /app
COPY worker/requirements.txt /app/worker-requirements.txt
RUN pip install --no-cache-dir --target /opt/worker-libs -r worker-requirements.txt
COPY shared shared
COPY worker worker
RUN PYTHONPATH=/opt/worker-libs:/app python -c "from worker.main import create_app; from worker.model_path import resolve"
EXPOSE 80
ENTRYPOINT ["bash", "/app/worker/start.sh"]

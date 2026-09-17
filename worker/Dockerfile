ARG VLLM_IMAGE=vllm/vllm-openai:v0.26.0
FROM ${VLLM_IMAGE}
WORKDIR /app
COPY worker/requirements.txt /app/worker-requirements.txt
RUN pip install --no-cache-dir --target /opt/worker-libs -r worker-requirements.txt
COPY shared shared
COPY worker worker
EXPOSE 50051
ENTRYPOINT ["bash", "/app/worker/start.sh"]

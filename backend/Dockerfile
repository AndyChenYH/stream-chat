FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared shared
COPY agent_sdk agent_sdk
COPY backend backend
RUN useradd --uid 10001 --create-home app
USER app
EXPOSE 8080
CMD ["uvicorn", "backend.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8080", "--workers", "1", "--no-access-log", "--timeout-graceful-shutdown", "15"]

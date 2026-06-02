FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./
COPY static ./static

ENV CACHE_PROXY_URL=http://cache-proxy:8000 \
    DB_PATH=/data/viz.sqlite \
    POLL_INTERVAL_SECONDS=1.0 \
    LOG_LEVEL=INFO

EXPOSE 8000

# /health does no DB work, so a generous start-period only covers the one-time
# startup backfill/hydration scan on a large existing DB.
HEALTHCHECK --interval=10s --timeout=10s --start-period=120s --retries=3 \
    CMD curl -fsS http://localhost:8000/health || exit 1

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

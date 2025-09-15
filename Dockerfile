FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/app ./ingestion/app

RUN useradd -m appuser
USER appuser

CMD ["python", "-m", "ingestion.app.run_prices"]

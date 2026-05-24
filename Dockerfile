FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
COPY src ./src
COPY tests ./tests
COPY data ./data

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-m", "cli", "--input", "data", "--output", "outputs"]

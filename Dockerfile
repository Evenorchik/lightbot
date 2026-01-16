FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    CHROME_BINARY=/usr/bin/chromium

WORKDIR /app

# System deps for Chromium headless
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     chromium \
     chromium-driver \
     fonts-dejavu-core \
     ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN python -m pip install -U pip \
  && pip install -r requirements.txt

COPY . /app

# Create runtime dirs (mounted as volumes in compose)
RUN mkdir -p /app/logs /app/debug /app/tmp

CMD ["python", "main.py"]



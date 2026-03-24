FROM python:3.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    nano \
    vim \
    build-essential \
    gcc \
    python3-dev \
    libsodium-dev \
    && rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt /tmp/requirements.txt

RUN python -m pip install --upgrade pip \
    && pip install -r /tmp/requirements.txt \
    && pip install uWSGI==2.0.31

CMD ["uwsgi", "--ini", "/app/uwsgi.ini"]

FROM python:3.13-slim

ENV USER=sentinel
ENV PATH=/home/$USER/.local/bin:$PATH
RUN adduser --gecos "" --disabled-password $USER

COPY --from=ghcr.io/astral-sh/uv:0.6 /uv /bin/uv

RUN mkdir /app
WORKDIR /app

COPY . /app

ENV PATH=/home/$USER/.local/bin:$PATH

RUN uv sync --frozen --no-editable
ENV PATH="/app/.venv/bin:$PATH"

USER $USER

COPY CHECKS .
COPY Procfile .
CMD sanic sentinel.web:create_app --factory --port 5000 --host 0.0.0.0

FROM python:3.14-slim AS builder


COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

ENV UV_PYTHON=python3.14 \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app \
    UV_LINK_MODE=copy

WORKDIR /src
COPY uv.lock pyproject.toml ./
RUN uv sync \
    --locked \
    --no-dev \
    --no-install-project

COPY src /src/src
COPY pyproject.toml /src/pyproject.toml
COPY uv.lock /src/uv.lock
WORKDIR /src

RUN uv sync \
        --locked \
        --no-dev \
        --no-editable

FROM python:3.14-slim
COPY --from=builder /app /app

ENV USER=sentinel
RUN adduser --gecos "" --disabled-password $USER

WORKDIR /app

ENV PATH=/home/$USER/.local/bin:$PATH
ENV PATH="/app/bin:$PATH"

USER $USER
CMD ["uvicorn", "sentinel.web:create_app", "--factory", "--port", "8080", "--host", "0.0.0.0"]

FROM python:3.14-slim AS builder


COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

ENV UV_PYTHON=python3.14 \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app \
    UV_LINK_MODE=copy

RUN --mount=type=cache,target=/root/.cache \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync \
    --locked \
    --no-dev \
    --no-install-project

COPY . /src
WORKDIR /src

RUN --mount=type=cache,target=/root/.cache \
    uv sync \
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

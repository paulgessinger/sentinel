FROM python:3.10-slim

ENV USER=sentinel
RUN adduser --gecos "" --disabled-password $USER
USER $USER

COPY pyproject.toml .
COPY poetry.lock .
COPY src src

RUN pip install poetry uvicorn[standard] \
&& ~/.local/bin/poetry export -o requirements.txt \
&& pip install -r requirements.txt

COPY CHECKS .
COPY Procfile .
ENV PATH=/home/$USER/.local/bin:$PATH

COPY nginx.conf.sigil .

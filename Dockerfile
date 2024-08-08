FROM python:3.10-slim

ENV USER=sentinel
ENV PATH=/home/$USER/.local/bin:$PATH
RUN adduser --gecos "" --disabled-password $USER
USER $USER

COPY pyproject.toml .
COPY poetry.lock .
COPY src src


RUN pip install poetry poetry-plugin-export uvicorn[standard] \
&& poetry export -o /tmp/requirements.txt \
&& pip install -r /tmp/requirements.txt

COPY CHECKS .
COPY Procfile .

COPY nginx.conf.sigil .

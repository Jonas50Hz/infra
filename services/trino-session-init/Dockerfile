FROM python:3.12-slim AS build

WORKDIR /build

COPY services/trino-session-init/requirements.txt /build/requirements.txt
RUN pip install --no-cache-dir --prefix=/install -r /build/requirements.txt

COPY services/trino-session-init/src /build/src

FROM python:3.12-slim AS runtime

WORKDIR /app

COPY --from=build /install /usr/local
COPY --from=build /build/src /app

ENV PYTHONPATH=/app

FROM runtime AS test

COPY services/trino-session-init/tests /app/tests
RUN python -m unittest discover -s /app/tests -v

FROM runtime

CMD ["python", "-m", "trino_session_init.main"]
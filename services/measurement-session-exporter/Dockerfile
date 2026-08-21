FROM python:3.12-slim AS build

WORKDIR /build

COPY services/measurement-session-exporter/requirements.txt /build/requirements.txt
RUN pip install --no-cache-dir --prefix=/install -r /build/requirements.txt

COPY services/measurement-session-exporter/src /build/src

FROM python:3.12-slim AS runtime

WORKDIR /app

COPY --from=build /install /usr/local
COPY --from=build /build/src /app

ENV PYTHONPATH=/app

FROM runtime AS test

COPY services/measurement-session-exporter/tests /app/tests
RUN python -m unittest discover -s /app/tests -v

FROM runtime

CMD ["uvicorn", "measurement_session_exporter.main:app", "--host", "0.0.0.0", "--port", "8080"]
FROM python:3.12-slim AS build

WORKDIR /build

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        libprotobuf-dev \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY services/measurement-session-api/requirements.txt /build/requirements.txt
RUN pip install --no-cache-dir --prefix=/install -r /build/requirements.txt

COPY docs/wama/schema/measurement_session.proto /build/proto/measurement_session.proto
COPY services/measurement-session-common/src /build/common
COPY services/measurement-session-api/src /build/src
RUN protoc \
    --proto_path=/build/proto \
    --proto_path=/usr/include \
    --python_out=/build/common/measurement_session_common/generated \
    /build/proto/measurement_session.proto

FROM python:3.12-slim AS runtime

WORKDIR /app

COPY --from=build /install /usr/local
COPY --from=build /build/common /app
COPY --from=build /build/src /app
COPY services/measurement-session-api/tests /app/tests

ENV PYTHONPATH=/app

FROM runtime AS test

RUN python -m unittest discover -s /app/tests -v

FROM runtime

CMD ["uvicorn", "measurement_session_api.main:app", "--host", "0.0.0.0", "--port", "8080"]
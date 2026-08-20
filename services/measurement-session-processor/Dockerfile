FROM python:3.12-slim AS build

WORKDIR /build

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        libprotobuf-dev \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY services/measurement-session-processor/requirements.txt /build/requirements.txt
RUN pip install --no-cache-dir --prefix=/install -r /build/requirements.txt

COPY docs/wama/schema/measurement_session.proto /build/proto/measurement_session.proto
COPY docs/wama/schema/blobmeta.proto /build/proto/blobmeta.proto
COPY services/measurement-session-common/src /build/common
COPY services/measurement-session-processor/src /build/src
RUN protoc \
    --proto_path=/build/proto \
    --proto_path=/usr/include \
    --python_out=/build/common/measurement_session_common/generated \
    /build/proto/measurement_session.proto \
    /build/proto/blobmeta.proto

FROM python:3.12-slim AS runtime

WORKDIR /app

COPY --from=build /install /usr/local
COPY --from=build /build/common /app
COPY --from=build /build/src /app

ENV PYTHONPATH=/app:/app/measurement_session_common/generated

FROM runtime AS test

COPY services/measurement-session-common/tests /app/common-tests
COPY services/measurement-session-processor/tests /app/tests
RUN python -m unittest discover -s /app/common-tests -v \
    && python -m unittest discover -s /app/tests -v

FROM runtime

CMD ["python", "-m", "measurement_session_processor.main"]
FROM python:3.12-slim AS build

WORKDIR /build

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        libprotobuf-dev \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY services/pmu-gateway/requirements.txt /build/requirements.txt
RUN pip install --no-cache-dir --prefix=/install -r /build/requirements.txt

COPY docs/wama/schema/rtd_schema.proto /build/proto/rtd_schema.proto
COPY services/pmu-gateway/src /build/src
RUN protoc \
    --proto_path=/build/proto \
    --proto_path=/usr/include \
    --python_out=/build/src/pmu_gateway/generated \
    /build/proto/rtd_schema.proto

FROM python:3.12-slim

WORKDIR /app

COPY --from=build /install /usr/local
COPY --from=build /build/src /app
COPY services/pmu-gateway/tests /app/tests

ENV PYTHONPATH=/app

CMD ["python", "-m", "pmu_gateway.main"]
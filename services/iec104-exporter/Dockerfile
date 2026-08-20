FROM python:3.12-slim AS build

WORKDIR /build

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        libprotobuf-dev \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY services/iec104-exporter/requirements.txt /build/requirements.txt
RUN pip install --no-cache-dir --prefix=/install -r /build/requirements.txt

COPY docs/wama/schema/iec104_export.proto /build/proto/iec104_export.proto
COPY services/iec104-common/src /build/common
COPY services/iec104-exporter/src /build/src
RUN protoc \
    --proto_path=/build/proto \
    --proto_path=/usr/include \
    --python_out=/build/common/iec104_common/generated \
    /build/proto/iec104_export.proto

FROM python:3.12-slim AS runtime

WORKDIR /app

COPY --from=build /install /usr/local
COPY --from=build /build/common /app
COPY --from=build /build/src /app
COPY services/iec104-common/tests /app/common-tests
COPY services/iec104-exporter/tests /app/tests

ENV PYTHONPATH=/app

FROM runtime AS test

RUN python -m unittest discover -s /app/common-tests -v \
    && python -m unittest discover -s /app/tests -v

FROM runtime

CMD ["python", "-m", "iec104_exporter.main"]
FROM python:3.12-slim AS build

WORKDIR /build

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        libprotobuf-dev \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY processors/processor-apparent-power/requirements.txt /build/requirements.txt
RUN pip install --no-cache-dir --prefix=/install -r /build/requirements.txt

COPY contracts/rtd_schema.proto /build/proto/rtd_schema.proto
COPY processors/processor-apparent-power/src /build/src
RUN protoc \
    --proto_path=/build/proto \
    --proto_path=/usr/include \
    --python_out=/build/src/processor_apparent_power/generated \
    /build/proto/rtd_schema.proto

FROM python:3.12-slim AS runtime

WORKDIR /app

COPY --from=build /install /usr/local
COPY --from=build /build/src /app
COPY processors/processor-apparent-power/tests /app/tests

ENV PYTHONPATH=/app

FROM runtime AS test

RUN python -m unittest discover -s /app/tests -v

FROM runtime

CMD ["python", "-m", "processor_apparent_power.main"]
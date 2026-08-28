FROM python:3.12-slim AS build

WORKDIR /build

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        libprotobuf-dev \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /build/requirements.txt
RUN pip install --no-cache-dir --prefix=/install -r /build/requirements.txt

COPY contracts/rtd_schema.proto /build/proto/rtd_schema.proto
COPY contracts/measurement_session.proto /build/proto/measurement_session.proto
COPY src /build/src
RUN protoc \
    --proto_path=/build/proto \
    --proto_path=/usr/include \
    --python_out=/build/src/processor_frequency_measurement_session/generated \
    /build/proto/rtd_schema.proto \
    /build/proto/measurement_session.proto

FROM python:3.12-slim AS runtime

WORKDIR /app

COPY --from=build /install /usr/local
COPY --from=build /build/src /app
COPY scripts /app/scripts
COPY tests /app/tests
COPY tooling-tests /app/tooling-tests

ENV PYTHONPATH=/app

FROM runtime AS test

RUN apt-get update \
    && apt-get install --yes --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN python -m unittest discover -s /app/tooling-tests -v \
    && python -m unittest discover -s /app/tests -v

FROM runtime

CMD ["python", "-m", "processor_frequency_measurement_session.main"]
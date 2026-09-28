FROM python:3.12-slim AS build

WORKDIR /build

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        libprotobuf-dev \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /build/requirements.txt
RUN pip install --no-cache-dir --prefix=/install -r /build/requirements.txt

COPY contracts/masterdata.proto contracts/rtd_schema.proto /build/proto/
COPY src /build/src
RUN protoc \
    --proto_path=/build/proto \
    --proto_path=/usr/include \
    --python_out=/build/src/gateway_c37_118_onboarding/generated \
    /build/proto/masterdata.proto \
    /build/proto/rtd_schema.proto

FROM python:3.12-slim AS runtime

WORKDIR /app

COPY --from=build /install /usr/local
COPY --from=build /build/src /app
COPY catalog /app/catalog
COPY scripts /app/scripts

ENV PYTHONPATH=/app

FROM runtime AS test

RUN apt-get update \
    && apt-get install --yes --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY tests /app/tests
COPY tooling-tests /app/tooling-tests
RUN python -m unittest discover -s /app/tooling-tests -v \
    && python -m unittest discover -s /app/tests -v

FROM runtime

CMD ["python", "-m", "gateway_c37_118_onboarding.main"]
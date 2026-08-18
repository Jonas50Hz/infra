FROM debian:bookworm-slim AS descriptor

WORKDIR /build

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        libprotobuf-dev \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY docs/wama/schema/rtd_schema.proto /build/proto/rtd_schema.proto
RUN protoc \
    --proto_path=/build/proto \
    --proto_path=/usr/include \
    --include_imports \
    --descriptor_set_out=/build/rtd_schema.desc \
    /build/proto/rtd_schema.proto

FROM descriptor AS test

RUN protoc \
    --decode=google.protobuf.FileDescriptorSet \
    --proto_path=/usr/include \
    google/protobuf/descriptor.proto \
    < /build/rtd_schema.desc > /build/rtd_schema.txt \
    && grep -F 'package: "rtd_schema.v1"' /build/rtd_schema.txt \
    && grep -F 'name: "MCCSMeasurementValue"' /build/rtd_schema.txt

FROM apache/druid:37.0.0

COPY --from=descriptor --chown=druid:druid /build/rtd_schema.desc /opt/wama/rtd_schema.desc
COPY --chown=druid:druid services/druid/config /opt/wama/config
COPY --chown=druid:druid --chmod=755 services/druid/start-druid.sh /opt/wama/start-druid.sh
COPY --chown=druid:druid --chmod=755 services/druid/healthcheck-druid.sh /opt/wama/healthcheck-druid.sh

USER druid

ENTRYPOINT ["/bin/bash", "/opt/wama/start-druid.sh"]
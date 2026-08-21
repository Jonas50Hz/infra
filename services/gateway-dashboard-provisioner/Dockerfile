FROM python:3.12-slim AS build

WORKDIR /build

RUN apt-get update \
	&& apt-get install --yes --no-install-recommends \
		libprotobuf-dev \
		protobuf-compiler \
	&& rm -rf /var/lib/apt/lists/*

COPY services/gateway-dashboard-provisioner/requirements.txt /build/requirements.txt
RUN pip install --no-cache-dir --prefix=/install -r /build/requirements.txt

COPY docs/wama/schema/masterdata.proto /build/proto/masterdata.proto
COPY services/gateway-dashboard-provisioner/src /build/src
RUN protoc \
	--proto_path=/build/proto \
	--proto_path=/usr/include \
	--python_out=/build/src/gateway_dashboard_provisioner/generated \
	/build/proto/masterdata.proto

FROM python:3.12-slim AS runtime

WORKDIR /app

COPY --from=build /install /usr/local
COPY --from=build /build/src /app

ENV PYTHONPATH=/app

FROM runtime AS test

COPY services/gateway-dashboard-provisioner/tests /app/tests
RUN python -m unittest discover -s /app/tests -v

FROM runtime

CMD ["python", "-m", "gateway_dashboard_provisioner.main"]
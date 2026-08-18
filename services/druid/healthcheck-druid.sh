#!/bin/bash

set -euo pipefail

for endpoint in \
  http://localhost:8081/status/health \
  http://localhost:8082/status/health \
  http://localhost:8083/status/health \
  http://localhost:8888/status/health \
  http://localhost:8888/druid/indexer/v1/supervisor; do
  /busybox/busybox wget -q -O /dev/null "$endpoint"
done
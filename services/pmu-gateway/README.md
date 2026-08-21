# Deprecated Fake PMU Gateway

This reference fixture is excluded from the root Compose stack. Use an explicit
Compose override only when a local producer is needed for data-dependent tests.

`pmu-gateway` is a configurable stand-in for one PMU source. It reads its YAML
fixture once at container startup, then repeatedly publishes the configured
measurements to Kafka topic `LiveMeasurement` as raw
`rtd_schema.v1.MCCSMeasurementValue` Protobuf records.

It does not implement a PMU protocol. A PMU sample is represented by multiple
scalar Common Format measurements, such as phase voltages, phase currents,
frequency, and ROCOF.

## Startup fixture

The default fixture is [messages.yaml](messages.yaml). It has one positive
`publish_interval_ms` and a non-empty `messages` list:

```yaml
publish_interval_ms: 1000
messages:
  - mrid: urn:wama:poc:pmu:bay-01:frequency
    value:
      double_value: 50.01
    value_jitter: 0.01
    quality:
      valid: true
    field_timestamp_offset_ms: 20
```

Each message requires a non-empty `mrid` and a `value` mapping containing
exactly one of `double_value`, `int_value`, `uint_value`, `bool_value`,
`string_value`, or `timestamp_value`. `timestamp_value` must be an RFC 3339
timestamp with a timezone. `quality` may contain only `valid`, `substituted`,
`operator_blocked`, `overflow`, and `old_data`, each as a boolean.

`field_timestamp_offset_ms` is optional and must be a non-negative integer.
For every publish cycle, the gateway derives one UTC millisecond timestamp,
uses it for Kafka's record timestamp plus `timestamp_gateway` and
`timestamp_mccs`, then derives `timestamp_field` by subtracting the optional
offset. The resulting timestamp ordering follows the Common Format contract.

`value_jitter` is optional, finite, and non-negative. It is supported only for
`double_value`; when greater than zero, each publish cycle samples a new value
uniformly from the configured nominal value plus or minus the amplitude. Omit
it or set it to `0` to retain a fixed value. The default PMU fixture uses small
per-signal jitter for voltage, current, frequency, and ROCOF; custom fixtures
remain fixed unless they opt individual values in.

The fixture is intentionally not watched. Recreate or restart the service
after changing it:

```sh
docker compose up -d --force-recreate pmu-gateway
```

To start with a different fixture, use an absolute host path so Compose's
included-file path handling is unambiguous:

```sh
PMU_GATEWAY_CONFIG_SOURCE="$PWD/my-pmu-messages.yaml" \
  docker compose up -d --force-recreate pmu-gateway
```

Set `PMU_GATEWAY_PUBLISH_INTERVAL_MS` to override the file interval for one
startup. The service also accepts `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, and
`PMU_GATEWAY_CONFIG_PATH`; Compose supplies suitable defaults for this PoC.

## Image and operation

The Docker build compiles
[`docs/wama/schema/rtd_schema.proto`](../../docs/wama/schema/rtd_schema.proto)
into Python bindings. The application calls `SerializeToString()` directly; it
does not use JSON or a schema registry. Records are keyed by MRID and sent in
fixture order. Startup configuration errors are logged and terminate the
container; Compose restarts it according to the service policy.
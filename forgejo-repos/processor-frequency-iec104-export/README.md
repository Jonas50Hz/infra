# Processor Frequency IEC 104 Export

This standalone Forgejo repository owns only the
`processor-frequency-iec104-export` service. It performs the direct PoC slice:
it consumes the configured fake-PMU frequency from `LiveMeasurement` and writes
a raw-Protobuf `wama.iec104.v1.ExportRecord` to `Export`.

## Authoring Contract

[processor.yaml](processor.yaml) declares this as a `custom` processor. Its
input signal and typed `ExportRecord` output contract are pinned to reviewed
catalog and approval revisions. The custom mode is intentional: an `Export`
record is not a derived `LiveMeasurement` value, so the standard formula and
latest-values adapters cannot represent it safely. Keep pipeline changes in the
existing focused tests and do not replace its typed-output behavior with a
standard mode.

The output contains one `M_ME_NC_1` information object with defaults CA `1`, IOA
`1001`, and spontaneous COT `3`. These values are configuration, not hidden
mapping logic:

| Setting | Default |
| --- | --- |
| `FREQUENCY_IEC104_SOURCE_MRID` | `urn:wama:poc:pmu:bay-01:frequency` |
| `FREQUENCY_IEC104_COMMON_ADDRESS` | `1` |
| `FREQUENCY_IEC104_INFORMATION_OBJECT_ADDRESS` | `1001` |
| `FREQUENCY_IEC104_CAUSE_CODE` | `3` |

Only a matching Kafka key, finite `double_value`, and explicit
`quality.valid=true` produce an export. Source `substituted`, `operator_blocked`,
`overflow`, and `old_data` flags map to IEC quality fields. The output key is a
deterministic UUIDv5 `export_id`; its Kafka timestamp and `created_at` both
preserve the triggering input Kafka timestamp.

This is deliberately **not** the full LFR per-second preferred-frequency
algorithm described in the parent infrastructure documentation. It does not
aggregate PMUs, select a preferred frequency, handle voltage/status evidence,
produce RoCoF, persist audit evidence, or implement a heartbeat.

Run tests from this repository root:

```sh
docker build --target test -f Dockerfile .
```

## Delivery

A pull request validates this repository only. A trusted push to `main` tests,
publishes, and deploys only this processor image to its dedicated
`/var/lib/wama-processor-frequency-iec104-export` deployment root on external
`wama-infra`. It never deploys or modifies the root-owned IEC exporter, browser,
Kafka broker, or infrastructure Compose project.
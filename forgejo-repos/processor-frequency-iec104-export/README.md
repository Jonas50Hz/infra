# Processor Frequency IEC 104 Export

This standalone Forgejo repository owns only the
`processor-frequency-iec104-export` service. It performs the direct PoC slice:
it consumes configured reviewed C37.118 gateway frequencies from
`LiveMeasurement` and writes raw-Protobuf `wama.iec104.v1.ExportRecord` values
to `Export`.

[`config/frequency-iec104-export.yaml`](config/frequency-iec104-export.yaml)
is the processor-owned reviewed mapping. Every current entry emits one
`M_ME_NC_1` information object with spontaneous COT `3`:

| Input MRID | CA | IOA | COT |
| --- | --- | --- | --- |
| `urn:wama:poc:pmu:bay-01:frequency` | `1001` | `1001` | `3` |
| `urn:wama:poc:pmu:bay-02:frequency` | `1002` | `1001` | `3` |
| `urn:wama:poc:pmu:bay-03:frequency` | `1003` | `1001` | `3` |
| `urn:wama:poc:pmu:bay-04:frequency` | `1004` | `1001` | `3` |
| `urn:wama:poc:pmu:bay-05:frequency` | `1005` | `1001` | `3` |

The current CA values deliberately match the reviewed source PMU IDCODEs, but
they are explicit mapping data and are never inferred at runtime. A newly
onboarded gateway cannot emit IEC 104 data until a reviewed mapping entry is
added. `FREQUENCY_IEC104_CONFIG_PATH` selects the map and defaults to
`/etc/wama/frequency-iec104-export.yaml`. The former
`FREQUENCY_IEC104_SOURCE_MRID`, `FREQUENCY_IEC104_COMMON_ADDRESS`,
`FREQUENCY_IEC104_INFORMATION_OBJECT_ADDRESS`, and
`FREQUENCY_IEC104_CAUSE_CODE` variables are rejected at startup.

Only a mapped MRID with a matching Kafka key, finite `double_value`, and
explicit `quality.valid=true` produces an export. Source `substituted`,
`operator_blocked`, `overflow`, and `old_data` flags map to IEC quality fields.
The output key is a deterministic UUIDv5 `export_id`; its Kafka timestamp and
`created_at` both preserve the triggering input Kafka timestamp.

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
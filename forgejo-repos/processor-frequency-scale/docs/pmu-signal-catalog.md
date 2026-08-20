# Frequency Signal Catalog

Use this signal for the local frequency-scale processor. The authoritative fake
PMU fixture remains the root infrastructure file
`services/pmu-gateway/messages.yaml`; this small copy keeps routine processor
work self-contained. Update both intentionally when the fixture changes.

| Python name | MRID | Value type | Unit | Nominal value |
| --- | --- | --- | --- | --- |
| `frequency_hz` | `urn:wama:poc:pmu:bay-01:frequency` | `double_value` | Hz | 50.01 |

The fake gateway publishes the source once per second with `quality.valid=true`.
Use a new MRID for every derived output. This repository publishes
`urn:wama:poc:pmu:bay-01:frequency-millihertz` in millihertz.

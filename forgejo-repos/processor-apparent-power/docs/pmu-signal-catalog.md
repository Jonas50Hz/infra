# Apparent-Power Signal Catalog

Use these phase voltage and current signals for the local apparent-power
processor. The authoritative fake PMU fixture remains the root infrastructure
file `services/pmu-gateway/messages.yaml`; this copy lets an EE work from this
repository without searching the infrastructure checkout. Update both
intentionally when the fixture changes.

| Python name | MRID | Value type | Unit | Nominal value |
| --- | --- | --- | --- | --- |
| `voltage_l1` | `urn:wama:poc:pmu:bay-01:voltage-l1` | `double_value` | V | 230.4 |
| `voltage_l2` | `urn:wama:poc:pmu:bay-01:voltage-l2` | `double_value` | V | 229.8 |
| `voltage_l3` | `urn:wama:poc:pmu:bay-01:voltage-l3` | `double_value` | V | 230.1 |
| `current_l1` | `urn:wama:poc:pmu:bay-01:current-l1` | `double_value` | A | 318.2 |
| `current_l2` | `urn:wama:poc:pmu:bay-01:current-l2` | `double_value` | A | 316.7 |
| `current_l3` | `urn:wama:poc:pmu:bay-01:current-l3` | `double_value` | A | 317.4 |

The fake gateway publishes every source once per second with `quality.valid=true`.
Use a new MRID for every derived output. This repository publishes
`apparent-power-l1`, `apparent-power-l2`, and `apparent-power-l3` in VA.

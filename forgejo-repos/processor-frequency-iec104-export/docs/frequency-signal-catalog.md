# Direct Frequency Export Signal

This repository directly maps the existing fake-PMU frequency to one configured
IEC 60870-5-104 short-float export request. It is not the LFR preferred-frequency
selection algorithm.

| Input | MRID | Type | Unit |
| --- | --- | --- | --- |
| `frequency_hz` | `urn:wama:poc:pmu:bay-01:frequency` | `double_value` | Hz |

The input must have the matching Kafka key, a finite numeric value, and an
explicit `quality.valid=true`. The IEC common address, information-object
address, and COT are configured through the Compose environment.
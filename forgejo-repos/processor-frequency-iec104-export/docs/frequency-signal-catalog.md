# Direct Frequency Export Signal

This repository directly maps reviewed C37.118 gateway frequency signals to IEC
60870-5-104 short-float export requests. It is not the LFR preferred-frequency
selection algorithm.

| Input | MRID | Type | Unit | CA | IOA | COT |
| --- | --- | --- | --- | --- | --- | --- |
| `frequency_hz_bay_01` | `urn:wama:poc:pmu:bay-01:frequency` | `double_value` | Hz | `1001` | `1001` | `3` |
| `frequency_hz_bay_02` | `urn:wama:poc:pmu:bay-02:frequency` | `double_value` | Hz | `1002` | `1001` | `3` |
| `frequency_hz_bay_03` | `urn:wama:poc:pmu:bay-03:frequency` | `double_value` | Hz | `1003` | `1001` | `3` |
| `frequency_hz_bay_04` | `urn:wama:poc:pmu:bay-04:frequency` | `double_value` | Hz | `1004` | `1001` | `3` |
| `frequency_hz_bay_05` | `urn:wama:poc:pmu:bay-05:frequency` | `double_value` | Hz | `1005` | `1001` | `3` |

The exact mapping is versioned in
[`../config/frequency-iec104-export.yaml`](../config/frequency-iec104-export.yaml).
Its address values are reviewed configuration, not derivations from gateway
identities. The input must have the matching Kafka key, a finite numeric value,
and explicit `quality.valid=true`. Unmapped MRIDs produce no export.
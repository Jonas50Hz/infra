# Processor Apparent Power

`processor-apparent-power` derives per-phase apparent power on
`LiveMeasurement` as raw `MCCSMeasurementValue` Protobuf records. It accepts
only the exact fake PMU voltage/current source key and MRID pairs:

- `urn:wama:poc:pmu:bay-01:voltage-l1` + `...:current-l1`
- `urn:wama:poc:pmu:bay-01:voltage-l2` + `...:current-l2`
- `urn:wama:poc:pmu:bay-01:voltage-l3` + `...:current-l3`

For each phase, it emits `...:apparent-power-l1`, `...:apparent-power-l2`, or
`...:apparent-power-l3` with $S = U \times I$ in VA. It does not claim active
power because the fixture has no phase angle or power factor.

The service keeps the latest explicitly `quality.valid=true` voltage and
current in an in-process cache. It emits only when a complete valid phase pair
exists, rejects invalid/incomplete/non-source records and its own output, and
uses the triggering input's Common Format context and Kafka timestamp. The
single-partition PoC source republishes all phase values every second, so the
cache repopulates after a restart; replay can produce deterministic duplicate
derived records under Kafka at-least-once delivery.

It connects only to the external `wama-infra` network and has no cross-project
`depends_on` entries. Run its test target from the processors repository root:

```sh
docker build --target test -f processors/processor-apparent-power/Dockerfile .
```
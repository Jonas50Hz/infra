# Processor Frequency Scale

`processor-frequency-scale` converts only the fake PMU source
`urn:wama:poc:pmu:bay-01:frequency` from hertz to millihertz. It consumes and
publishes raw `MCCSMeasurementValue` records on `LiveMeasurement`, emitting
`urn:wama:poc:pmu:bay-01:frequency-millihertz` with the source value multiplied
by `1000`.

The processor accepts only an exact source Kafka key, source MRID, and
`double_value`. It rejects all other records, including its own output, so
replayed source records produce deterministic duplicate events without a
feedback loop. The derived record copies Common Format timestamps and quality,
and it retains the source Kafka timestamp explicitly through Quixstreams.

It connects only to the external `wama-infra` network and has no cross-project
`depends_on` entries. Run its tests from the processors repository root:

```sh
docker build --target test -f processors/processor-frequency-scale/Dockerfile .
```
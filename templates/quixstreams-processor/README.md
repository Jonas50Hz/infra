# Quixstreams processor template

This inactive scaffold is the source for each `services/processor-*` service.
It consumes raw `rtd_schema.v1.MCCSMeasurementValue` Protobuf records from Kafka
with Quixstreams and publishes raw Protobuf only when the developer changes
[`src/processor_template/pipeline.py`](src/processor_template/pipeline.py).

The starter `transform()` deliberately returns `None`. It is therefore safe to
build and test without publishing output. Replace it with a pure function that
returns either one derived `MCCSMeasurementValue` or `None` for records that
should not be emitted.

`processor.yaml` holds the Kafka endpoint, consumer group, and topic names.
Give every provisioned processor a distinct consumer group. Quixstreams uses
at-least-once delivery, so transformations must be idempotent. If input and
output are both `LiveMeasurement`, filter generated records so the processor
cannot consume its own output. `to_topic()` preserves Kafka timestamps and
headers by default; copy or deliberately set the message-level Common Format
timestamps in custom code. Keep raw and waveform data off Kafka.

Run the template test target from the repository root:

```sh
docker build --target test -f templates/quixstreams-processor/Dockerfile .
```

Provisioned services replace `processor-template`, `processor_template`, and
`templates/quixstreams-processor` with their service-specific names. The root
Compose file must not include this template directly.
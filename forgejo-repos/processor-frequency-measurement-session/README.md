# Processor Frequency Measurement Session

This standalone Forgejo repository owns only the
`processor-frequency-measurement-session` service. It consumes raw-Protobuf
`rtd_schema.v1.MCCSMeasurementValue` records from `LiveMeasurement` and emits
raw-Protobuf `wama.measurement_session.v1.MeasurementSessionRequest` records to
the existing `MeasurementSession` topic.

## EE Policy

Edit only
[policy.py](src/processor_frequency_measurement_session/policy.py) to add,
remove, or revise reviewed source mappings. It declares each approved frequency
MRID and the exact eight sorted MRIDs included in that source's session. The
initial policy is the reviewed `pmu-bay-01` through `pmu-bay-05` catalog. It
does not read YAML, consume `Masterdata`, or infer MRIDs from a prefix.

Update [test_policy.py](tests/test_policy.py) when changing reviewed mappings.
Update [test_capture.py](tests/test_capture.py) for capture behavior changes;
[test_pipeline.py](tests/test_pipeline.py) covers the raw-Protobuf topic
wiring and output event timestamp.

A record qualifies only when its decoded Kafka key exactly equals its configured
frequency MRID, it has a finite `double_value`, `quality.valid=true` without a
true substituted, operator-blocked, overflow, or old-data flag, and has a newer
`timestamp_mccs`. That timestamp is the event time.

For each frequency MRID, a capture starts only above $50.2$ Hz and closes on
the first qualifying value at or below $50.2$ Hz. The emitted interval is
$[onset - 10 seconds, clearance + 10 seconds)$. Its canonical UUIDv5,
raw-Protobuf payload, key, and request timestamp are deterministic from the
event-time episode; the Kafka record timestamp is `requested_at` rounded down
to milliseconds. Metadata is fixed to `request_origin` equal to this service
name and `capture_reason=frequency_gt_50_2_hz`.

This is a best-effort low-rate PoC. The transform synchronously waits ten
seconds after a normal clearance before it emits a request. A re-entry received
after that wait begins a separate episode; no delay-window merge occurs. Open
episodes are in-memory and are discarded on restart, so a restart never emits a
truncated session. A padded interval over 24 hours is discarded, logged at
error level as structured JSON, and increments the in-process
`over_limit_dropped_total` counter; it is not split or capped.

Run the seed tests from this repository root:

```sh
docker build --target test -f Dockerfile .
```

## Delivery

A pull request validates this repository only. A trusted push to `main` tests,
publishes, and deploys only this processor image to its dedicated
`/var/lib/wama-processor-frequency-measurement-session` deployment root on the
external `wama-infra` network. It never deploys or modifies the root
infrastructure Compose project, Kafka, or any gateway.
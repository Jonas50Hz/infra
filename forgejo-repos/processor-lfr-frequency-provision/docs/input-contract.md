# LFR PMU Input Contract

The processor consumes configured raw-Protobuf `MCCSMeasurementValue` records
from `LiveMeasurement`. It does not own source protocol decoding, C37.118
transport, raw frames, the root `pmu-gateway`, or Master Data.

## Required Source Values

For each configured PMU, the upstream converter must publish:

| Signal | Required Common Format fields |
| --- | --- |
| Frequency | configured MRID, finite absolute-Hz `double_value`, `timestamp_field`, and quality evidence |
| Voltage | configured MRID, finite configured-unit `double_value`, `timestamp_field`, and quality evidence |

`timestamp_field` assigns a record to `floor(timestamp_field)` UTC. The
processor rejects missing timestamps, records more than the configured future
window ahead of receipt, and records for seconds that have already closed.

## Current Provisional Quality Mapping

The checked-in configuration requires:

```yaml
status_evidence:
  mode: generic_quality_provisional
```

For this temporary mode, a source record is usable only when
`quality.valid=true` is explicitly present and none of `substituted`,
`operator_blocked`, `overflow`, or `old_data` is true. Missing `quality.valid`
becomes `missing_status`; a false value and asserted generic flags become
recorded rejection reasons.

This is deliberately **not** a complete C37.118 status mapping. The current
Common Format Quality block cannot establish every LFR-required condition such
as test mode, synchronization, data-modified state, and source time quality.
It must not be interpreted as proof that generic `quality.valid` alone meets
the completed LFR input contract.

## Gate Before Production Configuration

Inspect one actual C37.118 data-frame decode or the source converter mapping
before using this processor for a real PMU. Define how the per-sample `STAT`
and time-quality evidence maps to all required normalized conditions. If the
source cannot convey those conditions through existing Quality fields, extend
the canonical Common Format with a typed optional normalized PMU-status block
and deliberately regenerate every consumer contract copy.

C37.118 command error-response frames are control-plane diagnostics; they are
not per-sample measurement status evidence and cannot satisfy this contract.
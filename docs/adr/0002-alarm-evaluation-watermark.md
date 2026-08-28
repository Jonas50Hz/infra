# Separate alarm evaluation watermark

**Status:** accepted

`AlarmEvaluationWatermark` is a root-owned compacted current-state topic keyed
byte-for-byte like `Alarm`. It preserves the latest qualifying evaluation time
per `(rule_id, mrid)` after active-only `Alarm` tombstones compact away, while
`Alarm` remains desired active state. The watermark is not an audit, episode,
notification, or acknowledgement ledger.
# Compacted Alarm desired state

**Status:** accepted

`Alarm` is a root-owned compacted desired-active-state topic rather than an
append-only lifecycle or audit stream. A non-null raw-Protobuf
`AlarmDesiredState` describes one active `(rule_id, exact_mrid)` identity and a
same-key tombstone clears it, so replaying the compacted topic reconstructs the
current active set. `episode_id` preserves correlation through evidence and rule
revision refreshes, while acknowledgement, notification, and historical audit
records remain outside this contract and can be modelled on separate interfaces.
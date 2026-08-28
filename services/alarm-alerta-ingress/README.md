# Alarm Alerta ingress

`alarm-alerta-ingress` is the root-owned raw-Protobuf consumer for compacted
`Alarm` desired state. Its image carries a local generated copy of the canonical
`alarm.proto` contract without editing the root schema.

On every start it manually assigns every `Alarm` partition, captures end
offsets, folds active records and tombstones through that snapshot, reconciles
only tagged and attributed WAMA-managed open/ack Alerta alerts, then tails
idempotently. It never uses consumer-group offsets as restart state. Its health
file is created only after both Kafka snapshot and remote reconciliation finish.

Each active record maps to resource `MRID`, event
`wama-alarm/<episode_id>`, environment `WAMA`, customer `wama`, and fixed native
Alerta severity `indeterminate`. WAMA `WARNING` or `CRITICAL` remains explicit
in `[WAMA ...]` text and attributes. Keeping native severity fixed lets Alerta
preserve an operator acknowledgement across evidence and rule-revision refreshes.
Tombstones close only matching ingress-owned active/ack alerts through Alerta's
native status endpoint; no alert is deleted, and foreign or closed historic
alerts are not touched.

The ingress sends the root Alerta service's fixed trusted-PoC key only on its
internal API requests. That gives the required `customer=wama` context while the
loopback UI remains unauthenticated.

The ingress has no VictoriaMetrics, Druid, Grafana, Trino, SeaweedFS,
PostgreSQL, or Forgejo access.
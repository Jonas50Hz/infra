(exp_wama_documentation)=

```{meta}
:description: WAMA proof-of-concept documentation organized as explanations and technical reference.
```

# WAMA documentation

This documentation describes the WAMA Docker Compose proof of concept. The
[WAMA source assets](wama/) remain the canonical locations for schemas, BPMN
models, the approved C37.118 standard copy, and the architecture image.

## Einstieg

- [WAMA PoC: Einstieg in zehn Minuten](how-to/wama-einstieg.md)

## Explanation

- [WAMA platform overview and processes](explanation/wama-platform-overview.md)
- [WAMA architecture and technology choices](explanation/wama-architecture.md)
- [WAMA PoC Docker Compose plan](explanation/wama-poc-compose-plan.md)
- [WAMA processor authoring experience](explanation/wama-processor-authoring.md)
- [Large-scale time-series storage research](explanation/large-scale-time-series-storage-research.md)
- [Diode-first live-data architecture research](explanation/diode-first-live-data-architecture-research.md)
- [Gateway-to-platform telemetry transport research](explanation/gateway-to-platform-telemetry-transport-research.md)
- [Alarm and incident management research and implemented PoC boundary](explanation/alarm-incident-management-research.md)

## Reference

- [C37.118 implementation reference](reference/c37-118-implementation-reference.md)
- [WAMA data flow and contracts](reference/wama-data-flow-contracts.md)
- [LFR per-second frequency provision](reference/lfr-frequency-provision.md)
- [C37.118 simulator](reference/c37-118-simulator.md)
- [C37.118 Masterdata and C37.118 gateway](reference/c37-118-masterdata-gateway.md)
- [Source documents](reference/source-documents.md)

## Decisions

- [ADR 0001: Compacted Alarm desired state](adr/0001-compacted-alarm-desired-state.md)
- [ADR 0002: Separate alarm evaluation watermark](adr/0002-alarm-evaluation-watermark.md)

## Source assets

- [Common Format schema](wama/schema/rtd_schema.proto)
- [Measurement session schema](wama/schema/measurement_session.proto)
- [Blobmeta schema](wama/schema/blobmeta.proto)
- [Alarm desired-state schema](wama/schema/alarm.proto)
- [IEC 104 export schema](wama/schema/iec104_export.proto)
- [Masterdata schema](wama/schema/masterdata.proto)
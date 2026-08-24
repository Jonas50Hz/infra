(exp_wama_documentation)=

```{meta}
:description: WAMA proof-of-concept documentation organized as explanations and technical reference.
```

# WAMA documentation

This documentation describes the WAMA Docker Compose proof of concept. The
[WAMA source assets](wama/) remain the canonical locations for schemas, BPMN
models, the approved C37.118 standard copy, and the architecture image.

## Explanation

- [WAMA platform overview and processes](explanation/wama-platform-overview.md)
- [WAMA architecture and technology choices](explanation/wama-architecture.md)
- [WAMA PoC Docker Compose plan](explanation/wama-poc-compose-plan.md)
- [WAMA processor authoring experience](explanation/wama-processor-authoring.md)

## Reference

- [C37.118 implementation reference](reference/c37-118-implementation-reference.md)
- [WAMA data flow and contracts](reference/wama-data-flow-contracts.md)
- [LFR per-second frequency provision](reference/lfr-frequency-provision.md)
- [C37.118 simulator](reference/c37-118-simulator.md)
- [C37.118 Masterdata and gateway onboarding](reference/c37-118-masterdata-gateway-onboarding.md)
- [Source documents](reference/source-documents.md)

## Source assets

- [Common Format schema](wama/schema/rtd_schema.proto)
- [Measurement session schema](wama/schema/measurement_session.proto)
- [Blobmeta schema](wama/schema/blobmeta.proto)
- [IEC 104 export schema](wama/schema/iec104_export.proto)
- [Masterdata schema](wama/schema/masterdata.proto)
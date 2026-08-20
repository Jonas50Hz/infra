# WAMA Platform — Overview & Processes

Sources: **WAMA Platform Concept** (Gerbrand Jonas, Chapter Core Data, 22 Jul
2026), **WAMA Platform** deck (Gerbrand Jonas / Olsen Ida), the
[architecture image](image.png), and the
[user-configuration BPMN](User_Konfiguration_Nexus.bpmn). The image and BPMN
are authoritative for target component and process vocabulary. They do not
replace the Compose PoC's Common Format, raw-Protobuf, or Kafka KRaft choices.

## Framing
WAMA is a **data & analytics platform**, not a collection of applications.
Use cases (PQ, IKN, CoMo, PMU analysis, ...) share one technical foundation:
acquisition, data contracts, streaming, storage, quality handling, export.
A single use case can run on a locally optimised architecture; the shared
foundation matters once multiple use cases reuse and extend it.

## Core processes
### Onboarding a source
- Minimal input: masterdata = **IP + location**, entered via Git.
- Gateway container provisioned automatically; capabilities requested; stream started.
- Decommissioning built in: stop command + container teardown on removal.

### Configuration & deployment
- A Power User requests a change to live calculations, live values,
  `MeasurementSession` handling, or alarms and updates the configuration in
  version control.
- An automated pipeline validates the proposed configuration, including
  cybersecurity, quality checks, unit tests, and integration tests.
- A **Systemexperte** decides whether the validated change is accepted or sent
  back for revision. The version, test results, decision, and deployment record
  provide an audit-safe trace.
- An accepted configuration is executed in the gateway, storage, and live-data
  processing components. The Power User receives the deployment outcome.

### Live data
- Live data received from gateway → converted to **Common Format**
  (`MCCSMeasurementValue`).
- Some data goes to the processing (Datenfluss) engine; all data is stored.
- The Compose PoC's root-owned Druid service directly ingests raw-Protobuf
  `LiveMeasurement` records into the no-rollup `live_measurements` datasource.
  Its only host API is the Router on port 8888; Kafka remains a single KRaft
  broker and has no ZooKeeper service.
- Grafana's root-owned `WAMA Measurements` dashboard queries Druid directly to
  show valid PMU voltage, current, frequency, and ROCOF values over time. It is
  separate from VictoriaMetrics-backed infrastructure dashboards.
- Raw data remains in SeaweedFS. Retention, deletion, aggregation, compaction,
  and archival policy for both raw and live data remain explicit future decisions;
  the current Druid datasource configures none of them.
- Optional outbound control to external systems (e.g. IEC 104) or alarms.

### Measurement session & alarm
- A bounded `MeasurementSession` request identifies start point, end point, and
  sorted measurement MRIDs. The root-owned worker queries the historical Druid
  interval and records the resulting samples as an immutable Parquet artifact.
- Compacted `Blobmeta` captures the artifact pointer, SHA-256, row counts, and
  complete/partial/rejected status. PostgreSQL materializes that metadata for
  queries; measurements remain in Druid and SeaweedFS.
- The session is retained for long-term access and may result in an optional
  alarm notification. Browser/file-export presentation remains future work.
- Long-term measurement-session storage has no six-week deletion policy, unlike
  raw data.

## Capability model (what belongs where)
- **Application platform:** containerization, IAM, streaming, logging, alarming,
  timeseries, dashboards.
- **WAMA core:** master data, gateways, raw-data retention, quality handling,
  derived-value publication, common data contract, topic/schema governance,
  observability, export interfaces, historical access.
- **Individual use cases:** use-case-specific processors and logic (PQ, IKN, CoMo).

## Planned C37.118 source simulation

The current `pmu-gateway` is a fast Common-Format fixture, not a C37.118
endpoint. The planned memory-bounded C37.118 TCP simulator is specified in
[05-c37-118-simulator.md](05-c37-118-simulator.md). It will exercise a future
source-protocol gateway before that gateway publishes `LiveMeasurement` records.

## Governance (from the WAMA Platform deck)
- Architecture owner + ADR process; technology-decision backlog.
- Processors declare inputs/outputs; derived values published + stored; shared
  processing functions reused across use cases.

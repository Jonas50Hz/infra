(exp_wama_platform_overview)=

```{meta}
:description: WAMA platform processes and the boundaries of the local Compose proof of concept.
```

# WAMA platform overview and processes

Sources: **WAMA Platform Concept** (Gerbrand Jonas, Chapter Core Data, 22 Jul
2026), **WAMA Platform** deck (Gerbrand Jonas / Olsen Ida), the
[architecture image](../wama/image.png), and the
[user-configuration BPMN](../wama/User_Konfiguration_Nexus.bpmn). The image and BPMN
are authoritative for target component and process vocabulary. They do not
replace the Compose PoC's Common Format, raw-Protobuf, or Kafka KRaft choices.

## Framing
WAMA is a **data & analytics platform**, not a collection of applications.
Use cases (PQ, IKN, CoMo, PMU analysis, ...) share one technical foundation:
acquisition, data contracts, streaming, storage, quality handling, export.
A single use case can run on a locally optimized architecture; the shared
foundation matters once multiple use cases reuse and extend it.

## Core processes
### Activating a source
- V1 masterdata is a reviewed Git record containing a stable source ID, site
  ID, display name, literal IP address, TCP port, C37.118 PMU IDCODE, legacy
  wire version 2, and explicit signal-to-MRID mappings.
- The private `gateway-c37-118` repository validates an approved
  catalog revision, projects it as keyed raw-Protobuf `SourceMasterdata`
  records on compacted `Masterdata`, and reconciles one isolated legacy-v2 TCP
  adapter per active catalog source. The current root-owned `pmu-gateway` is
  not controlled by that repository.
- Removing a catalog source publishes a source-keyed Kafka tombstone and stops
  only its matching previously managed adapter.

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
- For source activation, the Systemexperte-approved `main` revision publishes
  Masterdata and reconciles only catalog-derived adapters in its marker-owned
  deployment root. It does not alter root infrastructure, including the root
  `pmu-gateway` fixture or the simulator.

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
- The root-owned `gateway-dashboard-provisioner` replays compacted Masterdata
  into Grafana's `WAMA Gateways` fleet page and one Druid-backed live dashboard
  per active source. A source tombstone removes its generated page; this
  membership signal does not claim that a source adapter is running.
- Raw data remains in SeaweedFS. Retention, deletion, aggregation, compaction,
  and archival policy for both raw and live data remain explicit future decisions;
  the current Druid datasource configures none of them.
- Optional outbound control to external systems (e.g. IEC 104) or alarms.

### Measurement session & alarm
- A bounded `MeasurementSession` request identifies start point, end point, and
  sorted measurement MRIDs. The root-owned worker queries the historical Druid
  interval and records the resulting samples as an immutable Parquet artifact.
- Compacted `Blobmeta` captures the artifact pointer, Parquet schema version,
  SHA-256, row counts, and complete/partial/rejected status. PostgreSQL
  materializes that metadata, while the root-owned query indexer registers only
  verified v2 artifacts in Iceberg for read-only Trino access.
- The session is retained for long-term access and may result in an optional
  alarm notification. Grafana passes its selected interval and MRIDs to the
  local confirmation UI, which publishes one bounded request; selected-session
  presentation remains read-only through Trino. File export and broader
  analytics remain future work.
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

The retained `pmu-gateway` is a deprecated fast Common-Format fixture, not a
default service or a C37.118 endpoint. The separately managed, memory-bounded
C37.118 TCP simulator is specified in
[the C37.118 simulator reference](../reference/c37-118-simulator.md).
An operator manually starts its five-PMU V2 fixture from
`~/c37-118-simulator` as the reviewed source for the declared C37.118 gateway
demonstration. It remains a standalone source and protocol-test service: it
does not implement, deploy, or validate a gateway or publish `LiveMeasurement`
records itself.

## Governance (from the WAMA Platform deck)
- Architecture owner + ADR process; technology-decision backlog.
- Processors declare inputs/outputs; derived values published + stored; shared
  processing functions reused across use cases.
- The deferred [processor authoring experience](wama-processor-authoring.md)
  describes how ordinary electrical calculations can become easier to create
  without weakening the Common Format, review, or processor-delivery boundaries.

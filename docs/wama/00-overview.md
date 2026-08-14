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
- Raw data stored, then deleted after X weeks; live data aggregated + archived.
- Optional outbound control to external systems (e.g. IEC 104) or alarms.

### Measurement session & alarm
- A `MeasurementSession` is recognised (or a Störschrieb is processed) and
  recorded with start point, end point, and measurements.
- The session is stored for long-term access, can be viewed in-system and
  exported as CSV, and may result in an optional alarm notification.
- Long-term measurement-session storage has no six-week deletion policy, unlike
  raw data.

## Capability model (what belongs where)
- **Application platform:** containerization, IAM, streaming, logging, alarming,
  timeseries, dashboards.
- **WAMA core:** master data, gateways, raw-data retention, quality handling,
  derived-value publication, common data contract, topic/schema governance,
  observability, export interfaces, historical access.
- **Individual use cases:** use-case-specific processors and logic (PQ, IKN, CoMo).

## Governance (from the WAMA Platform deck)
- Architecture owner + ADR process; technology-decision backlog.
- Processors declare inputs/outputs; derived values published + stored; shared
  processing functions reused across use cases.

# WAMA Platform — Overview & Processes

Source: **WAMA Platform Concept** (Gerbrand Jonas, Chapter Core Data, 22 Jul 2026)
and **WAMA Platform** deck (Gerbrand Jonas / Olsen Ida). This plan only.
Not WAMA Nexus (different system, different data flow).

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
- Power User opens config dashboard, selects desired inputs/outputs/logic, saves.
- Version-controlled to Git → CI/CD runs to test the config.
- If it passes, config is deployed; Power User is notified.

### Live data
- Live data received from gateway → converted to **Common Format**
  (`MCCSMeasurementValue`).
- Some data goes to the processing (Datenfluss) engine; all data is stored.
- Raw data stored, then deleted after X weeks; live data aggregated + archived.
- Optional outbound control to external systems (e.g. IEC 104) or alarms.

### Event & alarm
- Event recognised (or Störschrieb processed) → recorded (start point, end point,
  measurements) → stored in event list (DB) → optional alarm notification.
- Event data can be viewed in-system and exported (CSV).
- Long-term event storage (no deletion after 6 weeks, unlike raw data).

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

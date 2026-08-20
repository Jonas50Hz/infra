# Source documents (SharePoint)

This context pack is distilled from the WAMA Platform Concept, its companion
deck, and the authoritative process/component references below. Keep originals
in SharePoint; update these summaries when the source docs change.

## Authoritative WAMA sources
- WAMA_Platform_Concept 1.pptx / .pdf — Gerbrand Jonas, 22 Jul 2026
  (processes, technology-choice matrix, architecture at a glance, per-component
  rationale).
- WAMA Platform.pptx — Gerbrand Jonas / Olsen Ida
  (data-platform framing, capability model, storage split, governance).
- Business_Descpt_UseCase_Sek_Frequenzwertbereit_final.pdf — LFR per-second
  frequency provision; distilled in
  [04-lfr-frequency-provision.md](04-lfr-frequency-provision.md).
- schema/rtd_schema.proto — Common Format contract (`MCCSMeasurementValue`,
  `rtd_schema.v1`). Same schema as MCCS. PoC uses its own MRIDs first.
- schema/iec104_export.proto — PoC raw-Protobuf contract for one-way IEC 104
  export records (`wama.iec104.v1`).
- [image.png](image.png) — authoritative target component model and component
  vocabulary.
- [User_Konfiguration_Nexus.bpmn](User_Konfiguration_Nexus.bpmn) —
  authoritative configuration/deployment roles and workflow.
- [Livedaten_Prozess_WAMA.bpmn](Livedaten_Prozess_WAMA.bpmn) — authoritative
  live-data process vocabulary.

## Deliberately excluded Nexus data-plane choices
- WAMA_Nexus_2006_Architecture_Document_1.1 — Siegel / Tchoubraev.
- Solution-Architecture-Specification_WAMA-Nexus V03 — Wenger Carsten.
- WAMA dev platform reqs (draft) En — Wenger Carsten.
- WAMA_UCf_Kickoff_DataFlow — Gerbrand / Wenger (describes the Nexus flow:
  Concentrator/TimeStep, Confluent Schema Registry, PMUReading/STAT).

The Nexus references above are authoritative only for the documented process
and component vocabulary. They do not replace the WAMA PoC's Common Format,
plain Kafka KRaft transport, or raw-Protobuf serialization.

Note: distilled by an assistant for agent context; on any conflict the
SharePoint originals win.

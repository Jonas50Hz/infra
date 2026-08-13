# Source documents (SharePoint)

This context pack is distilled from **only the WAMA Platform Concept authored by
Gerbrand Jonas** and its companion deck. Keep originals in SharePoint; update
these summaries when the source docs change.

## Included (this plan)
- WAMA_Platform_Concept 1.pptx / .pdf — Gerbrand Jonas, 22 Jul 2026
  (processes, technology-choice matrix, architecture at a glance, per-component
  rationale).
- WAMA Platform.pptx — Gerbrand Jonas / Olsen Ida
  (data-platform framing, capability model, storage split, governance).
- schema/rtd_schema.proto — Common Format contract (`MCCSMeasurementValue`,
  `rtd_schema.v1`). Same schema as MCCS. PoC uses its own MRIDs first.

## Deliberately EXCLUDED (WAMA Nexus — different system, different data flow)
- WAMA_Nexus_2006_Architecture_Document_1.1 — Siegel / Tchoubraev.
- Solution-Architecture-Specification_WAMA-Nexus V03 — Wenger Carsten.
- WAMA dev platform reqs (draft) En — Wenger Carsten.
- WAMA_UCf_Kickoff_DataFlow — Gerbrand / Wenger (describes the Nexus flow:
  Concentrator/TimeStep, Confluent Schema Registry, PMUReading/STAT).

Reason for exclusion: Nexus shares the same base concept but is a separate
system with a different data flow. Mixing the two would contaminate the PoC.

Note: distilled by an assistant for agent context; on any conflict the
SharePoint originals win.

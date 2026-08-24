(ref_source_documents)=

```{meta}
:description: Authoritative WAMA source documents and the local assets retained for this proof of concept.
```

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
  [the LFR frequency provision reference](lfr-frequency-provision.md).
- [IEEE Std C37.118.2-2024](https://eliagroup.sharepoint.com/sites/MCCSTopicGroups/Shared%20Documents/Forms/AllItems.aspx?viewid=1483051c%2Dd713%2D488d%2D9aaf%2D15e353515e52&csf=1&FolderCTID=0x0120005D0DF4ED6F06CE4EAABC52695A2BA16B&id=%2Fsites%2FMCCSTopicGroups%2FShared%20Documents%2FProduct%20Cluster%20Grid%2C%20Asset%20%26%20System%2F02%20%2D%20Product%20Lines%2F06%2DFuture%5FProduct%5FDevelopment%2FInitiatives%2FWAMA%2F10%5FWAMA%5FData%5FConcepts%2FStandards%2FIEEE%20Std%20C37%2E118%2E2%E2%84%A2%2D2024%2EPDF&parent=%2Fsites%2FMCCSTopicGroups%2FShared%20Documents%2FProduct%20Cluster%20Grid%2C%20Asset%20%26%20System%2F02%20%2D%20Product%20Lines%2F06%2DFuture%5FProduct%5FDevelopment%2FInitiatives%2FWAMA%2F10%5FWAMA%5FData%5FConcepts%2FStandards) — normative C37.118.2 wire-format reference for the standalone simulator. The
  locally approved implementation copy is
  [IEEE Std C37.118.2-2024.PDF](../wama/IEEE%20Std%20C37.118.2%E2%84%A2-2024.PDF),
  SHA-256 `ee776f9b78ccc95980d05e04e570f6dbbdad3993ae7412dc81ed772d5cbd7546`; the
  implementation brief is [the C37.118 simulator reference](c37-118-simulator.md).
- schema/rtd_schema.proto — Common Format contract (`MCCSMeasurementValue`,
  `rtd_schema.v1`). Same schema as MCCS. PoC uses its own MRIDs first.
- schema/masterdata.proto — raw-Protobuf C37.118 source masterdata contract
  (`wama.masterdata.v1.SourceMasterdata`) for compacted Kafka runtime state.
- schema/iec104_export.proto — PoC raw-Protobuf contract for one-way IEC 104
  export records (`wama.iec104.v1`).
- [image.png](../wama/image.png) — authoritative target component model and component
  vocabulary.
- [User_Konfiguration_Nexus.bpmn](../wama/User_Konfiguration_Nexus.bpmn) —
  authoritative configuration/deployment roles and workflow.
- [Livedaten_Prozess_WAMA.bpmn](../wama/Livedaten_Prozess_WAMA.bpmn) — authoritative
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

(exp_diode_first_live_data_architecture_research)=

```{meta}
:description: Research explanation of a diode-first clean-sheet architecture for WAMA live data crossing from OT to IT.
```

# Diode-first live-data architecture research

This Explanation examines a clean-sheet WAMA live-data architecture in which, after gateway deployment and configuration, source/OT data crosses a strict physical data diode to IT/core north. It explains the architectural and procurement reasoning for a replacement of Kafka plus Druid; it is neither an implementation guide nor a product selection.

## Scope and conclusion

This research does **not** change the current Docker Compose proof of concept. Its data plane remains Kafka in KRaft mode with Druid ingesting `LiveMeasurement`, as described in the [WAMA data-flow contract](../reference/wama-data-flow-contracts.md). This note is architecture and procurement research only.

Network access was unavailable while this note was written. No vendor or product was live-verified, selected, or benchmarked. The official links at the end are verification targets, not product claims. Before an ADR or procurement, confirm both the physically enforced OT-south-to-IT-north direction and the vendor support matrix for the deployed appliance, version, and application profile.

The central conclusion is simple: do **not** create a WAMA UDP protocol, outer wrapper, custom write-ahead-log protocol, application fragment format, signature envelope, or retry scheme. Use an approved commercial unidirectional-gateway/data-diode appliance with a documented, vendor-supported application profile. Its appliance/proxy pair owns crossing transport, buffering, transfer recovery, and audit.

Native TCP, REST, Kafka/Redpanda, MQTT, NATS, RabbitMQ, and QUIC do not directly cross a strict physical diode. They can terminate locally on one or both appliance sides only when the appliance explicitly supports that application profile. The physical boundary, not an application transport choice, establishes one-way behavior.

```{mermaid}
flowchart LR
    subgraph South["OT / source south"]
        PMU["C37.118 PMU / PDC"]
        Gateway["C37.118 gateway"]
        Stage["Approved gateway release and source mapping\nstaged offline or separately governed"]
        PMU <-->|"C37.118 TCP: data, CFG, start, stop, reconfigure"| Gateway
        Stage -. "local staging and activation" .-> Gateway
    end
    subgraph Appliance["Approved unidirectional-gateway appliance"]
        SouthEndpoint["OT-side application endpoint\nexact supported profile confirmed by vendor"]
        Diode["Physical data diode\nOT south -> IT north"]
        NorthEndpoint["IT-side application endpoint\nexact supported profile confirmed by vendor"]
        SouthEndpoint --> Diode --> NorthEndpoint
    end
    Gateway -->|"documented vendor-supported profile"| SouthEndpoint
    subgraph North["IT / core north"]
        Historian["Choice A: industrial historian\nor historian replication"]
        Ingress["Choice B: standard ingress\nor maintained connector"]
        ClickHouse["ClickHouse"]
        Archive["Parquet / Iceberg"]
        NorthEndpoint --> Historian
        NorthEndpoint --> Ingress
        Ingress --> ClickHouse
        Ingress --> Archive
    end
    subgraph Allowed["Allowed zone: entirely south or entirely north"]
        Iec104["IEC 104 exporter"]
        Center["IEC 104 control center"]
        Iec104 <-->|"IEC 104"| Center
    end
```

All C37 CFG, start, stop, and reconfigure TCP interactions remain south. Approved configuration is staged south by offline or separately governed means, never through the diode. The IEC 104 exporter and its control center remain together in one allowed zone; their placement must not create an exception to the physical boundary.

## Existing crossing profiles

The following are existing standards or vendor-supported profile categories, not claims that any named appliance supports them. The selection question is whether a vendor documents the exact profile and versions for the source, appliance, and receiver.

| Crossing profile | Decision-oriented position |
|---|---|
| Vendor-supported historian replication | First choice where "no bespoke development" is literal. It can replace both the broker and Druid for retention and query, but processing needs, schema fidelity, and scale still require validation. |
| Vendor-supported OPC UA PubSub/UADP profile | Standards-based choice for fixed, one-way publish/subscribe. It depends on static configuration and out-of-band security/key provisioning, and is viable only when the appliance and gateway or historian products document it. |
| Vendor-supported C37.118 relay or statically configured C37.118 UDP profile | Close fit for PMU sources. The current WAMA PoC and simulator are TCP-only; CFG and control remain south. Exact PMU/PDC and diode support must be proven. |
| IEC 61850-90-5 | Utility-native candidate for new IEC-compatible systems, but a poor retrofit assumption for the present legacy C37 version 2 scope. |
| Vendor-supported MQTT proxy or replication | A local integration facade only, never direct MQTT across the diode. QoS and retained-state behavior require testing. |
| DDS/RTPS | A specialized existing standard. Do not recommend it unless a vendor profile and static-discovery, no-reverse-traffic behavior are specifically proven. |
| Vendor-supported Kafka replication | Lowest-migration fallback, explicitly **not** a Kafka replacement. It remains conditional on a documented appliance profile and must not be mistaken for direct Kafka crossing. |

This model deliberately transfers responsibility for delivery behavior to the approved appliance/proxy pair. A WAMA-specific protocol would turn an appliance evaluation into a custom transport implementation, defeating the requirement to use proven technology.

## North-side storage choices

The north-side choice begins after the appliance's IT-side endpoint. It must serve live query, durable retention, recovery, and session extraction without implying that a database can be exposed directly through the diode.

| North-side store | Decision-oriented position |
|---|---|
| Industrial historian | The most turnkey alternative to Druid where supported by the diode and historian vendors. AVEVA PI, Canary, and AspenTech IP.21 are examples only; exact compatibility is unverified, and this makes no promise of 100,000 samples per second. |
| ClickHouse plus Parquet/Iceberg | Best open analytical candidate, but only under a no-custom-protocol rule. It still needs a vendor-supported standard connector or maintained integration; it is not no-code when a receiver exposes proprietary records. |
| InfluxDB 3, TDengine, IoTDB, TimescaleDB, and QuestDB | Secondary stores that need product, connector, high-availability, and workload benchmarks. None can be exposed directly through the diode. |
| VictoriaMetrics | Infrastructure telemetry only. Its current WAMA role does not change and it is not the canonical live-measurement or session store. |

## Configuration and control boundaries

Configuration mapping is not a bespoke protocol. A product configuration mapping C37.118 or OPC UA tags into the WAMA Common Format is acceptable. If a vendor receiver emits proprietary frames with no maintained connector, the result is an unsupported custom integration and should be rejected rather than hidden as a protocol implementation.

The proposed boundary preserves the current contract vocabulary while changing where durable responsibilities live:

- A signed, approved gateway release and source mapping are staged south out-of-band. Source credentials and C37 commands remain south.
- `Masterdata` and `Schema` are north-side authoritative configuration/read models, together with northbound activation evidence.
- `MeasurementSession`, `Blobmeta` or a session manifest, `Alarm`, and `Export` are north-side durable jobs, outbox records, or API records.
- REST ends locally. No south-side caller can receive a north-side acceptance response through the diode.
- Raw C37 `STAT` and time-quality data need an existing product archive or transfer field, or the design must declare an explicit non-audit limitation.

These distinctions derive from the current contract facts in the materials linked below. In particular, current normalization does not retain raw `STAT` or time-quality evidence, so a future product profile cannot be assumed to recreate them after transfer.

## Procurement and acceptance evidence

An appliance/profile proposal is credible only when vendor evidence covers the following. The checklist intentionally describes acceptance outcomes, not how the vendor implements them.

| Acceptance concern | Required evidence |
|---|---|
| One-way boundary | Physically enforced direction and no reverse control path. |
| Application profile | Exact supported application profile and version for the source, appliance endpoints, and receiver. |
| Recovery behavior | Sender and receiver spool durability, overflow behavior, restart behavior, and loss reporting. |
| Transfer evidence | Identity, ordering, duplicate, gap, and correlation evidence. |
| Measurement semantics | Timestamp and quality propagation, including the treatment of C37 `STAT` and time-quality data. |
| Mapping and security | Payload, key, and schema mapping; local authentication, TLS, and ACL options. |
| Operations | Audit export plus target-rate and recovery testing. |

## Proposed evaluation

The evaluation has two stages because a long-store benchmark cannot prove the diode crossing.

1. **Vendor appliance proof.** Test the exact PMU/PDC/gateway, documented profile, and appliance endpoints. Demonstrate no return path, restart behavior, spool overflow, loss behavior, and audit behavior.
2. **Long-store benchmark.** Use the [large-scale time-series storage research](large-scale-time-series-storage-research.md) as the workload framing: 24 hours at 100,000 normalized samples per second, then a 125,000-sample-per-second burst, with query, session extraction, retention, and recovery checks.

These are proposed benchmark workloads, not product capacities or evidence of a universal throughput limit.

## Current repository context

The proposal is intentionally separate from the present implementation. These repository materials define the current-versus-proposed distinction and the facts that a future product profile must preserve:

- [WAMA data-flow contract](../reference/wama-data-flow-contracts.md): current Kafka topics, Druid path, session artifacts, and normalization limits.
- [Common Format schema](../wama/schema/rtd_schema.proto): the current `MCCSMeasurementValue` payload and quality semantics.
- [Gateway runtime](../../forgejo-repos/gateway-c37-118/src/gateway_c37_118/gateway_runtime.py): source-local C37.118 TCP configuration and publication behavior.
- [C37.118 simulator reference](../reference/c37-118-simulator.md): V2 and V3 source exchanges and present source-side limitations.
- [IEC 104 exporter README](../../services/iec104-exporter/README.md): the current controlled-station role that must remain in one allowed zone with its control center.
- [Large-scale time-series storage research](large-scale-time-series-storage-research.md): clean-sheet storage comparison context and proposed workload.

## Unverified primary-source verification targets

Network access was unavailable for this research. The following official or product-source links must be tested against an actual vendor support matrix and deployed version; they do not establish that any named product supports a particular profile.

- [OPC UA Part 14](https://reference.opcfoundation.org/Core/Part14/v105/docs/)
- [IEEE C37.118.2](https://standards.ieee.org/ieee/C37.118.2/5534/)
- [IEC 61850-90-5 IEC Webstore search](https://webstore.iec.ch/en/search?query=IEC%2061850-90-5)
- [OASIS MQTT 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
- [OMG DDS](https://www.omg.org/spec/DDS/) and [OMG DDSI-RTPS](https://www.omg.org/spec/DDSI-RTPS/)
- [Waterfall](https://waterfall-security.com/), [Owl Cyber Defense](https://owlcyberdefense.com/), and [Advenica](https://advenica.com/)
- [AVEVA PI](https://www.aveva.com/en/products/pi-system/), [Canary](https://www.canarylabs.com/), and [Aspen InfoPlus.21](https://www.aspentech.com/en/products/industrial-data-management/aspen-infoplus-21)
- [ClickHouse](https://clickhouse.com/), [Apache Iceberg](https://iceberg.apache.org/), and [Apache Parquet](https://parquet.apache.org/)
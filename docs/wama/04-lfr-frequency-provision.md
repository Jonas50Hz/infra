# LFR Per-Second Frequency Provision

Source: [Business_Descpt_UseCase_Sek_Frequenzwertbereit_final.pdf](https://eliagroup.sharepoint.com/sites/MCCSTopicGroups/Shared%20Documents/Product%20Cluster%20Grid,%20Asset%20%26%20System/02%20-%20Product%20Lines/06-Future_Product_Development/Initiatives/WAMA/01%20Business%20Documentation/01%20Use-cases/02%20Sek%C3%BCndliche%20Bereitstellung%20Frequenz/Business_Descpt_UseCase_Sek_Frequenzwertbereit_final.pdf?EntityRepresentationId=1e8a9462-81df-485e-b876-6c46514dd4e4).

## Scope

This specification defines a future Kafka processor that derives one preferred frequency (Vorzugsfrequenz) per UTC second for the legacy LFR, assigns its quality, and creates IEC 60870-5-104 export requests.

The processor starts with normalized raw-Protobuf `MCCSMeasurementValue` records already present on `LiveMeasurement`. IEEE C37.118 parsing, soft-PDC behaviour, source connection, raw-frame persistence, and reception of data frames are deliberately out of scope.

Upstream normalization must already convert the C37.118 frequency deviation to an absolute `double_value` in Hz: $f_Hz = 50 + FREQ_mHz / 1000$.

This document does not claim that the existing `processor-frequency-scale` sample implements the LFR algorithm. That seed only scales one fixture value from Hz to mHz. A real implementation must be a separately owned `processor-*` repository and must not deploy or modify the root-owned IEC 104 exporter.

The checked-in `processor-frequency-iec104-export` seed is a separate direct
PoC mapping: one explicitly valid fake-PMU frequency produces one configured
`M_ME_NC_1` `ExportRecord`. It deliberately does not satisfy this document's
per-second aggregation, status, voltage, preferred-frequency selection,
timeout-hold, audit, or heartbeat requirements.

## Current PoC Implementation

`forgejo-repos/processor-lfr-frequency-provision/` is a separate private
Forgejo seed for the first LFR core increment. It uses a configurable
multi-PMU input map, evaluates closed UTC seconds $400$-$800$ ms after their
boundary, calculates mean frequency and voltage per PMU, combines count and
voltage classifications, and publishes a configured preferred-frequency
`MCCSMeasurementValue` back to `LiveMeasurement`. Its state/outbox is durable
across application restart; Kafka delivery remains at least once.

This increment intentionally does not create `ExportRecord` values, alter the
root-owned IEC 104 exporter, implement hold/resend, RoCoF, heartbeat, or the
six-week audit store. Its temporary `generic_quality_provisional` mode uses
existing Common Format Quality flags only to reject plainly unusable records.
It does not claim complete C37.118 `STAT` or time-quality coverage; an actual
data-frame-to-Common-Format mapping remains the gate before production use.

## Kafka Input Contract

Master Data configuration must map the logical signals below to MRIDs, PMU identity, PMU type, and voltage level.

| Input | Required representation | Use |
| --- | --- | --- |
| Frequency | `double_value`, absolute Hz | Plausibility and per-PMU mean |
| Voltage | `double_value`, unit and nominal voltage configured | Voltage sub-status |
| RoCoF (optional) | `double_value`, configured unit | Analogous best-value calculation |
| PMU timestamp | `timestamp_field` | Assignment to second $T$ |
| Gateway and MCCS timestamps | `timestamp_gateway`, `timestamp_mccs` | Traceability and latency evidence |
| PMU status evidence | Normalized status mapping or companion metadata | Per-value status validation |

`MCCSMeasurementValue.quality` has generic flags, but the current Common Format does not contain the original C37.118 `STAT` word or all of its interpretations. The owning input contract must define how missing, invalid, error, test-mode, synchronization, data-modified, and time-quality status evidence reaches this processor. It must not infer a valid PMU status from `quality.valid` alone.

Each processed sample needs durable audit evidence for at least six weeks after real time. Retain input identity and timestamps, every plausibility outcome, and all reason codes. The current Common Format and Druid retention policy do not yet define that evidence contract.

## Time Model

Let $T$ be an elapsed UTC wall-clock second. At the second transition, evaluate every input record for $T$ that arrived before the configured close decision. A late record must not retroactively change an already emitted result.

`timestamp_field` is the PMU measurement instant. A record belongs to $T$ only when `floor(timestamp_field) = T`. `timestamp_gateway` and `timestamp_mccs` remain traceability and latency evidence, but cannot replace the source timestamp for this check.

The close decision and export path must make the value available at the IEC 104 interface 400 ms to 800 ms after the end of the evaluated second, with an overall provision delay no greater than one second from measurement.

## Algorithm

### Stage 1 - Per-Value Plausibility

For every frequency sample assigned to $T$, evaluate every check below. A sample is good only when all positive criteria pass. A failed criterion makes it bad; persist every applicable reason rather than stopping after the first failure.

| Check | Good condition | Failure reason |
| --- | --- | --- |
| Frequency band | $f_min <= f_Hz <= f_max$ | `out_of_band` |
| Status present | Required normalized PMU status evidence exists | `missing_status` |
| Status valid | Status mapping permits use of the sample | `invalid_status` |
| Timestamp present | `timestamp_field` exists | `missing_timestamp` |
| Timestamp window | `floor(timestamp_field) = T` | `stale_timestamp` or `future_timestamp` |

A timestamp before $T$ is stale; one after $T$ is future dated. A malformed or nonnumeric configured frequency signal is not usable and must also be retained as invalid input evidence.

### Stage 2 - Per-Second Aggregation per PMU

For each PMU, use every good frequency sample in its bucket for $T$ that is available at the close decision. If at least one exists, calculate the configured aggregation; the default is `mean[p,T] = sum(good[p,T]) / n_good[p,T]`.

The aggregation interface must be replaceable, so an approved future method such as minimum or maximum does not require changes to selection or export. Do not emit a per-PMU mean when no good sample exists.

### Stage 3 - Availability per PMU

Record `n_good[p,T]` for every PMU and second. Where the received count is known, also record `n_total[p,T]` and `availability[p,T] = n_good[p,T] / n_total[p,T]` when the denominator is nonzero.

All good samples available at the close decision participate in the aggregate. Availability is optional because the true expected sample count can be unknown.

### Stage 4 - Per-PMU Classification

The count-based sub-status is centrally configured by PMU type:

| Good-value count | Required class |
| --- | --- |
| $n_good > 25$ | `very_good` |
| $10 < n_good < 25$ | `good` |
| $n_good < 10$ | `bad` |

The voltage-based sub-status is centrally configured by voltage level:

| Voltage deviation from nominal | Required class |
| --- | --- |
| $|Delta U| < X$ | `very_good` |
| $X <= |Delta U| <= Y$ | `good` |
| $|Delta U| > Y$ | `bad` |

Set `status[p,T]` to the worse sub-status. PMUs with `bad` status cannot enter subsequent frequency-selection stages.

The source leaves $n_good = 10$ and $n_good = 25$ unspecified. Central configuration must define a complete, non-overlapping boundary policy before coding. The source also does not define how to obtain or aggregate the per-second voltage deviation; that definition is a prerequisite for the voltage sub-status.

### Stage 5 - Preferred-Frequency Selection

1. Discard every PMU with `bad` status or no mean frequency.
2. Find the highest remaining status class.
3. Select all remaining PMUs in that class.
4. Sort their mean frequencies.
5. For an odd count, select the central value. For an even count, select the central value whose absolute deviation from 50 Hz is larger; do not average the two values.

The selection strategy must be replaceable. The source does not define a deterministic tie-break when the two central values are equally distant from 50 Hz; configuration or a documented stable rule is required before implementation.

Best RoCoF is optional and follows the same cross-PMU approach once its input mapping, per-PMU aggregation, and export mapping are defined.

### Stage 6 - IEC 104 Export

- A valid preferred frequency from a non-`bad` selected subset creates export requests with good quality and resets the consecutive missing-value counter.
- When no preferred frequency is available, send nothing and increment the counter.
- When the counter reaches configured timeout $x$ and a last valid value exists, send that value once with disturbed/bad quality, then reset the counter.
- Without a last valid value, there is nothing to resend.

The processor creates raw-Protobuf `wama.iec104.v1.ExportRecord` values on `Export`. The root-owned `iec104-exporter` remains responsible for IEC 104 transport, APDU framing, and Kafka commit after an accepted send. The processor must configure common address, information-object addresses, COT, and `M_ME_NC_1` mapping for preferred frequency, preferred RoCoF, and per-PMU values. The exact mapping from good/disturbed to `Iec104Quality` remains open.

The source requires a current-second-of-day sawtooth heartbeat that is both sent and received to supervise the link. The current PoC exporter only sends monitor-direction values and rejects inbound application ASDUs, so it cannot meet the receive half of this requirement without an explicit interface extension.

Selection excludes `bad` PMUs, while the source also says to send a computed result with disturbed status when the overall status is `bad`. The condition under which a selected result can be `bad` must be clarified before coding; the normal no-candidate path is the timeout hold behaviour above.

## Central Configuration

| Parameter | Scope | Default or source rule |
| --- | --- | --- |
| `frequency_min_hz`, `frequency_max_hz` | All configured frequency points | No concrete values supplied |
| Count thresholds and boundary inclusivity | PMU type | Source gives 25 and 10, but not values at the boundaries |
| `voltage_very_good_max`, `voltage_good_max` | Voltage level | Source names $X$ and $Y$; no concrete values supplied |
| Voltage-deviation calculation | Voltage level / signal mapping | Source and aggregation undefined |
| `missing_value_timeout_seconds` | LFR export | Parametrizable $x$ seconds |
| Aggregation strategy | Processor | Arithmetic mean by default; pluggable |
| Preferred-value strategy | Processor | Highest-quality subset plus median/tie-break by default; pluggable |
| Source mappings | PMU / Master Data | Frequency, voltage, RoCoF, status, PMU type, voltage level |
| Kafka close/export timing | Processor | Meet the 400 ms-to-800 ms target |
| IEC 104 mappings | LFR interface | Common address, IOAs, COT, quality bits, heartbeat point |

Configuration changes must be centrally versioned and validated before deployment. Master Data remains the authority for MRID meaning; the processor must not hard-code signal semantics or voltage levels.

## Required Verification

1. Every plausibility failure reason, including missing status/timestamp and stale/future timestamp, is persisted with the sample.
2. Good samples alone enter the per-PMU aggregate and availability count.
3. Count and voltage sub-statuses combine using the worst status.
4. `bad` PMUs cannot influence the selected frequency.
5. Odd and even selected sets follow the required median rule, including the larger-deviation tie-break.
6. No-candidate seconds produce no export before the hold timeout, and the last valid value is sent once with disturbed quality at the timeout.
7. The result is emitted within the post-second latency budget under the configured input rate.
8. Generated `ExportRecord` values conform to the canonical export schema and configured IEC 104 point mapping.

## Decisions Still Required

- Concrete frequency-band and voltage thresholds.
- A normalized Kafka representation of every C37.118 status condition needed for Stage 1, including retention of source status evidence.
- The voltage signal source and per-second $Delta U$ calculation.
- Inclusion policy for count thresholds exactly equal to 10 or 25.
- Deterministic handling when the even-median candidates are equally distant from 50 Hz.
- The intended meaning of a computed `bad` result after `bad` PMUs have been excluded.
- RoCoF aggregation and IEC 104 mapping.
- IEC quality-bit mapping and the bidirectional heartbeat interface.
- The durable six-week audit store and its retention enforcement.
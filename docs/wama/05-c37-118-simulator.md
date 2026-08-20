# C37.118 Simulator Implementation Brief

## Status and authority

This is the implementation brief for a planned root-owned
`c37-118-simulator` service. It is a test source for the future C37.118
gateway; it is not the existing `pmu-gateway`, and this document does not add a
Compose service yet.

[IEEE Std C37.118.2-2024](_sources.md#authoritative-wama-sources) is the
normative wire-format reference. Implementers must consult the approved PDF for
every frame type, command value, field width, flag, checksum, and validity
rule. This brief defines WAMA behavior and resource limits; it intentionally
does not reproduce the standard's tables.

## Goal and boundary

The simulator must let a gateway exercise a real C37.118 TCP session before
the gateway publishes Common Format records:

```text
c37-118-simulator -- C37.118.2 TCP --> c37-118-gateway
  --> MCCSMeasurementValue / LiveMeasurement --> Kafka
```

It emits C37.118 configuration and data frames only. It does not connect to
Kafka, Druid, SeaweedFS, or Forgejo, and it never persists raw frames. The
future gateway owns protocol-to-Common-Format normalization. The current
YAML-to-Kafka `pmu-gateway` remains the fast fixture for existing infrastructure
checks.

The first release is a deterministic simulator, not a conformity claim or a
replacement for a real PMU/PDC. Its initial transport scope is C37.118.2-2024
over unicast TCP. UDP, TLS, authentication, multicast, dynamic configuration,
and a forwarding PDC are out of scope until a concrete interoperability need
exists.

## V1 scale target: 100 PMUs

The simulator has a hard V1 ceiling of **100 logical PMUs per process**. This
means 100 independently addressable C37.118 sources, not 100 containers. One
root-owned simulator container runs one event loop and binds one internal TCP
listener for each PMU:

| Property | V1 decision |
| --- | --- |
| Logical PMUs | 1 to 100; reject a profile above 100 at startup |
| Endpoint model | One PMU block and one TCP listener per logical PMU |
| Internal port range | `4712` through `4811` for the 100-PMU fleet |
| Client policy | One gateway client per endpoint; a second client is rejected or closed deterministically |
| Host exposure | No host port mappings by default; the future gateway reaches `c37-118-simulator:<port>` on `wama-infra` |
| Baseline rate | 50 data frames per second per PMU, scheduled from one shared 20 ms tick |
| Baseline signals | Three voltage phasors, three current phasors, frequency deviation, and ROCOF |

The full baseline produces $100 \times 50 = 5{,}000$ C37.118 data frames per
second. If the gateway maps all eight logical signals, it then publishes up to
$100 \times 50 \times 8 = 40{,}000$ `MCCSMeasurementValue` records per second
to `LiveMeasurement`.

V1 deliberately models one PMU per endpoint because the gateway must exercise
independent source connections, configuration exchanges, reconnects, and
status transitions. A PDC-style aggregate endpoint is a later compatibility
mode, not the default scale mechanism. It may be added only after the 100
independent-endpoint profile is proven and its own exact frame-size and memory
budget are documented.

## Required V1 protocol behavior

1. Listen on configured TCP endpoints. A conventional port such as `4712` may
   be the default, but the port is always configuration, not protocol meaning.
2. Correctly reassemble fragmented command frames and process multiple frames
   delivered by one `read`. Validate sync, declared frame size, frame type,
   IDCODE, and checksum before acting.
3. Support the gateway startup sequence: request header, request current
   configuration frame 2, start data, and stop data. Return the frame requested
   by the peer, never a different configuration type as a substitute. Reject or
   close unsupported valid commands in one documented, deterministic way.
4. Do not emit data until a valid start command. Stop immediately after a valid
   stop command and reset the streaming state on disconnect.
5. Prebuild the stable header and configuration responses at startup. The data
   frame must exactly follow the configured format flags, PMU-block ordering,
   channel ordering, data rate, time base, nominal frequency, configuration
   count, and status settings.
6. Encode all multi-byte wire fields explicitly in network byte order. Do not
   cast native structs or rely on compiler layout, alignment, or host endian.
7. Cap every received frame at the standard's legal `FRAMESIZE` maximum. A
   malformed or oversized input must close only that connection and must not
   cause a growing receive buffer or an error-frame loop.

For every configured endpoint, calculate the exact configuration and data frame
sizes at startup. Reject any profile that exceeds the 16-bit `FRAMESIZE` limit;
never truncate or buffer an oversized frame. An eventual aggregate mode must
shard its logical PMUs across endpoints when its calculated frame size exceeds
that limit.

## Memory-first architecture

Use a compiled, single-process event-loop implementation. Rust with a direct
readiness/event library such as `mio` is the baseline. Do not use one process,
container, thread, task, timer, or heap-allocated measurement object per PMU.
One simulator process may own many TCP listeners and many logical PMUs; deploy
additional simulator containers only when CPU or network capacity requires it.

| Area | Required behavior |
| --- | --- |
| Startup config | Parse and validate YAML once, compile it to compact endpoint descriptors, then release the parsed YAML tree. Retain channel names only in their fixed-width encoded form needed for configuration frames. |
| Static frames | Store one immutable header/configuration byte sequence per endpoint. Do not rebuild strings, units, or configuration frames per client or per sample. |
| Data frames | Allocate one fixed-capacity data-frame template per endpoint, with precomputed offsets for timestamps, status, phasors, frequency, ROCOF, analog, and digital values. Mutate only dynamic bytes for a tick. |
| Sample model | Derive every value from `(sample_index, endpoint_id, pmu_id, channel_id, seed)`. Do not retain a history, a per-sample object, a random-event list, or waveform data. |
| Scheduling | Use a monotonic clock for wakeups. Derive the wall-clock C37.118 timestamp from a startup epoch plus `sample_index / data_rate`. When late, advance to the current index and count skipped ticks; never catch up by queueing old frames. |
| Receive path | Use one 4 KiB bounded command buffer per active TCP connection. Parse and consume frames in place. Reject any declared size above the accepted V1 command limit before growing or copying data; also enforce the standard's legal `FRAMESIZE` maximum. |
| Send path | Keep no application-level history or queue. An endpoint owns one fixed-capacity current data-frame buffer and a write offset. If it remains partially written at the next tick, increment a counter and close that client rather than retaining another frame. |
| Metrics and logs | Expose only fixed-cardinality aggregate counters. Do not label metrics by PMU, channel, client address, frame timestamp, or error text. Rate-limit malformed-frame and slow-client logs. |
| Reconfiguration | Do not hot-reload. A configuration change is applied by a controlled process restart, which releases the old compiled configuration before loading the new one. |

The intended application-memory shape is:

```text
base process + compiled endpoint configuration + endpoint frame buffers
  + active connections * (bounded receive buffer + one bounded send budget)
```

The TCP stack has its own kernel buffers. Keep the application send cap no
larger than one legal data frame by default, configure socket write watermarks
where the implementation permits it, and disconnect slow consumers instead of
trying to absorb their backlog in user space.

The 100-PMU release gate is deliberately measurable:

- With 100 listeners, one connected gateway client per endpoint, and the
  50 Hz baseline profile, the release build must remain below 24 MiB process
  RSS and 64 MiB container cgroup memory after warm-up for 15 minutes. Memory
  may grow by no more than 2 MiB after the first minute.
- With 100 idle listeners and no clients, the release build must remain below
  16 MiB process RSS and 32 MiB container cgroup memory for the same duration.
  It may grow by no more than 1 MiB after the first minute.
- Configure small socket buffers for this closed V1 protocol profile: request
  4 KiB receive and 4 KiB send buffers per accepted connection, then record the
  effective kernel values in the benchmark output. The application budgets stay
  fixed even if the kernel raises its minimum.
- The benchmark must read the container cgroup memory counter when available
  and fall back to process RSS otherwise. It must fail on a regression rather
  than raising the budget silently.

These are release targets, not a reason to reduce protocol correctness. Record
the exact profile, binary build mode, host kernel, and observed peak with every
benchmark so later changes can be compared honestly.

## Deterministic signal and time model

At startup, capture a realtime UTC epoch and a monotonic epoch. For each data
rate interval, derive `sample_index` from elapsed monotonic time. Build the
data-frame `SOC` and `FRACSEC` values from the corresponding UTC measurement
instant and encode time-quality bits exactly as required by the selected
profile and IEEE standard.

Use analytic, seeded value recipes rather than retained random state. For
example, a voltage, current, frequency-deviation, or ROCOF recipe can use a
nominal value plus a phase-shifted sine term derived from the sample index.
With the same profile and seed, a test must receive the same values and status
sequence. Fault injection must be expressed as bounded rules such as an index
range, modulo interval, or deterministic predicate, never as an unbounded list
of future events.

The packaged PMU profile must include three-phase voltage, three-phase current,
frequency deviation, and ROCOF. Frequency is a C37.118 deviation from the
configured nominal frequency. The gateway integration test must prove the
normalization rule:

```text
absolute_frequency_hz = nominal_frequency_hz + c37_frequency_deviation_hz
```

Use only finite values. If a scenario needs invalid, missing, test-mode,
synchronization, data-modified, or time-quality behavior, emit the applicable
C37.118 `STAT`/time-quality evidence according to the standard. Do not encode
those cases merely by changing the numerical value.

## Configuration contract

Use one human-edited YAML profile per simulator process. The schema is an
implementation input, not a mirror of wire fields, and must reject unknown
keys and invalid combinations before binding a socket. A representative shape
is:

```yaml
seed: 20260820
limits:
  max_clients_per_endpoint: 1
  max_rx_frame_bytes: 65535
  max_tx_queued_bytes: 65535
endpoints:
  - name: bay-01
    listen_port: 4712
    mode: pmu
    idcode: 1001
    data_rate_hz: 50
    time_base: 1000000
    pmus:
      - idcode: 1001
        station_name: WAMA-BAY-01
        nominal_frequency_hz: 50
        format: floating_point
        phasors:
          - name: VL1
            kind: voltage
            nominal: 230000.0
            variation: 400.0
        frequency_deviation_hz:
          nominal: 0.01
          variation: 0.002
        rocof_hz_per_s:
          nominal: 0.0
          variation: 0.001
```

The actual schema must additionally require every C37.118 configuration value
needed to encode a valid frame: protocol/version selection, channel order and
names, phasor type and unit/scaling information, analog and digital definitions
when used, format flags, nominal frequency, configuration count, status profile,
and data rate. Validate fixed-width field encodings, duplicate listener ports,
duplicate PMU IDCODEs within an endpoint, positive data rates, finite recipe
values, legal frame size, and all standard-specific constraints at startup.

`mode: pmu` contains one PMU block. `mode: pdc` may contain several blocks and
is deferred from V1. If implemented later, the compiler must make the chosen
mode explicit in the configuration response; it must not pretend a multi-PMU
aggregate is a single PMU.

### Fleet profile for the 100-PMU target

Do not maintain 100 near-identical YAML endpoint blocks. The shipping
integration profile must use a compact fleet declaration which the startup
compiler expands into fixed endpoint descriptors, then discards. The exact
schema can vary, but its behavior must be equivalent to:

```yaml
limits:
  max_logical_pmus: 100
  max_clients_per_endpoint: 1
  max_command_frame_bytes: 4096
  requested_socket_receive_buffer_bytes: 4096
  requested_socket_send_buffer_bytes: 4096
fleet:
  count: 100
  first_listen_port: 4712
  first_idcode: 1001
  station_name_prefix: WAMA-PMU-
  data_rate_hz: 50
  time_base: 1000000
  nominal_frequency_hz: 50
  format: floating_point
  signals:
    voltage_phasors: 3
    current_phasors: 3
    frequency_deviation_hz:
      nominal: 0.01
      variation: 0.002
    rocof_hz_per_s:
      nominal: 0.0
      variation: 0.001
```

For fleet index $i$ from 0 through 99, the compiler assigns port $4712 + i$,
IDCODE $1001 + i$, and a fixed-width station name such as `WAMA-PMU-001`.
It must reject an overridden port, IDCODE, channel definition, or encoded name
that collides or violates the C37.118 profile. Rare per-PMU signal or status
differences may be declared as bounded index-based overrides; they must compile
into the same fixed descriptors and cannot create runtime objects per sample.

The integration profile must expose no ports on the Docker host. A developer
who needs a host-side decoder maps one selected endpoint explicitly in a local
Compose override rather than publishing 100 ports for normal operation.

## Gateway normalization contract

The simulator and gateway need a checked mapping fixture. It maps the stable
source identity `(endpoint, PMU IDCODE, configured channel)` to a WAMA MRID and
unit. Keep that mapping in gateway configuration, not in the simulator's wire
encoder.

For each decoded C37.118 sample, the future gateway must set:

| WAMA field | Source |
| --- | --- |
| `timestamp_field` | C37.118 measurement time decoded from `SOC` and `FRACSEC` |
| `timestamp_gateway` | gateway receipt/normalization time |
| `timestamp_mccs` and Kafka record timestamp | gateway Kafka-publication time |
| frequency `double_value` | configured nominal frequency plus decoded C37.118 frequency deviation |

The timestamp ordering invariant remains
`timestamp_field <= timestamp_gateway <= timestamp_mccs`. A test profile may
need a bounded transport delay so that this can be exercised without relying on
clock coincidence.

The current `MCCSMeasurementValue.quality` fields do not retain every C37.118
status condition. Until the separate status-evidence contract is decided, the
gateway must not claim that `quality.valid` fully represents `STAT`. The
simulator must still expose explicit good and degraded status scenarios so the
gateway's eventual mapping can be tested rather than inferred.

## 100-PMU rollout plan

The simulator and the downstream infrastructure have separate capacity limits.
The simulator implementation is complete only when it passes its own memory
gates; it must not claim that the current Kafka and Druid assembly sustains the
full normalized load until that is measured.

| Stage | Scope | Expected C37.118 frames/s | Maximum normalized records/s | Exit gate |
| --- | --- | ---: | ---: | --- |
| 0 | Obtain approved standard evidence | 0 | 0 | Independent golden capture or decoder is available for V1 frame checks. |
| 1 | One PMU, simulator and wire codec | 50 | 400 | Header/configuration/start/data/stop exchange and golden-frame validation pass. |
| 2 | Ten PMUs, future gateway to Kafka | 500 | 4,000 | Stable MRID mapping, absolute-frequency conversion, timestamp ordering, and no simulator memory growth. |
| 3 | Twenty-five PMUs, 15-minute soak | 1,250 | 10,000 | No simulator drops except deliberate slow-client closure; Kafka producer latency and consumer lag remain bounded. |
| 4 | One hundred PMUs, simulator-only and gateway soak | 5,000 | 40,000 | Both 100-PMU memory gates pass; reconnecting one endpoint does not delay any other endpoint. |
| 5 | One hundred PMUs, full Kafka-to-Druid run | 5,000 | 40,000 | Druid has zero parse exceptions, the topic has no growing lag, and retained query data matches sampled Kafka records. |

Today `LiveMeasurement` has one Kafka partition and the Druid supervisor has
one task. That is adequate for the fixture but is not evidence of 40,000
records/s capacity. Stage 5 must record Kafka producer latency, topic bytes and
records rate, consumer lag, Druid ingestion lag, parse exceptions, task CPU,
and memory. If it fails, make a separate measured infrastructure decision about
`LiveMeasurement` partitions and matching Druid task parallelism before
re-running the stage. Do not change those topology settings merely because the
simulator profile exists.

Implementation order is therefore: codec and profile compiler; one-endpoint
TCP behavior; shared-tick 10-PMU fleet; 100-PMU memory benchmark; gateway
normalization integration; then Kafka/Druid capacity testing. Each stage adds
one falsifiable load boundary and avoids building a 100-container fleet before
the low-memory single-process design is proven.

## Delivery and verification checklist

The implementing agent must add a root-owned service directory at
`services/c37-118-simulator/` containing its `compose.yaml`, Dockerfile,
runtime profile(s), source, tests, and README. When the service is actually
added, update the root `docker-compose.yml` include list, root README, and the
repository service instructions in the same change. Keep it outside Forgejo
processor deployment scope and connect it only to the required root network.

Build a small static release image. A `scratch`-style runtime image is suitable
only if the binary can perform its own Docker health check; otherwise use the
smallest image that can. Image size is secondary to bounded resident memory.

Required tests are:

1. Unit tests for configuration validation, exact frame-size calculation,
   big-endian encoding, checksum validation, command parsing, partial reads,
   concatenated reads, start/stop state, and rejected malformed input.
2. Golden-frame interoperability tests against approved C37.118 captures or an
   independent decoder. Self-encoding followed by self-decoding is insufficient.
3. A TCP integration test that requests configuration, starts streaming, checks
   frame timestamps/status/order, stops streaming, and verifies that no data is
   sent afterward.
4. A simulator-to-gateway-to-Kafka test that decodes raw Protobuf and checks
   the MRID mapping, absolute-frequency conversion, all three timestamps, and
   valid status handling.
5. A slow-client test proving that the configured output budget is never
   exceeded and that a disconnected slow client cannot delay another endpoint.
6. The two 100-PMU memory benchmark profiles above, including the no-growth
  assertion and effective socket-buffer values.
7. The staged 1, 10, 25, and 100 PMU profiles above. The 100-PMU full-path
  test must make the Kafka/Druid capacity result visible; it may not quietly
  reduce the data rate or signal count to report success.

Do not add a direct Kafka publishing shortcut to make the end-to-end test pass.
That would bypass the C37.118 gateway behavior this service exists to test.

## Open decisions that do not block V1

- The approved gateway interoperability target, if a specific vendor PMU/PDC
  must be emulated beyond the strict V1 command set.
- The durable normalized representation for all C37.118 `STAT` and time-quality
  evidence required by the LFR use case.
- Any future need for UDP, TLS, multicast, multi-client fanout, dynamic
  configuration, or raw-frame capture.

Do not implement those extensions speculatively. Add them only with a testable
consumer requirement and a new bounded-memory budget.
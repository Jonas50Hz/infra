# C37.118 Simulator

`c37-118-simulator` is an opt-in, root-owned C37.118 TCP source simulator. It
has no Kafka, Common Format, Protobuf, Druid, SeaweedFS, Forgejo, or gateway
dependency. It is not a gateway and does not validate one.

The simulator runs one Rust event loop for 1 to 100 independent PMU listeners.
Each profile selects C37.118.2-2011 V2 or C37.118.2-2024 V3. V2 accepts
HDR/CFG-1/CFG-2/start/stop and emits fixed-point polar phasors, frequency
deviation, and ROCOF; V3 accepts capability/stream-configuration/start/stop.
The default V3 profile has one PMU on TCP/4712; matching V2 profiles use the
same internal ports and IDs.

Both wire subsets are derived from the authenticated local IEEE C37.118.2-2024
standard. V2 uses the genuine nibble `0010` (`0x02` data, `0x12` HDR, `0x22`
CFG-1, `0x32` CFG-2, and `0x42` command), not Annex-A's V1 illustrations. An
approved external capture or decoder remains required before interoperability
claims.

## Run

The simulator is excluded from normal startup. Start only it with its default
one-PMU profile:

```sh
docker compose --profile c37-118 up -d --build c37-118-simulator
```

Select a fleet profile with an absolute source path:

```sh
C37_118_SIMULATOR_PROFILE_SOURCE="$PWD/services/c37-118-simulator/profiles/ten-pmu.yaml" \
  docker compose --profile c37-118 up -d --force-recreate c37-118-simulator
```

Select V2 explicitly with a V2 profile. A client on the Compose network uses
the matching probe flag:

```sh
C37_118_SIMULATOR_PROFILE_SOURCE="$PWD/services/c37-118-simulator/profiles/ten-pmu-v2.yaml" \
  docker compose --profile c37-118 up -d --force-recreate c37-118-simulator

docker compose --profile c37-118 exec c37-118-simulator \
  c37-118-probe --wire-version 2 --host c37-118-simulator --first-port 4712 \
  --first-stream-id 1001 --count 10 --duration-seconds 1 --data-rate-hz 50
```

For the five-PMU Forgejo onboarding demonstration, use the dedicated V2 profile.
It exposes five internal endpoints at `172.30.0.10:4712` through
`172.30.0.10:4716`, with matching stream and PMU IDs `1001` through `1005`:

```sh
C37_118_SIMULATOR_PROFILE_SOURCE="$PWD/services/c37-118-simulator/profiles/five-pmu-v2.yaml" \
  docker compose --profile c37-118 up -d --force-recreate c37-118-simulator

docker compose --profile c37-118 exec c37-118-simulator \
  c37-118-probe --wire-version 2 --host 172.30.0.10 --first-port 4712 \
  --first-stream-id 1001 --count 5 --duration-seconds 1 --data-rate-hz 50
```

No C37.118 port is mapped to the host. A test client on the Compose network can
connect to `c37-118-simulator:4712` through `c37-118-simulator:4811`.

## Tests

The regular test target validates V2/V3 profile compilation, frame encoding,
bounded command parsing, and standalone TCP exchanges:

```sh
docker build --target test --file services/c37-118-simulator/Dockerfile .
```

The 25-PMU five-minute and 100-PMU 15-minute tests are manually armed only.
They build an isolated simulator and probe pair on a private Docker network,
set a simulator cgroup memory cap, and remove only labelled test resources.
They do not use `wama-infra` or start the root infrastructure stack.

```sh
C37_118_RUN_25_PMU=1 services/c37-118-simulator/scripts/test-25-pmu.sh
C37_118_RUN_100_PMU=1 services/c37-118-simulator/scripts/test-100-pmu.sh
C37_118_RUN_100_PMU=1 services/c37-118-simulator/scripts/test-100-pmu-idle.sh
```

Set `C37_118_WIRE_VERSION=2` together with an existing manual-arm variable to
run the V2 form of a soak. V3 remains the default and no 25- or 100-PMU test is
started automatically.

The 100-PMU runner requires cgroup memory accounting and fails on protocol
errors, insufficient data rate, memory-budget violations, or post-warm-up
growth above 2 MiB. Its result records the image ID, profile SHA-256, kernel,
Docker, and cgroup versions. It is intentionally not part of normal tests or
lifecycle validation.
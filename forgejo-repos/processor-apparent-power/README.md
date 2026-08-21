# Processor Apparent Power

This standalone Forgejo repository owns the `processor-apparent-power` service.
It is the stateful example that calculates apparent power for each PMU phase:

See the [apparent-power signal catalog](docs/pmu-signal-catalog.md) before
choosing or renaming a source signal.

$$
S = U \times I
$$

| Inputs | Outputs | Unit |
| --- | --- | --- |
| `voltage_l1`, `current_l1` | `apparent_power_l1` | VA |
| `voltage_l2`, `current_l2` | `apparent_power_l2` | VA |
| `voltage_l3`, `current_l3` | `apparent_power_l3` | VA |

The authored surface is [processor.yaml](processor.yaml),
[calculation.py](src/processor_apparent_power/calculation.py), and
[cases.yaml](cases.yaml). The generated adapter in
[processor.py](src/processor_apparent_power/processor.py) owns Kafka,
raw-Protobuf, key, timestamp, and cache mechanics. Update the calculations and
engineering cases rather than the generated adapter.

The cache keeps the latest finite voltage and current for each phase only when
`quality.valid=true`. It publishes after both values for the same phase are
available and within 2,000 ms. Cache state is in-process: after a restart it is
empty, then the PoC's once-per-second source repopulates it. Replays may publish
deterministic duplicates, which is expected for Kafka at-least-once delivery.

This calculates apparent rather than active power because the fixture does not
include phase angle or power factor. The shared runtime handles source/key
matching, Protobuf context, timestamps, and feedback protection.

Run this processor's tests from the repository root:

```sh
docker build --target test -f Dockerfile .
```

## Delivery

A pull request runs the test target. A trusted push to `main` publishes only
this processor image and deploys only this service into its dedicated
`/var/lib/wama-processor-apparent-power` root on the external `wama-infra`
network. The workflow never includes or changes the infrastructure Compose
project or the root-owned `pmu-gateway`.

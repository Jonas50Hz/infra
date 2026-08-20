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

The exact MRIDs are declared at the top of
[processor.py](src/processor_apparent_power/processor.py). Edit the
`PhaseCache.transform()` method for the engineering calculation, and update
[test_processor.py](tests/test_processor.py) with the expected behavior.

The cache keeps the latest numeric voltage and current for each phase only when
`quality.valid=true`. It publishes after both values for the same phase are
available. Cache state is in-process: after a restart it is empty, then the
PoC's once-per-second source repopulates it. Replays may publish deterministic
duplicates, which is expected for Kafka at-least-once delivery.

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

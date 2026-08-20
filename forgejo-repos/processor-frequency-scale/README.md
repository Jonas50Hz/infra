# Processor Frequency Scale

This standalone Forgejo repository owns the `processor-frequency-scale` service.
It is the smallest stateless example and converts fake-PMU frequency from hertz
to millihertz:

See the [frequency signal catalog](docs/pmu-signal-catalog.md) before choosing
or renaming a source signal.

$$
f_{mHz} = f_{Hz} \times 1000
$$

| Name in Python | MRID | Unit |
| --- | --- | --- |
| `frequency_hz` | `urn:wama:poc:pmu:bay-01:frequency` | Hz |
| `frequency_millihertz` | `urn:wama:poc:pmu:bay-01:frequency-millihertz` | mHz |

Edit [processor.py](src/processor_frequency_scale/processor.py) to change the
calculation or signal names, then update
[test_pipeline.py](tests/test_pipeline.py) with the expected engineering value.
The processor needs a numeric `double_value`; it does not require
`quality.valid` for this conversion.

The shared runtime accepts only the declared source/key pair, copies source
context and timestamp, writes the declared output key, and prevents the derived
measurement from feeding back into this processor. Replaying the same source
therefore produces the same derived record.

Run this processor's tests from the repository root:

```sh
docker build --target test -f Dockerfile .
```

## Delivery

A pull request runs the test target. A trusted push to `main` publishes only
this processor image and deploys only this service into its dedicated
`/var/lib/wama-processor-frequency-scale` root on the external `wama-infra`
network. The workflow never includes or changes the infrastructure Compose
project or the root-owned `pmu-gateway`.

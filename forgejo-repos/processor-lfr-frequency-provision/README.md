# Processor LFR Frequency Provision

This standalone Forgejo repository owns only the
`processor-lfr-frequency-provision` service. It calculates one configured
preferred frequency per closed UTC second from multiple PMU frequency and
voltage inputs, then publishes the derived raw-Protobuf
`MCCSMeasurementValue` back to `LiveMeasurement`.

The processor closes a source second $T$ at its configured $400$-$800$ ms
post-second deadline, retains a durable local state/outbox, and does not change
a closed result when a late record arrives. Its current configuration uses a
provisional mapping from Common Format `Quality` flags; read the
[input contract](docs/input-contract.md) before configuring real PMUs.

## Configuration

[config/lfr-config.yaml](config/lfr-config.yaml) is an illustrative complete
configuration, not a production default. Before deployment, replace its PMU
MRIDs, nominal voltages, frequency band, count boundaries, voltage thresholds,
and deterministic even-median tie break with approved engineering values.

Each PMU maps one frequency and one voltage source. The processor accepts only
finite `double_value` records with `timestamp_field`; it aggregates good
frequency samples and mean voltage per PMU per second, applies the worse count
or voltage class, discards `bad` PMUs, and selects the highest-class subset's
median. For an even subset, it chooses the central value farthest from 50 Hz;
the configured tie break resolves exact equal-distance cases.

The processor currently publishes no `Export` records. The root-owned IEC 104
exporter, the direct `processor-frequency-iec104-export` demonstration,
hold/resend semantics, heartbeat, RoCoF, and six-week durable audit evidence
remain separate work.

## Runtime

Quixstreams' DataFrame windows do not fire while input is idle. This processor
therefore uses its supported consumer/producer APIs in a deadline-driven loop
rather than a record-only `Application.run(stream)` transform. It polls no more
than every `LFR_POLL_INTERVAL_MS` milliseconds, persists engine state and
outbox intent before committing an input offset, and retries undelivered output
after restart. Replayed output is deterministic and may be delivered more than
once, consistent with Kafka at-least-once delivery.

The app-local Compose project persists this state in its `lfr-state` volume and
connects only to the pre-existing external `wama-infra` network.

Run the full seed validation from this repository root:

```sh
python3 -m unittest discover -s tooling-tests -v
docker build --target test --file Dockerfile .
```

## Delivery

A pull request runs validation only. A trusted push to `main` publishes this
processor image and deploys only `processor-lfr-frequency-provision` to its
marker-owned `/var/lib/wama-processor-lfr-frequency-provision` root. The
workflow does not deploy or modify the root Compose project, `pmu-gateway`, or
the root-owned IEC 104 services.
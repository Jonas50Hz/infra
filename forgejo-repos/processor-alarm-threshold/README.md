# Processor Alarm Threshold

This standalone Forgejo repository owns only the
`processor-alarm-threshold` service. It is a serialized direct
`kafka-python-ng` worker: it replays compacted raw-Protobuf `Masterdata` and
`Alarm` plus `AlarmEvaluationWatermark`, then consumes raw-Protobuf
`LiveMeasurement` records and maintains current desired active state on the
root-owned compacted `Alarm` topic.

[`config/alarm-threshold.yaml`](config/alarm-threshold.yaml) is strict reviewed
configuration. The initial policy is scoped to catalog
`wama-c37-118` and applies `frequency-over-50-1-hz` to all matching
`urn:wama:poc:pmu:*:frequency` signals with `double` / `frequency` / `Hz`
metadata. It activates at `f > 50.1` with `warning` severity.

The selector accepts only whole colon-delimited MRID segments and a whole
segment `*`; it is anchored implicitly by requiring every segment to match.
Unknown YAML fields, incompatible semantics, duplicate rule IDs, malformed
globs, non-finite thresholds, or unsupported rule operators are rejected.

## State

At startup the worker captures end offsets, folds `Masterdata`, `Alarm`, and
`AlarmEvaluationWatermark` through those offsets, and only then tails all three
compacted topics plus
`LiveMeasurement`. Empty eligible membership is a valid ready-idle operating
state. Scoped malformed, duplicate, or semantically incompatible Masterdata
keeps the processor out of its tailing loop and causes a replay retry. This
closest seed has no compatible health/readiness endpoint pattern, so this seed
does not add one.

Only an exact LiveMeasurement key/MRID match with a finite `double_value`, an
explicit `quality.valid=true`, no asserted disqualifying quality flag, a
`timestamp_mccs` no more than 30 seconds old, and a newer observed timestamp
changes state. Active evidence refreshes use the existing episode ID; a clear
emits a same-key tombstone. Removing a current scoped member also emits a
same-key tombstone for every active reviewed rule while retaining its evaluation
watermark. A watermark with no active Alarm is valid inactive state, so a clear
whose Alarm tombstone later compacts away cannot reactivate from an older
measurement after restart. Rule revision changes retain the same watermark.

For every newer qualifying eligible condition, the worker derives any Alarm
upsert or tombstone first, waits for every Alarm acknowledgement, writes and
waits for the matching exact-`timestamp_mccs` watermark, then commits the source
offset. An inactive no-transition evaluation therefore writes only a watermark.
The worker is intentionally serial and uses direct `kafka-python-ng`, not
transactions, Quixstreams, or a sidecar.

`WAMA_ALARM_EVALUATION_WATERMARK_TOPIC` defaults to
`AlarmEvaluationWatermark`. A pre-watermark active Alarm is rejected during
recovery unless `WAMA_ALARM_EVALUATION_WATERMARK_MIGRATION` is explicitly set to
`accept-forward-only-alarm-evaluation-watermark-v1`; that reviewed one-time fence
bootstraps only from the active Alarm's current evidence. This is deliberately
strict because a process crash after an Alarm acknowledgement but before its
watermark acknowledgement cannot be atomically repaired without Kafka
transactions. A crash after the watermark acknowledgement but before the source
offset commit is safe: replay sees the watermark and emits no duplicate Alarm
transition.

## One-Time Watermark Recovery

This trusted-PoC recovery acknowledgement is for a retained active `Alarm` topic
that predates `AlarmEvaluationWatermark`; it is forward-only and does not
reconstruct historical evaluations. A trusted maintainer performs these steps:

1. With the root PoC Kafka already running, run the root initializer once from
	`/home/jonas/infra` with the exact guard:

	```sh
	WAMA_ALARM_EVALUATION_WATERMARK_MIGRATION=accept-forward-only-alarm-evaluation-watermark-v1 \
	  docker compose run --rm kafka-init
	```

2. Manually dispatch **Validate and deploy processor-alarm-threshold** for
	`main`. Set `confirm_legacy_active_alarm_recovery` to exactly
	`CONFIRM_LEGACY_ACTIVE_ALARM_RECOVERY`. The workflow then passes only its
	semantic recovery flag to the app-local deployment helper; any blank or
	different value performs the ordinary deployment without recovery bootstrap.
	The helper supplies Compose an empty env file, so a marker-owned deployment
	root `.env` cannot affect this migration control. Normal deployments scrub
	`WAMA_ALARM_EVALUATION_WATERMARK_MIGRATION`; the confirmed one-time
	deployment is the sole route that injects the exact reviewed token.

3. Verify that the processor recovers the eligible active Alarm from its current
	evidence and writes the corresponding compacted `AlarmEvaluationWatermark`
	records. The processor must not report a pre-watermark active Alarm recovery
	rejection.

4. Let the next normal trusted push-to-`main` deployment run without the
	confirmation. It removes
	`WAMA_ALARM_EVALUATION_WATERMARK_MIGRATION` from the app-local Compose
	environment and ignores the deployment-root `.env`, so the one-time token
	cannot persist into later redeployments.

The workflow never deploys the root stack: `kafka-init` is deliberately the
separate, operator-run root step, while the workflow deploys only this
marker-owned `processor-alarm-threshold` application project.

## Alarm Contract

The copied [`contracts/alarm.proto`](contracts/alarm.proto) exactly mirrors the
root contract. Kafka keys use its required collision-free encoding:

`alarm/v1/<base64url-no-padding(UTF-8(rule_id))>/<base64url-no-padding(UTF-8(mrid))>`.

The root contract requires a canonical UUID episode ID but does not prescribe
how a producer derives one. This worker supplies a deterministic UUIDv5 from
that canonical alarm key and the triggering `LiveMeasurement` topic, partition,
and offset. Retrying the same uncommitted source record therefore reproduces
the same activation; a later clear and a later source offset create a new one.
No root contract file is changed.

## Validation

Run from this repository root:

```sh
python3 -m unittest discover -s tooling-tests -v
docker build --target test --file Dockerfile .
docker compose -f compose.yaml config --quiet
```

## Delivery

A pull request validates this repository only. A trusted push to `main` keeps
the existing `validate -> publish -> deploy` flow and deploys only this one
service from `/var/lib/wama-processor-alarm-threshold` through its
marker-owned application-local Compose project. It joins only the pre-existing
external `wama-infra` network and never deploys or changes root infrastructure.
# WAMA Processor Authoring Experience (Deferred Design)

Status: **design only; not implemented**.

This document defines a future authoring experience for electrical engineers
who have basic Python knowledge and need to create, review, test, and evolve
ordinary WAMA measurement processors. It does not change the current processor
repositories, Forgejo bootstrap, Kafka contracts, Docker Compose assembly, or
deployment behavior.

The design builds on the existing WAMA processor runtime. That runtime already
owns Kafka, raw-Protobuf decoding and encoding, declared-MRID filtering,
feedback prevention, output validation, timestamp preservation, and the
at-least-once delivery model. The future authoring layer should make those
platform concerns unavailable by default, leaving an engineer to describe
signals, engineering rules, expected examples, and explicit exceptional cases.

## Problem

The current `processor-frequency-scale` and `processor-apparent-power` seeds
prove that a calculation itself can be small and readable. For example,

$$
f_{mHz} = f_{Hz} \times 1000
$$

and

$$
S = U \times I
$$

are straightforward electrical rules. Creating a deployable processor still
requires an author to understand or copy several unrelated concerns:

- MRID declarations and Kafka key behavior.
- Common Format Protobuf serialization.
- Quixstreams application setup and consumer-group behavior.
- Dockerfile, contract generation, Compose, and Forgejo workflow files.
- Unit-test fixtures that exercise runtime rather than engineering behavior.
- The distinction between an ordinary derived value and a stateful,
  deadline-driven algorithm such as LFR preferred-frequency selection.

Copying a seed is useful for platform developers, but it is not a durable
authoring workflow for a Power User or electrical engineer. A copied seed can
also make platform runtime fixes difficult to roll out consistently.

## Goal

Provide a supported **standard processor** path in which normal work is
expressed as:

1. A reviewed declaration of inputs, outputs, units, and operating policies.
2. A small pure Python calculation, or a selected standard calculation
   template.
3. Executable engineering examples with expected outputs.

The platform generates or supplies the Kafka/Protobuf/runtime/build/deployment
plumbing. The authored repository remains a normal, reviewable Forgejo
repository with one processor service and one isolated application Compose
project.

The target result is that a unit conversion, threshold, three-phase apparent
power calculation, rolling aggregate, or simple alarm can be read and changed
as an engineering rule without requiring the author to edit Quixstreams or
Docker mechanics.

## Non-goals

This is not a proposal to:

- Replace Python with an opaque visual programming system.
- Guess signal meaning, units, quality interpretation, or safety limits.
- Let a processor create its own source Masterdata, MRIDs, Kafka topics, or
  deployment root.
- Let a processor repository deploy root infrastructure, a root-owned gateway,
  Druid, Kafka, or IEC 104 services.
- Generalize deadline-driven, durable, or safety-sensitive algorithms into a
  misleading no-code form.
- Introduce Kubernetes, Helm, ArgoCD, Confluent components, or a schema
  registry into this Compose PoC.

The Common Format `MCCSMeasurementValue` contract, raw-Protobuf transport,
plain Kafka KRaft topology, and per-processor Forgejo delivery boundary remain
unchanged.

## User Model

### Electrical engineer

An electrical engineer should be able to:

- Search a reviewed signal catalog by source, quantity, unit, and phase.
- Give selected signals local engineering names such as `voltage_l1` and
  `current_l1`.
- Declare a new output signal and its intended unit.
- Write or choose a calculation.
- State what happens when a value is invalid, missing, stale, or outside an
  engineering range.
- Run a small set of examples locally and see input/output values in familiar
  units.
- Submit a reviewable pull request.

An engineer should not need to construct Kafka keys, deserialize Protobuf
messages, recreate source context, choose a consumer group, or write a
deployment script for standard processors.

### Systemexperte

The Systemexperte remains responsible for approval of:

- Input signal identity and suitability.
- New derived-output MRIDs and their ownership.
- Units, ranges, quality policy, and any safety or operational consequence.
- The selected authoring mode and whether the standard runtime is sufficient.
- Deployment of an approved processor revision through the existing trusted
  Forgejo `main` flow.

### Platform engineer

The platform engineer owns:

- The versioned authoring SDK and its templates.
- Kafka/Protobuf/runtime semantics and security patches.
- Generator behavior, validation rules, and simulated-data tooling.
- The approved processor-registration workflow.
- Advanced-runtime support for algorithms outside the standard contract.

## Authoring Modes

The standard path should offer deliberately small, named modes rather than one
overloaded configuration language. Every mode publishes derived
`MCCSMeasurementValue` records to `LiveMeasurement` unless a later approved
mode has a different typed output contract.

| Mode | Suitable work | Platform behavior | Author edits |
| --- | --- | --- | --- |
| `formula` | Unit conversion, threshold, single-input transformation | One eligible input produces at most one output | Signal declaration, pure calculation, examples |
| `latest-values` | $U \times I$, power factor, phase-pair calculation | Caches declared latest eligible inputs and emits when a configured group is complete | Signal declaration, grouping policy, pure calculation, examples |
| `window` | Mean, min/max, RMS, fixed-duration aggregate | Bounded window state with explicit timestamp and close semantics | Signal declaration, window policy, aggregate calculation, examples |
| `custom` | LFR, durable outbox, deadline loops, bespoke typed output | No generated stream semantics beyond shared libraries and delivery guardrails | Full Python implementation plus focused tests |

`formula` and `latest-values` are the first implementation candidates. A
`window` mode must not be offered until its event-time, idle-input, late-data,
memory-bound, and replay semantics are fully specified and tested. The existing
LFR processor remains a `custom` processor because its closed-second timing,
state restoration, and persistent outbox are essential domain behavior.

## Proposed Repository Contract

Each generated standard-processor repository should retain the existing
one-service ownership boundary, but expose only a small authored surface:

```text
processor-apparent-power/
  processor.yaml              # signals, policy, SDK version, mode
  calculation.py              # electrical calculation only
  cases.yaml                  # executable engineering examples
  README.md                   # generated operating and review summary
  compose.yaml                # generated application-local service definition
  Dockerfile                  # generated/pinned platform build wrapper
  .forgejo/workflows/         # generated/pinned validation, publish, deploy flow
```

Generated files may be regenerated by a platform command, but local hand edits
must either be rejected by validation or clearly supported. The design should
avoid a half-generated state where a user edits a Dockerfile or workflow only
to have it silently overwritten on the next scaffold update.

The repository must pin a released SDK version. It must never fetch a mutable
runtime branch during build or deploy. A future platform-maintained Python
package and base image are preferable to copying the runtime and generated
Protobuf code into every new processor repository.

## Illustrative Authoring Surface

The following is illustrative syntax, not an accepted or implemented file
format.

### Signal and policy declaration

```yaml
api_version: wama.processor/v1alpha1
kind: latest-values
name: processor-apparent-power
sdk: 1.0.0

inputs:
  voltage_l1:
    signal: bay-01.voltage.l1
    expected_value: double
    expected_unit: V
  current_l1:
    signal: bay-01.current.l1
    expected_value: double
    expected_unit: A

outputs:
  apparent_power_l1:
    mrid: urn:wama:poc:pmu:bay-01:apparent-power-l1
    value: double
    unit: VA

latest_values:
  groups:
    - output: apparent_power_l1
      inputs: [voltage_l1, current_l1]
      maximum_age_ms: 2000
  eligibility:
    require_quality_valid: true
```

`signal` is a stable catalog reference selected from reviewed Masterdata, not
a free-text substitute for an MRID. Before build and deployment, validation
resolves it to an immutable input MRID from an approved catalog revision. The
resolved mapping becomes part of the build evidence and must be visible in the
pull request.

An output remains different: it needs an explicitly approved, unique MRID.
The authoring tool may propose a name, but it must not mint or reuse one
without an approval workflow.

### Calculation

```python
def apparent_power_l1(voltage_l1: float, current_l1: float) -> float:
    return voltage_l1 * current_l1
```

The generated adapter invokes this function only after it has applied the
declared mode's eligibility and grouping rules. The calculation receives only
the declared quantities, never raw Kafka messages or mutable runtime objects.
It returns an engineering value, not a Kafka record. The platform adapter
copies permitted Common Format context, assigns the declared output MRID,
preserves the triggering record timestamp, serializes raw Protobuf, and uses
the output MRID as the Kafka key.

The SDK should reject non-finite results by default. Any clamping, rounding,
deadband, fallback value, or alternate quality rule must be an explicit
declaration with a test case and reviewer-visible rationale.

### Engineering examples

```yaml
cases:
  - name: nominal phase one
    inputs:
      voltage_l1: {value: 230.4, valid: true, timestamp_ms: 1000}
      current_l1: {value: 318.2, valid: true, timestamp_ms: 1100}
    expect:
      apparent_power_l1: {value: 73313.28, unit: VA}

  - name: invalid current produces no result
    inputs:
      voltage_l1: {value: 230.4, valid: true, timestamp_ms: 1000}
      current_l1: {value: 318.2, valid: false, timestamp_ms: 1100}
    expect: no_output
```

The test runner should render a failure in terms of named signals and
engineering units before exposing implementation details. It should still
retain enough raw input/output evidence for a platform engineer to diagnose a
runtime defect.

## Signal Selection and Units

MRIDs identify measurements. They do not, by themselves, make a signal's unit
or engineering interpretation safe. The source Masterdata/catalog is the
authority for signal meaning, scalar kind, quantity, unit, and source identity.

The authoring experience should therefore provide a read-only signal selector
with these minimum fields:

| Field | Purpose |
| --- | --- |
| Logical catalog reference | Stable author-facing signal selection |
| Resolved MRID | Exact runtime identity used by Kafka filtering |
| Source and location | Avoid accidental cross-site or cross-device pairing |
| Quantity and unit | Validate formula suitability and reviewer intent |
| Common Format scalar kind | Prevent a `double` calculation from accepting a discrete value |
| Catalog revision | Make a processor build reproducible and auditable |

The first release may use an exported, reviewed catalog file rather than a UI.
The important rule is that a processor declares a stable reference and the
platform records exactly which source-catalog revision resolved it. It must not
infer that two values are compatible because their labels happen to look alike.

Unit conversion should be explicit. The platform may provide reviewed utility
functions such as `hz_to_millihz`, but it must not silently convert all
compatible-looking inputs. An explicit conversion makes engineering review and
later debugging much clearer.

## Input Eligibility and State Rules

The most important simplification is not the Python syntax. It is making
runtime behavior visible and constrained.

Every standard processor declaration must state or inherit a documented policy
for these questions:

| Concern | Required standard behavior |
| --- | --- |
| Kafka identity | Process only a record whose key is the exact UTF-8 source MRID |
| Feedback | Never treat a declared output MRID as an input to the same processor |
| Scalar type | Reject incompatible Common Format `oneof` values before the calculation |
| Quality | Default to explicit `quality.valid=true`; an exception requires review |
| Missing values | Produce no result unless a mode explicitly supports a reviewed fallback |
| Stale values | `latest-values` must expire cached inputs according to declared age |
| Timestamp | Preserve the triggering Kafka timestamp on output records |
| Delivery | Remain at least once; outputs and side effects must tolerate replay |
| Result validity | Reject non-finite values; do not silently turn an error into zero |

For `latest-values`, each output group needs a deterministic triggering rule.
The recommended first rule is: update the relevant cache entry for every
eligible input; when all members are present and within `maximum_age_ms`, emit
one result using the newest input's Kafka timestamp. This must be tested in
both arrival orders. A later input may produce a deterministic replay of the
same calculation, consistent with Kafka at-least-once delivery.

No standard mode should silently retain values across a service restart unless
its state persistence and recovery behavior are explicit. The initial
`latest-values` mode can use intentionally ephemeral state, as the current
apparent-power seed does. A processor that needs durable state, an outbox, or a
deadline must use `custom` until a separately reviewed standard mode exists.

## Generated Runtime and Delivery

The standard-processor generator should produce a repository that follows the
existing delivery boundaries:

1. A pull request runs declaration validation, generated tests, engineering
   examples, and the container test target.
2. A trusted `main` revision publishes only that repository's processor image.
3. Its deployment job synchronizes only that repository to its marked,
   dedicated deployment root.
4. Its application-local Compose project starts only its one `processor-*`
   service on the pre-existing external `wama-infra` network.

The generated workflow must retain the established `validate -> publish ->
deploy` ordering, pinned actions, protected package credential handling, and
repository-specific serialized deployment group. It must not run the root
`docker-compose.yml`, create infrastructure resources, or receive broader
Docker access than the existing trusted local-PoC processor deployment model.

## Processor Registration

Creating files alone cannot safely create a deployable processor. Today,
`forgejo-init` explicitly knows each private repository, its seed location,
runner connections, and its marker-owned deployment root. That allowlist is an
important control boundary.

A future `wama processor new` command should therefore have two separate
outcomes:

1. It generates a local candidate repository from an approved template.
2. It creates a platform-reviewable registration request containing the
   repository name, one service name, intended deployment root, template mode,
   owner, and signal/output approvals.

Only an approved platform registration should add the repository to the
bootstrap allowlist and arrange its isolated Forgejo CI/deploy connections. A
template must not self-register, mount a new host path, or gain deployment
privileges by merely existing in a developer checkout.

The exact implementation can be a reviewed registry file, a controlled
administrative command, or an equivalent platform workflow. It must preserve
the following invariants:

- One repository owns exactly one internal `processor-*` service.
- The root infrastructure repository is never pushed to Forgejo.
- Each deployment root is absolute, distinct, marker-owned, and outside the
  runner workspace.
- The processor connects only to the existing external `wama-infra` network.
- A registration cannot grant control over a root-owned gateway or
  infrastructure service.

## Simulation and Test Experience

The authoring tools should support an inner loop that does not need a Kafka
broker or Docker daemon:

```sh
wama-processor validate processor.yaml
wama-processor simulate --cases cases.yaml
```

These commands are proposed names only. Their required behavior is more
important than their spelling:

- Validate declared names, references, MRID uniqueness, output ownership,
  scalar kinds, policy ranges, and SDK compatibility.
- Resolve signal references against a specified reviewed catalog revision.
- Run engineering examples against the same adapter rules used in production.
- Show no-output, stale-input, invalid-quality, and non-finite-result cases
  clearly.
- Produce a machine-readable report that Forgejo CI can retain as build
  evidence.

Container tests remain required because they prove the pinned SDK, generated
Protobuf bindings, and production-like dependency set. Integration tests with
the local WAMA stack should be added only for processor types where the
contract or delivery behavior makes them worthwhile; they should not become a
mandatory barrier for changing a simple formula.

## Review Experience

A standard-processor pull request should make the engineering delta easy to
inspect. The generated summary should show:

- Added, removed, or changed input and output signals.
- Resolved MRIDs, source/catalog revision, scalar kinds, and units.
- Formula or template parameter changes.
- Quality, staleness, grouping, deadband, clamping, and fallback-policy
  changes.
- Added or changed engineering examples and their expected values.
- SDK version change, if any.

This summary is review assistance, not an approval replacement. A changed
signal reference, output MRID, or permissive quality policy must be visible to
a Systemexperte and preserve an auditable connection to the accepted catalog
revision.

## Error Handling and Observability

Standard processors should expose platform-owned counters and structured logs
without asking each author to implement them. At minimum, include counters for:

- Accepted input records by named input.
- Ignored records by reason: unknown MRID, Kafka-key mismatch, scalar mismatch,
  invalid quality, stale input, incomplete group, and feedback prevention.
- Successful outputs by named output.
- Rejected results by reason: exception, non-finite result, or policy breach.

Measurement values and raw Kafka payloads must not enter VictoriaMetrics.
Infrastructure telemetry remains in VictoriaMetrics; values remain in the
normal Common Format/Druid path. Logs must avoid leaking credentials and should
refer to signal names and MRIDs only as allowed by the existing data model.

## Advanced Escape Hatch

The standard path must have a clearly documented exit before authors work
around missing semantics in a formula template. Use `custom` when a processor
needs any of the following:

- A durable state store or persistent outbox.
- A deadline that must fire while Kafka input is idle.
- Event-time window closure, late-data policy, or watermark semantics not
  supplied by a reviewed standard mode.
- More than one typed output contract, such as a deliberate `ExportRecord`.
- External side effects, protocol interactions, or cross-topic transactional
  reasoning.
- A domain-specific algorithm whose intermediate evidence is itself material
  to correctness or audit.

The custom route should still reuse platform contract helpers, deployment
guardrails, test conventions, and review summary conventions where practical.
It is an escape hatch from constrained authoring semantics, not an escape hatch
from WAMA security, data-contract, or delivery boundaries.

## Rollout Plan

The future implementation should proceed in small, reversible increments.

### Phase A: Freeze and prove the author contract

- Write acceptance tests that reproduce the current frequency-scale seed as a
  `formula` processor.
- Write acceptance tests that reproduce the current apparent-power seed as a
  `latest-values` processor, including both input orders, invalid quality,
  stale inputs, and restart behavior.
- Decide the initial manifest schema and catalog-reference representation.
- Publish an ADR for quality defaults, output-MRID approval, and ephemeral
  `latest-values` state.

### Phase B: Build local author tooling

- Implement manifest validation and simulation against executable cases.
- Package the common runtime as a versioned platform SDK.
- Add generated repository scaffolding and a deterministic generated-file
  ownership policy.
- Produce CI review summaries from the manifest and cases.

### Phase C: Pilot with existing examples

- Migrate `processor-frequency-scale` without changing its runtime behavior.
- Migrate `processor-apparent-power` without changing its runtime behavior.
- Compare generated outputs, test behavior, Docker image behavior, and
  deployment guard behavior against the existing seeds.
- Keep the original seeds available until the migration proof is accepted.

### Phase D: Registration and operational hardening

- Implement the approved processor-registration mechanism.
- Prove new generated repositories receive only their intended isolated
  deployment root and runner connections.
- Add SDK upgrade guidance, compatibility testing, deprecation policy, and
  rollback procedure.

### Phase E: Consider additional modes

- Evaluate `window` only after a concrete use case supplies complete timing,
  replay, memory, and late-data requirements.
- Keep LFR and other durable/deadline-sensitive work on `custom` until a mode
  can make its semantics more explicit, not less.

## Acceptance Criteria for the First Usable Version

The first implementation is ready for a limited pilot only when all of the
following are true:

- A new formula processor can be authored without importing Quixstreams or
  Protobuf APIs.
- A new latest-values processor can express declared inputs, grouping, quality,
  maximum age, output, and engineering examples without editing the runtime.
- The same adapter semantics run in simulation, container tests, and deployed
  images.
- Signal references resolve deterministically to a reviewed catalog revision.
- Output MRIDs are explicit, unique, and approval-gated.
- Generated processor delivery has no capability to alter root infrastructure
  or a root-owned gateway.
- SDK versions are pinned and reproducible.
- The two existing simple seeds can be reproduced with behaviorally equivalent
  output, timestamp, replay, and deployment-guard tests.
- An author can see why a result was not emitted without reading Kafka or
  Protobuf implementation code.

## Open Decisions

The following decisions should be made before implementation rather than
embedded accidentally in a generator:

1. What is the stable catalog-reference format and how is catalog revision
   selection approved?
2. Which organization owns derived-output MRID allocation and lifecycle?
3. Which unit vocabulary and compatibility rules are authoritative?
4. Is an output's quality copied, recomputed, or declared by a constrained
   policy for each standard mode?
5. What is the exact `latest-values` cache key for multi-source or
   multi-device calculations?
6. How are stale cached values treated after a processor restart and during
   source outages?
7. Where will the versioned SDK and base image be published, and what is the
   supported upgrade/rollback policy?
8. What review evidence is required before processor registration and
   deployment privileges are granted?
9. Which future calculation types genuinely need a new standard mode, rather
   than a well-tested custom processor?

## References

- [Overview and processes](00-overview.md)
- [Architecture and technology choices](01-architecture.md)
- [Data flow and Kafka contracts](02-dataflow-contracts.md)
- [PoC Compose plan](03-poc-compose-plan.md)
- [LFR per-second frequency provision](04-lfr-frequency-provision.md)
- [Forgejo processor repository seeds](../../forgejo-repos/README.md)
- [Frequency-scale seed](../../forgejo-repos/processor-frequency-scale/README.md)
- [Apparent-power seed](../../forgejo-repos/processor-apparent-power/README.md)
(exp_alarm_incident_management_research)=

```{meta}
:description: Research comparing alarm and incident-management systems for the WAMA Kafka Alarm boundary.
```

# Alarm and incident management research

This Explanation compares existing alarm and incident-management systems for a
small WAMA proof of concept. Alerta is the selected PoC alarm manager;
the `Alarm` contract and state/lifecycle decisions are settled. The root-owned
Alerta, Mailpit, isolated PostgreSQL, and compacted-Alarm ingress implementation
uses the pinned Alerta 9.1.0 and Mailpit 1.31.0 images.

## Decision context

This comparison uses the following boundary:

- Only live processors originate domain alarms. They publish canonical,
  raw-Protobuf records to Kafka topic `Alarm`.
- One root-owned adapter consumes and decodes `Alarm`, then maps it into the
  selected product. A candidate product does not read WAMA Kafka directly.
- An operator needs a UI that records a domain-alarm acknowledgement. A silence,
  mute, downtime, inhibition, or notification pause suppresses delivery; it
  does not acknowledge, own, or resolve the alarm.
- The first proof needs a local SMTP test inbox. It does not need a dedicated
  WAMA audit ledger or WAMA account system.
- Kubernetes is out of scope. The current plain-Kafka Compose topology remains
  intact, and alarm payloads must not go to VictoriaMetrics. The
  [data-flow contract](../reference/wama-data-flow-contracts.md) reserves that
  store for infrastructure telemetry rather than Kafka message records.

The implementation verified the selected image digests locally and inspected
the Alerta 9.1.0 plugin, Mailer, PostgreSQL, and native status-route code. The
first-party links below remain the authoritative source for release support,
licensing, and future product upgrades.

## Candidate comparison

| Candidate | Domain acknowledgement versus silencing | Raw-Protobuf Kafka input | SMTP fit | Compose footprint | PoC position |
|---|---|---|---|---|---|
| [Alerta](https://docs.alerta.io/) | Evaluate its alert acknowledgement, assignment, and closure lifecycle as the operator record. Suppression must remain distinct from that lifecycle. | A root-owned adapter is required to decode `Alarm` and call the selected Alerta API/event shape; no native WAMA raw-Protobuf Kafka consumer is assumed. | Verify the released email notification plug-in or relay path against the local SMTP inbox. | Alerta API/UI plus its required datastore, with the adapter as a separate root-owned service. | **Selected PoC alarm manager and smallest self-hosted candidate for this need.** It is incident-focused without importing a full monitoring estate. |
| [Zabbix](https://www.zabbix.com/documentation/current/en/manual/acknowledgment) | Problem acknowledgement is the relevant operator action; maintenance or notification suppression must not stand in for it. | The adapter must translate raw-Protobuf Kafka records to the selected Zabbix event, trapper, or API boundary. | Its email media type is a plausible SMTP test-inbox path; verify the selected version. | Server, database, and web UI, then adapter. | Functional but broad: discovery, polling, host inventory, and monitoring administration exceed this narrow alarm boundary. |
| [Icinga](https://icinga.com/docs/icinga-2/latest/doc/16-cli/#acknowledge-host-or-service-problems) | A service/host acknowledgement can record operator handling; downtime and notification suppression are separate controls. | The adapter must emit a supported passive-check or event representation after decoding Kafka. | Verify the notification command and SMTP configuration against the local inbox. | Icinga 2, web UI, chosen persistence/reporting pieces, then adapter. | Functional but check-oriented. Its monitoring-object model is larger than a processor-originated alarm UI needs. |
| [OpenNMS](https://docs.opennms.com/horizon/latest/operation/deep-dive/alarms.html) | Evaluate its alarm acknowledgement/clear state separately from any reduction, escalation, or suppression behaviour. | The adapter must post a selected event/alarm representation; no direct raw-Protobuf WAMA Kafka input is assumed. | Verify the released notification and SMTP path. | OpenNMS service and database, normally with its web UI, then adapter. | Not preferred for the first proof because it introduces a comparatively large network-management platform and inventory model. |
| [Prometheus Alertmanager](https://prometheus.io/docs/alerting/latest/alertmanager/) | Routing, grouping, inhibition, and silences are useful notification controls, but do not provide the required domain-alarm acknowledgement record. | The adapter must translate `Alarm` to the supported HTTP alert payload; it cannot pass raw Protobuf through unchanged. | Its `email_configs` are a compact SMTP test-inbox candidate. | Alertmanager plus adapter; a rule evaluator is needed only if this proof also generates Prometheus alerts. | Smallest notification-only option, but insufficient by itself for the required acknowledgement UI. |
| [Grafana Alerting](https://grafana.com/docs/grafana/latest/alerting/) | Alert rules and notification silences/pauses must not be treated as a processor-domain acknowledgement workflow. | A root-owned bridge is required; Grafana Alerting does not become a raw-Protobuf Kafka `Alarm` consumer. | Grafana email contact points may fit the local inbox after version-specific verification. | Extends the root-owned Grafana deployment and its alerting persistence/configuration, plus adapter. | Reuses an existing UI, but risks mixing infrastructure/measurement dashboards with alarm operations and still needs an acknowledgement design. |
| [PagerDuty](https://developer.pagerduty.com/docs/events-api-v2/overview/) | Its hosted incident acknowledgement and resolution model is the relevant comparison; escalation or suppression controls remain separate. | The adapter must translate records to the Events API. It is not a direct Kafka/raw-Protobuf consumer. | It does not provide the desired local Compose SMTP-inbox path; verify any email-integration alternative separately. | Local adapter only, but a hosted account, service, routing key, and outbound network path are required. | Strong managed incident workflow, but not appropriate for the initial local proof because it introduces account and credential dependencies. |

The first-party pages in the table identify the product surfaces to verify. In
particular, confirm that the named acknowledgement is durable and visible in
the UI/API, that silencing is a separate action, and that the supplied SMTP
configuration can address the chosen local test inbox. Do not infer these
properties from a similarly named feature in another release or edition.

## Recommendation and proof boundary

Alerta is the selected PoC alarm manager and the smallest self-hosted candidate
that plausibly combines a focused operator alarm UI with a distinct
acknowledgement state. The root implementation replays compacted `Alarm` state
through captured Kafka end offsets, reconciles only tagged and attributed WAMA
alerts, and uses Alerta's native status route to close tombstoned episodes.
Alertmanager has the smaller notification
footprint, but its silences
do not satisfy the acknowledgement requirement. Zabbix, Icinga, and OpenNMS
can model acknowledgement, but carry substantially more monitoring platform
than this PoC requires. Grafana Alerting is best treated as a notification/UI
adjacent option, and PagerDuty belongs in a later managed-service evaluation.

A bounded proof establishes only these facts for the selected release:

1. The root-owned adapter consumes raw-Protobuf `Alarm` records by manual
  partition assignment and captured end offsets, never consumer-group offsets.
2. Alerta records acknowledgement separately from close; fixed native severity
  preserves acknowledgement across WAMA evidence and rule-revision refreshes.
3. A first active WAMA episode reaches the local Mailpit inbox. Delivery is
  best-effort only: no durable retry queue or outbox is claimed.
4. Alarm payloads stay out of VictoriaMetrics, no WAMA account/audit subsystem
  is added, and the Compose addition introduces no Kubernetes artifact.

Other managed incident services can be assessed later with the same PagerDuty
questions: whether their account, credential, data-residency, and local-SMTP
constraints are acceptable. They do not remove the need for the root-owned
Kafka adapter.

## Release-specific primary-source verification targets

- **Alerta:** [documentation](https://docs.alerta.io/),
  [API reference](https://docs.alerta.io/api/reference.html), and
  [official source](https://github.com/alerta/alerta).
- **Zabbix:** [problem acknowledgement](https://www.zabbix.com/documentation/current/en/manual/acknowledgment),
  [email media type](https://www.zabbix.com/documentation/current/en/manual/config/notifications/media/email),
  and [container installation](https://www.zabbix.com/documentation/current/en/manual/installation/containers).
- **Icinga:** [acknowledgements](https://icinga.com/docs/icinga-2/latest/doc/16-cli/#acknowledge-host-or-service-problems),
  [notifications](https://icinga.com/docs/icinga-2/latest/doc/03-monitoring-basics/#notifications),
  and [official containers](https://github.com/Icinga/docker-icinga2).
- **OpenNMS:** [alarms](https://docs.opennms.com/horizon/latest/operation/deep-dive/alarms.html),
  [notifications](https://docs.opennms.com/horizon/latest/operation/deep-dive/notifications.html),
  and [official Compose project](https://github.com/OpenNMS/opennms-docker-compose).
- **Prometheus Alertmanager:** [overview](https://prometheus.io/docs/alerting/latest/alertmanager/)
  and [configuration, including email](https://prometheus.io/docs/alerting/latest/configuration/#email_config).
- **Grafana Alerting:** [alerting documentation](https://grafana.com/docs/grafana/latest/alerting/),
  [email contact points](https://grafana.com/docs/grafana/latest/alerting/configure-notifications/manage-contact-points/integrations/configure-email/),
  and [Docker installation](https://grafana.com/docs/grafana/latest/setup-grafana/installation/docker/).
- **PagerDuty:** [Events API v2](https://developer.pagerduty.com/docs/events-api-v2/overview/)
  and [REST API reference](https://developer.pagerduty.com/api-reference/).
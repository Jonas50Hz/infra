# VictoriaMetrics

`victoria-metrics` is the single-node operational metrics store for this PoC.
It directly scrapes itself, Grafana, node-exporter, and cAdvisor every 15
seconds using [prometheus.yml](prometheus.yml), retains metrics for one month,
and persists them in the root `victoria-metrics-data` volume.

Its API and VMUI are bound to `127.0.0.1:8428`. The service is not used for
Common Format records, Kafka data, raw objects, events, or alarms.
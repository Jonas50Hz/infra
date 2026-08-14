# Kafka Exporter

`kafka-exporter` collects Kafka broker availability, WAMA-topic partition and
offset metrics, and consumer-group lag through Kafka's internal `kafka:9092`
endpoint. It exposes Prometheus-format metrics only to the Compose network on
port 9308; VictoriaMetrics scrapes that endpoint directly.

The root Compose assembly starts the exporter only after Kafka is healthy and
the idempotent topic initializer has completed. This makes its topic metrics a
readiness signal for the initialized WAMA contract rather than merely broker
reachability.

The collector is limited to the current WAMA topic contract:
`LiveMeasurement`, `MeasurementSession`, `Alarm`, `Export`, `Masterdata`,
`Schema`, and `Blobmeta`. Update the filter in [compose.yaml](compose.yaml)
when the contract changes.

It does not enable Kafka JMX or publish a new Kafka listener. Container CPU,
memory, and network usage remain available through cAdvisor.
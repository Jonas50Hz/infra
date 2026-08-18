# Druid supervisor initializer

`druid-init` is a root-owned one-shot helper. After Kafka topic initialization
and Druid Router health, it creates or updates the `live_measurements` Kafka
supervisor with `skipRestartIfUnmodified=true`.

The immutable supervisor specification reads raw-Protobuf
`MCCSMeasurementValue` records from `LiveMeasurement` using the descriptor built
into the Druid image. It uses the Kafka record timestamp as `__time`, preserves
the individual typed value fields and quality flags, and explicitly disables
rollup.

Run the focused unit suite:

```sh
docker compose run --rm --no-deps druid-init \
  python -m unittest discover -s /app/tests -v
```

Normal initialization never resets Kafka offsets. A clean `druid-data` volume
starts from the earliest available `LiveMeasurement` record; persistent restarts
reuse Druid's stored supervisor offsets and metadata.
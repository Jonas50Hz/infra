# Processor Weather Map

This standalone Forgejo repository owns `processor-weather-map`. Every five
minutes it requests current weather for Berlin from the Open-Meteo forecast API
and publishes three raw-Protobuf `MCCSMeasurementValue` records to Kafka topic
`LiveMeasurement`.

| Signal | MRID | Unit |
| --- | --- | --- |
| Temperature at 2 m | `urn:wama:poc:weather:berlin:temperature-2m-c` | °C |
| Wind speed at 10 m | `urn:wama:poc:weather:berlin:wind-speed-10m-kmh` | km/h |
| Wind direction at 10 m | `urn:wama:poc:weather:berlin:wind-direction-10m-degrees` | degrees |

Each record has a numeric `double_value`, an explicit valid quality flag, a
UTC `timestamp_mccs`, and its MRID as the Kafka key. Open-Meteo failures or
incomplete/non-numeric replies are logged and retried on the next five-minute
interval; no synthetic weather values are published.

Run this processor's tests from the repository root:

```sh
docker build --target test -f Dockerfile .
python3 -m unittest discover -s tooling-tests -v
```

## Delivery

A pull request runs the processor and deployment-guard tests. A trusted push to
`main` publishes only this processor image and deploys only this service into
its dedicated `/var/lib/wama-processor-weather-map` root on the external
`wama-infra` network. The workflow never includes or changes the infrastructure
Compose project or Grafana dashboard provisioning.

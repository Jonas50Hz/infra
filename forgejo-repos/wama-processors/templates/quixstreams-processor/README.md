# Quixstreams Processor Template

This inactive processors-repository scaffold is copied into
`processors/processor-<name>/`. It connects only to the external `wama-infra`
network and reaches Kafka as `kafka:9092`; it has no cross-project
`depends_on` entries.

Edit [`src/processor_template/pipeline.py`](src/processor_template/pipeline.py)
to implement a processor. The starter `transform()` returns `None`, so it does
not publish data until an author changes it. Run its test target from the
processors repository root:

```sh
docker build --target test -f templates/quixstreams-processor/Dockerfile .
```
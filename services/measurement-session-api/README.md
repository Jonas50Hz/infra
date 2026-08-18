# Measurement Session API

`measurement-session-api` consumes raw-Protobuf `MeasurementSession` records
from Kafka and materializes an immutable, service-owned catalog in the prepared
PostgreSQL database. Kafka remains the source of truth: the catalog
only accepts identical replays for a session ID and stops visibly on a
conflicting replay.

The API has no host port. It provides anonymous read-only list, detail, and
artifact endpoints to the companion browser. Before returning a detail or
artifact, it downloads and SHA-256-checks the cataloged manifest, validates its
session namespace, and compares artifact length and digest metadata with
SeaweedFS. Artifact bytes are streamed through this API; it never returns S3
credentials, object-store URLs, or presigned links.

For the PoC `waveform` CSV, the API also verifies that every declared
measurement is present in strictly increasing timestamp order from the session
start through its end. Incomplete immutable legacy records remain stored as
evidence but are hidden from catalog listings and refused for detail/download.
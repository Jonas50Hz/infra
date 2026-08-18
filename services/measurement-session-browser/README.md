# Measurement Session Browser

`measurement-session-browser` is a disposable static browser for the trusted
PoC. It publishes `http://<host-ip>:3002` by default and proxies its same-origin
`/api/` path to the internal catalog API. It has no volume, database, Kafka, or
S3 credentials.

Session listings, detail metadata, and artifact downloads use the API only. The
browser cannot construct object-store URLs and never receives a presigned link.
Access is anonymous and read-only for this trusted local environment; do not
expose the browser outside that scope.

The catalog presents only sessions whose waveform CSV is complete: its row count
must match the declared measurement count and span the full start/end interval.
Incomplete immutable records are not presented as downloadable sessions.

Downloads are named `<source>_<UTC-start-date>_<artifact>.<extension>`, for
example `urn-wama-poc-pmu-bay-01_2026-08-18_waveform.csv`. The browser uses the
API-provided name so it matches the verified attachment response.
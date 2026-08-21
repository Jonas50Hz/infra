# Grafana

`grafana` exposes WAMA dashboards on port 3001 and persists its state in the
root `grafana-data` volume. Its local image installs the signed community
`grafadruid-druid-datasource@1.7.0` and `trino-datasource@1.1.1` plugins in
`/opt/grafana/plugins`, outside the state volume. The versions are pinned because
they support the repository's Grafana 12.1.0 image.

The service provisions three datasources and read-only dashboard folders from the
tracked files in this directory:

- **VictoriaMetrics** remains the default datasource for **WAMA Infrastructure**
	dashboards: host, Compose-container, and Kafka operations telemetry.
- **Druid** connects internally to `http://druid:8888` for **WAMA Measurements**.
	**WAMA PMU Live Measurements** plots valid phase voltages, phase currents,
	frequency, and ROCOF with separate unit-safe axes and recent PMU timestamp
	evidence.
- **Trino** connects internally to `http://trino:8080` for **WAMA Measurements**.
	**WAMA Measurement Sessions** selects one immutable `blob_id` and queries its
	registered Iceberg artifact without parsing object paths.

Dashboard edits belong in the tracked JSON files rather than the UI. Common
Format measurements remain in Druid and are never copied to VictoriaMetrics.

The tracked [grafana.env](grafana.env) file supplies intentionally public local
PoC credentials, so no first-start setup is required. The initial Grafana
administrator username and password are both `wama-admin`.

Anonymous access and self-registration are disabled. Set
`GRAFANA_ROOT_URL=http://<host-ip>:3001/` when Grafana needs to generate URLs
for a LAN address. The administrator password is applied only while the
`grafana-data` volume is first initialized; rotate an existing password through
Grafana rather than replacing `grafana.env`.

Validate the complete PMU dashboard path locally:

```sh
scripts/test-grafana-pmu-dashboard.sh
```

No Grafana alert rules are provisioned in this PoC. The Trino datasource and
dashboard remain read-only and do not grant Grafana access to the internal
Iceberg writer.
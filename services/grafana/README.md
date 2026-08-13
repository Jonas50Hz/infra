# Grafana

`grafana` exposes the WAMA infrastructure dashboards on port 3001 and persists
its state in the root `grafana-data` volume. It provisions the VictoriaMetrics
datasource and dashboard files from this directory; dashboard edits belong in
the tracked JSON files rather than the UI. The provisioned views cover host
infrastructure, Compose containers, and Kafka operations.

Before the first start, create the ignored local credential file:

```sh
[ -e grafana.env ] || install -m 600 grafana.env.example grafana.env
```

The initial Grafana administrator username and password are both `wama-admin`.

Anonymous access and self-registration are disabled. Set
`GRAFANA_ROOT_URL=http://<host-ip>:3001/` when Grafana needs to generate URLs
for a LAN address. The administrator password is applied only while the
`grafana-data` volume is first initialized; rotate an existing password through
Grafana rather than replacing `grafana.env`.
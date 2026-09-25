# WAMA local access links

Open this page from the Docker host after starting the root WAMA Compose stack.
Each **Open** link uses the default local port mapping and is ready to click in
a Markdown preview. Values overridden through the root `.env` file may use
different ports or credentials.

## Browser interfaces

| Service | Link | Default access |
| --- | --- | --- |
| Kafka UI | [Open Kafka UI](http://localhost:8080) | No login |
| Forgejo | [Open Forgejo](http://localhost:3000) | `wama-admin` / `wama-admin` |
| Grafana | [Open Grafana](http://localhost:3001) | `wama-admin` / `wama-admin` |
| SeaweedFS admin UI | [Open SeaweedFS admin](http://localhost:23646) | `wama-admin` / `wama-admin` |
| SeaweedFS S3 API | [Open S3 endpoint](http://localhost:8333) | S3 key credentials required |
| Mailpit | [Open Mailpit](http://localhost:8025) | No login |
| Alerta | [Open Alerta](http://localhost:18081) | No login |
| IEC 104 live monitor | [Open IEC 104 monitor](http://localhost:3003) | No login |
| Measurement-session request | [Open request page](http://localhost:3004) | No login; loopback-only |
| Measurement-session CSV export | [Open CSV-export endpoint](http://localhost:3005) | No login; loopback-only |
| Trino | [Open Trino](http://localhost:8085) | No login |
| Druid Router and web console | [Open Druid](http://localhost:8888) | No login |
| VictoriaMetrics API and VMUI | [Open VMUI](http://127.0.0.1:8428/vmui/) · [Open API root](http://127.0.0.1:8428/) | No login; loopback-only |

## Grafana dashboards

| Dashboard | Link |
| --- | --- |
| WAMA Infrastructure Overview | [Open dashboard](http://localhost:3001/d/wama-infrastructure) |
| WAMA Compose Containers | [Open dashboard](http://localhost:3001/d/wama-compose-containers) |
| WAMA Kafka Operations | [Open dashboard](http://localhost:3001/d/wama-kafka-operations) |
| WAMA Measurement Sessions | [Open dashboard](http://localhost:3001/d/wama-measurement-sessions/wama-measurement-sessions) |
| WAMA Gateway Fleet | [Open dashboard](http://localhost:3001/d/wama-gateway-fleet) |

The Gateway Fleet page links to the generated per-gateway dashboards. Those
pages appear only after active `Masterdata` has been provisioned.

## Default local credentials

These intentionally public credentials apply to a fresh local PoC volume. A
previously initialized Forgejo or Grafana volume retains its configured
password.

| Interface | User or access key | Password or secret |
| --- | --- | --- |
| Forgejo | `wama-admin` | `wama-admin` |
| Grafana | `wama-admin` | `wama-admin` |
| SeaweedFS admin UI | `wama-admin` | `wama-admin` |
| SeaweedFS S3 API | `wama-s3-admin` | `wama-s3-admin-secret` |

## Non-browser connection endpoints

These endpoints are published for clients rather than browser navigation.

| Service | Connection endpoint |
| --- | --- |
| Kafka bootstrap server | `localhost:29092` |
| PostgreSQL | `postgresql://wama@localhost:5432/wama` |
| Forgejo Git over SSH | `ssh://git@localhost:2222/wama-admin/<repository>.git` |
| IEC 60870-5-104 controlled station | `tcp://127.0.0.1:2404` |

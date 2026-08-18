# Forgejo Repository Seeds

This directory contains tracked seed content for repositories that may be
initialized and pushed to Forgejo. Each child directory is a separate
repository boundary. The parent `infra` repository is infrastructure-only and
must never be added as a Forgejo remote or pushed to Forgejo. Forgejo is
reserved for internal processor deployment and a deliberately declared
gateway-deployment test, not for any other infrastructure or repository asset.

[`wama-processors/`](wama-processors/) is the processors repository seed. It
owns internal processor code, its Forgejo workflow, processor-only Compose
files, and processor deployment state. `forgejo-init` seeds it into a private
Forgejo repository only when the remote has no refs; an existing nonempty
private repository is left unchanged. It may contain a gateway only for an
explicit gateway-deployment test. The parent `infra` repository retains all
other assets, including the current `pmu-gateway` and every infrastructure
service.
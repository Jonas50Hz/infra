# Forgejo Repository Seeds

This directory contains tracked seed content for repositories that may be
initialized and pushed to Forgejo. Each child directory is a separate
repository boundary. The parent `infra` repository is infrastructure-only and
must never be added as a Forgejo remote or pushed to Forgejo.

[`wama-applications/`](wama-applications/) is the application repository seed.
It owns processor code, its Forgejo workflow, application-only Compose files,
and application deployment state. Initialize Git and add a Forgejo remote only
from inside that directory.
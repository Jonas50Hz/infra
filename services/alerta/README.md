# Alerta

`alerta` is the root-owned alarm incident UI and API. Its custom image derives
from the pinned
`alerta/alerta-web:9.1.0@sha256:693845fb8a95a483c8c9d560786f661f5b6198e5ea0baa6ddf368a7e53c94f39`
release and installs the local `alerta.plugins` entry-point package.

The mounted `config/alertad.conf` explicitly configures the isolated
`alerta-postgres` database and Mailpit SMTP settings. It sets
`AUTH_REQUIRED=False`; the UI/API is published on all host interfaces at port
`18081`.

The image's built-in admin-key bootstrap creates one fixed trusted-PoC key used
only by `alarm-alerta-ingress` to set the required `customer=wama` mapping. It
does not enable UI authentication or grant the key outside the Docker network.

The plugin sends a best-effort Mailpit email only in `post_receive` when Alerta
creates the first active WAMA-managed episode. It requires `open`,
`repeat=False`, `duplicate_count=0`, prior native severity `indeterminate`, and
one initial `new` history entry. Refreshes, acknowledgements, closures, and
rule-only revisions do not send email. SMTP failures are logged by Alerta's
Mailer; this service provides no durable retry or outbox.
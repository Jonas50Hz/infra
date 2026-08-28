# Mailpit

`mailpit` is the local SMTP test inbox for the root-owned Alerta alarm slice.
It is pinned to
`axllent/mailpit:v1.31.0@sha256:c96991d9bef73594c246d89ca81411d4e916f03e76a7d2d72fa2ab5dd3c9ce24`.

SMTP remains internal on port 1025. The web UI is available only on
all host interfaces at port `${MAILPIT_UI_PORT:-8025}`. Its Compose healthcheck uses Mailpit's
supported `/mailpit readyz` command.

Mailpit is a local inspection tool, not a durable notification outbox. The
Alerta plugin sends only the fixed local test recipient; there is no relay,
retry queue, or external email delivery in this PoC.
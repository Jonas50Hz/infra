FROM alerta/alerta-web:9.1.0@sha256:693845fb8a95a483c8c9d560786f661f5b6198e5ea0baa6ddf368a7e53c94f39 AS runtime

USER root

COPY services/alerta/plugin /opt/wama-alerta-mailer
RUN /venv/bin/pip install --no-cache-dir --no-deps --no-build-isolation /opt/wama-alerta-mailer \
    && rm -rf /opt/wama-alerta-mailer

USER 1001

FROM runtime AS test

USER root
COPY services/alerta/tests /app/wama-tests
USER 1001
RUN /venv/bin/python -m unittest discover -s /app/wama-tests -v

FROM runtime
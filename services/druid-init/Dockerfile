FROM python:3.12-slim AS runtime

WORKDIR /app

COPY services/druid-init/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY services/druid-init/src /app
COPY services/druid-init/supervisors /etc/wama/supervisors

ENV PYTHONPATH=/app

FROM runtime AS test

COPY services/druid-init/tests /app/tests
RUN python -m unittest discover -s /app/tests -v

FROM runtime

CMD ["python", "-m", "druid_init.main"]
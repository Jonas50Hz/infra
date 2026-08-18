FROM python:3.12-slim

WORKDIR /app

COPY services/druid-init/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY services/druid-init/src /app
COPY services/druid-init/supervisors /etc/wama/supervisors
COPY services/druid-init/tests /app/tests

ENV PYTHONPATH=/app

CMD ["python", "-m", "druid_init.main"]
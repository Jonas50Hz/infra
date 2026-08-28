FROM python:3.12-slim AS build

WORKDIR /build

COPY services/iec104-browser/requirements.txt /build/requirements.txt
RUN pip install --no-cache-dir --prefix=/install -r /build/requirements.txt

COPY services/iec104-browser/src /build/src
COPY services/iec104-browser/public /build/public

FROM node:22-alpine AS frontend-test

WORKDIR /test

COPY services/iec104-browser/public/live_window.mjs /test/public/live_window.mjs
COPY services/iec104-browser/public/app.js /test/public/app.js
COPY services/iec104-browser/tests/live_window.test.mjs /test/tests/live_window.test.mjs
COPY services/iec104-browser/tests/app_socket_lifecycle.test.mjs /test/tests/app_socket_lifecycle.test.mjs

RUN node --experimental-vm-modules --test \
    /test/tests/live_window.test.mjs \
    /test/tests/app_socket_lifecycle.test.mjs \
    && touch /test/passed

FROM python:3.12-slim AS runtime

WORKDIR /app

COPY --from=build /install /usr/local
COPY --from=build /build/src /app
COPY --from=build /build/public /app/public
COPY services/iec104-browser/tests /app/tests

ENV PYTHONPATH=/app

FROM runtime AS test

COPY --from=frontend-test /test/passed /test/frontend-test-passed
RUN python -m unittest discover -s /app/tests -v \
    && test -s /app/public/index.html \
    && test -s /app/public/app.js \
    && test -s /app/public/styles.css \
    && grep -q "new WebSocket" /app/public/app.js \
    && ! grep -q "innerHTML" /app/public/app.js

FROM runtime

CMD ["uvicorn", "iec104_browser.main:app", "--host", "0.0.0.0", "--port", "8080"]
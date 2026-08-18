FROM nginx:1.27-alpine AS test

COPY services/measurement-session-browser/nginx.conf /etc/nginx/conf.d/default.conf
COPY services/measurement-session-browser/public /usr/share/nginx/html

RUN nginx -t \
    && test -s /usr/share/nginx/html/index.html \
    && grep -q "artifact.download_name" /usr/share/nginx/html/app.js \
    && ! grep -R -E "S3_|seaweedfs|wama-s3-admin|wama-postgres" /usr/share/nginx/html

FROM nginx:1.27-alpine

COPY services/measurement-session-browser/nginx.conf /etc/nginx/conf.d/default.conf
COPY services/measurement-session-browser/public /usr/share/nginx/html
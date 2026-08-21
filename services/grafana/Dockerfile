FROM grafana/grafana:12.1.0

USER root

RUN mkdir -p /opt/grafana/plugins \
	&& grafana cli --pluginsDir /opt/grafana/plugins \
		plugins install grafadruid-druid-datasource 1.7.0 \
	&& grafana cli --pluginsDir /opt/grafana/plugins \
		plugins install trino-datasource 1.1.1 \
	&& chown -R grafana:root /opt/grafana/plugins

USER grafana
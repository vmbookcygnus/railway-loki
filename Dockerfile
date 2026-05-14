FROM grafana/loki:2.9.3
COPY loki-config.yaml /etc/loki/local-config.yaml
CMD ["/usr/bin/loki", "-config.file=/etc/loki/local-config.yaml"]
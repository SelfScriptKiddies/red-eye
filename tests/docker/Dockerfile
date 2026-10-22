FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends python3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY red-eye-agent /usr/local/bin/red-eye-agent
RUN chmod +x /usr/local/bin/red-eye-agent && mkdir -p /data

# python http server on 8080 + agent sniffing eth0 (REAL interface — no
# loopback double-tap). Host reaches the service via published port 31338.
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]

#!/bin/sh
set -x
python3 -m http.server 8080 --bind 0.0.0.0 &
sleep 1
exec /usr/local/bin/red-eye-agent \
  -i eth0 -p 8080 \
  --nats-host nats:4222 \
  --nats-login red-eye-client \
  --machine-ip 0.0.0.0 \
  --data-dir /data

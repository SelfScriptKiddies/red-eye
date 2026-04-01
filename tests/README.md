# Red Eye — Testing

## Prerequisites

- Red Eye stack running (`docker compose up -d`)
- Agent running with pcap on target interface (`sudo`)
- Target HTTP server accessible from the network interface agent sniffs
- Python 3.10+ with `requests` installed (`pip install requests`)

## Integration Test

Validates the full pipeline: agent → NATS → analyzer → DB → backend API.

```bash
# Abstract
python3 tests/integration_test.py \
  --api-url http://<BACKEND_HOST>:8000 \
  --password <WEB_PASSWORD> \
  --target-host <TARGET_HTTP_HOST> \
  --http-port <HTTP_PORT> \
  --tcp-port <TCP_PORT>

# With local target servers (if no external targets)
python3 tests/integration_test.py --spawn-targets

# Expected: 11/11 passed (TCP test needs echo server)
```

### What it tests

| Test | What |
|------|------|
| API Health | Login, agent list, service list |
| HTTP Service | Streams detected, HTTP messages parsed, headers extracted, header search (single + AND) |
| Binary TCP | Raw packets captured, packet_data present |
| DB Health | Queries respond, agent online |

## Stress Test

Simulates CTF-like traffic load. Sends HTTP requests at target RPS, monitors API health every 30s.

### Usage

```bash
python3 tests/stress_test.py \
  --target-host <TARGET_HTTP_HOST> \
  --http-port <HTTP_PORT> \
  --tcp-port <TCP_PORT> \
  --rps <REQUESTS_PER_SECOND> \
  --duration <SECONDS> \
  --http-ratio <0.0-1.0> \
  --workers <THREAD_COUNT> \
  --monitor-interval <SECONDS> \
  --output <RESULTS_FILE.json>
```

### Recommended test plan

Run sequentially, each for 5 minutes. Stop and check between runs.

| Step | RPS | Duration | Workers | Purpose |
|------|-----|----------|---------|---------|
| 1 | 100 | 300s | 50 | Baseline — should be 0 errors, API p50 < 1s |
| 2 | 500 | 300s | 100 | Medium load — 0 errors, API p50 < 2s acceptable |
| 3 | 1000 | 300s | 200 | High load — 0 errors, API p99 < 5s acceptable |
| 4 | 500 | 3600s | 100 | Sustained — 1 hour, check DB doesn't grow unbounded |

```bash
# Step 1: Baseline
python3 tests/stress_test.py --rps 100 --duration 300 --workers 50 \
  --target-host <TARGET> --http-port <PORT> --output tests/stress_100.json

# Step 2: Medium
python3 tests/stress_test.py --rps 500 --duration 300 --workers 100 \
  --target-host <TARGET> --http-port <PORT> --output tests/stress_500.json

# Step 3: High
python3 tests/stress_test.py --rps 1000 --duration 300 --workers 200 \
  --target-host <TARGET> --http-port <PORT> --output tests/stress_1000.json

# Step 4: Sustained (optional, 1 hour)
python3 tests/stress_test.py --rps 500 --duration 3600 --workers 100 \
  --target-host <TARGET> --http-port <PORT> --output tests/stress_sustained.json
```

### Health criteria

| Metric | Healthy | Warning | Critical |
|--------|---------|---------|----------|
| Error rate | 0% | < 1% | > 5% |
| API p99 | < 1s | < 5s | > 5s |
| Header search | < 100ms | < 500ms | > 1s |
| Agent | Online | — | Offline |

### Between runs

Check DB state:
```bash
# Packet count (should be bounded by partition cleanup)
docker exec red-eye-db-1 psql -U red-eye-user -d red-eye \
  -c "SELECT count(*) FROM packets;"

# DB size
docker exec red-eye-db-1 psql -U red-eye-user -d red-eye \
  -c "SELECT pg_size_pretty(pg_database_size('red-eye'));"

# Partition count
docker exec red-eye-db-1 psql -U red-eye-user -d red-eye \
  -c "SELECT count(*) FROM pg_inherits WHERE inhparent = 'packets'::regclass;"

# Header set dedup efficiency
docker exec red-eye-db-1 psql -U red-eye-user -d red-eye \
  -c "SELECT count(*) as header_sets FROM header_set;"
```

Check agent:
```bash
curl -s -b cookies http://localhost:8000/api/agent | python3 -m json.tool
```

---

## Concrete example: dev.lan + build.lan

### Network setup

```
dev.lan  (10.77.0.110) — Red Eye stack + agent
build.lan (10.77.0.97) — Target HTTP server (via harpoon port forward)
```

Agent sniffs `eth1` (10.77.0.0/24 network). Traffic from dev.lan to build.lan
goes through eth1, so pcap captures it.

### Step 1: Start the stack on dev.lan

```bash
cd ~/programming/traffic-analyzer-stack/red-eye
docker compose up -d
```

### Step 2: Start HTTP server on build.lan

SSH into build.lan:
```bash
ssh build.lan
cd /some/dir
python3 -m http.server 31337
```

Or use harpoon to forward from build.lan:31337 → dev.lan:31337.

### Step 3: Create service in Red Eye UI

Go to `http://dev.lan:3000/settings`, create service:
- Name: `http-test`
- Port: `31337`
- HTTP Parse: checked

### Step 4: Start agent on dev.lan

```bash
sudo /home/tonysd/programming/traffic-analyzer-stack/red-eye/agent/target/release/red-eye-agent \
  --nats-host 127.0.0.1:4222 \
  --nats-login red-eye-client \
  --nats-password 6781261b9d9f2d24 \
  --interface eth1 \
  --ports 31337 \
  --data-dir /tmp/red-eye-agent
```

Verify it works:
```bash
curl http://10.77.0.97:31337/
# Check UI — stream should appear
```

### Step 5: Run integration test

```bash
python3 tests/integration_test.py \
  --target-host 10.77.0.97 \
  --http-port 31337
```

### Step 6: Run stress tests

```bash
# Baseline
python3 tests/stress_test.py \
  --target-host 10.77.0.97 --http-port 31337 \
  --rps 100 --duration 300 --output tests/stress_100.json

# Medium
python3 tests/stress_test.py \
  --target-host 10.77.0.97 --http-port 31337 \
  --rps 500 --duration 300 --workers 100 --output tests/stress_500.json

# High
python3 tests/stress_test.py \
  --target-host 10.77.0.97 --http-port 31337 \
  --rps 1000 --duration 300 --workers 200 --output tests/stress_1000.json
```

### Step 7: Check results

```bash
# Quick summary from JSON
python3 -c "
import json
for f in ['tests/stress_100.json', 'tests/stress_500.json', 'tests/stress_1000.json']:
    try:
        d = json.load(open(f))
        t = d['totals']
        total = t['http_sent'] + t['tcp_sent']
        errs = t['http_errors'] + t['tcp_errors']
        snaps = d['snapshots']
        p99 = max(s['api_p99'] for s in snaps) if snaps else 0
        search = max(s.get('api_search_ms', 0) for s in snaps) if snaps else 0
        print(f'{f:40s} sent={total:>8} err={errs:>4} p99={p99:>6.0f}ms search={search:>6.0f}ms')
    except FileNotFoundError:
        pass
"
```

### Known issues

- **Agent on loopback**: Traffic to `localhost` goes through `lo`, not `eth1`.
  Agent must sniff the interface where traffic actually flows. For local testing,
  route traffic through another machine (build.lan via harpoon).

- **nftables redirect**: If protection/proxy was enabled for the service port,
  nftables may redirect traffic to harpoon's listen port. Disable protection
  before stress testing, or the target server won't receive connections.

- **JetStream stale messages**: After changing Service DTO (adding fields),
  old bincode messages in JetStream cause `Error decoding update`. Fix:
  `docker compose restart nats` or delete `volume/nats/jetstream/`.

- **sudo env vars**: `sudo` strips environment by default. Use CLI args:
  `sudo ./red-eye-agent --nats-host 127.0.0.1:4222 ...`
  NOT: `sudo NATS_HOST=... ./red-eye-agent`

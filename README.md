# Red Eye

Traffic analyzer & SIEM for Attack/Defense CTF competitions.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│ Central Server (docker compose)                     │
│                                                     │
│  Frontend (:3000) ← Backend (:8000) ← PostgreSQL    │
│                         ↕                           │
│                       NATS (:4222)                   │
│                         ↕                           │
│                   Traffic Analyzer                   │
└─────────────────────────────────────────────────────┘
        ↕ NATS
┌─────────────────┐  ┌─────────────────┐
│ Agent (vuln-1)  │  │ Agent (vuln-2)  │  ...
│ pcap + harpoon  │  │ pcap + harpoon  │
└─────────────────┘  └─────────────────┘
```

**Components:**
- **Agent** — single binary deployed on vuln machines. Captures traffic (pcap), proxies with TLS MITM (harpoon), filters/blocks attacks, forwards to central server
- **Traffic Analyzer** — parses packets, applies filters, detects HTTP/gRPC, stores in DB
- **Backend** — REST API, agent management, WebSocket events, flag farm integration
- **Frontend** — React SPA: packet viewer, HTTP analysis, agent control, dashboard

## Quick Start

### 1. Deploy Central Server

```bash
cp .env-example .env
# Edit .env — set passwords, NATS credentials

docker compose up -d --build
```

Services:
- Frontend: `http://localhost:3000` (password from `WEB_PASSWORD` in .env)
- Backend API: `http://localhost:8000`
- NATS: `localhost:4222`
- Swagger: `http://localhost:8000/api/docs/`

### 2. Build Agent

```bash
# Install dependencies
sudo apt install libpcap-dev

# Build
cd agent
cargo build --release
```

Binary: `agent/target/release/red-eye-agent`

### 3. Deploy Agent to Vuln Machine

```bash
# Simple deploy
./scripts/deploy-agent.sh root@10.0.0.1 \
  --nats 10.0.0.100:4222 \
  --nats-password YOUR_NATS_PASSWORD \
  --interface eth0

# With the nftables package preinstalled if missing (needed for
# proxy/firewall features; nftables.service is NOT enabled)
./scripts/deploy-agent.sh root@10.0.0.1 \
  --nats 10.0.0.100:4222 \
  --nats-password YOUR_NATS_PASSWORD \
  --interface eth0 \
  --install-nftables

# Using .env file for credentials
./scripts/deploy-agent.sh root@10.0.0.1 --env-file .env --install-nftables

# Remove agent
./scripts/deploy-agent.sh root@10.0.0.1 --uninstall
```

The agent deploys WITHOUT a ports list: service ports come from the
central registry (Settings page) via revisioned snapshots, so assigning
a service takes effect at runtime without redeploying or restarting the
agent. `PORTS` is removed and ignored if present in old env files.

The script:
1. Uploads binary via scp
2. Installs the nftables package if the `nft` binary is missing (optional, `--install-nftables`). It deliberately does NOT enable or start `nftables.service`: loading `/etc/nftables.conf` runs `flush ruleset` and would wipe all host rules (e.g. Docker's). The agent manages its own `inet red-eye-agent` table at runtime.
3. Creates systemd service with `CAP_NET_RAW` + `CAP_NET_ADMIN`
4. Starts and enables the service

### 4. Configure Services

1. Open `http://YOUR_SERVER:3000`, login
2. Go to **Settings** → Add services (name + port)
3. Enable flags: **HTTP Parse** for web services, **HTTP/2 Parse** for gRPC
4. Add filter patterns (regex/substr) for exploit detection
5. Set special types: `flag` (farm submit), `alert` (notification), `block` (auto-drop)

### 5. Enable Protection

1. Go to **Agents** → select agent
2. In "Service Protection" section, click **Proxy** or **Proxy + TLS**
3. This sets up harpoon proxy + nftables redirect automatically
4. Block filters are synced to all agents

## LLM-SOC and MCP

Two optional, OFF-by-default services extend Red Eye with AI triage and a
remote MCP interface. Deploy order matters and mirrors the design doc
(docs/superpowers/specs/): backend first, then read-only MCP, then the
queue and manual jobs, then the worker, then automatic triage, and full
control only as a separate explicit redeployment after mutation safety
tests.

### red-eye-mcp (remote MCP interface)

Streamable HTTP MCP at `/mcp`. Read-only is the only secure default: the
process refuses to expose mutations unless it was started with BOTH
`MCP_MODE=full_control` and `MCP_ALLOW_FULL_CONTROL=true`, AND the caller
holds a `full_control` token, AND the backend service credential carries
`full_control` scope. Changing MCP mode is a between-games redeployment,
not a runtime setting.

1. `docker compose up -d red-eye-mcp` (starts fail-closed: no tokens, no
   access)
2. Settings -> MCP: generate the **service token** once, put it into
   `.env` as `MCP_SERVICE_TOKEN`, `docker compose up -d red-eye-mcp`
3. Create an MCP token per harness (name + capability + optional expiry);
   the plaintext is shown ONCE
4. Point clients at `https://<central>/mcp` behind the central nginx,
   which must terminate TLS and proxy `/mcp` with `proxy_buffering off`,
   a bounded `client_max_body_size` and timeouts. Plaintext public
   exposure is invalid.

Token validation runs on initialization and every tool call; revocation
bites immediately. Tokens in query strings are rejected. The service has
no PostgreSQL/NATS access and no arbitrary backend proxy: every tool maps
to a bounded backend endpoint.

Full-control mutations additionally require `expected_revision`
(optimistic concurrency against the control-plane revision),
`confirm=true`, an audit `reason`, and for delete/block/proxy/agent
operations a prior `dry_run=true` call whose `operation_id` the mutation
must reference. Every audit record carries ids and statuses only.

#### get_selected_streams (UI-selection bundle)

The `get_selected_streams` MCP tool is the only way an MCP client turns an
operator-provided URL into data: the operator selects streams in the Red
Eye UI, copies the link (`/packets?streams=1,2,3` or the single
`?stream=N` form) and hands it to the agent. The tool accepts ONLY links
whose origin (scheme + host + port) equals the configured
`MCP_PUBLIC_URL` origin. It never opens a network connection to that
origin: the url is parsed, the ids are extracted, and all data comes from
the service's own backend connection. External URLs are never fetchable.

Rules:

- Any authenticated token may call it (read-only or full-control - it
  mutates nothing), subject to the same per-token and per-address rate
  limits as every other tool.
- `MCP_PUBLIC_URL` unset, empty or unparseable -> the call fails with a
  configuration error telling the operator to set it (fail closed).
- Non-http(s) scheme, embedded credentials, mismatched origin, missing
  `streams` query param, junk or non-positive ids, and more than 25 ids
  (fixed cap) are rejected with typed client errors.
- Output: `{"streams": [{"stream_id", "summary", "messages"}...],
  "failed_ids": [{"stream_id", "error"}...]}`. Each stream carries the
  same bounded summary + message evidence as `get_stream_summary` /
  `get_stream_messages`; an unknown stream id becomes an entry in
  `failed_ids` instead of failing the whole call. Audit records carry the
  url only, never the bundle.

### soc-worker (LLM triage)

Persistent queue (`soc_job`) in PostgreSQL, leased workers, bounded
retries and a daily job limit. The worker holds no database credentials:
it claims jobs over the backend API and reads evidence through read-only
MCP only. The model never controls the investigation sequence and every
attack verdict must cite packet/message ids that the MCP service actually
fetched for that job.

1. Deploy the LLM endpoint yourself (see the commented `llm` example in
   docker-compose.yaml); default `soc_base_url` is `http://llm:8080/v1`
2. Settings -> SOC: generate the **worker token** into `.env` as
   `SOC_WORKER_TOKEN`, create a read-only **MCP token** for evidence
   access into `.env` as `MCP_TOKEN`, `docker compose up -d soc-worker`
3. Configure the SOC card (endpoint, model, thresholds, compute limits)
   and enable SOC
4. Manual triage: "Triage with SOC" on any stream, works without JEV
5. Automatic triage: only after manual triage passed validation, enable
   SOC auto mode; qualifying NEW JEV verdicts (is_attack, severity >=
   `soc_auto_min_jev_severity`) enqueue exactly one job each

Every combination of JEV/SOC configuration and outage has explicit
non-looping behavior shown in Settings. SOC alerts and recommended
actions are informational only: nothing is ever blocked, filtered or
reconfigured automatically.

## Features

### Traffic Capture
- Passive pcap sniffing (all TCP/UDP)
- Active proxy via harpoon (TLS MITM, HTTP/2, filtering)
- Automatic port management (sniffer ↔ proxy)

### L7 Analysis
- HTTP/1.1 parsing with header extraction
- HTTP/2 via harpoon proxy (feature-gated)
- gRPC protobuf raw decode
- Cookie parsing (individual cookie search)
- Header AND-search (click multiple headers → intersect streams)

### Firewall
- `block` tag type → auto-sync drop filters to all agents
- Quick block from packet view (select text → block)
- Hex dump block (select bytes → context menu → block)
- Per-service protection toggle (proxy + nftables)

### Flag Farm
- `flag` tag type → auto-submit matched text to farm URL
- Farm URL + enable/disable toggle in Settings
- Dashboard shows flag stats

### Alerting
- `alert` tag type → WebSocket toast notifications
- Clickable alerts → navigate to stream
- Tag metrics sparklines on dashboard

### Tools
- **Share** — copy stream URL + auto-favorite
- **Export** — pwntools / requests / curl code generation
- **Diff** — LCS-based side-by-side comparison (text + hex)
- **Interactive hex dump** — hover sync, drag selection, context menu (copy as hex/ascii/python/C)

## Integration Test Examples

```bash
cd examples

# Start test HTTP server
python3 -m http.server 31337 &

# Basic HTTP tests
python3 test_http_basic.py http://YOUR_IP:31337

# Exploit patterns (for tag testing)
python3 test_exploit_patterns.py http://YOUR_IP:31337

# Multi-session (cookie grouping)
python3 test_multi_session.py http://YOUR_IP:31337

# Binary protocol (hex view testing)
python3 test_binary_protocol.py YOUR_IP 9999

# gRPC simulation
python3 test_grpc_service.py http://YOUR_IP:31337

# TLS (see examples/test_tls_service.py for instructions)
```

## Environment Variables

### Central Server (.env)
| Variable | Description | Default |
|----------|-------------|---------|
| `NATS_HOST` | NATS broker address | `nats:4222` |
| `NATS_LOGIN` | NATS username | `red-eye-client` |
| `NATS_PASSWORD` | NATS password | — |
| `DB_HOST` | PostgreSQL host | `db` |
| `DB_PORT` | PostgreSQL port | `5432` |
| `DB_USERNAME` | DB username | — |
| `DB_PASSWORD` | DB password | — |
| `DB_SCHEMA` | DB name | `red-eye` |
| `WEB_PORT` | Backend port | `8000` |
| `WEB_PASSWORD` | UI login password | — |
| `CLEANUP_STREAM_AGE_MINUTES` | Delete streams older than | `60` |
| `CLEANUP_INTERVAL_MINUTES` | Cleanup check interval | `15` |

### Agent
| Variable | Description | Default |
|----------|-------------|---------|
| `NATS_HOST` | NATS server | `nats:4222` |
| `NATS_LOGIN` | NATS login | `username` |
| `NATS_PASSWORD` | NATS password | `password` |
| `INTERFACE` | Network interface | `eth0` |
| `AGENT_ID` | Agent identifier | hostname |
| `DATA_DIR` | CA certs directory | `/var/lib/red-eye-agent` |
| `LOGGING_LEVEL` | Log level | `info` |

### LLM-SOC / MCP (.env, all optional)
| Variable | Description | Default |
|----------|-------------|---------|
| `MCP_SERVICE_TOKEN` | red-eye-mcp service credential for backend API calls | empty (MCP fail-closed) |
| `MCP_PUBLIC_URL` | public origin of the Red Eye UI; `get_selected_streams` accepts only links on this origin | empty (tool fails closed) |
| `SOC_WORKER_TOKEN` | soc-worker job credential for backend API calls | empty (worker retries) |
| `MCP_TOKEN` | read-only MCP token the worker uses for evidence | empty |

## API Documentation

Swagger UI: `http://YOUR_SERVER:8000/api/docs/`

Key endpoints:
- `GET /api/agent` — list agents
- `POST /api/agent/{id}/command` — send command to agent
- `POST /api/protect` — toggle service protection
- `GET /api/stream` — list streams
- `GET /api/http/search/multi` — AND-search by headers
- `GET /api/tag/metrics` — tag trigger metrics
- `GET /api/settings` — app settings

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

Two optional, disabled by default services extend Red Eye with AI triage and a remote MCP interface.

### red-eye-mcp (remote MCP interface)

Streamable HTTP MCP at `/mcp`. Read-only is secure default: the process refuses to expose mutations unless it was started with BOTH `MCP_MODE=full_control` and `MCP_ALLOW_FULL_CONTROL=true`, AND the caller holds a `full_control` token, AND the backend service credential carries `full_control` scope. Changing MCP mode is a between-games redeployment, not a runtime setting.

1. `docker compose up -d red-eye-mcp` (starts fail-closed: no tokens, no access)
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
| `MCP_ENABLE_FETCH_URL` | enable the fetch_url MCP tool (network egress to public hosts only) | `false` |
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

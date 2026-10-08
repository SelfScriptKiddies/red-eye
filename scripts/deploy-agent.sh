#!/bin/bash
# Red Eye Agent deployment script
# Usage: ./deploy-agent.sh <target_host> [options]
#
# Examples:
#   ./deploy-agent.sh root@10.0.0.1
#   ./deploy-agent.sh root@10.0.0.1 --interface eth0 --ports 8080,443
#   ./deploy-agent.sh root@10.0.0.1 --nats 10.0.0.100:4222 --interface ens18

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

# Defaults
AGENT_BINARY="./agent/target/release/red-eye-agent"
REMOTE_DIR="/opt/red-eye-agent"
NATS_HOST=""
NATS_LOGIN="red-eye-client"
NATS_PASSWORD=""
INTERFACE="eth0"
PORTS=""
SERVICE_NAME="red-eye-agent"
DATA_DIR="/var/lib/red-eye-agent"
LOGGING_LEVEL="info"

usage() {
    echo "Usage: $0 <user@host> [options]"
    echo ""
    echo "Options:"
    echo "  --nats HOST:PORT       NATS server address (required if not in .env)"
    echo "  --nats-login USER      NATS login (default: red-eye-client)"
    echo "  --nats-password PASS   NATS password (required if not in .env)"
    echo "  --interface IFACE      Network interface (default: eth0)"
    echo "  --ports PORTS          Comma-separated ports to sniff (required)"
    echo "  --binary PATH          Path to agent binary (default: ./agent/target/release/red-eye-agent)"
    echo "  --env-file FILE        Read settings from .env file"
    echo "  --install-nftables     Install nftables package if missing (service NOT enabled)"
    echo "  --uninstall            Remove agent from target host"
    echo ""
    echo "Example:"
    echo "  $0 root@vuln1.ctf --nats 10.0.0.100:4222 --nats-password secret --ports 8080,443"
    exit 1
}

# Parse arguments
TARGET_HOST="$1"
shift || usage

if [ -z "$TARGET_HOST" ]; then
    usage
fi

UNINSTALL=false
INSTALL_NFTABLES=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --nats) NATS_HOST="$2"; shift 2 ;;
        --nats-login) NATS_LOGIN="$2"; shift 2 ;;
        --nats-password) NATS_PASSWORD="$2"; shift 2 ;;
        --interface) INTERFACE="$2"; shift 2 ;;
        --ports) PORTS="$2"; shift 2 ;;
        --binary) AGENT_BINARY="$2"; shift 2 ;;
        --env-file)
            if [ -f "$2" ]; then
                echo -e "${YELLOW}[*] Loading settings from $2${NC}"
                source "$2"
                NATS_HOST="${NATS_HOST:-$NATS_BROKER}"
            fi
            shift 2 ;;
        --install-nftables) INSTALL_NFTABLES=true; shift ;;
        --uninstall) UNINSTALL=true; shift ;;
        *) echo -e "${RED}Unknown option: $1${NC}"; usage ;;
    esac
done

# Uninstall
if [ "$UNINSTALL" = true ]; then
    echo -e "${YELLOW}[*] Uninstalling agent from ${TARGET_HOST}${NC}"
    ssh "$TARGET_HOST" "systemctl stop $SERVICE_NAME 2>/dev/null; systemctl disable $SERVICE_NAME 2>/dev/null; rm -rf $REMOTE_DIR /etc/systemd/system/${SERVICE_NAME}.service; systemctl daemon-reload" 2>/dev/null
    echo -e "${GREEN}[+] Agent uninstalled${NC}"
    exit 0
fi

# Validate
if [ ! -f "$AGENT_BINARY" ]; then
    echo -e "${RED}[!] Agent binary not found: $AGENT_BINARY${NC}"
    echo -e "${YELLOW}[*] Build it first: cd agent && cargo build --release${NC}"
    exit 1
fi

if [ -z "$NATS_HOST" ]; then
    echo -e "${RED}[!] NATS host not specified. Use --nats or --env-file${NC}"
    exit 1
fi

if [ -z "$NATS_PASSWORD" ]; then
    echo -e "${RED}[!] NATS password not specified. Use --nats-password or --env-file${NC}"
    exit 1
fi

# An empty port list no longer means "capture everything" - the agent
# disables its sniffer when PORTS is empty (capturing all traffic would
# mirror the agent's own NATS connection into the analyzer). Refuse to
# deploy a configuration that would ship a dead sniffer.
PORTS_TRIMMED="${PORTS//[[:space:]]/}"
if [ -z "$PORTS_TRIMMED" ]; then
    echo -e "${RED}[!] Ports not specified. DEPLOY.md requires --ports: without it the agent's sniffer stays disabled.${NC}"
    echo -e "${YELLOW}[*] Pass --ports 8080,4242 (comma-separated service ports) or set PORTS=... in the env file${NC}"
    exit 1
fi

echo -e "${GREEN}[*] Deploying Red Eye Agent${NC}"
echo -e "    Target:    ${TARGET_HOST}"
echo -e "    NATS:      ${NATS_HOST}"
echo -e "    Interface: ${INTERFACE}"
echo -e "    Ports:     ${PORTS}"
echo ""

# 1. Upload binary
echo -e "${YELLOW}[1/4] Uploading agent binary...${NC}"
ssh "$TARGET_HOST" "mkdir -p $REMOTE_DIR"
scp -q "$AGENT_BINARY" "${TARGET_HOST}:${REMOTE_DIR}/red-eye-agent"
ssh "$TARGET_HOST" "chmod +x ${REMOTE_DIR}/red-eye-agent"

# 2. Install nftables (optional)
# SAFETY: we only ensure the `nft` BINARY is present. We must NOT enable or
# start nftables.service: on Debian the stock /etc/nftables.conf begins with
# "flush ruleset", so starting the service wipes ALL nftables/iptables rules
# on the host (including Docker's). The agent applies/removes its OWN table
# (inet red-eye-agent) at runtime via NftablesManager, and the red-eye-agent
# systemd unit re-applies rules on boot — the host-wide service is neither
# needed nor safe to touch from a deploy script.
if [ "$INSTALL_NFTABLES" = true ]; then
    echo -e "${YELLOW}[2/5] Ensuring nft binary is present (package only)...${NC}"
    ssh "$TARGET_HOST" bash -s << 'NFTEOF'
        if command -v nft &>/dev/null; then
            echo "nft binary already present: $(nft --version)"
        else
            if command -v apt-get &>/dev/null; then
                apt-get update -qq && apt-get install -y -qq nftables
            elif command -v yum &>/dev/null; then
                yum install -y nftables
            elif command -v apk &>/dev/null; then
                apk add nftables
            else
                echo "ERROR: Unknown package manager, install nftables manually"
                exit 1
            fi
        fi
        # Do not enable/start nftables.service (see SAFETY note above).
        # If it is already active on the host, leave it alone: its config
        # governs, and we never restart/stop a running host's rules.
        if command -v systemctl &>/dev/null && systemctl is-active --quiet nftables 2>/dev/null; then
            echo "NOTE: nftables.service is already active on this host;"
            echo "      leaving it untouched (its /etc/nftables.conf governs)."
        fi
        echo "nftables ready (package only; service NOT enabled — the agent manages"
        echo "its own table. Enabling nftables.service loads /etc/nftables.conf which"
        echo "may 'flush ruleset' and break Docker.)"
NFTEOF
    STEPS=5
else
    STEPS=4
fi

# 3. Create environment file
echo -e "${YELLOW}[3/${STEPS}] Creating configuration...${NC}"
# PORTS is guaranteed non-empty by the validation above.
ENV_CONTENT="NATS_HOST=${NATS_HOST}
NATS_LOGIN=${NATS_LOGIN}
NATS_PASSWORD=${NATS_PASSWORD}
INTERFACE=${INTERFACE}
LOGGING_LEVEL=${LOGGING_LEVEL}
DATA_DIR=${DATA_DIR}
MACHINE_IP=0.0.0.0
PORTS=${PORTS}"

ssh "$TARGET_HOST" "cat > ${REMOTE_DIR}/agent.env << 'ENVEOF'
${ENV_CONTENT}
ENVEOF"
ssh "$TARGET_HOST" "chmod 600 ${REMOTE_DIR}/agent.env"

# 4. Create systemd unit
echo -e "${YELLOW}[4/${STEPS}] Installing systemd service...${NC}"
UNIT_CONTENT="[Unit]
Description=Red Eye Traffic Capture Agent
After=network.target

[Service]
Type=simple
ExecStart=${REMOTE_DIR}/red-eye-agent
EnvironmentFile=${REMOTE_DIR}/agent.env
Restart=always
RestartSec=3
AmbientCapabilities=CAP_NET_RAW CAP_NET_ADMIN

[Install]
WantedBy=multi-user.target"

ssh "$TARGET_HOST" "cat > /etc/systemd/system/${SERVICE_NAME}.service << 'UNITEOF'
${UNIT_CONTENT}
UNITEOF"

# 5. Start service
echo -e "${YELLOW}[${STEPS}/${STEPS}] Starting agent...${NC}"
ssh "$TARGET_HOST" "systemctl daemon-reload && systemctl enable $SERVICE_NAME && systemctl restart $SERVICE_NAME"

# Verify
sleep 2
STATUS=$(ssh "$TARGET_HOST" "systemctl is-active $SERVICE_NAME" 2>/dev/null)

if [ "$STATUS" = "active" ]; then
    echo -e ""
    echo -e "${GREEN}[+] Agent deployed and running!${NC}"
    echo -e "    Service: systemctl status $SERVICE_NAME"
    echo -e "    Logs:    journalctl -u $SERVICE_NAME -f"
    echo -e "    Config:  ${REMOTE_DIR}/agent.env"
else
    echo -e ""
    echo -e "${RED}[!] Agent failed to start. Check logs:${NC}"
    echo -e "    ssh $TARGET_HOST journalctl -u $SERVICE_NAME -n 20"
fi

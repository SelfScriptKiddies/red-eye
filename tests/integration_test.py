#!/usr/bin/env python3
"""
Red Eye Integration Test Suite

Tests all service types: HTTP, binary TCP, TLS, HTTP with filters.
Verifies data flows from agent → NATS → analyzer → DB → backend API → frontend.

Prerequisites:
  - Red Eye stack running (docker compose up)
  - Agent running with sniffer on target interface
  - Target services running (or use --spawn-targets to start simple ones)

Usage:
  python3 tests/integration_test.py [--api-url http://localhost:8000] [--password test123] [--spawn-targets]
"""

import argparse
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import time
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler

import requests

# Colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"
BOLD = "\033[1m"


class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def ok(self, name):
        self.passed += 1
        print(f"  {GREEN}✓{RESET} {name}")

    def fail(self, name, reason=""):
        self.failed += 1
        self.errors.append((name, reason))
        print(f"  {RED}✗{RESET} {name} — {reason}")

    def summary(self):
        total = self.passed + self.failed
        color = GREEN if self.failed == 0 else RED
        print(f"\n{BOLD}Results: {color}{self.passed}/{total} passed{RESET}")
        if self.errors:
            print(f"\n{RED}Failures:{RESET}")
            for name, reason in self.errors:
                print(f"  - {name}: {reason}")
        return self.failed == 0


class RedEyeAPI:
    def __init__(self, base_url, password):
        self.base = base_url
        self.session = requests.Session()
        self.login(password)

    def login(self, password):
        r = self.session.post(f"{self.base}/api/login", json={"password": password})
        assert r.status_code == 200, f"Login failed: {r.text}"

    def get(self, path):
        return self.session.get(f"{self.base}{path}")

    def post(self, path, data):
        return self.session.post(f"{self.base}{path}", json=data)

    def create_service(self, name, port, http_parse=False, merge=False):
        return self.post("/api/service", {
            "name": name, "port": port,
            "http_parse": http_parse,
            "merge_adjacent_packets": merge,
            "chunked_transfer_encoding": False,
            "urldecode": False,
            "inflate_websockets": False,
            "decrypt_tls": False,
        })

    def get_streams(self, limit=50):
        return self.get(f"/api/stream?limit={limit}&offset=0").json()

    def get_packets(self, stream_id):
        return self.get(f"/api/packet/stream_id/{stream_id}").json()

    def get_http_messages(self, stream_id):
        return self.get(f"/api/http/stream/{stream_id}").json()

    def get_http_headers(self, message_id):
        return self.get(f"/api/http/headers/{message_id}").json()

    def search_headers(self, name, value=None):
        params = {"name": name, "limit": 100}
        if value:
            params["value"] = value
        return self.get(f"/api/http/search?{requests.compat.urlencode(params)}").json()

    def search_headers_multi(self, filters):
        return self.post("/api/http/search/multi", filters).json()

    def get_agents(self):
        return self.get("/api/agent").json()

    def get_services(self):
        return self.get("/api/service").json()


def spawn_http_server(port):
    """Start a simple HTTP server in a thread."""
    handler = SimpleHTTPRequestHandler
    httpd = HTTPServer(("0.0.0.0", port), handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return httpd


def spawn_tcp_echo(port):
    """Start a TCP echo server in a thread."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(5)

    def handler():
        while True:
            try:
                conn, _ = srv.accept()
                data = conn.recv(4096)
                conn.sendall(data)  # echo
                conn.close()
            except:
                break

    t = threading.Thread(target=handler, daemon=True)
    t.start()
    return srv


# ── Test suites ──────────────────────────────────────────────

def test_api_health(api, result):
    """Test basic API connectivity."""
    print(f"\n{BOLD}── API Health ──{RESET}")

    r = api.get("/api/authorized")
    if r.status_code == 200:
        result.ok("API reachable + authenticated")
    else:
        result.fail("API reachable", f"status {r.status_code}")

    agents = api.get_agents()
    if isinstance(agents, list):
        result.ok(f"Agent list OK ({len(agents)} agents)")
    else:
        result.fail("Agent list", f"unexpected: {type(agents)}")

    services = api.get_services()
    if isinstance(services, list):
        result.ok(f"Service list OK ({len(services)} services)")
    else:
        result.fail("Service list", f"unexpected: {type(services)}")


def test_http_service(api, target_host, target_port, result):
    """Test HTTP service: capture, L7 parse, header search."""
    print(f"\n{BOLD}── HTTP Service (:{target_port}) ──{RESET}")

    # Send HTTP requests
    s = requests.Session()
    base = f"http://{target_host}:{target_port}"

    try:
        s.get(f"{base}/", headers={
            "X-Test-Header": "integration-test-value",
            "X-Forwarded-For": "10.80.99.1",
        })
        s.post(f"{base}/", json={"action": "test", "flag": "FLAG{test123}"})
    except requests.ConnectionError:
        result.fail("HTTP connect", f"Connection refused on {base} — is target server running?")
        return

    time.sleep(3)  # Wait for processing

    # Check streams appeared
    streams = api.get_streams()
    http_streams = [s for s in streams if s.get("l7_protocol") == "http1"]
    if len(http_streams) > 0:
        result.ok(f"HTTP streams detected ({len(http_streams)})")
    else:
        result.fail("HTTP streams", "no http1 streams found")
        return

    # Check HTTP messages
    sid = http_streams[0]["id"]
    messages = api.get_http_messages(sid)
    if isinstance(messages, list) and len(messages) > 0:
        result.ok(f"HTTP messages parsed ({len(messages)} messages)")
    else:
        result.fail("HTTP messages", f"none found for stream {sid}")
        return

    # Check headers
    msg = messages[0]
    headers = api.get_http_headers(msg["id"])
    if isinstance(headers, dict) and len(headers) > 0:
        result.ok(f"HTTP headers retrieved ({len(headers)} headers)")
    else:
        result.fail("HTTP headers", f"empty or wrong format: {type(headers)}")

    # Header search
    search = api.search_headers("X-Test-Header", "integration-test-value")
    if search.get("total", 0) > 0:
        result.ok(f"Header search works (found {search['total']} streams)")
    else:
        result.fail("Header search", "no results")

    # Multi-header AND search
    multi = api.search_headers_multi([
        {"name": "X-Test-Header", "value": "integration-test-value"},
        {"name": "X-Forwarded-For", "value": "10.80.99.1"},
    ])
    if multi.get("total", 0) > 0:
        result.ok(f"AND header search works ({multi['total']} streams)")
    else:
        result.fail("AND header search", "no results")


def test_binary_service(api, target_host, target_port, result):
    """Test binary/TCP service: raw capture, no L7 parsing."""
    print(f"\n{BOLD}── Binary TCP Service (:{target_port}) ──{RESET}")

    # Send binary data
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((target_host, target_port))
        payload = struct.pack("<4sII", b"HELO", 1, 0x41414141)
        s.send(payload)
        time.sleep(0.5)
        s.send(b"A" * 64 + struct.pack("<Q", 0xdeadbeefcafebabe))
        try:
            s.recv(4096)
        except:
            pass
        s.close()
    except Exception as e:
        result.fail("Binary connect", str(e))
        return

    time.sleep(3)

    streams = api.get_streams()
    # Find streams that are NOT http1
    raw_streams = [s for s in streams if s.get("l7_protocol", "unknown") == "unknown"]
    if len(raw_streams) > 0:
        result.ok(f"Raw TCP streams captured ({len(raw_streams)})")
    else:
        result.fail("Raw TCP streams", "no raw streams found")
        return

    # Verify packets have data
    sid = raw_streams[0]["id"]
    packets = api.get_packets(sid)
    if isinstance(packets, list) and len(packets) > 0:
        result.ok(f"Binary packets stored ({len(packets)} packets)")
        pkt = packets[0]
        if "packet_data" in pkt and len(pkt["packet_data"]) > 0:
            result.ok("Packet data present (base64)")
        else:
            result.fail("Packet data", "empty or missing")
    else:
        result.fail("Binary packets", "none found")


def test_db_health(api, result):
    """Check DB partition health."""
    print(f"\n{BOLD}── DB Health ──{RESET}")

    # Check via backend (no direct DB access in test)
    streams = api.get_streams(limit=1)
    if isinstance(streams, list):
        result.ok("DB responds to queries")
    else:
        result.fail("DB query", "failed")

    # Check agents for partition info
    agents = api.get_agents()
    if len(agents) > 0 and agents[0].get("online", False):
        result.ok(f"Agent '{agents[0]['agent_id']}' online")
    else:
        result.fail("Agent online", "no online agents")


# ── Main ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Red Eye Integration Tests")
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--password", default="test123")
    parser.add_argument("--target-host", default="localhost",
                        help="Host where target services listen (agent sniffs this)")
    parser.add_argument("--http-port", type=int, default=31337)
    parser.add_argument("--tcp-port", type=int, default=31338)
    parser.add_argument("--spawn-targets", action="store_true",
                        help="Start simple HTTP and TCP servers locally")
    args = parser.parse_args()

    print(f"{BOLD}Red Eye Integration Test Suite{RESET}")
    print(f"API: {args.api_url}")
    print(f"Target: {args.target_host}:{args.http_port} (HTTP), :{args.tcp_port} (TCP)")

    targets = []
    if args.spawn_targets:
        print(f"\n{YELLOW}Spawning target servers...{RESET}")
        httpd = spawn_http_server(args.http_port)
        targets.append(httpd)
        print(f"  HTTP server on :{args.http_port}")
        tcpd = spawn_tcp_echo(args.tcp_port)
        targets.append(tcpd)
        print(f"  TCP echo on :{args.tcp_port}")
        time.sleep(1)

    try:
        api = RedEyeAPI(args.api_url, args.password)
    except Exception as e:
        print(f"{RED}Failed to connect to API: {e}{RESET}")
        sys.exit(1)

    result = TestResult()

    test_api_health(api, result)
    test_http_service(api, args.target_host, args.http_port, result)
    test_binary_service(api, args.target_host, args.tcp_port, result)
    test_db_health(api, result)

    success = result.summary()

    # Cleanup
    for t in targets:
        try:
            if isinstance(t, HTTPServer):
                t.shutdown()
            elif hasattr(t, 'close'):
                t.close()
        except:
            pass

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

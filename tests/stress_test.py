#!/usr/bin/env python3
"""
Red Eye Stress Test

Simulates CTF-like traffic load and monitors system health.
Tests: DB growth, response times, packet loss, memory usage.

Usage:
  # Quick test (5 min, 100 req/sec)
  python3 tests/stress_test.py --duration 300 --rps 100

  # Full CTF simulation (8 hours, 1000 req/sec)
  python3 tests/stress_test.py --duration 28800 --rps 1000

  # Custom
  python3 tests/stress_test.py --duration 600 --rps 500 --http-port 31337 --tcp-port 31338
"""

import argparse
import json
import os
import random
import socket
import string
import struct
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer, SimpleHTTPRequestHandler

import requests

# Colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"
BOLD = "\033[1m"


class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.http_sent = 0
        self.http_errors = 0
        self.tcp_sent = 0
        self.tcp_errors = 0
        self.api_latencies = []
        self.snapshots = []  # periodic snapshots

    def inc_http(self, error=False):
        with self.lock:
            if error:
                self.http_errors += 1
            else:
                self.http_sent += 1

    def inc_tcp(self, error=False):
        with self.lock:
            if error:
                self.tcp_errors += 1
            else:
                self.tcp_sent += 1

    def add_api_latency(self, ms):
        with self.lock:
            self.api_latencies.append(ms)

    def snapshot(self, api_data):
        with self.lock:
            self.snapshots.append({
                "time": time.time(),
                "http_sent": self.http_sent,
                "http_errors": self.http_errors,
                "tcp_sent": self.tcp_sent,
                "tcp_errors": self.tcp_errors,
                "api_p50": self._percentile(50),
                "api_p99": self._percentile(99),
                **api_data,
            })
            # Reset latencies for next window
            self.api_latencies = []

    def _percentile(self, p):
        if not self.api_latencies:
            return 0
        sorted_l = sorted(self.api_latencies)
        idx = int(len(sorted_l) * p / 100)
        return sorted_l[min(idx, len(sorted_l) - 1)]


# ── Traffic generators ───────────────────────────────────────

PATHS = ["/", "/api/login", "/api/flag", "/api/users", "/api/status",
         "/api/submit", "/admin/panel", "/static/main.js"]

EXPLOIT_PATTERNS = [
    "'; DROP TABLE users; --",
    "{{7*7}}",
    "../../../etc/passwd",
    "<script>alert(1)</script>",
    "AAAA%08x.%08x.%08x",
    '{"__proto__": {"admin": true}}',
]

USER_AGENTS = [
    "python-requests/2.31", "curl/7.88", "Mozilla/5.0", "Go-http-client/1.1",
    "exploit-scanner/1.0", "", "sqlmap/1.7",
]


def random_flag():
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=31)) + "="


def gen_http_request(base_url):
    """Generate a random HTTP request."""
    path = random.choice(PATHS)
    method = random.choice(["GET", "GET", "GET", "POST", "PUT"])
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "X-Forwarded-For": f"10.80.{random.randint(1, 20)}.{random.randint(1, 254)}",
        "X-Request-Id": f"req-{random.randint(1, 999999)}",
    }

    # Sometimes add exploit payload
    if random.random() < 0.1:
        headers["X-Attack"] = random.choice(EXPLOIT_PATTERNS)

    # Sometimes add flag in response (simulated via header)
    if random.random() < 0.05:
        headers["X-Flag"] = random_flag()

    body = None
    if method in ("POST", "PUT"):
        body = json.dumps({
            "action": random.choice(["login", "submit", "query", "update"]),
            "data": "".join(random.choices(string.ascii_letters, k=random.randint(10, 500))),
        })

    return method, f"{base_url}{path}", headers, body


def send_http(base_url, metrics):
    """Send one HTTP request."""
    method, url, headers, body = gen_http_request(base_url)
    try:
        if method == "GET":
            requests.get(url, headers=headers, timeout=5)
        else:
            requests.request(method, url, headers=headers, data=body, timeout=5)
        metrics.inc_http()
    except:
        metrics.inc_http(error=True)


def send_tcp(host, port, metrics):
    """Send one binary TCP packet."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect((host, port))
        # Random binary payload
        size = random.randint(16, 512)
        payload = os.urandom(size)
        s.send(struct.pack("<4sI", b"DATA", size) + payload)
        try:
            s.recv(4096)
        except:
            pass
        s.close()
        metrics.inc_tcp()
    except:
        metrics.inc_tcp(error=True)


# ── Monitor ──────────────────────────────────────────────────

def monitor_loop(api_url, password, metrics, interval=30):
    """Periodically check API health and collect metrics."""
    session = requests.Session()
    session.post(f"{api_url}/api/login", json={"password": password})

    while True:
        time.sleep(interval)

        api_data = {}
        try:
            # Streams count
            t0 = time.time()
            r = session.get(f"{api_url}/api/stream?limit=1&offset=0")
            api_data["api_streams_ms"] = round((time.time() - t0) * 1000, 1)
            metrics.add_api_latency(api_data["api_streams_ms"])

            # Agent status
            t0 = time.time()
            r = session.get(f"{api_url}/api/agent")
            api_data["api_agents_ms"] = round((time.time() - t0) * 1000, 1)
            agents = r.json()
            api_data["agents_online"] = sum(1 for a in agents if a.get("online"))

            # Services
            t0 = time.time()
            r = session.get(f"{api_url}/api/service")
            api_data["api_services_ms"] = round((time.time() - t0) * 1000, 1)

            # HTTP search
            t0 = time.time()
            r = session.get(f"{api_url}/api/http/search?name=User-Agent&limit=10")
            api_data["api_search_ms"] = round((time.time() - t0) * 1000, 1)
            metrics.add_api_latency(api_data["api_search_ms"])

        except Exception as e:
            api_data["error"] = str(e)

        metrics.snapshot(api_data)

        # Print status line
        snap = metrics.snapshots[-1]
        total_sent = snap["http_sent"] + snap["tcp_sent"]
        total_err = snap["http_errors"] + snap["tcp_errors"]
        elapsed = snap["time"] - metrics.snapshots[0]["time"] if len(metrics.snapshots) > 1 else 0
        rps_actual = total_sent / elapsed if elapsed > 0 else 0

        print(
            f"  {CYAN}[{time.strftime('%H:%M:%S')}]{RESET} "
            f"sent={total_sent} err={total_err} "
            f"rps={rps_actual:.0f} "
            f"api_p50={snap['api_p50']:.0f}ms "
            f"api_p99={snap['api_p99']:.0f}ms "
            f"search={snap.get('api_search_ms', '?')}ms "
            f"agents={snap.get('agents_online', '?')}"
        )


# ── Main ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Red Eye Stress Test")
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--password", default="test123")
    parser.add_argument("--target-host", default="localhost")
    parser.add_argument("--http-port", type=int, default=31337)
    parser.add_argument("--tcp-port", type=int, default=31338)
    parser.add_argument("--rps", type=int, default=100,
                        help="Target requests per second")
    parser.add_argument("--duration", type=int, default=300,
                        help="Test duration in seconds")
    parser.add_argument("--http-ratio", type=float, default=0.8,
                        help="Ratio of HTTP vs TCP traffic (0.0-1.0)")
    parser.add_argument("--workers", type=int, default=50,
                        help="Thread pool size")
    parser.add_argument("--monitor-interval", type=int, default=30,
                        help="Monitoring interval in seconds")
    parser.add_argument("--spawn-targets", action="store_true")
    parser.add_argument("--output", default=None,
                        help="Save results to JSON file")
    args = parser.parse_args()

    print(f"{BOLD}Red Eye Stress Test{RESET}")
    print(f"Target: {args.rps} req/sec for {args.duration}s ({args.duration/60:.1f} min)")
    print(f"HTTP ratio: {args.http_ratio:.0%}, Workers: {args.workers}")
    print(f"API: {args.api_url}, Targets: {args.target_host}:{args.http_port}/{args.tcp_port}")

    targets = []
    if args.spawn_targets:
        print(f"\n{YELLOW}Spawning target servers...{RESET}")
        httpd = HTTPServer(("0.0.0.0", args.http_port), SimpleHTTPRequestHandler)
        t = threading.Thread(target=httpd.serve_forever, daemon=True)
        t.start()
        targets.append(httpd)

        # TCP echo
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", args.tcp_port))
        srv.listen(100)

        def echo_loop():
            while True:
                try:
                    c, _ = srv.accept()
                    c.sendall(c.recv(4096))
                    c.close()
                except:
                    pass

        t = threading.Thread(target=echo_loop, daemon=True)
        t.start()
        targets.append(srv)
        time.sleep(1)

    metrics = Metrics()

    # Start monitor in background
    monitor = threading.Thread(
        target=monitor_loop,
        args=(args.api_url, args.password, metrics, args.monitor_interval),
        daemon=True,
    )
    monitor.start()

    # Main load loop
    http_base = f"http://{args.target_host}:{args.http_port}"
    interval = 1.0 / args.rps
    end_time = time.time() + args.duration

    print(f"\n{GREEN}Starting load...{RESET}\n")

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        try:
            while time.time() < end_time:
                t0 = time.time()

                if random.random() < args.http_ratio:
                    pool.submit(send_http, http_base, metrics)
                else:
                    pool.submit(send_tcp, args.target_host, args.tcp_port, metrics)

                # Pace to target RPS
                elapsed = time.time() - t0
                sleep_time = interval - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            print(f"\n{YELLOW}Interrupted{RESET}")

    # Final summary
    time.sleep(2)  # Let last monitor snapshot complete
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}Stress Test Summary{RESET}")
    print(f"{'='*60}")

    total_http = metrics.http_sent + metrics.http_errors
    total_tcp = metrics.tcp_sent + metrics.tcp_errors
    total = total_http + total_tcp
    total_err = metrics.http_errors + metrics.tcp_errors

    print(f"  Duration:    {args.duration}s")
    print(f"  Total sent:  {total}")
    print(f"  HTTP:        {metrics.http_sent} ok / {metrics.http_errors} err")
    print(f"  TCP:         {metrics.tcp_sent} ok / {metrics.tcp_errors} err")
    print(f"  Error rate:  {total_err/max(total,1)*100:.2f}%")

    if metrics.snapshots:
        p50s = [s["api_p50"] for s in metrics.snapshots if s["api_p50"] > 0]
        p99s = [s["api_p99"] for s in metrics.snapshots if s["api_p99"] > 0]
        searches = [s.get("api_search_ms", 0) for s in metrics.snapshots if s.get("api_search_ms", 0) > 0]

        if p50s:
            print(f"  API p50:     {sum(p50s)/len(p50s):.1f}ms (max {max(p50s):.1f}ms)")
        if p99s:
            print(f"  API p99:     {sum(p99s)/len(p99s):.1f}ms (max {max(p99s):.1f}ms)")
        if searches:
            print(f"  Search avg:  {sum(searches)/len(searches):.1f}ms (max {max(searches):.1f}ms)")

    # Health check
    color = GREEN
    issues = []
    if total_err / max(total, 1) > 0.05:
        color = RED
        issues.append(f"High error rate: {total_err/max(total,1)*100:.1f}%")
    if metrics.snapshots:
        max_p99 = max((s["api_p99"] for s in metrics.snapshots), default=0)
        if max_p99 > 5000:
            color = RED
            issues.append(f"API p99 > 5s: {max_p99:.0f}ms")
        elif max_p99 > 1000:
            color = YELLOW
            issues.append(f"API p99 > 1s: {max_p99:.0f}ms")

    if issues:
        print(f"\n  {color}Issues:{RESET}")
        for issue in issues:
            print(f"    - {issue}")
    else:
        print(f"\n  {GREEN}All healthy{RESET}")

    # Save results
    if args.output:
        with open(args.output, "w") as f:
            json.dump({
                "config": vars(args),
                "totals": {
                    "http_sent": metrics.http_sent,
                    "http_errors": metrics.http_errors,
                    "tcp_sent": metrics.tcp_sent,
                    "tcp_errors": metrics.tcp_errors,
                },
                "snapshots": metrics.snapshots,
            }, f, indent=2)
        print(f"\n  Results saved to {args.output}")

    # Cleanup
    for t in targets:
        if hasattr(t, 'shutdown'):
            t.shutdown()
        elif hasattr(t, 'close'):
            t.close()

    sys.exit(0 if not issues else 1)


if __name__ == "__main__":
    main()

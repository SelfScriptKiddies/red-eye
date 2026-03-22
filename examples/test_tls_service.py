#!/usr/bin/env python3
"""TLS service test — start HTTPS server and send requests through it.

Usage:
  1. Generate self-signed cert:
     openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 1 -nodes -subj '/CN=localhost'

  2. Start HTTPS server:
     python3 test_tls_service.py --server --port 8443

  3. In another terminal, send test traffic:
     python3 test_tls_service.py --client --target https://localhost:8443

  4. For testing TLS MITM via harpoon proxy:
     python3 test_tls_service.py --client --target https://localhost:18443 --no-verify
"""

import argparse
import ssl
import sys

def run_server(port):
    from http.server import HTTPServer, SimpleHTTPRequestHandler

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain('cert.pem', 'key.pem')

    httpd = HTTPServer(('0.0.0.0', port), SimpleHTTPRequestHandler)
    httpd.socket = context.wrap_socket(httpd.socket, server_side=True)

    print(f"[*] HTTPS server running on port {port}")
    print(f"[*] Press Ctrl+C to stop")
    httpd.serve_forever()

def run_client(target, no_verify):
    import requests

    verify = not no_verify

    print(f"[*] Sending requests to {target} (verify={verify})")

    s = requests.Session()

    # Basic GET
    r = s.get(f"{target}/", verify=verify)
    print(f"    GET / → {r.status_code}")

    # With auth header
    r = s.get(f"{target}/", headers={
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.test.signature",
        "X-Secret": "supersecret123",
    }, verify=verify)
    print(f"    GET / with auth → {r.status_code}")

    # POST with sensitive data
    r = s.post(f"{target}/", json={
        "password": "admin123",
        "credit_card": "4111-1111-1111-1111",
    }, verify=verify)
    print(f"    POST / with sensitive data → {r.status_code}")

    # Cookie test over TLS
    s.cookies.set("secure_session", "encrypted_session_token_xyz")
    r = s.get(f"{target}/", verify=verify)
    print(f"    GET / with secure cookie → {r.status_code}")

    print("[+] TLS client test done")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", action="store_true", help="Run HTTPS server")
    parser.add_argument("--client", action="store_true", help="Run test client")
    parser.add_argument("--port", type=int, default=8443, help="Server port")
    parser.add_argument("--target", default="https://localhost:8443", help="Target URL")
    parser.add_argument("--no-verify", action="store_true", help="Skip TLS verification")
    args = parser.parse_args()

    if args.server:
        run_server(args.port)
    elif args.client:
        run_client(args.target, args.no_verify)
    else:
        parser.print_help()

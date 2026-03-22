#!/usr/bin/env python3
"""Basic HTTP test — different methods, headers, cookies."""

import requests
import sys

TARGET = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:31337"

s = requests.Session()

# Simple GET
print("[*] GET /")
r = s.get(f"{TARGET}/")
print(f"    Status: {r.status_code}")

# GET with custom headers
print("[*] GET / with custom headers")
r = s.get(f"{TARGET}/", headers={
    "X-Custom-Header": "test-value-123",
    "X-Forwarded-For": "10.80.3.1",
    "User-Agent": "RedEye-Test/1.0",
})
print(f"    Status: {r.status_code}")

# POST with JSON body
print("[*] POST / with JSON")
r = s.post(f"{TARGET}/", json={"username": "admin", "password": "test123"}, headers={
    "Content-Type": "application/json",
})
print(f"    Status: {r.status_code}")

# GET with cookies
print("[*] GET / with cookies")
s.cookies.set("session", "abc123def456")
s.cookies.set("csrf_token", "xyz789")
s.cookies.set("user_id", "42")
r = s.get(f"{TARGET}/")
print(f"    Status: {r.status_code}")

# Multiple requests to same endpoint (for path filter testing)
print("[*] 3x GET /red-eye/ (path filter test)")
for i in range(3):
    r = s.get(f"{TARGET}/red-eye/", headers={"X-Request-Id": f"req-{i}"})
    print(f"    [{i}] Status: {r.status_code}")

# Different User-Agents (for UA grouping test)
print("[*] Requests with different User-Agents")
for ua in ["python-requests/2.31", "curl/7.88", "Mozilla/5.0 (exploit)", ""]:
    r = requests.get(f"{TARGET}/", headers={"User-Agent": ua} if ua else {})
    print(f"    UA='{ua[:30]}' Status: {r.status_code}")

print("[+] Basic HTTP test done")

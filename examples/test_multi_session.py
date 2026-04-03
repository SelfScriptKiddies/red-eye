#!/usr/bin/env python3
"""Multi-session test — simulate multiple attackers with different cookies/sessions.

Useful for testing cookie-based grouping and session tracking.
"""

import requests
import sys
import time
import random
import string

TARGET = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:31337"

def random_token(n=16):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=n))

# Simulate 3 different "attackers" with different sessions
attackers = [
    {"name": "Team1", "session": random_token(), "ua": "Team1-Bot/1.0"},
    {"name": "Team2", "session": random_token(), "ua": "python-requests/2.31"},
    {"name": "Team3", "session": random_token(), "ua": "curl/7.88"},
]

print(f"[*] Simulating {len(attackers)} attackers against {TARGET}")

for round_num in range(3):
    print(f"\n--- Round {round_num + 1} ---")
    for attacker in attackers:
        s = requests.Session()
        s.cookies.set("session", attacker["session"])
        s.headers["User-Agent"] = attacker["ua"]

        # Each attacker does a different sequence
        endpoints = ["/", "/red-eye/", "/harpoon/"]
        endpoint = random.choice(endpoints)

        r = s.get(f"{TARGET}{endpoint}")
        print(f"  [{attacker['name']}] GET {endpoint} → {r.status_code} (session={attacker['session'][:8]}...)")

        time.sleep(0.1)

print(f"\n[+] Multi-session test done")
print(f"[*] Session tokens for filtering:")
for a in attackers:
    print(f"    {a['name']}: session={a['session']}")

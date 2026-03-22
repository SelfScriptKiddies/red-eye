#!/usr/bin/env python3
"""Binary protocol test — raw TCP with binary data (pwn-style).

Start a listener first:
  nc -l -p 9999

Then run:
  python3 test_binary_protocol.py localhost 9999
"""

import socket
import struct
import sys
import time

HOST = sys.argv[1] if len(sys.argv) > 1 else "localhost"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 9999

print(f"[*] Connecting to {HOST}:{PORT}")

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))

# Send a binary "handshake"
print("[*] Sending handshake")
handshake = struct.pack("<4sII", b"HELO", 1, 0x41414141)
s.send(handshake)
time.sleep(0.5)

# Send a "login" packet
print("[*] Sending login packet")
username = b"admin\x00"
password = b"password123\x00"
login_pkt = struct.pack("<4sHH", b"AUTH", len(username), len(password)) + username + password
s.send(login_pkt)
time.sleep(0.5)

# Send a "command" packet with potential exploit payload
print("[*] Sending command with overflow")
cmd = b"A" * 64 + struct.pack("<Q", 0xdeadbeefcafebabe)  # Buffer overflow
cmd_pkt = struct.pack("<4sI", b"EXEC", len(cmd)) + cmd
s.send(cmd_pkt)
time.sleep(0.5)

# Try to receive response
try:
    s.settimeout(2)
    resp = s.recv(4096)
    print(f"[*] Received: {resp.hex()}")
except socket.timeout:
    print("[*] No response (timeout)")

s.close()
print("[+] Binary protocol test done")

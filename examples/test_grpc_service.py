#!/usr/bin/env python3
"""gRPC test — simulate gRPC-like traffic over HTTP/2.

This sends raw HTTP/2 with application/grpc content-type and protobuf-encoded body.
For testing without a real gRPC server, we send the raw bytes that look like gRPC.

Usage:
  python3 test_grpc_service.py http://TARGET_IP:PORT

Note: For real gRPC testing, use grpcurl or a Python gRPC client with .proto files.
"""

import struct
import sys
import requests

TARGET = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:31337"

def encode_varint(value):
    """Encode integer as protobuf varint."""
    result = bytearray()
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)

def encode_string_field(field_number, value):
    """Encode a string field in protobuf wire format."""
    tag = encode_varint((field_number << 3) | 2)  # wire type 2 = length-delimited
    data = value.encode('utf-8')
    return tag + encode_varint(len(data)) + data

def encode_int_field(field_number, value):
    """Encode an integer field in protobuf wire format."""
    tag = encode_varint((field_number << 3) | 0)  # wire type 0 = varint
    return tag + encode_varint(value)

def make_grpc_body(protobuf_payload):
    """Wrap protobuf in gRPC frame: [compressed: 1 byte] [length: 4 bytes BE] [payload]"""
    return struct.pack('>BI', 0, len(protobuf_payload)) + protobuf_payload

# Simulated gRPC messages
print("[*] Simulating gRPC-like traffic")

# 1. Login request: { field_1: "admin", field_2: "password123" }
login_payload = encode_string_field(1, "admin") + encode_string_field(2, "password123")
login_body = make_grpc_body(login_payload)

print(f"[*] Sending LoginRequest ({len(login_body)} bytes)")
try:
    r = requests.post(
        f"{TARGET}/UserService/Login",
        data=login_body,
        headers={
            "Content-Type": "application/grpc",
            "TE": "trailers",
        }
    )
    print(f"    Status: {r.status_code}")
except Exception as e:
    print(f"    Error: {e}")

# 2. GetUser request: { field_1: 42 }
getuser_payload = encode_int_field(1, 42)
getuser_body = make_grpc_body(getuser_payload)

print(f"[*] Sending GetUserRequest ({len(getuser_body)} bytes)")
try:
    r = requests.post(
        f"{TARGET}/UserService/GetUser",
        data=getuser_body,
        headers={
            "Content-Type": "application/grpc+proto",
            "TE": "trailers",
        }
    )
    print(f"    Status: {r.status_code}")
except Exception as e:
    print(f"    Error: {e}")

# 3. SubmitFlag: { field_1: "FLAG{test_grpc_flag_12345}" }
flag_payload = encode_string_field(1, "FLAG{test_grpc_flag_12345}")
flag_body = make_grpc_body(flag_payload)

print(f"[*] Sending SubmitFlag ({len(flag_body)} bytes)")
try:
    r = requests.post(
        f"{TARGET}/FlagService/Submit",
        data=flag_body,
        headers={
            "Content-Type": "application/grpc",
            "TE": "trailers",
        }
    )
    print(f"    Status: {r.status_code}")
except Exception as e:
    print(f"    Error: {e}")

print("[+] gRPC test done")
print("\nExpected in Red Eye:")
print("  - Streams with l7_protocol = 'grpc'")
print("  - Decoded protobuf fields (field_1, field_2)")
print("  - Service/method names from path (UserService/Login, etc.)")

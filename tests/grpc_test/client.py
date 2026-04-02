#!/usr/bin/env python3
"""gRPC test client — sends requests to the test server."""

import grpc
import sys
import os

# Generate pb2 files if needed
proto_dir = os.path.dirname(__file__)
if not os.path.exists(os.path.join(proto_dir, "service_pb2.py")):
    os.system(f"python3 -m grpc_tools.protoc -I{proto_dir} --python_out={proto_dir} --grpc_python_out={proto_dir} {proto_dir}/service.proto")

sys.path.insert(0, proto_dir)
import service_pb2
import service_pb2_grpc


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else "localhost:50051"
    print(f"[*] Connecting to gRPC server at {target}")

    channel = grpc.insecure_channel(target)
    stub = service_pb2_grpc.UserServiceStub(channel)

    # 1. Login
    print("\n[*] Login(admin, password123)")
    resp = stub.Login(service_pb2.LoginRequest(username="admin", password="password123"))
    print(f"    success={resp.success} token={resp.token}")

    # 2. Login with wrong password
    print("\n[*] Login(admin, wrong)")
    resp = stub.Login(service_pb2.LoginRequest(username="admin", password="wrong"))
    print(f"    success={resp.success} token={resp.token}")

    # 3. GetUser
    print("\n[*] GetUser(42)")
    resp = stub.GetUser(service_pb2.GetUserRequest(user_id=42))
    print(f"    user_id={resp.user_id} username={resp.username} email={resp.email} role={resp.role}")

    # 4. SubmitFlag (valid)
    print("\n[*] SubmitFlag(FLAG{test_grpc_12345})")
    resp = stub.SubmitFlag(service_pb2.SubmitFlagRequest(flag="FLAG{test_grpc_12345}", service_name="vuln-service"))
    print(f"    accepted={resp.accepted} message={resp.message}")

    # 5. SubmitFlag (invalid)
    print("\n[*] SubmitFlag(not_a_flag)")
    resp = stub.SubmitFlag(service_pb2.SubmitFlagRequest(flag="not_a_flag", service_name="vuln-service"))
    print(f"    accepted={resp.accepted} message={resp.message}")

    print("\n[+] gRPC client test done")
    channel.close()


if __name__ == "__main__":
    main()

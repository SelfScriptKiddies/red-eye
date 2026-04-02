#!/usr/bin/env python3
"""Simple gRPC test server for Red Eye testing."""

import grpc
from concurrent import futures
import sys
import os

# Generate pb2 files
os.system(f"python3 -m grpc_tools.protoc -I{os.path.dirname(__file__)} --python_out={os.path.dirname(__file__)} --grpc_python_out={os.path.dirname(__file__)} {os.path.dirname(__file__)}/service.proto")

sys.path.insert(0, os.path.dirname(__file__))
import service_pb2
import service_pb2_grpc


class UserServiceServicer(service_pb2_grpc.UserServiceServicer):
    def Login(self, request, context):
        print(f"[gRPC] Login: {request.username}")
        if request.username == "admin" and request.password == "password123":
            return service_pb2.LoginResponse(token="tok_abc123def456", success=True)
        return service_pb2.LoginResponse(token="", success=False)

    def GetUser(self, request, context):
        print(f"[gRPC] GetUser: {request.user_id}")
        return service_pb2.GetUserResponse(
            user_id=request.user_id,
            username="admin",
            email="admin@ctf.local",
            role="ADMIN"
        )

    def SubmitFlag(self, request, context):
        print(f"[gRPC] SubmitFlag: {request.flag} for {request.service_name}")
        if request.flag.startswith("FLAG{"):
            return service_pb2.SubmitFlagResponse(accepted=True, message="Flag accepted!")
        return service_pb2.SubmitFlagResponse(accepted=False, message="Invalid flag format")


def serve(port=50051):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service_pb2_grpc.add_UserServiceServicer_to_server(UserServiceServicer(), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"[*] gRPC server listening on :{port}")
    server.wait_for_termination()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 50051
    serve(port)

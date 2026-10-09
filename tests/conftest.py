"""
Shared pytest fixtures.

We launch real gRPC servers on ephemeral ports (port=0) inside each test,
so the test suite is hermetic: no fixed ports, no orphan processes.
"""

import socket
import threading
import time
from concurrent import futures

import grpc
import pytest

import counter_pb2
import counter_pb2_grpc
import server as server_mod


def _free_port():
    """Ask the OS for a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class RunningServer:
    """Wrapper around a running gRPC server for a single replica."""

    def __init__(self, port, replica_id="A", delay_ms=0, fault=None):
        self.port = port
        self.replica_id = replica_id
        self.delay_ms = delay_ms
        self.fault = fault

        self.server_impl = server_mod.CounterServer(
            replica_id=replica_id, delay_ms=delay_ms, fault=fault
        )
        self.grpc_server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=8)
        )
        counter_pb2_grpc.add_CounterServicer_to_server(
            self.server_impl, self.grpc_server
        )
        self.grpc_server.add_insecure_port(f"127.0.0.1:{port}")
        self.grpc_server.start()

    def new_client(self, deadline=2.0):
        """Return a thin test client bound to this replica."""
        return TestClient(port=self.port, deadline=deadline)

    def stop(self):
        self.grpc_server.stop(0).wait(timeout=2)

    # expose state for assertions
    @property
    def values(self):
        return self.server_impl.values


class TestClient:
    """
    Minimal client used only by tests.
    Handles retries with the SAME idempotency key, mirroring client.py.
    """

    def __init__(self, port, deadline=2.0):
        self.deadline = deadline
        self.channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        self.stub = counter_pb2_grpc.CounterStub(self.channel)

    def incr(self, counter_id, delta, key, retries=3):
        """Increment with bounded retries; key is reused across attempts."""
        for attempt in range(retries + 1):
            try:
                req = counter_pb2.IncrementRequest(
                    counter_id=counter_id, delta=delta,
                    idempotency_key=key, lamport_time=0,
                )
                return self.stub.Increment(req, timeout=self.deadline)
            except grpc.RpcError as e:
                if e.code() not in (
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                    grpc.StatusCode.UNAVAILABLE,
                ):
                    raise
                if attempt == retries:
                    raise
                time.sleep(0.1 * (2 ** attempt))

    def get(self, counter_id):
        req = counter_pb2.GetRequest(counter_id=counter_id, lamport_time=0)
        return self.stub.Get(req, timeout=self.deadline)

    def close(self):
        self.channel.close()


@pytest.fixture
def running_server():
    """A single fresh replica on a random port, shut down afterwards."""
    port = _free_port()
    srv = RunningServer(port=port, replica_id="A")
    try:
        yield srv
    finally:
        srv.stop()

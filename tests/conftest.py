"""
Shared pytest fixtures.

We launch real gRPC servers on ephemeral ports inside each test,
so the test suite is hermetic: no fixed ports, no orphan processes.

Two fixtures:
    running_server   -- a single replica
    three_replicas   -- three independent replicas for quorum tests
"""

import socket
import threading  # noqa: F401  (kept for parity with assignment)
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


# ---------------------------------------------------------------------
# Single-replica fixture
# ---------------------------------------------------------------------

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
        """Return a thin client bound to this replica."""
        return CounterClient(port=self.port, deadline=deadline)

    def stop(self):
        # grace=0.5 lets in-flight handlers flush their logs before
        # the underlying thread pool is torn down.
        self.grpc_server.stop(grace=0.5).wait(timeout=3)

    @property
    def values(self):
        return self.server_impl.values


class CounterClient:
    """
    Minimal client used only by tests (single replica).
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


# ---------------------------------------------------------------------
# Three-replica fixture for Part C (quorum) tests
# ---------------------------------------------------------------------

class ThreeReplicas:
    """
    Manages three independent RunningServer instances.

    Each replica is a fully separate object with its own values/seen dicts.
    """

    def __init__(self):
        self.ports = [_free_port() for _ in range(3)]
        self.replicas = [
            RunningServer(port=self.ports[i], replica_id=chr(ord("A") + i))
            for i in range(3)
        ]

    def client(self, deadline=2.0):
        return QuorumTestClient(ports=self.ports, deadline=deadline)

    def stop_replica(self, idx):
        """Simulate a crash of replica #idx (0..2)."""
        self.replicas[idx].stop()

    def values(self, idx):
        return self.replicas[idx].values

    def stop(self):
        for r in self.replicas:
            try:
                r.stop()
            except Exception:
                pass


class QuorumTestClient:
    """Test client that talks to N replicas and enforces majority commit."""

    def __init__(self, ports, deadline=2.0):
        self.ports = list(ports)
        self.deadline = deadline
        self.channels = [
            grpc.insecure_channel(f"127.0.0.1:{p}") for p in self.ports
        ]
        self.stubs = [
            counter_pb2_grpc.CounterStub(ch) for ch in self.channels
        ]

    def incr(self, counter_id, delta, key):
        """
        Send to all replicas. Returns (committed: bool, new_value, acks).
        Idempotency key is generated by caller and reused across replicas.
        """
        req = counter_pb2.IncrementRequest(
            counter_id=counter_id, delta=delta,
            idempotency_key=key, lamport_time=0,
        )
        replies = []
        for stub in self.stubs:
            try:
                r = stub.Increment(req, timeout=self.deadline)
                replies.append(r)
            except grpc.RpcError:
                pass
        n = len(self.stubs)
        majority = n // 2 + 1
        if len(replies) >= majority:
            best = max(r.new_value for r in replies)
            return True, best, len(replies)
        return False, None, len(replies)

    def get(self, counter_id, replica_idx=0):
        """Read from a single replica (default: the first)."""
        req = counter_pb2.GetRequest(counter_id=counter_id, lamport_time=0)
        return self.stubs[replica_idx].Get(req, timeout=self.deadline)

    def close(self):
        for ch in self.channels:
            ch.close()


@pytest.fixture
def three_replicas():
    """Three fresh replicas on random ports, all shut down afterwards."""
    group = ThreeReplicas()
    try:
        yield group
    finally:
        group.stop()
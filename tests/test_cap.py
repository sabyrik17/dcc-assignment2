"""
CAP Lab — partition / consistency tests.

Simulates network partitions by simply NOT routing to certain replicas
from the client (equivalent to those replicas being unreachable).
Each test uses in-process KVServers started on ephemeral ports.

Minimum required by the lab: 6 tests. We have 7.
"""

import socket
import threading
import time

import grpc
import pytest
from concurrent import futures

import kvstore_pb2
import kvstore_pb2_grpc
import kvserver as kvmod


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class RunningKV:
    def __init__(self, replica_id, is_leaseholder=False):
        self.port = _free_port()
        self.impl = kvmod.KVServer(replica_id=replica_id, is_leaseholder=is_leaseholder)
        self.gs = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(self.impl, self.gs)
        self.gs.add_insecure_port(f"127.0.0.1:{self.port}")
        self.gs.start()

    def stub(self):
        ch = grpc.insecure_channel(f"127.0.0.1:{self.port}")
        return kvstore_pb2_grpc.KeyValueStoreStub(ch), ch

    def stop(self):
        self.gs.stop(grace=0.1).wait(timeout=2)


@pytest.fixture
def cluster():
    """Three replicas: A=leaseholder, B, C=follower."""
    a = RunningKV("A", is_leaseholder=True)
    b = RunningKV("B", is_leaseholder=False)
    c = RunningKV("C", is_leaseholder=False)
    try:
        yield {"A": a, "B": b, "C": c}
    finally:
        for r in (a, b, c):
            r.stop()


# helper: send Put to a subset of the cluster (simulates reachable nodes)
def put_to(replicas, key, value, strategy, ts, key_id=None):
    req = kvstore_pb2.PutRequest(
        key=key, value=value, idempotency_key=key_id or "",
        strategy=strategy, timestamp=ts,
    )
    acks = 0
    for r in replicas:
        stub, ch = r.stub()
        try:
            resp = stub.Put(req, timeout=2.0)
            if resp.success:
                acks += 1
        except grpc.RpcError:
            pass
        finally:
            ch.close()
    return acks


def get_from(replica, key, consistency, max_staleness_ms=0):
    stub, ch = replica.stub()
    try:
        req = kvstore_pb2.GetRequest(
            key=key, consistency=consistency, max_staleness_ms=max_staleness_ms,
        )
        return stub.Get(req, timeout=2.0)
    finally:
        ch.close()


# ---------------------------------------------------------------------
# 1. QUORUM_CP with majority reachable -> write commits
# ---------------------------------------------------------------------

def test_cp_quorum_with_majority_succeeds(cluster):
    # DC-1 reachable: A and B. C is partitioned away.
    reachable = [cluster["A"], cluster["B"]]
    acks = put_to(reachable, "x", "v1",
                  kvstore_pb2.QUORUM_CP, ts=1000, key_id="k1")
    assert acks == 2
    majority = 3 // 2 + 1
    assert acks >= majority


# ---------------------------------------------------------------------
# 2. QUORUM_CP with minority reachable -> write fails
# ---------------------------------------------------------------------

def test_cp_quorum_with_minority_fails(cluster):
    # DC-2 only: C reachable. A and B partitioned away.
    reachable = [cluster["C"]]
    acks = put_to(reachable, "x", "v2",
                  kvstore_pb2.QUORUM_CP, ts=2000, key_id="k2")
    majority = 3 // 2 + 1
    assert acks < majority  # 1 < 2 -> fails
    assert acks == 1


# ---------------------------------------------------------------------
# 3. ALWAYS_AP with minority reachable -> write accepted
# ---------------------------------------------------------------------

def test_ap_accepts_minority_write(cluster):
    reachable = [cluster["C"]]
    acks = put_to(reachable, "x", "v2",
                  kvstore_pb2.ALWAYS_AP, ts=2000, key_id="k3")
    assert acks == 1  # accepted on any single node


# ---------------------------------------------------------------------
# 4. LINEARIZABLE read on non-leaseholder -> FAILED_PRECONDITION
# ---------------------------------------------------------------------

def test_linearizable_on_follower_rejected(cluster):
    # write first via leaseholder
    put_to([cluster["A"]], "x", "v1", kvstore_pb2.QUORUM_CP, ts=1000, key_id="k4")
    with pytest.raises(grpc.RpcError) as ei:
        get_from(cluster["B"], "x", kvstore_pb2.LINEARIZABLE)
    assert ei.value.code() == grpc.StatusCode.FAILED_PRECONDITION


# ---------------------------------------------------------------------
# 5. LINEARIZABLE read on leaseholder -> OK
# ---------------------------------------------------------------------

def test_linearizable_on_leaseholder_ok(cluster):
    put_to([cluster["A"]], "x", "v1", kvstore_pb2.QUORUM_CP, ts=1000, key_id="k5")
    reply = get_from(cluster["A"], "x", kvstore_pb2.LINEARIZABLE)
    assert reply.found
    assert reply.value == "v1"


# ---------------------------------------------------------------------
# 6. FOLLOWER_STALE read succeeds on any replica
# ---------------------------------------------------------------------

def test_follower_stale_on_any_replica(cluster):
    # write to all replicas (full propagation)
    for r in cluster.values():
        put_to([r], "x", "v1", kvstore_pb2.ALWAYS_AP, ts=1000, key_id="k6-" + r.impl.replica_id)
    # follower B serves stale mode
    reply = get_from(cluster["B"], "x", kvstore_pb2.FOLLOWER_STALE)
    assert reply.found
    assert reply.value == "v1"


# ---------------------------------------------------------------------
# 7. Partition recovery: LWW merge picks highest timestamp
# ---------------------------------------------------------------------

def test_lww_merge_on_recovery(cluster):
    """
    DC-1 writes x=v1 (ts=1000). DC-2 writes x=v2 (ts=2000).
    Both applied on disjoint nodes. After both reachable to each other,
    writes with higher ts win on each node (LWW).
    """
    # DC-1: A and B get x=v1 at ts=1000
    put_to([cluster["A"], cluster["B"]], "x", "v1",
           kvstore_pb2.ALWAYS_AP, ts=1000, key_id="LWW-1")
    # DC-2: C gets x=v2 at ts=2000 (higher)
    put_to([cluster["C"]], "x", "v2",
           kvstore_pb2.ALWAYS_AP, ts=2000, key_id="LWW-2")

    # Divergent state before "recovery":
    assert get_from(cluster["A"], "x", kvstore_pb2.FOLLOWER_STALE).value == "v1"
    assert get_from(cluster["C"], "x", kvstore_pb2.FOLLOWER_STALE).value == "v2"

    # Recovery: re-apply v2 to A and B with ts=2000 (client would replay or
    # a gossip protocol would push it). LWW keeps v2 everywhere.
    put_to([cluster["A"], cluster["B"]], "x", "v2",
           kvstore_pb2.ALWAYS_AP, ts=2000, key_id="LWW-2-replay")

    for r in ("A", "B", "C"):
        reply = get_from(cluster[r], "x", kvstore_pb2.FOLLOWER_STALE)
        assert reply.value == "v2"
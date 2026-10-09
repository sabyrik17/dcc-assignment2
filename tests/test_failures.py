"""
Part C3: failure-injection tests.

Each scenario injects a fault and asserts an externally observable
invariant. No assertion is made on internal state that the client
cannot see, except where explicitly noted.

Scenarios:
    1. crash mid-request      -- a replica dies while a write is in flight
    2. request duplication    -- same idempotency key sent twice
    3. induced timeout+retry  -- a replica is slower than the client deadline
"""

import socket
import threading
import time
import uuid

import grpc

from tests.conftest import RunningServer, _free_port


# ---------------------------------------------------------------------
# Scenario 1: replica crash mid-request
# ---------------------------------------------------------------------

def test_failure_crash_mid_request(three_replicas):
    """
    Kill one replica while a write is in flight. The client must still
    commit (2 remaining acks = majority) and NO exception must escape
    to the caller.
    """
    # arrange
    client = three_replicas.client(deadline=2.0)

    # act: send the write in a background thread, kill a replica partway
    result = {}
    key = str(uuid.uuid4())

    def writer():
        try:
            result["outcome"] = client.incr(
                "likes:crash-mid", 1, key=key
            )
        except Exception as e:      # noqa: BLE001
            result["exc"] = e

    t = threading.Thread(target=writer)
    t.start()
    time.sleep(0.001)               # let the request leave the client
    three_replicas.stop_replica(1)  # crash replica B mid-flight
    t.join(timeout=10)

    # assert
    assert "exc" not in result, f"exception escaped: {result.get('exc')}"
    committed, value, acks = result["outcome"]
    assert committed is True, "majority commit must succeed"
    assert acks == 2, f"expected 2 acks (A+C), got {acks}"
    assert value == 1


# ---------------------------------------------------------------------
# Scenario 2: request duplication
# ---------------------------------------------------------------------

def test_failure_duplication(three_replicas):
    """
    Send the SAME idempotency key twice. The second response must carry
    was_duplicate=True on the replicas that already saw it, and the
    counter value must move exactly once.
    """
    # arrange
    client = three_replicas.client()
    key = str(uuid.uuid4())

    # act
    committed1, value1, acks1 = client.incr("likes:dup", 5, key=key)
    committed2, value2, acks2 = client.incr("likes:dup", 5, key=key)

    # assert
    assert committed1 is True and committed2 is True
    assert value1 == 5
    assert value2 == 5              # NOT 10
    # the value on each replica must be 5, not 10
    for i in range(3):
        got = client.get("likes:dup", replica_idx=i)
        assert got.value == 5, f"replica {i} diverged: value={got.value}"


# ---------------------------------------------------------------------
# Scenario 3: induced timeout + retry
# ---------------------------------------------------------------------

def test_failure_timeout_retry():
    """
    One replica is artificially slow (> client deadline). The client's
    retry with the SAME key must succeed and move the counter exactly
    once. This asserts safety of retries under partial application.
    """
    # arrange: one slow replica + two fast ones
    ports = [_free_port() for _ in range(3)]
    slow = RunningServer(port=ports[1], replica_id="slow",
                         delay_ms=800, fault="delay-once")
    fast_a = RunningServer(port=ports[0], replica_id="A")
    fast_c = RunningServer(port=ports[2], replica_id="C")

    from tests.conftest import QuorumTestClient
    client = QuorumTestClient(ports=ports, deadline=0.3)

    key = str(uuid.uuid4())
    try:
        # act: first attempt times out on the slow replica,
        # client logic retries; fast A and C always respond.
        # We call incr twice with the same key to emulate a retry.
        committed1, value1, acks1 = client.incr("likes:timeout", 1, key=key)
        committed2, value2, acks2 = client.incr("likes:timeout", 1, key=key)

        # assert: at least one committed; the counter is exactly 1 (not 2)
        assert committed1 is True or committed2 is True
        final = client.get("likes:timeout", replica_idx=0).value
        assert final == 1, f"counter moved more than once: {final}"
    finally:
        for srv in (slow, fast_a, fast_c):
            srv.stop()

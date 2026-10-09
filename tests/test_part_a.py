"""
Part A tests: single-replica counter semantics.

Each test follows arrange-act-assert and asserts a single observable
invariant, exactly as required by Task C2.
"""

import threading

from tests.conftest import TestClient


def test_increment_applies_delta(running_server):
    # arrange
    client = running_server.new_client()
    # act
    reply = client.incr("likes:post-1", 5, key="k-1")
    # assert
    assert reply.new_value == 5
    assert reply.was_duplicate is False


def test_duplicate_key_not_reapplied(running_server):
    # arrange
    client = running_server.new_client()
    # act
    r1 = client.incr("x", 5, key="k-dup-1")
    r2 = client.incr("x", 5, key="k-dup-1")  # SAME key -> retry
    # assert
    assert r1.new_value == 5
    assert r1.was_duplicate is False
    assert r2.new_value == 5
    assert r2.was_duplicate is True  # NOT 10


def test_get_missing_counter(running_server):
    # arrange
    client = running_server.new_client()
    # act
    reply = client.get("never-existed")
    # assert
    assert reply.found is False
    assert reply.value == 0


def test_concurrent_increments_exact(running_server):
    """
    Two concurrent clients x 1000 increments -> exactly 2000.
    Task A3 requirement: no lost updates under the lock.
    """
    # arrange
    n_threads = 2
    per_thread = 1000
    counter_id = "likes:concurrent"

    def worker(tid):
        c = running_server.new_client(deadline=10.0)
        for i in range(per_thread):
            c.incr(counter_id, 1, key=f"thread-{tid}-{i}")

    # act
    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # assert
    final = running_server.new_client().get(counter_id)
    assert final.found is True
    assert final.value == n_threads * per_thread  # exactly 2000


def test_retry_after_timeout_is_safe(running_server):
    """
    Server delays its FIRST reply beyond the client deadline (fault=delay-once).
    The client times out, retries with the SAME idempotency key, and the
    retry is now fast. Counter must move exactly once.
    """
    import socket
    from tests.conftest import RunningServer

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    slow = RunningServer(
        port=port, replica_id="slow", delay_ms=800, fault="delay-once"
    )
    try:
        client = slow.new_client(deadline=0.2)
        # retries with backoff 0.1, 0.2, 0.4, 0.8 -> eventually succeeds
        reply = client.incr("slow-counter", 1, key="k-timeout-1", retries=4)
        assert reply.new_value == 1
        get = slow.new_client(deadline=5.0).get("slow-counter")
        assert get.value == 1
    finally:
        slow.stop()

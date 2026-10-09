"""
Part C tests: three-replica quorum semantics.

Covers the C2 test table:
    - test_majority_commit_two_acks
    - test_no_commit_below_majority
    - test_replicas_converge
"""

import uuid


def test_majority_commit_two_acks(three_replicas):
    """
    With ONE replica stopped, a write still commits: 2 of 3 acks = majority.
    """
    # arrange: kill replica #1 (B)
    three_replicas.stop_replica(1)
    client = three_replicas.client()

    # act
    committed, value, acks = client.incr(
        "likes:post-c1", 5, key=str(uuid.uuid4())
    )

    # assert
    assert committed is True
    assert value == 5
    assert acks == 2  # A and C ack


def test_no_commit_below_majority(three_replicas):
    """
    With TWO replicas stopped, the write reports FAILURE.
    The surviving replica may still have applied the delta, but the client
    must NOT present this as committed (partial-application anomaly).
    """
    # arrange: kill replicas #1 and #2 (B and C)
    three_replicas.stop_replica(1)
    three_replicas.stop_replica(2)
    client = three_replicas.client()

    # act
    committed, value, acks = client.incr(
        "likes:post-c2", 5, key=str(uuid.uuid4())
    )

    # assert
    assert committed is False
    assert value is None
    assert acks == 1  # only replica A acked


def test_replicas_converge(three_replicas):
    """
    After a batch of quorum writes, all live replicas hold identical values.
    """
    # arrange
    client = three_replicas.client()
    n_writes = 20
    expected = 0

    # act
    for i in range(n_writes):
        delta = 1
        expected += delta
        committed, value, acks = client.incr(
            "likes:post-c3", delta, key=f"key-{i}"
        )
        assert committed is True
        assert acks == 3

    # assert
    values = [
        client.get("likes:post-c3", replica_idx=i).value for i in range(3)
    ]
    assert values == [expected, expected, expected]
    assert expected == n_writes

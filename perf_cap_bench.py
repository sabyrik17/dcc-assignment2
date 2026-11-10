"""
PACELC benchmark for CAP Lab.

Scenario 1: Get with LINEARIZABLE consistency -> leaseholder (A).
Scenario 2: Get with FOLLOWER_STALE consistency -> local follower (B).

For each scenario, 1000 iterations. Measures median, p95, availability %.
"""

import math
import socket
import statistics
import sys
import time
import uuid

import grpc
from concurrent import futures

import kvstore_pb2
import kvstore_pb2_grpc
import kvserver as kvmod


N_ITER = 1000
MAX_STALENESS_MS = 5000
PUT_KEY = "bench-key"


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class RunningKV:
    def __init__(self, replica_id, is_leaseholder=False):
        self.port = _free_port()
        self.impl = kvmod.KVServer(replica_id=replica_id, is_leaseholder=is_leaseholder)
        self.gs = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
        kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(self.impl, self.gs)
        self.gs.add_insecure_port(f"127.0.0.1:{self.port}")
        self.gs.start()

    def stub(self):
        ch = grpc.insecure_channel(f"127.0.0.1:{self.port}")
        return kvstore_pb2_grpc.KeyValueStoreStub(ch), ch

    def stop(self):
        self.gs.stop(grace=0.1).wait(timeout=2)


def p95(sorted_latencies):
    n = len(sorted_latencies)
    idx = math.ceil(0.95 * n) - 1
    return sorted_latencies[idx]


def bench(stub, key, consistency, max_staleness_ms=0, n=N_ITER):
    latencies = []
    errors = 0
    for _ in range(n):
        t0 = time.perf_counter()
        try:
            req = kvstore_pb2.GetRequest(
                key=key, consistency=consistency,
                max_staleness_ms=max_staleness_ms,
            )
            stub.Get(req, timeout=2.0)
            latencies.append((time.perf_counter() - t0) * 1000.0)
        except grpc.RpcError:
            errors += 1
    sorted_lat = sorted(latencies)
    if not sorted_lat:
        return {"median": 0.0, "p95": 0.0, "avail": 0.0, "n": n}
    return {
        "median": statistics.median(sorted_lat),
        "p95": p95(sorted_lat),
        "avail": 100.0 * len(latencies) / n,
        "n": n,
    }


def main():
    print(f"Starting 3 in-process replicas (N={N_ITER} iterations per scenario)\n")

    a = RunningKV("A", is_leaseholder=True)
    b = RunningKV("B", is_leaseholder=False)
    c = RunningKV("C", is_leaseholder=False)

    try:
        seed_ts = int(time.time() * 1000)
        seed_req = kvstore_pb2.PutRequest(
            key=PUT_KEY, value="hello",
            idempotency_key=str(uuid.uuid4()),
            strategy=kvstore_pb2.ALWAYS_AP,
            timestamp=seed_ts,
        )
        for r in (a, b, c):
            stub, ch = r.stub()
            stub.Put(seed_req, timeout=2.0)
            ch.close()

        stub_a, ch_a = a.stub()
        r1 = bench(stub_a, PUT_KEY, kvstore_pb2.LINEARIZABLE)
        ch_a.close()

        stub_b, ch_b = b.stub()
        r2 = bench(stub_b, PUT_KEY, kvstore_pb2.FOLLOWER_STALE,
                   max_staleness_ms=MAX_STALENESS_MS)
        ch_b.close()

        print(f"{'Read Mode':<18} {'Target Node':<18} {'Median (ms)':>12} {'p95 (ms)':>10} {'Availability (%)':>18}")
        print("-" * 80)
        print(f"{'Linearizable':<18} {'Leaseholder (A)':<18} {r1['median']:>12.3f} {r1['p95']:>10.3f} {r1['avail']:>18.2f}")
        print(f"{'Follower Stale':<18} {'Local Follower (B)':<18} {r2['median']:>12.3f} {r2['p95']:>10.3f} {r2['avail']:>18.2f}")
        print()
        print(f"Iterations per scenario: {N_ITER}")
        print(f"max_staleness_ms for follower reads: {MAX_STALENESS_MS}")

    finally:
        for r in (a, b, c):
            r.stop()


if __name__ == "__main__":
    main()

"""
Benchmark for the counter service (Part C4).

Runs 4 configurations:
    1. single replica, 1 client
    2. single replica, 16 concurrent clients
    3. quorum (3 replicas), 1 client
    4. quorum (3 replicas), 16 concurrent clients

For each: >= 2000 requests, per-request latency, median and p95.

Usage:
    python bench/benchmark.py
"""

import argparse
import concurrent.futures
import math
import socket
import statistics
import sys
import threading
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import grpc
from concurrent import futures

import counter_pb2
import counter_pb2_grpc
import server as server_mod


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_server(port):
    impl = server_mod.CounterServer(replica_id="bench")
    gs = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    counter_pb2_grpc.add_CounterServicer_to_server(impl, gs)
    gs.add_insecure_port(f"127.0.0.1:{port}")
    gs.start()
    return gs


def make_stub(port):
    ch = grpc.insecure_channel(f"127.0.0.1:{port}")
    return counter_pb2_grpc.CounterStub(ch), ch


def p95(sorted_lat):
    """Assignment rule: sorted(latencies)[ceil(0.95 * n) - 1]."""
    n = len(sorted_lat)
    idx = math.ceil(0.95 * n) - 1
    return sorted_lat[idx]


def run_single(stub, total, n_clients):
    lat = []
    lock = threading.Lock()

    def worker():
        per = total // n_clients
        local = []
        for _ in range(per):
            req = counter_pb2.IncrementRequest(
                counter_id="bench", delta=1,
                idempotency_key=str(uuid.uuid4()), lamport_time=0,
            )
            t0 = time.perf_counter()
            stub.Increment(req, timeout=5.0)
            local.append((time.perf_counter() - t0) * 1000.0)
        with lock:
            lat.extend(local)

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_clients) as ex:
        for f in [ex.submit(worker) for _ in range(n_clients)]:
            f.result()
    return lat


def run_quorum(stubs, total, n_clients):
    lat = []
    lock = threading.Lock()

    def worker():
        per = total // n_clients
        local = []
        for _ in range(per):
            req = counter_pb2.IncrementRequest(
                counter_id="bench-quorum", delta=1,
                idempotency_key=str(uuid.uuid4()), lamport_time=0,
            )
            t0 = time.perf_counter()
            acks = 0
            for st in stubs:
                try:
                    st.Increment(req, timeout=5.0)
                    acks += 1
                except grpc.RpcError:
                    pass
            if acks < 2:
                raise RuntimeError(f"quorum lost: acks={acks}")
            local.append((time.perf_counter() - t0) * 1000.0)
        with lock:
            lat.extend(local)

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_clients) as ex:
        for f in [ex.submit(worker) for _ in range(n_clients)]:
            f.result()
    return lat


def summarise(name, lat):
    s = sorted(lat)
    return {"name": name, "n": len(s),
            "median": statistics.median(s), "p95": p95(s)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--clients", type=str, default="1,16")
    args = parser.parse_args()

    n_req = args.requests
    levels = [int(c) for c in args.clients.split(",")]

    print(f"Requests per config: {n_req}; concurrency: {levels}\n")

    single_port = _free_port()
    single_gs = start_server(single_port)
    single_stub, single_ch = make_stub(single_port)

    qports = [_free_port() for _ in range(3)]
    qgs = [start_server(p) for p in qports]
    qstubs, qchs = zip(*[make_stub(p) for p in qports])

    # warmup
    for _ in range(50):
        single_stub.Increment(
            counter_pb2.IncrementRequest(
                counter_id="warmup", delta=1,
                idempotency_key=str(uuid.uuid4()), lamport_time=0),
            timeout=5.0,
        )

    results = []
    for nc in levels:
        results.append(summarise(
            f"Single replica, {nc} client(s)",
            run_single(single_stub, n_req, nc)))
    for nc in levels:
        results.append(summarise(
            f"Quorum (3 replicas), {nc} client(s)",
            run_quorum(list(qstubs), n_req, nc)))

    print(f"{'Configuration':<32} {'Median (ms)':>12} {'p95 (ms)':>10} {'Requests':>10}")
    print("-" * 68)
    for r in results:
        print(f"{r['name']:<32} {r['median']:>12.3f} {r['p95']:>10.3f} {r['n']:>10}")

    for gs in [single_gs, *qgs]:
        gs.stop(grace=0.1).wait(timeout=2)
    single_ch.close()
    for ch in qchs:
        ch.close()


if __name__ == "__main__":
    main()
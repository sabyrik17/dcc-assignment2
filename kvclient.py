"""
Key-Value Store gRPC client (CAP Lab).

Usage:
    python kvclient.py put foo bar
    python kvclient.py put foo bar --strategy cp --replicas 50051,50052,50053
    python kvclient.py put foo bar --strategy ap --port 50053
    python kvclient.py get foo
    python kvclient.py get foo --consistency linearizable --port 50051
    python kvclient.py get foo --consistency stale --max-staleness-ms 5000 --port 50052

Strategies:
    cp  -> QUORUM_CP:   needs majority (N/2+1) ACKs; else fail
    ap  -> ALWAYS_AP:   accept if any replica confirms

Consistency:
    linearizable -> must be served by leaseholder (port 50051 by default)
    stale        -> any replica; optional staleness bound
"""

import argparse
import sys
import time
import uuid

import grpc

import kvstore_pb2
import kvstore_pb2_grpc


DEFAULT_PORT = 50051
DEFAULT_REPLICAS = "50051,50052,50053"
DEADLINE_S = 2.0


def _stub(port):
    ch = grpc.insecure_channel(f"localhost:{port}")
    return kvstore_pb2_grpc.KeyValueStoreStub(ch), ch


def _parse_ports(replicas: str):
    return [int(p) for p in replicas.split(",") if p.strip()]


def cmd_put(args):
    key = args.key
    value = args.value
    key_id = args.key_id or str(uuid.uuid4())
    strategy = (
        kvstore_pb2.QUORUM_CP if args.strategy == "cp" else kvstore_pb2.ALWAYS_AP
    )
    timestamp = args.timestamp or int(time.time() * 1000)

    if args.strategy == "cp":
        ports = _parse_ports(args.replicas)
        required = len(ports) // 2 + 1
    else:
        ports = _parse_ports(args.replicas) if args.replicas else [args.port]
        required = 1

    req = kvstore_pb2.PutRequest(
        key=key,
        value=value,
        idempotency_key=key_id,
        strategy=strategy,
        timestamp=timestamp,
    )

    acks = 0
    errors = []
    for port in ports:
        stub, ch = _stub(port)
        try:
            r = stub.Put(req, timeout=DEADLINE_S)
            if r.success:
                acks += 1
            else:
                errors.append(f"{port}: {r.error_message}")
        except grpc.RpcError as e:
            errors.append(f"{port}: {e.code().name}")
        finally:
            ch.close()

    n = len(ports)
    if acks >= required:
        print(f"OK put key={key} value={value!r} acks={acks}/{n} required={required} ts={timestamp}")
        return 0
    else:
        print(f"FAIL put key={key} acks={acks}/{n} required={required} errors={errors}")
        return 1


def cmd_get(args):
    consistency = (
        kvstore_pb2.LINEARIZABLE if args.consistency == "linearizable"
        else kvstore_pb2.FOLLOWER_STALE
    )
    port = args.port if args.port else 50051
    stub, ch = _stub(port)
    try:
        req = kvstore_pb2.GetRequest(
            key=args.key,
            consistency=consistency,
            max_staleness_ms=args.max_staleness_ms,
        )
        reply = stub.Get(req, timeout=DEADLINE_S)
        if reply.found:
            print(
                f"value={reply.value!r} ts={reply.read_timestamp} "
                f"stale={reply.is_stale} (port={port})"
            )
        else:
            print(f"value=<not found> (port={port})")
        return 0
    except grpc.RpcError as e:
        print(f"FAIL get key={args.key} code={e.code().name} details={e.details()}")
        return 1
    finally:
        ch.close()


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    p_put = sub.add_parser("put")
    p_put.add_argument("key")
    p_put.add_argument("value")
    p_put.add_argument("--strategy", choices=["cp", "ap"], default="cp")
    p_put.add_argument("--replicas", default=DEFAULT_REPLICAS)
    p_put.add_argument("--port", type=int, default=DEFAULT_PORT)
    p_put.add_argument("--key-id", default=None)
    p_put.add_argument("--timestamp", type=int, default=None)

    p_get = sub.add_parser("get")
    p_get.add_argument("key")
    p_get.add_argument("--consistency", choices=["linearizable", "stale"],
                       default="linearizable")
    p_get.add_argument("--max-staleness-ms", type=int, default=0)
    p_get.add_argument("--port", type=int, default=None)

    args = p.parse_args()

    if args.cmd == "put":
        sys.exit(cmd_put(args))
    elif args.cmd == "get":
        sys.exit(cmd_get(args))


if __name__ == "__main__":
    main()
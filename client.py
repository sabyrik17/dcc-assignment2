"""
Counter gRPC client.

Usage:
    python client.py incr likes:post-42 --by 5
    python client.py get likes:post-42
"""

import argparse
import logging
import sys
import time
import uuid

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("counter-client")

CLIENT_ID = "client-1"
DEFAULT_PORT = 50051
DEADLINE_S = 2.0
MAX_RETRIES = 3
BACKOFF_BASE = 0.2


class QuorumResult:
    def __init__(self, committed, new_value, acks, n_replicas, was_duplicate, errors):
        self.committed = committed
        self.new_value = new_value
        self.acks = acks
        self.n_replicas = n_replicas
        self.was_duplicate = was_duplicate
        self.errors = errors


class Client:
    def __init__(self, ports, client_id=CLIENT_ID):
        self.client_id = client_id
        self.ports = list(ports)
        self.channels = [grpc.insecure_channel(f"localhost:{p}") for p in self.ports]
        self.stubs = [counter_pb2_grpc.CounterStub(ch) for ch in self.channels]
        self.clock = LamportClock(pid=client_id)

    def _log_event(self, event, detail, L):
        log.info(f"[{self.client_id}] {event} {detail} L={L}")

    def incr_single(self, counter_id, delta, key=None):
        if key is None:
            key = str(uuid.uuid4())
        stub = self.stubs[0]
        for attempt in range(MAX_RETRIES + 1):
            L = self.clock.send()
            self._log_event("SEND", f"Increment(counter={counter_id}, delta={delta})", L)
            req = counter_pb2.IncrementRequest(
                counter_id=counter_id, delta=delta,
                idempotency_key=key, lamport_time=L,
            )
            try:
                reply = stub.Increment(req, timeout=DEADLINE_S)
                self.clock.receive(reply.lamport_time)
                self._log_event(
                    "RECV",
                    f"IncrementReply(new_value={reply.new_value}, was_duplicate={reply.was_duplicate}) (received L={reply.lamport_time})",
                    self.clock.t,
                )
                return reply
            except grpc.RpcError as e:
                code = e.code()
                if code not in (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE):
                    raise
                if attempt == MAX_RETRIES:
                    raise
                backoff = BACKOFF_BASE * (2 ** attempt)
                log.warning(f"attempt {attempt + 1} failed ({code.name}); retrying in {backoff:.2f}s with SAME key={key[:8]}")
                time.sleep(backoff)
        raise RuntimeError("unreachable")

    def get_single(self, counter_id):
        stub = self.stubs[0]
        L = self.clock.send()
        self._log_event("SEND", f"Get(counter={counter_id})", L)
        req = counter_pb2.GetRequest(counter_id=counter_id, lamport_time=L)
        reply = stub.Get(req, timeout=DEADLINE_S)
        self.clock.receive(reply.lamport_time)
        self._log_event(
            "RECV",
            f"GetReply(value={reply.value}, found={reply.found}) (received L={reply.lamport_time})",
            self.clock.t,
        )
        return reply

    def incr_quorum(self, counter_id, delta, key=None):
        if key is None:
            key = str(uuid.uuid4())
        L = self.clock.send()
        req = counter_pb2.IncrementRequest(
            counter_id=counter_id, delta=delta,
            idempotency_key=key, lamport_time=L,
        )
        replies = []
        errors = []
        for i, stub in enumerate(self.stubs):
            self._log_event("SEND", f"Increment -> replica {i + 1} (counter={counter_id}, delta={delta})", L)
            try:
                r = stub.Increment(req, timeout=DEADLINE_S)
                replies.append((i, r))
                self.clock.receive(r.lamport_time)
            except grpc.RpcError as e:
                errors.append((i, e.code()))
        n_replicas = len(self.stubs)
        majority = n_replicas // 2 + 1
        if len(replies) >= majority:
            best = max(replies, key=lambda t: t[1].new_value)
            was_dup = all(r.was_duplicate for _, r in replies)
            log.info(f"[{self.client_id}] COMMIT value={best[1].new_value} acks={len(replies)}/{n_replicas} duplicate={'yes' if was_dup else 'no'}")
            return QuorumResult(True, best[1].new_value, len(replies), n_replicas, was_dup, errors)
        else:
            log.error(f"[{self.client_id}] NO COMMIT acks={len(replies)}/{n_replicas} (need {majority})")
            return QuorumResult(False, None, len(replies), n_replicas, False, errors)


def parse_ports(args):
    if getattr(args, "replicas", None):
        return [int(p) for p in args.replicas.split(",")]
    return [args.port]


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_incr = sub.add_parser("incr")
    p_incr.add_argument("counter_id")
    p_incr.add_argument("--by", type=int, default=1)
    p_incr.add_argument("--key", default=None)
    p_incr.add_argument("--port", type=int, default=DEFAULT_PORT)
    p_incr.add_argument("--replicas", default=None)

    p_get = sub.add_parser("get")
    p_get.add_argument("counter_id")
    p_get.add_argument("--port", type=int, default=DEFAULT_PORT)
    p_get.add_argument("--replicas", default=None)

    args = parser.parse_args()
    ports = parse_ports(args)
    client = Client(ports)

    if args.cmd == "incr":
        if len(ports) > 1:
            res = client.incr_quorum(args.counter_id, args.by, key=args.key)
            if res.committed:
                print(f"OK committed value={res.new_value} (replicas acked: {res.acks}/{res.n_replicas}, duplicate: {'yes' if res.was_duplicate else 'no'})")
                sys.exit(0)
            else:
                print(f"FAIL not committed (replicas acked: {res.acks}/{res.n_replicas})")
                sys.exit(1)
        else:
            reply = client.incr_single(args.counter_id, args.by, key=args.key)
            print(f"OK committed value={reply.new_value} (duplicate: {'yes' if reply.was_duplicate else 'no'})")

    elif args.cmd == "get":
        reply = client.get_single(args.counter_id)
        if reply.found:
            print(f"value={reply.value}")
        else:
            print("value=0 (not found)")


if __name__ == "__main__":
    main()

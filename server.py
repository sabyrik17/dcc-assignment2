"""
Counter gRPC server.

Part A: single-node counter with idempotency-key deduplication.
Part B: Lamport logical clock instrumentation.
Part C: replication hooks (--replica-id, --fault, --delay-ms).

Run:
    python server.py --port 50051 --replica-id A
"""

import argparse
import logging
import sys
import threading
import time
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("counter-server")


class CounterServer(counter_pb2_grpc.CounterServicer):
    """
    State:
        values: counter_id -> current integer value.
        seen:   idempotency_key -> (counter_id, resulting_value).

    Both dicts are protected by a single lock: the check-then-act on
    `seen` and the mutation of `values` MUST happen atomically, otherwise
    two concurrent retries with the same key can both pass the check.
    """

    def __init__(self, replica_id="A", delay_ms=0, fault=None):
        self.replica_id = replica_id
        self.delay_ms = delay_ms
        self.fault = fault  # "drop-after-recv" | "delay" | None
        self.lock = threading.Lock()
        self.values = {}
        self.seen = {}
        self.clock = LamportClock(pid=f"replica-{replica_id}")

    def _log_event(self, event, detail, L):
        log.info(f"[replica-{self.replica_id}] {event} {detail} L={L}")

    # -- RPC handlers ----------------------------------------------------
    def Increment(self, request, context):
        # Lamport: RECV from client
        self.clock.receive(request.lamport_time)
        self._log_event(
            "RECV",
            f"Increment(counter={request.counter_id}, delta={request.delta}) "
            f"(received L={request.lamport_time})",
            self.clock.t,
        )

        # Fault injection: slow replica
        if self.delay_ms > 0:
            time.sleep(self.delay_ms / 1000.0)

        # Fault injection: reply lost (client sees DEADLINE/UNAVAILABLE)
        if self.fault == "drop-after-recv":
            context.abort(grpc.StatusCode.UNAVAILABLE, "fault: drop-after-recv")

        with self.lock:
            # 1. Duplicate suppression — MUST be inside the lock
            if request.idempotency_key in self.seen:
                cid, val = self.seen[request.idempotency_key]
                self.clock.tick()
                self._log_event(
                    "DUP",
                    f"key={request.idempotency_key[:8]} -> value={val} "
                    f"(was_duplicate=True)",
                    self.clock.t,
                )
                return counter_pb2.IncrementReply(
                    new_value=val,
                    was_duplicate=True,
                    lamport_time=self.clock.t,
                )

            # 2. Apply delta
            current = self.values.get(request.counter_id, 0)
            new_val = current + request.delta
            self.values[request.counter_id] = new_val

            # 3. Store result keyed by idempotency_key
            self.seen[request.idempotency_key] = (request.counter_id, new_val)

            self.clock.tick()
            self._log_event(
                "APPLY",
                f"counter={request.counter_id} -> {new_val}",
                self.clock.t,
            )

            return counter_pb2.IncrementReply(
                new_value=new_val,
                was_duplicate=False,
                lamport_time=self.clock.t,
            )

    def Get(self, request, context):
        self.clock.receive(request.lamport_time)
        self._log_event(
            "RECV",
            f"Get(counter={request.counter_id}) (received L={request.lamport_time})",
            self.clock.t,
        )

        with self.lock:
            found = request.counter_id in self.values
            value = self.values.get(request.counter_id, 0)
            self.clock.tick()
            self._log_event(
                "SEND",
                f"GetReply(value={value}, found={found})",
                self.clock.t,
            )
            return counter_pb2.GetReply(
                value=value,
                found=found,
                lamport_time=self.clock.t,
            )


def serve(port, replica_id, delay_ms, fault, max_workers=8):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    counter_pb2_grpc.add_CounterServicer_to_server(
        CounterServer(replica_id=replica_id, delay_ms=delay_ms, fault=fault),
        server,
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    log.info(f"replica-{replica_id} listening on port {port}")
    return server


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--replica-id", default="A")
    parser.add_argument("--delay-ms", type=int, default=0)
    parser.add_argument(
        "--fault",
        choices=["drop-after-recv", "delay"],
        default=None,
    )
    args = parser.parse_args()

    server = serve(
        port=args.port,
        replica_id=args.replica_id,
        delay_ms=args.delay_ms,
        fault=args.fault,
    )
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        log.info("shutting down")
        server.stop(0)
        sys.exit(0)


if __name__ == "__main__":
    main()
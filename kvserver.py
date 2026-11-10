"""
Key-Value Store gRPC server (CAP Lab).

Extends the Assignment 2 architecture:
  - Configurable read consistency: LINEARIZABLE (leaseholder) vs FOLLOWER_STALE (local)
  - Configurable write strategy:   QUORUM_CP (majority ack) vs ALWAYS_AP (single node ok)
  - Leaseholder emulation on port 50051 (replica A)
  - Last-Write-Wins (LWW) conflict resolution on timestamp
  - Bounded-staleness follower reads using wall-clock age in milliseconds

Run:
    python kvserver.py --port 50051 --replica-id A --is-leaseholder
    python kvserver.py --port 50052 --replica-id B
    python kvserver.py --port 50053 --replica-id C
"""

import argparse
import logging
import sys
import threading
import time
from concurrent import futures

import grpc

import kvstore_pb2
import kvstore_pb2_grpc


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("kvserver")


class KVServer(kvstore_pb2_grpc.KeyValueStoreServicer):
    """
    State:
        values:     key -> (value:str, timestamp:uint64, wall_ms:int)
        seen:       idempotency_key -> (key, value, timestamp)
        clock:      logical counter (monotonically increasing)

    `wall_ms` is the wall-clock time (ms) at which the value was written.
    It is used to enforce `max_staleness_ms` on FOLLOWER_STALE reads.
    """

    def __init__(self, replica_id="A", is_leaseholder=False):
        self.replica_id = replica_id
        self.is_leaseholder = is_leaseholder
        self.lock = threading.Lock()
        self.values = {}   # key -> (value, ts, wall_ms)
        self.seen = {}     # idempotency_key -> (key, value, ts)
        self.clock = 0

    def _log_event(self, event, detail):
        log.info(f"[replica-{self.replica_id}] {event} {detail}")

    # --- Put ---------------------------------------------------------
    def Put(self, request, context):
        key = request.key
        value = request.value
        ts = request.timestamp
        key_id = request.idempotency_key
        strategy = request.strategy
        wall_ms = int(time.time() * 1000)

        self._log_event(
            "PUT",
            f"key={key} value={value!r} ts={ts} "
            f"strategy={'QUORUM_CP' if strategy == kvstore_pb2.QUORUM_CP else 'ALWAYS_AP'} "
            f"idem={key_id[:8] if key_id else 'none'}",
        )

        with self.lock:
            if key_id and key_id in self.seen:
                k, v, t = self.seen[key_id]
                self._log_event("PUT-DUP", f"key={k} ts={t}")
                return kvstore_pb2.PutReply(
                    success=True, acks_received=1, error_message="",
                )

            existing = self.values.get(key)
            if existing is not None:
                _, stored_ts, _ = existing
                if ts < stored_ts:
                    self._log_event(
                        "PUT-REJECT",
                        f"key={key} incoming ts={ts} < stored ts={stored_ts}",
                    )
                    return kvstore_pb2.PutReply(
                        success=False, acks_received=0,
                        error_message="stale write (LWW rejected)",
                    )

            self.values[key] = (value, ts, wall_ms)
            if key_id:
                self.seen[key_id] = (key, value, ts)
            self.clock = max(self.clock, ts) + 1

            self._log_event("PUT-APPLY", f"key={key} -> {value!r} ts={ts}")
            return kvstore_pb2.PutReply(
                success=True, acks_received=1, error_message="",
            )

    # --- Get ---------------------------------------------------------
    def Get(self, request, context):
        key = request.key
        consistency = request.consistency

        mode = "LINEARIZABLE" if consistency == kvstore_pb2.LINEARIZABLE else "FOLLOWER_STALE"
        self._log_event("GET", f"key={key} mode={mode}")

        # Linearizable read: only leaseholder may serve it.
        if consistency == kvstore_pb2.LINEARIZABLE and not self.is_leaseholder:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"replica-{self.replica_id} is not leaseholder",
            )

        with self.lock:
            entry = self.values.get(key)
            if entry is None:
                return kvstore_pb2.GetReply(
                    value="", found=False, read_timestamp=0, is_stale=False,
                )

            value, ts, stored_wall_ms = entry

            # Bounded staleness: enforce max_staleness_ms for FOLLOWER_STALE.
            if consistency == kvstore_pb2.FOLLOWER_STALE and request.max_staleness_ms > 0:
                age_ms = int(time.time() * 1000) - stored_wall_ms
                if age_ms > request.max_staleness_ms:
                    self._log_event(
                        "GET-STALE-ERROR",
                        f"key={key} age={age_ms}ms > max={request.max_staleness_ms}ms",
                    )
                    # Per lab spec: STALE_READ_EXCEEDED is a gRPC error.
                    context.abort(
                        grpc.StatusCode.FAILED_PRECONDITION,
                        f"STALE_READ_EXCEEDED: age={age_ms}ms exceeds "
                        f"max_staleness_ms={request.max_staleness_ms}",
                    )

            self._log_event(
                "GET-OK", f"key={key} value={value!r} ts={ts}",
            )
            return kvstore_pb2.GetReply(
                value=value, found=True, read_timestamp=ts, is_stale=False,
            )


def serve(port, replica_id, is_leaseholder):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(
        KVServer(replica_id=replica_id, is_leaseholder=is_leaseholder),
        server,
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    role = "LEASEHOLDER" if is_leaseholder else "follower"
    log.info(f"replica-{replica_id} ({role}) listening on port {port}")
    return server


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--replica-id", default="A")
    parser.add_argument("--is-leaseholder", action="store_true")
    args = parser.parse_args()

    server = serve(args.port, args.replica_id, args.is_leaseholder)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        log.info("shutting down")
        server.stop(0)
        sys.exit(0)


if __name__ == "__main__":
    main()

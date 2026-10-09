"""
Part B2 scenario:

    client-1: 2 x Increment(x, +1)   (concurrently)
    client-2: 2 x Increment(y, +1)   (concurrently)
    client-3: 2 x Get(x)             (concurrently)

Each client writes its OWN event log to logs/<client_id>.log in the
required format:

    [client-N] EVENT detail L=<value>

The server writes its own log to stdout during the scenario; the harness
captures it into logs/replica.log by piping through the server process
(spawned as subprocess).

After this script runs, use scripts/merge_logs.py to produce logs/merged.txt.
"""

import subprocess
import sys
import threading
import time
from pathlib import Path

# allow running from repo root
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock


LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
SERVER_PORT = 50251
SERVER_LOG = LOG_DIR / "replica.log"


class LoggingClient:
    def __init__(self, client_id):
        self.client_id = client_id
        self.clock = LamportClock(pid=client_id)
        self.lines = []
        ch = grpc.insecure_channel(f"127.0.0.1:{SERVER_PORT}")
        self.stub = counter_pb2_grpc.CounterStub(ch)

    def _log(self, event, detail, L):
        self.lines.append(f"[{self.client_id}] {event} {detail} L={L}")

    def incr(self, counter_id, delta):
        L = self.clock.send()
        self._log("SEND", f"Increment(counter={counter_id}, delta={delta})", L)
        req = counter_pb2.IncrementRequest(
            counter_id=counter_id, delta=delta,
            idempotency_key=f"{self.client_id}-{counter_id}-{L}",
            lamport_time=L,
        )
        reply = self.stub.Increment(req, timeout=5.0)
        self.clock.receive(reply.lamport_time)
        self._log(
            "RECV",
            f"IncrementReply(new_value={reply.new_value}) "
            f"(received L={reply.lamport_time})",
            self.clock.t,
        )

    def get(self, counter_id):
        L = self.clock.send()
        self._log("SEND", f"Get(counter={counter_id})", L)
        req = counter_pb2.GetRequest(counter_id=counter_id, lamport_time=L)
        reply = self.stub.Get(req, timeout=5.0)
        self.clock.receive(reply.lamport_time)
        self._log(
            "RECV",
            f"GetReply(value={reply.value}, found={reply.found}) "
            f"(received L={reply.lamport_time})",
            self.clock.t,
        )

    def dump(self):
        path = LOG_DIR / f"{self.client_id}.log"
        path.write_text("\n".join(self.lines) + "\n", encoding="utf-8")


def run_client1():
    c = LoggingClient("client-1")
    c.incr("x", 1)
    time.sleep(0.05)
    c.incr("x", 1)
    c.dump()


def run_client2():
    c = LoggingClient("client-2")
    time.sleep(0.02)  # slight offset so logs interleave
    c.incr("y", 1)
    time.sleep(0.05)
    c.incr("y", 1)
    c.dump()


def run_client3():
    c = LoggingClient("client-3")
    time.sleep(0.03)
    c.get("x")
    time.sleep(0.1)
    c.get("x")
    c.dump()


def start_server():
    """Start server.py as a subprocess; redirect stdout to logs/replica.log."""
    logf = open(SERVER_LOG, "w", encoding="utf-8")
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "server.py"),
         "--port", str(SERVER_PORT), "--replica-id", "A"],
        stdout=logf, stderr=subprocess.STDOUT,
        cwd=str(ROOT),
    )
    time.sleep(1.0)  # give it time to bind
    return proc, logf


def main():
    proc, logf = start_server()
    try:
        t1 = threading.Thread(target=run_client1)
        t2 = threading.Thread(target=run_client2)
        t3 = threading.Thread(target=run_client3)
        for t in (t1, t2, t3):
            t.start()
        for t in (t1, t2, t3):
            t.join()
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
        logf.close()

    print(f"wrote {LOG_DIR / 'client-1.log'}")
    print(f"wrote {LOG_DIR / 'client-2.log'}")
    print(f"wrote {LOG_DIR / 'client-3.log'}")
    print(f"wrote {SERVER_LOG}")
    print("run: python scripts/merge_logs.py")


if __name__ == "__main__":
    main()
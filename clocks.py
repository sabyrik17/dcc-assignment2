"""
Lamport logical clock.

Rules (from assignment):
  - tick():    increment before every local event (send, apply, etc.).
  - receive(): on receiving a message with clock value `recv`,
               set local = max(local, recv) + 1.
  - send():    alias for tick() before sending a message.

Log format required by assignment:
    [process] EVENT detail L=<value>
"""

import threading


class LamportClock:
    def __init__(self, pid="unknown"):
        self.pid = pid
        self.t = 0
        self._lock = threading.Lock()

    def tick(self):
        with self._lock:
            self.t += 1
            return self.t

    def send(self):
        """Call before sending a message; returns the clock value to attach."""
        return self.tick()

    def receive(self, recv_time):
        """Call on message arrival with the sender's clock value."""
        with self._lock:
            self.t = max(self.t, recv_time) + 1
            return self.t

    def value(self):
        with self._lock:
            return self.t
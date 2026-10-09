# dcc-assignment2 — Replicated Counter Service

A gRPC-based replicated counter service with idempotency-key deduplication,
Lamport logical clocks, and a three-replica quorum write path.

Built in three stages:
- **Part A** — single-node gRPC counter with retries and dedup store.
- **Part B** — Lamport logical-clock instrumentation.
- **Part C** — three replicas with majority-acknowledged writes,
  automated unit/integration/failure-injection tests, and a benchmark.

---

## Environment

- Python 3.9 or later (tested on 3.13)
- Windows / Linux / macOS
- No external services required

### Setup

```bash
python3 -m venv .venv
source .venv/bin/activate           # Windows: .venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install grpcio grpcio-tools pytest
```

### Regenerate Python stubs (optional — stubs are committed)

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto
```

---

## Start one replica

**Terminal 1:**

```bash
python server.py --port 50051 --replica-id A
```

Expected output:

```
HH:MM:SS INFO replica-A listening on port 50051
```

**Terminal 2** — send a request:

```bash
python client.py incr likes:post-42 --by 5
python client.py get likes:post-42
```

Expected:

```
OK committed value=5 (duplicate: no)
value=5
```

Reusing the same idempotency key must NOT apply the delta twice:

```bash
python client.py incr likes:post-42 --by 5 --key test-key-001
python client.py incr likes:post-42 --by 5 --key test-key-001
python client.py get likes:post-42
```

Expected:

```
OK committed value=5 (duplicate: no)
OK committed value=5 (duplicate: yes)
value=5
```

---

## Start three replicas (quorum)

**Terminal 1:** `python server.py --port 50051 --replica-id A`
**Terminal 2:** `python server.py --port 50052 --replica-id B`
**Terminal 3:** `python server.py --port 50053 --replica-id C`

**Terminal 4** — write via the quorum client (majority = 2 of 3):

```bash
python client.py incr likes:post-99 --by 7 --replicas 50051,50052,50053
```

Expected:

```
OK committed value=7 (replicas acked: 3/3, duplicate: no)
```

Read from each replica individually:

```bash
python client.py get likes:post-99 --port 50051
python client.py get likes:post-99 --port 50052
python client.py get likes:post-99 --port 50053
```

All three must print `value=7`.

Stop one replica (`Ctrl+C` in Terminal 2) — writes still commit (2/3).
Stop two replicas — writes fail (no majority).

---

## Run the test suite

```bash
pytest tests/ -v
```

Expected: **11 tests passed** — 5 for Part A, 3 for Part C quorum,
3 for failure injection.

```bash
pytest tests/ -v --tb=short
```

---

## Run the benchmark (Part C4)

```bash
python bench/benchmark.py
```

Sends 2000 requests per configuration for four configurations
(single replica / quorum, 1 / 16 concurrent clients) and prints:

| Configuration                    | Median (ms) | p95 (ms) | Requests |
|----------------------------------|-------------|----------|----------|
| Single replica, 1 client(s)      | ...         | ...      | 2000     |
| Single replica, 16 client(s)     | ...         | ...      | 2000     |
| Quorum (3 replicas), 1 client(s) | ...         | ...      | 2000     |
| Quorum (3 replicas), 16 client(s)| ...         | ...      | 2000     |

Save to file:

```bash
python bench/benchmark.py | tee bench/results.txt
```

---

## Part B2 — three-client ordering scenario

```bash
python scripts/scenario_b2.py      # writes logs/client-*.log and logs/replica.log
python scripts/merge_logs.py       # writes logs/merged.txt
```

The merged log is a Lamport-ordered timeline of ~30 events used in the
report's happened-before analysis.

---

## Repository layout

```
dcc-assignment2/
├── counter.proto                 # gRPC service definition
├── counter_pb2.py                # generated — do not edit
├── counter_pb2_grpc.py           # generated — do not edit
├── clocks.py                     # Lamport logical clock
├── server.py                     # replica; flags: --port, --replica-id, --fault, --delay-ms
├── client.py                     # incr / get; supports --replicas for quorum
├── tests/
│   ├── conftest.py               # fixtures: running_server, three_replicas
│   ├── test_part_a.py            # 5 tests
│   ├── test_part_c.py            # 3 quorum tests
│   └── test_failures.py          # 3 failure-injection tests
├── bench/
│   └── benchmark.py              # median/p95 for four configurations
├── scripts/
│   ├── scenario_b2.py            # three concurrent clients + Lamport logs
│   └── merge_logs.py             # merge into logs/merged.txt
├── logs/                         # captured event logs used in the report
├── requirements.txt
└── README.md
```

---

## Fault injection flags (used by tests)

```bash
python server.py --port 50051 --fault drop-after-recv    # reply is dropped
python server.py --port 50051 --delay-ms 3000            # every request slow
python server.py --port 50051 --delay-ms 800 --fault delay-once  # first request slow
```

---

## Troubleshooting

- **`ModuleNotFoundError: counter_pb2`** — run commands from the repo root;
  the stub files must sit next to `server.py`.
- **`Failed to bind to address [::]:50051`** — port already in use.
  Find the process: `netstat -ano | findstr :50051`, kill it:
  `taskkill /PID <pid> /F`.
- **`I/O operation on closed file`** during pytest teardown is swallowed by
  `_SafeStreamHandler` in `server.py`. If it reappears, ensure the handler
  is registered before any `log.info()` call.
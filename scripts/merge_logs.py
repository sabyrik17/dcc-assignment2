"""
Merge the B2 logs into one timeline.

Reads logs/client-1.log, logs/client-2.log, logs/client-3.log,
logs/replica.log and writes logs/merged.txt sorted by Lamport value
(stable within equal L, preserving file order as tie-breaker).

The merged log is the artefact for the report's ordering analysis.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / "logs"

SOURCES = [
    LOG_DIR / "replica.log",
    LOG_DIR / "client-1.log",
    LOG_DIR / "client-2.log",
    LOG_DIR / "client-3.log",
]

L_RE = re.compile(r"L=(\d+)")


def parse_line(line):
    """Return (L, source, line) or None if the line has no L= value."""
    m = L_RE.search(line)
    if not m:
        return None
    return int(m.group(1)), line


def main():
    events = []
    for src in SOURCES:
        if not src.exists():
            print(f"WARN: {src} missing, skipping")
            continue
        for raw in src.read_text(encoding="utf-8").splitlines():
            parsed = parse_line(raw)
            if parsed is None:
                # server "listening on port ..." lines etc.
                continue
            L, line = parsed
            events.append((L, line))

    # stable sort by Lamport value (Python's sort is stable).
    events.sort(key=lambda t: t[0])

    out = LOG_DIR / "merged.txt"
    with out.open("w", encoding="utf-8") as f:
        for L, line in events:
            f.write(line + "\n")

    print(f"wrote {out} ({len(events)} events)")


if __name__ == "__main__":
    main()
"""Subprocess driver for crash-injection tests of consumer-group persistence.

Usage:
  python3 tests/groups_crash_driver.py <data_dir> <crash_phase> <ready_file>

Prepares 12 events (segments [0..4],[5..9], open [10,11]) and registers
group "g1" at start=0, then:

  batch_persisted       claims a batch and crashes right after the batch
                        is journaled (before the response could be used)
  checkpoint_persisted  settles the batch and crashes after the checkpoint
                        commit but before the gate ledger moves
  gate_moved            settles and crashes right after the gate ledger
                        is committed

The phase hook writes <ready_file> immediately before os._exit(17) so the
parent test can tell the crash point was genuinely reached.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from eventarch.config import Config
from eventarch.models import fmt_ts, utcnow
from eventarch.store import ArchiveStore

PHASES = {"batch_persisted", "checkpoint_persisted", "gate_moved"}


def ev(i):
    return {"device_id": "d1", "event_id": f"e{i}", "seq": i,
            "device_ts": fmt_ts(utcnow()), "payload": {"i": i}}


def main():
    data_dir, phase, ready_file = sys.argv[1], sys.argv[2], sys.argv[3]
    assert phase in PHASES
    cfg = Config(data_dir=data_dir, segment_max_records=5,
                 segment_max_age_sec=3600, janitor_interval_sec=3600)
    s = ArchiveStore(cfg)
    s.open()
    s.ingest([ev(i) for i in range(12)])
    s.groups.register("g1", 0, None, 60.0)

    crashed = {"done": False}

    def hook(name, ph):
        if ph == phase and not crashed["done"]:
            crashed["done"] = True
            with open(ready_file, "w") as fh:
                fh.write(ph)
                fh.flush()
                os.fsync(fh.fileno())
            os._exit(17)  # hard crash: no close(), no atexit

    s.groups._phase_hook = hook
    if phase == "batch_persisted":
        s.groups.claim("g1", "w1", limit=5)
    else:
        b = s.groups.claim("g1", "w1", limit=5)
        s.groups.settle("g1", "w1", b["lease_key"], b["batch_key"],
                        b["next_at"])
    s.close()


if __name__ == "__main__":
    main()

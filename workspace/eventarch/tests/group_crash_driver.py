"""Subprocess driver for crash-injection tests of group settle seams.

Usage:
  python3 tests/group_crash_driver.py <data_dir> <crash_phase> <ready_file>

Prepares 12 events (segments [0..4],[5..9], open [10,11]), registers a
dynamic consumer group "crash-g" (start=0), and claims its first batch
(limit=5 -> offsets 0..4, next_at=5).  It then settles the batch and exits
hard (os._exit) at the requested seam:

  batch_durable       crash right after the batch journal was fsynced at
                      claim time (before settle)
  checkpoint_durable  after the checkpoint write of settle, before the
                      batch journal trim / watergate migration
  gate_migrated       after the gate migration write (immediately before
                      settle would return)

The phase hook writes <ready_file> immediately before exiting so the parent
test knows the crash point was genuinely reached.  Batch credentials are
written next to the ready file as "<ready_file>.creds" JSON so the parent
can issue (or reject) operations after restart.
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from eventarch.config import Config
from eventarch.store import ArchiveStore

PHASES = {"batch_durable", "checkpoint_durable", "gate_migrated"}
GROUP = "crash-g"


def ev(i):
    return {"device_id": "d1", "event_id": f"e{i}", "seq": i,
            "device_ts": "2026-09-17T08:00:00Z", "payload": {"i": i}}


def main():
    data_dir, phase, ready_file = sys.argv[1], sys.argv[2], sys.argv[3]
    assert phase in PHASES

    cfg = Config(data_dir=data_dir, segment_max_records=5,
                 segment_max_age_sec=3600, janitor_interval_sec=3600,
                 group_max_batch=5)
    s = ArchiveStore(cfg)
    s.open()
    if not os.path.exists(os.path.join(data_dir, ".groups_prepared")):
        s.ingest([ev(i) for i in range(12)])
        with open(os.path.join(data_dir, ".groups_prepared"), "w") as fh:
            fh.write("1")

    crashed = {"done": False}

    def write_creds(batch):
        creds = {"lease_key": batch.get("lease_key"),
                 "batch_key": batch.get("batch_key"),
                 "next_at": batch.get("next_at"),
                 "epoch": batch.get("epoch")}
        with open(ready_file + ".creds", "w") as fh:
            json.dump(creds, fh)
            fh.flush()
            os.fsync(fh.fileno())

    def hook(group, batch, ph):
        if ph == phase and not crashed["done"]:
            crashed["done"] = True
            write_creds(batch)
            with open(ready_file, "w") as fh:
                fh.write(ph)
                fh.flush()
                os.fsync(fh.fileno())
            os._exit(17)  # hard crash: no close(), no atexit

    s.groups._phase_hook = hook

    s.groups.register(GROUP, 0, None, 300)
    b = s.groups.claim(GROUP)
    assert len(b["messages"]) == 5, len(b["messages"])
    assert b["next_at"] == 5, b["next_at"]

    if phase == "batch_durable":
        # The hook already fired during claim; park here (it must not return).
        import time
        while True:
            time.sleep(1)

    s.groups.settle(GROUP, b["lease_key"], b["batch_key"], b["next_at"])
    s.close()


if __name__ == "__main__":
    main()

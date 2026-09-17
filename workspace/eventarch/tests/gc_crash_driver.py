"""Subprocess driver for crash-injection tests of GC publish phases.

Usage:
  python3 tests/gc_crash_driver.py <data_dir> <crash_phase> <ready_file> [target_seg]

Prepares 12 events (segments [0..4],[5..9], open [10,11]), creates a GC
plan at cut=10, applies it, and exits hard (os._exit) the first time the
eviction worker reaches crash_phase (swapped|published|audited) for
target_seg (default: the first candidate).  The phase hook writes
<ready_file> immediately before exiting so the parent test can tell the
crash point was genuinely reached.
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from eventarch.config import Config
from eventarch.models import fmt_ts, utcnow
from eventarch.store import ArchiveStore

CRASH_PHASES = {"swapped", "published", "audited"}
FIRST_SEG = "seg-00000000000000000000"
SECOND_SEG = "seg-00000000000000000005"


def ev(i):
    return {"device_id": "d1", "event_id": f"e{i}", "seq": i,
            "device_ts": fmt_ts(utcnow()), "payload": {"i": i}}


def main():
    data_dir, phase, ready_file = sys.argv[1], sys.argv[2], sys.argv[3]
    target = sys.argv[4] if len(sys.argv) > 4 else FIRST_SEG
    assert phase in CRASH_PHASES
    cfg = Config(data_dir=data_dir, segment_max_records=5,
                 segment_max_age_sec=3600, janitor_interval_sec=3600)
    s = ArchiveStore(cfg)
    s.open()
    if not os.path.exists(os.path.join(data_dir, ".gc_prepared")):
        s.ingest([ev(i) for i in range(12)])
        with open(os.path.join(data_dir, ".gc_prepared"), "w") as fh:
            fh.write("1")

    crashed = {"done": False}

    def hook(job, seg_id, ph):
        if ph == phase and seg_id == target and not crashed["done"]:
            crashed["done"] = True
            with open(ready_file, "w") as fh:
                fh.write(ph)
                fh.flush()
                os.fsync(fh.fileno())
            os._exit(17)  # hard crash: no close(), no atexit

    s.gc._phase_hook = hook
    plan = s.gc.create_plan(10)
    with open(os.path.join(data_dir, ".gc_plan"), "w") as fh:
        fh.write(plan["plan_id"])
    s.gc.apply_plan(plan["plan_id"])
    deadline = time.time() + 10
    while time.time() < deadline:
        jobs = s.gc.list_jobs(1000)
        if jobs and jobs[0]["status"] in ("succeeded", "failed"):
            break
        time.sleep(0.02)
    s.close()


if __name__ == "__main__":
    main()

"""Acceptance tests for durable downstream consumer groups (API v2).

Covers the nine assessed scenarios:
  甲 repeated claims before settle return the SAME batch; the next batch
    appears only after settle;
  乙 a static group ends at its declared end and never sees later messages;
  丙 holder contention, renewal, expiry takeover, stale-credential blocking;
  丁 repeat settle is a no-op; regressive / cross / forged settles -> 409
    with the checkpoint untouched;
  戊 outstanding messages block capacity reclamation; after settle the old
    region is removable while the unread suffix stays fenced by the gate;
  己 starting inside an evicted region -> 410 with the exact usable start;
  庚 hard crashes at each of the three durability seams reconcile:
    batch_key / checkpoint / epoch / watergate all agree after restart;
  辛 pause and deregistration withdraw the gate; resume after restart
    continues from the original checkpoint;
  壬 subscription work runs concurrently with ingestion, lookups, static
    end production, repair and GC: every call answers promptly and existing
    cursor semantics / static ends are unchanged.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from eventarch import gc as gcmod
from eventarch.api import Handler
from eventarch.config import Config
from eventarch.groups import Conflict
from eventarch.models import fmt_ts, utcnow
from eventarch.store import ArchiveStore

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DRIVER = os.path.join(os.path.dirname(__file__), "group_crash_driver.py")


def make_cfg(tmp, **kw):
    base = dict(
        data_dir=tmp, segment_max_records=5, segment_max_age_sec=3600,
        late_threshold_sec=900, wal_retain_segments=8,
        janitor_interval_sec=0.05,
        repair_workers=2, repair_retry_backoff_sec=0.01,
        gc_workers=1, group_max_batch=5,
    )
    base.update(kw)
    return Config(**base)


def ev(i, device="d1", ts=None):
    return {"device_id": device, "event_id": f"{device}-{i}", "seq": i,
            "device_ts": fmt_ts(ts or utcnow()),
            "payload": {"i": i}}


def open_store(tmp, **kw):
    s = ArchiveStore(make_cfg(tmp, **kw))
    s.open()
    return s


def wait_gc(s, job_id, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        j = s.gc.get_job(job_id)
        if j["status"] in ("succeeded", "failed"):
            return j
        time.sleep(0.01)
    raise AssertionError(f"gc job {job_id} stuck")


def offsets(batch):
    return [m["offset"] for m in batch["messages"]]


# ---------------------------------------------------------------------- #
# 甲: same outstanding batch, next batch only after settle                #
# ---------------------------------------------------------------------- #

class TestJiaClaimSettle(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.s = open_store(self.tmp)
        self.s.ingest([ev(i) for i in range(12)])

    def tearDown(self):
        self.s.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_repeated_claim_returns_same_batch(self):
        self.s.groups.register("g", 0, None, 30)
        b1 = self.s.groups.claim("g")
        self.assertEqual(offsets(b1), [0, 1, 2, 3, 4])
        self.assertEqual(b1["next_at"], 5)
        for _ in range(3):
            b = self.s.groups.claim("g", lease_key=b1["lease_key"])
            self.assertEqual(b["batch_key"], b1["batch_key"])
            self.assertEqual(b["lease_key"], b1["lease_key"])
            self.assertEqual(b["epoch"], b1["epoch"])
            self.assertEqual(offsets(b), [0, 1, 2, 3, 4])
        # claim itself never moves the checkpoint
        self.assertEqual(self.s.groups.get_group("g")["checkpoint"], 0)
        with open(os.path.join(self.tmp, "state", "v2_groups.json")) as fh:
            cp_on_disk = json.load(fh)
        self.assertEqual(cp_on_disk["groups"][0]["checkpoint"], 0)
    def test_next_batch_only_after_settle(self):
        self.s.groups.register("g", 0, None, 30)
        b1 = self.s.groups.claim("g")
        r = self.s.groups.settle("g", b1["lease_key"], b1["batch_key"],
                                 b1["next_at"])
        self.assertEqual(r["checkpoint"], 5)
        b2 = self.s.groups.claim("g", lease_key=b1["lease_key"])
        self.assertNotEqual(b2["batch_key"], b1["batch_key"])
        self.assertEqual(offsets(b2), [5, 6, 7, 8, 9])
        self.assertEqual(b2["next_at"], 10)
        self.s.groups.settle("g", b2["lease_key"], b2["batch_key"],
                             b2["next_at"])
        b3 = self.s.groups.claim("g", lease_key=b2["lease_key"])
        self.assertEqual(offsets(b3), [10, 11])
        self.assertEqual(b3["next_at"], 12)
        self.s.groups.settle("g", b3["lease_key"], b3["batch_key"],
                             b3["next_at"])
        # dynamic drain: no messages, not finished
        b4 = self.s.groups.claim("g", lease_key=b3["lease_key"])
        self.assertTrue(b4["empty"])
        self.assertFalse(b4["finished"])
        self.assertIsNone(b4["batch_key"])
        # new messages arrive -> the same holder follows them
        self.s.ingest([ev(12), ev(13)])
        b5 = self.s.groups.claim("g", lease_key=b3["lease_key"])
        self.assertEqual(offsets(b5), [12, 13])


# ---------------------------------------------------------------------- #
# 乙: static end reached, later messages invisible                        #
# ---------------------------------------------------------------------- #

class TestYiStaticEnd(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.s = open_store(self.tmp)
        self.s.ingest([ev(i) for i in range(12)])

    def tearDown(self):
        self.s.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_static_group_ends_and_is_sealed_off(self):
        self.s.groups.register("g", 0, 7, 30)  # exclusive end
        b1 = self.s.groups.claim("g")
        self.assertEqual(offsets(b1), [0, 1, 2, 3, 4])
        self.s.groups.settle("g", b1["lease_key"], b1["batch_key"],
                             b1["next_at"])
        b2 = self.s.groups.claim("g", lease_key=b1["lease_key"])
        self.assertEqual(offsets(b2), [5, 6])
        self.assertEqual(b2["next_at"], 7)
        self.s.groups.settle("g", b2["lease_key"], b2["batch_key"],
                             b2["next_at"])
        done = self.s.groups.claim("g", lease_key=b2["lease_key"])
        self.assertTrue(done["finished"])
        self.assertEqual(done["checkpoint"], 7)
        view = self.s.groups.get_group("g")
        self.assertEqual(view["status"], "finished")
        self.assertIsNone(view["gate"])  # gate withdrawn

        # more messages arrive afterwards
        self.s.ingest([ev(20), ev(21)])
        again = self.s.groups.claim("g")
        self.assertTrue(again["finished"])
        self.assertEqual(again["messages"], [])
        self.assertEqual(again["checkpoint"], 7)
        # settling anything now is rejected
        with self.assertRaises(Conflict):
            self.s.groups.settle("g", b2["lease_key"], b2["batch_key"], 8)
        with self.assertRaises(Conflict):
            self.s.groups.pause("g")

    def test_start_at_end_is_finished_immediately(self):
        g = self.s.groups.register("g", 12, 12, 30)
        self.assertEqual(g["status"], "finished")
        self.assertIsNone(g["gate"])
        self.assertTrue(self.s.groups.claim("g")["finished"])

    def test_end_before_start_rejected(self):
        with self.assertRaises(ValueError):
            self.s.groups.register("g", 9, 3, 30)


# ---------------------------------------------------------------------- #
# 丙: contention / renew / expiry takeover / stale credentials            #
# ---------------------------------------------------------------------- #

class TestBingTakeover(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.s = open_store(self.tmp)
        self.s.ingest([ev(i) for i in range(10)])

    def tearDown(self):
        self.s.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_contention_renew_expiry_takeover(self):
        self.s.groups.register("g", 0, None, 0.3)
        a = self.s.groups.claim("g")
        self.assertEqual(a["epoch"], 1)

        # another claimant while the lease is live is refused
        with self.assertRaises(Conflict) as cm:
            self.s.groups.claim("g")
        self.assertEqual(cm.exception.reason, "held")

        # renewal extends the lease without changing the epoch
        rn = self.s.groups.renew("g", a["lease_key"])
        self.assertEqual(rn["epoch"], 1)
        self.assertEqual(rn["lease_key"], a["lease_key"])
        # a foreign renewal is refused
        with self.assertRaises(Conflict):
            self.s.groups.renew("g", "ltk-forged")

        # let the lease lapse
        time.sleep(0.35)
        b = self.s.groups.claim("g")
        self.assertEqual(b["epoch"], 2)
        self.assertNotEqual(b["lease_key"], a["lease_key"])
        self.assertNotEqual(b["batch_key"], a["batch_key"])
        self.assertEqual(b["from_offset"], 0)  # checkpoint never moved

        # old holder is blocked on every mutating call
        with self.assertRaises(Conflict) as cm:
            self.s.groups.claim("g", lease_key=a["lease_key"])
        self.assertEqual(cm.exception.reason, "bad_credential")
        with self.assertRaises(Conflict):
            self.s.groups.renew("g", a["lease_key"])
        with self.assertRaises(Conflict):
            self.s.groups.settle("g", a["lease_key"], a["batch_key"], 5)
        # old credential + the NEW batch_key must still be rejected
        with self.assertRaises(Conflict):
            self.s.groups.settle("g", a["lease_key"], b["batch_key"],
                                 b["next_at"])
        cp = self.s.groups.get_group("g")["checkpoint"]
        self.assertEqual(cp, 0)

        # the new holder can settle; the old batch_key is now cross/forged
        self.s.groups.settle("g", b["lease_key"], b["batch_key"],
                             b["next_at"])
        with self.assertRaises(Conflict):
            self.s.groups.settle("g", b["lease_key"], a["batch_key"], 5)

    def test_one_effective_holder_after_restart(self):
        self.s.groups.register("g", 0, None, 300)
        a = self.s.groups.claim("g")
        self.s.close()
        s2 = open_store(self.tmp)
        try:
            # live lease survives restart; a stranger cannot steal it
            with self.assertRaises(Conflict):
                s2.groups.claim("g")
            same = s2.groups.claim("g", lease_key=a["lease_key"])
            self.assertEqual(same["batch_key"], a["batch_key"])
            self.assertEqual(same["epoch"], a["epoch"])
        finally:
            s2.close()


# ---------------------------------------------------------------------- #
# 丁: repeat settle no-op; regression / cross / forgery -> 409            #
# ---------------------------------------------------------------------- #

class TestDingSettleValidation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.s = open_store(self.tmp)
        self.s.ingest([ev(i) for i in range(20)])

    def tearDown(self):
        self.s.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_settle_validation(self):
        self.s.groups.register("g", 0, None, 30)
        b = self.s.groups.claim("g")
        cp0 = 0

        with self.assertRaises(Conflict) as cm:
            self.s.groups.settle("g", b["lease_key"], b["batch_key"], 1)
        self.assertEqual(cm.exception.reason, "next_at_mismatch")  # regressive
        with self.assertRaises(Conflict) as cm:
            self.s.groups.settle("g", b["lease_key"], b["batch_key"], 99)
        self.assertEqual(cm.exception.reason, "next_at_mismatch")  # forged
        with self.assertRaises(Conflict) as cm:
            self.s.groups.settle("g", b["lease_key"], "btk-madeup", 5)
        self.assertEqual(cm.exception.reason, "unknown_batch")
        with self.assertRaises(Conflict):
            self.s.groups.settle("g", "ltk-wrong", b["batch_key"], 5)
        # checkpoint untouched by every failed settle
        self.assertEqual(self.s.groups.get_group("g")["checkpoint"], cp0)

        ok = self.s.groups.settle("g", b["lease_key"], b["batch_key"],
                                  b["next_at"])
        self.assertFalse(ok["idempotent"])
        # identical resubmission: handled, no side effects
        again = self.s.groups.settle("g", b["lease_key"], b["batch_key"],
                                     b["next_at"])
        self.assertTrue(again["idempotent"])
        self.assertEqual(again["checkpoint"], 5)
        # same old batch_key but a different next_at: still rejected
        with self.assertRaises(Conflict):
            self.s.groups.settle("g", b["lease_key"], b["batch_key"], 6)

        # cross-batch: a later batch settles, the older key must not move cp
        b2 = self.s.groups.claim("g", lease_key=b["lease_key"])
        self.s.groups.settle("g", b2["lease_key"], b2["batch_key"],
                             b2["next_at"])
        with self.assertRaises(Conflict):
            self.s.groups.settle("g", b2["lease_key"], b["batch_key"], 50)
        self.assertEqual(self.s.groups.get_group("g")["checkpoint"], 10)


# ---------------------------------------------------------------------- #
# 戊: outstanding batch blocks GC; settle moves the fence                 #
# ---------------------------------------------------------------------- #

class TestWuWatergateGC(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.s = open_store(self.tmp)
        self.s.ingest([ev(i) for i in range(12)])  # segs [0..4],[5..9], open 10,11

    def tearDown(self):
        self.s.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_outstanding_fences_then_settle_releases(self):
        self.s.groups.register("g", 0, None, 300)
        b = self.s.groups.claim("g")  # outstanding over [0..4]
        self.assertEqual(offsets(b), [0, 1, 2, 3, 4])

        plan = self.s.gc.create_plan(1000)
        self.assertEqual(plan["items"], [])  # gate boundary 0 fences all
        job, accepted = self.s.gc.apply_plan(plan["plan_id"])
        j = wait_gc(self.s, job["id"])
        self.assertEqual(j["status"], "succeeded")
        self.assertEqual(j["total"], 0)
        self.assertEqual(len(self.s.gc.tombstones), 0)

        # settle moves the gate to the segment containing 5 -> boundary 5
        self.s.groups.settle("g", b["lease_key"], b["batch_key"],
                             b["next_at"])
        self.assertEqual(self.s.groups.get_group("g")["gate"], 5)
        plan2 = self.s.gc.create_plan(1000)
        ids = {it["id"] for it in plan2["items"]}
        self.assertEqual(ids, {"seg-00000000000000000000"})  # old region only
        job2, _ = self.s.gc.apply_plan(plan2["plan_id"])
        wait_gc(self.s, job2["id"])
        tombs = self.s.gc.tombstones
        self.assertEqual([(t["first_offset"], t["last_offset"]) for t in tombs],
                         [(0, 4)])
        # unread suffix [5..] is still present and fenced
        meta_ids = {m["id"] for m in self.s.manifest["segments"]}
        self.assertIn("seg-00000000000000000005", meta_ids)

    def test_stale_plan_conflicts_when_gate_moves(self):
        self.s.groups.register("g", 5, None, 300)  # only protects [5..]
        plan = self.s.gc.create_plan(1000)
        self.assertEqual({it["id"] for it in plan["items"]},
                         {"seg-00000000000000000000"})
        # move the group's gate backwards conceptually is impossible;
        # instead register a second gate at 0 -> plan must now conflict
        self.s.groups.register("g2", 0, None, 300)
        with self.assertRaises(gcmod.PlanConflict):
            self.s.gc.apply_plan(plan["plan_id"])
        self.assertEqual(len(self.s.gc.tombstones), 0)  # disk untouched


# ---------------------------------------------------------------------- #
# 己: 410 with exact usable start inside an evicted region                #
# ---------------------------------------------------------------------- #

class TestJiEvictedStart(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.s = open_store(self.tmp)
        self.s.ingest([ev(i) for i in range(12)])
        plan = self.s.gc.create_plan(1000)
        job, _ = self.s.gc.apply_plan(plan["plan_id"])
        wait_gc(self.s, job["id"])
        self.assertEqual(len(self.s.gc.tombstones), 2)

    def tearDown(self):
        self.s.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_register_inside_evicted_region_410(self):
        # both sealed segments were fully below the cut; offsets 0..9 are gone
        with self.assertRaises(gcmod.Gone) as cm:
            self.s.groups.register("late", 0, None, 30)
        self.assertEqual(cm.exception.first_offset, 0)
        # contiguous run [0..4],[5..9] -> exact usable start 10
        self.assertEqual(cm.exception.cursor, 10)

        # a start inside the second tombstoned run likewise gives 10
        with self.assertRaises(gcmod.Gone) as cm:
            self.s.groups.register("late2", 7, None, 30)
        self.assertEqual(cm.exception.cursor, 10)

        # registering at the exact usable cursor succeeds; no silent skip
        g = self.s.groups.register("ok", 10, None, 30)
        self.assertEqual(g["checkpoint"], 10)
        b = self.s.groups.claim("ok")
        self.assertEqual(offsets(b), [10, 11])

    def test_http_410_body_carries_usable_start(self):
        port, store, httpd, thread = _serve(self.s)
        try:
            status, body = _http_json(port, "POST", "/v2/groups",
                                     {"name": "late", "start": 2,
                                      "lease_seconds": 30})
            self.assertEqual(status, 410)
            self.assertEqual(body["cursor"], 10)
            self.assertEqual(body["usable_start"], 10)
        finally:
            _stop(httpd, thread)

    def test_resume_into_evicted_range_leaves_group_paused(self):
        s2 = open_store(tempfile.mkdtemp())
        try:
            s2.ingest([ev(i) for i in range(12)])
            s2.groups.register("g", 5, None, 300)
            s2.groups.claim("g")
            s2.groups.pause("g")
            plan = s2.gc.create_plan(1000)
            job, _ = s2.gc.apply_plan(plan["plan_id"])
            wait_gc(s2, job["id"])
            with self.assertRaises(gcmod.Gone) as cm:
                s2.groups.resume("g")
            self.assertEqual(cm.exception.cursor, 10)
            # failed resume must not activate the group or create a gate
            view = s2.groups.get_group("g")
            self.assertEqual(view["status"], "paused")
            self.assertEqual(view["checkpoint"], 5)
            self.assertIsNone(view["gate"])
            with self.assertRaises(Conflict):
                s2.groups.claim("g")
        finally:
            s2.close()
            shutil.rmtree(s2.data_dir, ignore_errors=True)


# ---------------------------------------------------------------------- #
# 庚: crashes at all three durability seams                               #
# ---------------------------------------------------------------------- #

class TestGengCrashSeams(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.ready = os.path.join(self.tmp, "ready")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _crash(self, phase):
        if os.path.exists(self.ready):
            os.remove(self.ready)
        proc = subprocess.run(
            [sys.executable, DRIVER, self.tmp, phase, self.ready],
            cwd=ROOT, timeout=30)
        self.assertEqual(proc.returncode, 17)
        self.assertTrue(os.path.exists(self.ready))
        with open(self.ready + ".creds") as fh:
            return json.load(fh)

    def test_batch_durable_then_kill(self):
        creds = self._crash("batch_durable")
        s = open_store(self.tmp)
        try:
            # outstanding batch survives with the SAME batch_key
            view = s.groups.get_group("crash-g")
            self.assertEqual(view["batch_key"], creds["batch_key"])
            self.assertEqual(view["checkpoint"], 0)
            self.assertEqual(view["gate"], 0)
            got = s.groups.claim("crash-g", lease_key=creds["lease_key"])
            self.assertEqual(got["batch_key"], creds["batch_key"])
            self.assertEqual(offsets(got), [0, 1, 2, 3, 4])
            s.groups.settle("crash-g", creds["lease_key"],
                            creds["batch_key"], creds["next_at"])
            self.assertEqual(s.groups.get_group("crash-g")["checkpoint"], 5)
        finally:
            s.close()

    def test_checkpoint_durable_then_kill(self):
        creds = self._crash("checkpoint_durable")
        s = open_store(self.tmp)
        try:
            view = s.groups.get_group("crash-g")
            self.assertEqual(view["checkpoint"], 5)
            self.assertIsNone(view["batch_key"])
            self.assertEqual(view["gate"], 5)   # realigned to checkpoint
            # the settled batch journal entry was trimmed during recovery
            with open(os.path.join(self.tmp, "state", "v2_batches.json")) as fh:
                self.assertEqual(json.load(fh)["batches"], [])
            # The committing lease was still live when the process died, so
            # an anonymous claimant cannot steal it...
            with self.assertRaises(Conflict):
                s.groups.claim("crash-g")
            # ...while the original credential receives the NEXT batch; the
            # settled batch never reappears and its key is not reused.
            got = s.groups.claim("crash-g", lease_key=creds["lease_key"])
            self.assertEqual(offsets(got), [5, 6, 7, 8, 9])
            self.assertNotEqual(got["batch_key"], creds["batch_key"])
            self.assertEqual(got["epoch"], creds["epoch"])
            # checkpoint is not advanced twice on another restart
            s.close()
            s2 = open_store(self.tmp)
            try:
                self.assertEqual(
                    s2.groups.get_group("crash-g")["checkpoint"], 5)
            finally:
                s2.close()
        finally:
            s.close()

    def test_gate_migrated_then_kill(self):
        creds = self._crash("gate_migrated")
        s = open_store(self.tmp)
        try:
            view = s.groups.get_group("crash-g")
            self.assertEqual(view["checkpoint"], 5)
            self.assertEqual(view["gate"], 5)
            # the committing holder lease survived; it gets the next batch
            got = s.groups.claim("crash-g", lease_key=creds["lease_key"])
            self.assertEqual(offsets(got), [5, 6, 7, 8, 9])
            gates = json.load(open(os.path.join(self.tmp, "state",
                                                "v2_gates.json")))
            self.assertEqual(gates["gates"],
                             [{"group": "crash-g", "boundary": 5}])
        finally:
            s.close()

    def test_no_orphan_gates_after_recovery(self):
        # synthesize a gate file for a group that does not exist
        creds = self._crash("batch_durable")
        from eventarch.util import atomic_write_json
        atomic_write_json(os.path.join(self.tmp, "state", "v2_gates.json"),
                          {"gates": [{"group": "crash-g", "boundary": 0},
                                     {"group": "ghost", "boundary": 3}]})
        s = open_store(self.tmp)
        try:
            self.assertNotIn("ghost", s.groups._gates)
            self.assertIn("crash-g", s.groups._gates)
        finally:
            s.close()

    def test_expired_lease_outstanding_batch_keeps_gate(self):
        # An outstanding batch whose lease lapses before the process dies
        # still fences retention; on restart a new claim takes over with an
        # incremented epoch and the old credentials are dead.
        tmp = tempfile.mkdtemp()
        cfg = make_cfg(tmp, segment_max_records=100, group_max_batch=500)
        s = ArchiveStore(cfg)
        s.open()
        s.ingest([ev(i) for i in range(10)])
        s.groups.register("g", 0, None, 0.2)
        b = s.groups.claim("g")
        time.sleep(0.25)
        s.close()

        s2 = open_store(tmp)
        try:
            view = s2.groups.get_group("g")
            self.assertTrue(view["outstanding"])
            self.assertEqual(view["gate"], 0)
            self.assertEqual(s2.gc.create_plan(1000)["items"], [])
            b2 = s2.groups.claim("g")
            self.assertEqual(b2["epoch"], b["epoch"] + 1)
            self.assertNotEqual(b2["batch_key"], b["batch_key"])
            with self.assertRaises(Conflict):
                s2.groups.settle("g", b["lease_key"], b["batch_key"],
                                 b["next_at"])
        finally:
            s2.close()
            shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------- #
# 辛: pause / deregister withdraw gate; resume from old checkpoint         #
# ---------------------------------------------------------------------- #

class TestXinPauseDelete(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.s = open_store(self.tmp)
        self.s.ingest([ev(i) for i in range(12)])

    def tearDown(self):
        self.s.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_pause_withdraws_gate_and_resume_continues(self):
        self.s.groups.register("g", 0, None, 300)
        b = self.s.groups.claim("g")
        self.s.groups.settle("g", b["lease_key"], b["batch_key"], 5)
        self.assertEqual(self.s.groups.get_group("g")["gate"], 5)

        # while active at cp 5, seg [0..4] is removable
        self.assertEqual({it["id"] for it in self.s.gc.create_plan(1000)["items"]},
                         {"seg-00000000000000000000"})
        self.s.groups.pause("g")
        self.assertIsNone(self.s.groups.get_group("g")["gate"])
        # after pause everything is reclaimable
        self.assertEqual(
            {it["id"] for it in self.s.gc.create_plan(1000)["items"]},
            {"seg-00000000000000000000", "seg-00000000000000000005"})

        # restart while paused: checkpoint retained, still no gate
        self.s.close()
        s2 = open_store(self.tmp)
        try:
            view = s2.groups.get_group("g")
            self.assertEqual(view["checkpoint"], 5)
            self.assertIsNone(view["gate"])
            self.assertEqual(view["status"], "paused")
            with self.assertRaises(Conflict):
                s2.groups.claim("g")
            r = s2.groups.resume("g")
            self.assertEqual(r["checkpoint"], 5)
            self.assertEqual(r["gate"], 5)
            nxt = s2.groups.claim("g")
            self.assertEqual(offsets(nxt), [5, 6, 7, 8, 9])
        finally:
            s2.close()

    def test_deregister_removes_group_and_gate(self):
        self.s.groups.register("g", 0, None, 300)
        self.s.groups.claim("g")
        self.assertEqual(self.s.gc.create_plan(1000)["items"], [])
        self.s.groups.delete_group("g")
        self.assertEqual(self.s.groups.list_groups(), [])
        self.assertEqual(
            {it["id"] for it in self.s.gc.create_plan(1000)["items"]},
            {"seg-00000000000000000000", "seg-00000000000000000005"})
        # durable deletion
        self.s.close()
        s2 = open_store(self.tmp)
        try:
            self.assertEqual(s2.groups.list_groups(), [])
            self.assertEqual(s2.groups._gates, {})
        finally:
            s2.close()


# ---------------------------------------------------------------------- #
# 壬: concurrency: subscriptions never block other work                   #
# ---------------------------------------------------------------------- #

class TestRenConcurrency(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.s = open_store(self.tmp)
        self.s.ingest([ev(i) for i in range(20)])

    def tearDown(self):
        self.s.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_parallel_work_stays_responsive(self):
        # Several groups with long leases constantly claiming/settling while
        # ingestion, lookups, static-end production, repair and GC run.
        stop = threading.Event()
        latencies = []

        def subscriber(name):
            self.s.groups.register(name, 0, None, 300)
            lease = None
            while not stop.is_set():
                t0 = time.monotonic()
                try:
                    b = self.s.groups.claim(name, lease_key=lease)
                    lease = b["lease_key"]
                    if not b["empty"] and not b["finished"]:
                        self.s.groups.settle(name, b["lease_key"],
                                             b["batch_key"], b["next_at"])
                    else:
                        time.sleep(0.002)
                except Conflict:
                    lease = None
                    time.sleep(0.002)
                latencies.append(time.monotonic() - t0)

        subs = [threading.Thread(target=subscriber, args=(f"sub-{i}",))
                for i in range(4)]
        for t in subs:
            t.start()

        deadline = time.monotonic() + 2.0
        ingested = 0
        # register a static group once (end=1): repeated identical
        # declarations must return the same immutable object with 200.
        stat = self.s.groups.register("stat", 0, 1, 300)
        worst = 0.0
        while time.monotonic() < deadline:
            for label, fn in (
                ("ingest", lambda: self.s.ingest([ev(1000 + ingested)])),
                ("lookup", lambda: self.s.device_events("d1", limit=5)),
                ("static", lambda: self.s.groups.register("stat", 0, 1, 300)),
                ("repair", lambda: self.s.start_repair(
                    "seg-00000000000000000000")),
                ("gc-plan", lambda: self.s.gc.create_plan(1000)),
            ):
                t0 = time.monotonic()
                fn()
                worst = max(worst, time.monotonic() - t0)
            ingested += 1
            time.sleep(0.002)

        stop.set()
        for t in subs:
            t.join(timeout=5)
        # every foreground call answered promptly -- subscriptions never block regular
        # ingestion, lookup, static-end production, repair or GC planning
        self.assertLess(worst, 1.0)
        self.assertGreater(ingested, 20)

        # existing cursor semantics still intact
        tail = self.s.list_segments()["next_offset"]
        self.assertGreater(tail, 20)
        page = self.s.device_events("d1", from_offset=0, limit=3)
        self.assertEqual([e["offset"] for e in page["events"]][:3],
                         [0, 1, 2])

        # static end stays immutable under repeated identical declarations
        view = self.s.groups.get_group("stat")
        self.assertEqual(view["kind"], "static")
        self.assertEqual((view["start"], view["end"], view["checkpoint"]),
                         (0, 1, 0))
        # a differing declaration of the same name is still 409
        with self.assertRaises(Conflict):
            self.s.groups.register("stat", 0, 2, 300)
        view2 = self.s.groups.register("stat", 0, 1, 300)
        self.assertTrue(view2["redeclared"])
        self.assertEqual(view2["end"], 1)

    def test_http_end_to_end(self):
        port, store, httpd, thread = _serve(self.s)
        try:
            st, body = _http_json(port, "POST", "/v2/groups",
                                  {"name": "web", "start": 0,
                                   "lease_seconds": 30})
            self.assertEqual(st, 201)
            st, b1 = _http_json(port, "POST", "/v2/groups/web/claim", {})
            self.assertEqual(st, 200)
            self.assertEqual(len(b1["messages"]), 5)
            st, _ = _http_json(port, "POST", "/v2/groups/web/settle",
                               {"lease_key": b1["lease_key"],
                                "batch_key": b1["batch_key"],
                                "next_at": b1["next_at"]})
            self.assertEqual(st, 200)
            st, groups = _http_json(port, "GET", "/v2/groups")
            self.assertEqual(st, 200)
            self.assertEqual(groups["groups"][0]["checkpoint"], 5)
            # holder lease persists across settle: present it to claim again
            st, b2 = _http_json(port, "POST", "/v2/groups/web/claim",
                                {"lease_key": b1["lease_key"]})
            self.assertEqual(st, 200)
            self.assertEqual([m["offset"] for m in b2["messages"]],
                             [5, 6, 7, 8, 9])
            # bad next_at -> 409 over HTTP
            st, body = _http_json(port, "POST", "/v2/groups/web/settle",
                                  {"lease_key": b2["lease_key"],
                                   "batch_key": b2["batch_key"],
                                   "next_at": 999})
            self.assertEqual(st, 409)
            # name conflict -> 409; identical redeclaration -> 200
            st, _ = _http_json(port, "POST", "/v2/groups",
                               {"name": "web", "start": 1,
                                "lease_seconds": 30})
            self.assertEqual(st, 409)
            st, _ = _http_json(port, "POST", "/v2/groups",
                               {"name": "web", "start": 0,
                                "lease_seconds": 30})
            self.assertEqual(st, 200)
        finally:
            _stop(httpd, thread)


# ---------------------------------------------------------------------- #
# HTTP test harness                                                      #
# ---------------------------------------------------------------------- #

def _serve(store):
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    httpd.store = store
    httpd.daemon_threads = True
    port = httpd.server_address[1]
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return port, store, httpd, t


def _stop(httpd, thread):
    httpd.shutdown()
    httpd.server_close()
    thread.join(timeout=5)


def _http_json(port, method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


if __name__ == "__main__":
    unittest.main(verbosity=2)

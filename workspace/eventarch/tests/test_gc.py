"""Acceptance tests for capacity reclamation rehearsal and reader protection.

Covers (>=8 acceptance scenarios):
  1. rehearsal is a pure read: per-file digests + manifest identical before
     and after POST /v1/gc/plans (plan state is not persisted at all);
  2. snapshot (freeze) references alone exclude a candidate;
  3. in-progress repair alone excludes a candidate;
  4. unexpired hold alone excludes a candidate; renewal/release affect only
     plans created afterwards (TTL expiry included);
  5. stamp/reference/repair/hold/cut changes between plan and apply -> the
     WHOLE order returns 409 and nothing on disk changes;
  6. two applies of the same plan produce exactly one gc_job (202 then 200);
  7. process crashes injected at each of the three publish phases (directory
     swap / manifest publish / audit append) reconcile on restart: no orphan
     directories, audit/job stay trackable, same gc_job;
  8. after eviction: new writes fine, reads of untouched ranges/order keys
     fine, snapshot/history reads fine, and 410 carries an accurate cursor;
  plus: idempotence across restart, large cleanup keeps foreground live.
"""

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from eventarch import gc as gcmod
from eventarch import segments as segmod
from eventarch.config import Config
from eventarch.models import fmt_ts, utcnow
from eventarch.store import ArchiveStore


def make_cfg(tmp, **kw):
    base = dict(
        data_dir=tmp, segment_max_records=5, segment_max_age_sec=3600,
        late_threshold_sec=900, wal_retain_segments=8,
        janitor_interval_sec=0.05,
        repair_workers=2, repair_retry_backoff_sec=0.01,
        gc_workers=1,
    )
    base.update(kw)
    return Config(**base)


def ev(device, seq, event_id=None, ts=None):
    return {
        "device_id": device,
        "event_id": event_id or f"{device}-{seq}",
        "seq": seq,
        "device_ts": fmt_ts(ts or utcnow()),
        "payload": {"seq": seq},
    }


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
    raise AssertionError(f"gc job {job_id} stuck: {s.gc.get_job(job_id)}")


def file_digest(root):
    """Map of every file under root -> sha256 (rehearsal must be read-only)."""
    out = {}
    for base, _dirs, files in os.walk(root):
        for name in files:
            p = os.path.join(base, name)
            rel = os.path.relpath(p, root)
            h = hashlib.sha256()
            with open(p, "rb") as fh:
                h.update(fh.read())
            out[rel] = (h.hexdigest(), os.path.getsize(p))
    return dict(sorted(out.items()))


def seg_ids(s):
    return [m["id"] for m in s.list_segments()["segments"]]


class GCTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def prep(self, n=12, store=None):
        s = store or open_store(self.tmp)
        s.ingest([ev("d1", i) for i in range(n)])
        return s

    def evicted_ids(self, s):
        return {t["id"] for t in s.list_segments()["evicted"]}


class RehearsalReadonlyTest(GCTestBase):
    def test_01_plan_changes_no_bytes_on_disk(self):
        s = self.prep()
        before = file_digest(self.tmp)
        manifest_before = json.load(open(os.path.join(
            self.tmp, "state", "manifest.json")))
        listing_before = sorted(os.listdir(self.tmp))

        p1 = s.gc.create_plan(10)
        p2 = s.gc.create_plan(10)   # repeated rehearsals also read-only
        self.assertEqual(
            [i["stamp"] for i in p1["items"]],
            [i["stamp"] for i in p2["items"]])
        self.assertEqual(set(p1), {"plan_id", "stamp", "items", "size"})

        after = file_digest(self.tmp)
        self.assertEqual(before, after)
        manifest_after = json.load(open(os.path.join(
            self.tmp, "state", "manifest.json")))
        self.assertEqual(manifest_before, manifest_after)
        self.assertEqual(listing_before, sorted(os.listdir(self.tmp)))
        # no gc/holds state files were created just by rehearsing
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "state", "gc.json")))
        self.assertFalse(os.path.exists(
            os.path.join(self.tmp, "state", "holds.json")))
        self.assertFalse(os.path.exists(
            os.path.join(self.tmp, "state", "gc_audit.log")))
        s.close()


class ExclusionTest(GCTestBase):
    def _plan_ids(self, s, cut=10):
        return {i["id"] for i in s.gc.create_plan(cut)["items"]}

    def test_02_snapshot_reference_alone_excludes(self):
        s = self.prep()
        all_eligible = self._plan_ids(s)
        self.assertEqual(len(all_eligible), 2)
        frz = s.freeze()  # snapshots all sealed segments
        self.assertEqual(self._plan_ids(s), set())
        self.assertIn("seg-00000000000000000000", frz["segments"])
        s.close()

    def test_03_repair_in_progress_alone_excludes(self):
        s = self.prep()
        victim = "seg-00000000000000000005"
        entered = threading.Event()
        release = threading.Event()

        def hook(job, phase):
            if phase == "planned" and job["seg_id"] == victim:
                entered.set()
                release.wait(5)

        s.quarantine(victim, "test")
        s._repair_phase_hook = hook
        j, created = s.start_repair(victim)
        self.assertTrue(entered.wait(5))
        # quarantined segments aren't candidates anyway; the hold test below
        # covers "healthy" segments; here assert the repair set is visible
        # to rehearsal once the segment were healthy again -- use a healthy
        # segment pinned directly through _active_repairs to isolate the rule.
        healthy = "seg-00000000000000000000"
        s._active_repairs[healthy] = "job-synthetic"
        try:
            ids = self._plan_ids(s)
            self.assertNotIn(healthy, ids)
            self.assertEqual(
                {i["id"] for i in s.gc.create_plan(10)["items"]}, set())
        finally:
            s._active_repairs.pop(healthy, None)
            release.set()
        s.wait_repair(j["id"], timeout=10)
        s.close()

    def test_04_hold_alone_excludes_and_renewal_release_affect_new_plans(self):
        s = self.prep()
        # hold anchored at offset 6 -> boundary = first_offset of its
        # segment (5): protects seg-5 and everything at greater positions,
        # leaving seg-0 as the sole candidate.
        h = s.gc.create_hold("reader-a", pos=6, ttl_seconds=300)
        self.assertEqual(h["boundary"], 5)
        p1 = s.gc.create_plan(10)
        self.assertEqual({i["id"] for i in p1["items"]},
                         {"seg-00000000000000000000"})

        # idempotent renewal: same id, new ttl; only plans created AFTER it
        # must see the changed protection.
        h2 = s.gc.create_hold("reader-a", pos=0, ttl_seconds=300)
        self.assertTrue(h2["renewed"])
        self.assertEqual(h2["boundary"], 0)
        self.assertEqual({i["id"] for i in s.gc.create_plan(10)["items"]},
                         set())
        # a hold on the open tail (pos beyond sealed range) anchors at pos
        # itself: it protects nothing currently sealed but will pin any
        # future segment whose first_offset >= that boundary.
        tail = s.gc.create_hold("tail", pos=1000, ttl_seconds=300)
        self.assertEqual(tail["boundary"], 1000)
        # reader-a still protects all sealed segments; tail changes nothing
        self.assertEqual({i["id"] for i in s.gc.create_plan(10)["items"]},
                         set())
        s.gc.release_hold("reader-a")
        # only the tail hold remains: nothing currently sealed is at
        # first_offset >= 1000, so both candidates are eligible again.
        ids3 = {i["id"] for i in s.gc.create_plan(10)["items"]}
        self.assertEqual(ids3, {
            "seg-00000000000000000000", "seg-00000000000000000005"})

        # TTL expiry: expired hold is treated as absent.
        s.gc.create_hold("short", pos=0, ttl_seconds=0.05)
        time.sleep(0.15)
        p4 = s.gc.create_plan(10)
        self.assertEqual({i["id"] for i in p4["items"]}, ids3)
        with self.assertRaises(Exception):
            s.gc.release_hold("short")  # expired -> not found
        s.close()

    def test_04b_holds_survive_restart(self):
        s = self.prep()
        s.gc.create_hold("durable", pos=6, ttl_seconds=300)
        s.close()
        s2 = open_store(self.tmp)
        self.assertEqual({h["id"] for h in s2.gc.list_holds()}, {"durable"})
        self.assertEqual({i["id"] for i in s2.gc.create_plan(10)["items"]},
                         {"seg-00000000000000000000"})
        s2.close()


class ApplyValidationTest(GCTestBase):
    def _prep_plan(self, s, cut=10):
        plan = s.gc.create_plan(cut)
        self.assertEqual(len(plan["items"]), 2)
        return plan

    def test_05a_stamp_change_rejects_whole_order_zero_cleanup(self):
        s = self.prep()
        plan = self._prep_plan(s)
        # mutate the bytes of one candidate (append a byte -> sha/size stamp
        # changes) between rehearsal and apply.
        victim = plan["items"][0]["id"]
        path = segmod.events_path(s.seg_root, victim)
        before = open(path, "rb").read()
        with open(path, "r+b") as fh:
            fh.write(b"X")
        try:
            with self.assertRaises(gcmod.PlanConflict) as ctx:
                s.gc.apply_plan(plan["plan_id"])
            self.assertTrue(any("stamp changed" in r
                                for r in ctx.exception.reasons))
        finally:
            with open(path, "wb") as fh:
                fh.write(before)
        # repeat apply yields the SAME conclusion (sticky)
        with self.assertRaises(gcmod.PlanConflict):
            s.gc.apply_plan(plan["plan_id"])
        # nothing changed: all segments live, empty audit, no graves
        self.assertEqual(len(seg_ids(s)), 2)
        self.assertEqual(s.gc.list_audit()["audit"], [])
        graves = [n for n in os.listdir(s.seg_root)
                  if n.startswith(gcmod.GRAVE_PREFIX)]
        self.assertEqual(graves, [])
        s.close()

    def test_05b_reference_change_freezes_segment_after_rehearsal(self):
        s = self.prep()
        plan = self._prep_plan(s)
        s.freeze()  # snapshot references both candidates after rehearsal
        with self.assertRaises(gcmod.PlanConflict) as ctx:
            s.gc.apply_plan(plan["plan_id"])
        self.assertTrue(any("snapshot" in r for r in ctx.exception.reasons))
        self.assertEqual(s.gc.list_audit()["audit"], [])
        s.close()

    def test_05c_repair_state_change_rejects(self):
        s = self.prep()
        plan = self._prep_plan(s)
        victim = plan["items"][0]["id"]
        s.quarantine(victim, "post-rehearsal corruption")
        with self.assertRaises(gcmod.PlanConflict):
            s.gc.apply_plan(plan["plan_id"])
        self.assertEqual(
            [m for m in s.list_segments()["segments"]
             if m["id"] == victim][0]["status"], "quarantined")
        s.close()

    def test_05d_hold_added_after_rehearsal_rejects(self):
        s = self.prep()
        plan = self._prep_plan(s)
        s.gc.create_hold("late-reader", pos=0, ttl_seconds=300)
        with self.assertRaises(gcmod.PlanConflict) as ctx:
            s.gc.apply_plan(plan["plan_id"])
        self.assertTrue(any("hold" in r for r in ctx.exception.reasons))
        s.close()

    def test_05e_cut_rehearsed_lower_is_not_reevaluated(self):
        # An item newly below a *different* cut never appears in this order:
        # rehearsed item set is fixed; apply must not silently widen/narrow.
        s = self.prep()
        plan = s._plan_ids if False else s.gc.create_plan(5)
        self.assertEqual({i["id"] for i in plan["items"]},
                         {"seg-00000000000000000000"})
        job, accepted = s.gc.apply_plan(plan["plan_id"])
        self.assertTrue(accepted)
        wait_gc(s, job["id"])
        # seg-5 was NOT part of the order even though cut semantics of a
        # fresh plan at 10 would include it.
        self.assertEqual(self.evicted_ids(s),
                         {"seg-00000000000000000000"})
        self.assertIn("seg-00000000000000000005", seg_ids(s))
        s.close()

    def test_05f_apply_unknown_plan_404(self):
        s = self.prep()
        from eventarch.store import NotFound
        with self.assertRaises(NotFound):
            s.gc.apply_plan("gcp-doesnotexist")
        s.close()


class ApplyIdempotenceTest(GCTestBase):
    def test_06_two_applies_one_job_202_then_200_same_id(self):
        s = self.prep()
        plan = s.gc.create_plan(10)
        j1, a1 = s.gc.apply_plan(plan["plan_id"])
        self.assertTrue(a1)
        j2, a2 = s.gc.apply_plan(plan["plan_id"])
        self.assertFalse(a2)
        self.assertEqual(j1["id"], j2["id"])
        done = wait_gc(s, j1["id"])
        self.assertEqual(done["status"], "succeeded")
        # A third apply after completion is still the same id/conclusion.
        j3, a3 = s.gc.apply_plan(plan["plan_id"])
        self.assertFalse(a3)
        self.assertEqual(j3["id"], j1["id"])
        # exactly one job recorded
        jobs = s.gc.list_jobs(limit=100)
        self.assertEqual(len(jobs), 1)
        s.close()


class PostEvictionTest(GCTestBase):
    def _evict(self, s, cut=10):
        plan = s.gc.create_plan(cut)
        job, _ = s.gc.apply_plan(plan["plan_id"])
        return plan, wait_gc(s, job["id"])

    def test_08_post_eviction_reads_writes_and_410_cursor(self):
        s = self.prep(12)
        plan, done = self._evict(s, 10)
        self.assertEqual(done["completed"], 2)

        # new writes land beyond the evicted range, business keys intact
        r = s.ingest([ev("d1", i) for i in range(12, 15)])
        self.assertTrue(all(x["status"] == "stored" for x in r))

        # untouched reads: device events show an evicted gap w/ cursor 10,
        # then surviving offsets 10..14 in business order
        got = s.device_events("d1", limit=500)
        live_offsets = [e["offset"] for e in got["events"]]
        self.assertEqual(live_offsets, [10, 11, 12, 13, 14])
        self.assertEqual(len(got["gaps"]), 1)
        self.assertEqual(got["gaps"][0]["reason"], "evicted")
        self.assertEqual(got["gaps"][0]["resume_offset"], 10)

        # direct segment access -> Gone with cursor just past the run
        with self.assertRaises(gcmod.Gone) as ctx:
            s.segment_events("seg-00000000000000000000")
        self.assertEqual(ctx.exception.cursor, 10)
        self.assertEqual(ctx.exception.first_offset, 0)
        self.assertEqual(ctx.exception.last_offset, 4)

        # replay inside the hole -> 410 with accurate resume cursor
        with self.assertRaises(gcmod.Gone) as ctx:
            s.replay(from_offset=2, limit=100)
        self.assertEqual(ctx.exception.cursor, 10)

        # resume at the cursor: full tail, no error
        tail = s.replay(from_offset=10, limit=100)
        self.assertEqual([e["offset"] for e in tail["events"]],
                         [10, 11, 12, 13, 14])
        self.assertTrue(tail["complete"])

        # segment listing boundaries preserved; next_offset / ordering keys
        ls = s.list_segments()
        self.assertEqual(ls["next_offset"], 15)
        self.assertEqual(sorted(t["first_offset"] for t in ls["evicted"]),
                         [0, 5])
        # audit lists both items
        audit = s.gc.list_audit()["audit"]
        self.assertEqual({a["id"] for a in audit},
                         {i["id"] for i in plan["items"]})
        self.assertTrue(all(a["job_id"] == done["id"] for a in audit))

        # restart: tombstones, cursors and audit all survive
        s.close()
        s2 = open_store(self.tmp)
        with self.assertRaises(gcmod.Gone) as ctx:
            s2.replay(from_offset=0, limit=100)
        self.assertEqual(ctx.exception.cursor, 10)
        self.assertEqual(len(s2.gc.list_audit()["audit"]), 2)
        # repeat apply of the same plan after restart -> same job id (200)
        job2, accepted = s2.gc.apply_plan(plan["plan_id"])
        self.assertFalse(accepted)
        self.assertEqual(job2["id"], done["id"])
        s2.close()

    def test_08b_snapshot_history_reads_after_eviction(self):
        # Freeze while only seg-0 [0..4] exists: the snapshot pins it.
        s0 = open_store(self.tmp)
        s0.ingest([ev("d1", i) for i in range(5)])
        frz = s0.freeze()  # horizon=5, references seg-0 only
        s0.ingest([ev("d1", i) for i in range(5, 12)])
        plan = s0.gc.create_plan(10)
        # seg-0 is snapshot-protected; seg-5 is the sole candidate
        self.assertEqual({i["id"] for i in plan["items"]},
                         {"seg-00000000000000000005"})
        job, _ = s0.gc.apply_plan(plan["plan_id"])
        wait_gc(s0, job["id"])
        # history read through the pinned snapshot still serves seg-0
        hist = s0.replay(freeze_id=frz["id"], limit=100)
        self.assertEqual([e["offset"] for e in hist["events"]], list(range(5)))
        self.assertEqual(hist["end_offset"], 5)
        self.assertEqual(hist["gaps"], [])
        # the evicted sibling is reachable only via a 410 cursor on fresh
        # (non-snapshot) reads
        with self.assertRaises(gcmod.Gone) as ctx:
            s0.replay(from_offset=5, limit=10)
        self.assertEqual(ctx.exception.cursor, 10)
        s0.close()

    def test_08c_cursor_spans_contiguous_evicted_run(self):
        s = self.prep(12)
        # evict seg-0 only (cut=5)
        p1 = s.gc.create_plan(5)
        j1, _ = s.gc.apply_plan(p1["plan_id"])
        wait_gc(s, j1["id"])
        with self.assertRaises(gcmod.Gone) as ctx:
            s.replay(from_offset=0, limit=10)
        self.assertEqual(ctx.exception.cursor, 5)
        # then evict seg-5 (cut=10); the two ranges become a contiguous run
        p2 = s.gc.create_plan(10)
        j2, _ = s.gc.apply_plan(p2["plan_id"])
        wait_gc(s, j2["id"])
        with self.assertRaises(gcmod.Gone) as ctx:
            s.replay(from_offset=0, limit=10)
        self.assertEqual(ctx.exception.cursor, 10)
        s.close()

    def test_08d_large_cleanup_does_not_block_foreground(self):
        s = open_store(self.tmp)
        s.ingest([ev("d1", i) for i in range(60)])  # 12 sealed segments
        plan = s.gc.create_plan(60)
        self.assertEqual(len(plan["items"]), 12)
        gate = threading.Event()
        entered = threading.Event()

        def hook(job, seg_id, phase):
            if phase == "swapped":
                entered.set()
                gate.wait(5)

        s.gc._phase_hook = hook
        job, _ = s.gc.apply_plan(plan["plan_id"])
        self.assertTrue(entered.wait(5))
        # worker parked mid-eviction (hook holds the phase for seconds):
        # foreground calls must still return immediately.  A read that hits
        # the directory-swap window gets an instant retryable ReadRetry
        # (HTTP 503), never a stall.
        t0 = time.monotonic()
        s.ingest([ev("d1", 100)])
        got = s.device_events("d1", limit=10)
        self.assertTrue(got["events"] or got["gaps"])
        try:
            s.replay(from_offset=0, limit=1)
        except gcmod.ReadRetry:
            pass  # swap window: quick retryable answer, not a block
        s.ingest([ev("d1", 101)])
        # freeze while eviction pending must not include swapping segments
        frz = s.freeze()
        self.assertLess(time.monotonic() - t0, 2.0)
        gate.set()
        done = wait_gc(s, job["id"])
        self.assertEqual(done["status"], "succeeded")
        self.assertFalse(
            set(frz["segments"]) & self.evicted_ids(s))
        s.close()


class CrashRecoveryTest(unittest.TestCase):
    PHASES = ("swapped", "published", "audited")
    FIRST = "seg-00000000000000000000"
    SECOND = "seg-00000000000000000005"

    def setUp(self):
        self.driver = os.path.join(os.path.dirname(__file__), "gc_crash_driver.py")

    def test_07_crash_at_each_publish_phase_reconciles(self):
        for phase in self.PHASES:
            for target in (self.FIRST, self.SECOND):
                with self.subTest(phase=phase, target=target):
                    self._crash_case(phase, target)

    def _crash_case(self, phase, target):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        ready = os.path.join(tmp, ".ready")
        proc = subprocess.Popen(
            [sys.executable, self.driver, tmp, phase, ready, target],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if os.path.exists(ready):
                break
            if proc.poll() is not None:
                raise AssertionError(
                    "driver exited early:\n" + proc.stderr.read().decode()[-2000:])
            time.sleep(0.02)
        else:
            proc.kill()
            raise AssertionError("crash point never reached")
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and proc.poll() is None:
            time.sleep(0.02)
        rc = proc.poll()
        if rc is None:
            proc.kill()
            proc.wait()
            raise AssertionError("driver did not crash")
        self.assertEqual(rc, 17, proc.stderr.read().decode()[-1000:])

        seg_root = os.path.join(tmp, "segments")
        # A crash artifact consistent with the phase must exist immediately
        # after the crash (proves the hook fired at the intended point).
        graves = [n for n in os.listdir(seg_root)
                  if n.startswith(gcmod.GRAVE_PREFIX)]
        if target == self.FIRST and phase in ("swapped", "published"):
            self.assertTrue(graves, f"no grave after {phase} crash")

        # Restart: reconciliation resumes the SAME job or restores layout;
        # either way there must be no half-applied eviction.
        s = open_store(tmp)
        try:
            jobs = s.gc.list_jobs(limit=100)
            self.assertEqual(len(jobs), 1)
            job = jobs[0]
            done = wait_gc(s, job["id"])
            self.assertEqual(done["status"], "succeeded", done)
            self.assertEqual(done["completed"], 2)

            leftover = [n for n in os.listdir(seg_root)
                        if n.startswith((gcmod.GRAVE_PREFIX, "stage-", "bak-"))]
            self.assertEqual(leftover, [])
            live = [n for n in os.listdir(seg_root) if n.startswith("seg-")]
            self.assertEqual(live, [])  # both candidates evicted

            audit = s.gc.list_audit()["audit"]
            self.assertEqual({a["id"] for a in audit},
                             {self.FIRST, self.SECOND})
            self.assertTrue(all(a["job_id"] == job["id"] for a in audit))
            with self.assertRaises(gcmod.Gone) as ctx:
                s.replay(from_offset=0, limit=10)
            self.assertEqual(ctx.exception.cursor, 10)
            tail = s.replay(from_offset=10, limit=10)
            self.assertEqual([e["offset"] for e in tail["events"]], [10, 11])
            # foreground still accepts writes after reconciliation
            self.assertEqual(s.ingest([ev("d1", 20)])[0]["status"], "stored")

            # a further clean restart keeps the same job id/audit and stays idempotent
            s.close()
            s = open_store(tmp)
            j2 = s.gc.list_jobs(limit=100)
            self.assertEqual(len(j2), 1)
            self.assertEqual(j2[0]["id"], job["id"])
            self.assertEqual(len(s.gc.list_audit()["audit"]), 2)
        finally:
            s.close()


if __name__ == "__main__":
    unittest.main()

"""Acceptance tests for persistent consumer groups (/v2/groups).

Covers the nine acceptance groups:
  甲 claim redelivers the same batch until settled; the checkpoint only
     moves on settle (plus declaration idempotence / 409 on divergence);
  乙 static groups end permanently at the declared horizon and never see
     later arrivals, while dynamic groups follow new data;
  丙 lease contention: one active holder, renewals keep the epoch, expiry
     takeover increments it, old credentials are blocked everywhere;
  丁 settle guards: idempotent re-submit, regression / cross-batch /
     fabricated keys / stale credentials all rejected, checkpoint intact;
  戊 the reclamation gate blocks GC of unsettled/unread ranges; settling
     frees the consumed prefix while the unread suffix stays gated;
  己 registering with a start inside the evicted zone -> 410 with the
     exact available start;
  庚 crash injection at the three persistence seams (batch journaled /
     checkpoint committed / gate moved): batch_key, checkpoint, epoch and
     gate stay mutually consistent across restart;
  辛 pause/deregister drop the gate; after a restart a paused group
     resumes from its original checkpoint;
  壬 subscriptions running concurrently with ingest, queries, freeze,
     repairs, the janitor and GC never block those paths; cursor and
     static-horizon semantics are unchanged.
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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from eventarch import gc as gcmod
from eventarch import groups as groupsmod
from eventarch.config import Config
from eventarch.models import fmt_ts, utcnow
from eventarch.store import ArchiveStore, NotFound


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


class GroupTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def prep(self, n=12, **kw):
        s = open_store(self.tmp, **kw)
        s.ingest([ev("d1", i) for i in range(n)])
        return s


class RegistrationTest(GroupTestBase):
    def test_redeclaration_is_idempotent_divergence_conflicts(self):
        s = self.prep()
        v1, c1 = s.groups.register("g", 0, None, 30)
        self.assertTrue(c1)
        self.assertEqual(v1["checkpoint"], 0)
        self.assertEqual(v1["epoch"], 0)
        self.assertEqual(v1["status"], "active")
        # identical declaration -> the original object, nothing reset
        s.groups.claim("g", "w", limit=5)
        v2, c2 = s.groups.register("g", 0, None, 30)
        self.assertFalse(c2)
        self.assertEqual(v2["created_at"], v1["created_at"])
        self.assertEqual(v2["epoch"], 1)  # state kept, not re-initialized
        self.assertIsNotNone(v2["pending"])
        # diverging declarations on the same name -> 409
        with self.assertRaises(groupsmod.GroupConflict):
            s.groups.register("g", 1, None, 30)
        with self.assertRaises(groupsmod.GroupConflict):
            s.groups.register("g", 0, 100, 30)
        with self.assertRaises(groupsmod.GroupConflict):
            s.groups.register("g", 0, None, 60)
        # bad declarations -> 400-style validation errors
        with self.assertRaises(ValueError):
            s.groups.register("h", -1, None, 30)
        with self.assertRaises(ValueError):
            s.groups.register("h", 5, 5, 30)
        with self.assertRaises(ValueError):
            s.groups.register("h", 0, None, 0)
        s.close()

    def test_declaration_survives_restart(self):
        s = self.prep()
        s.groups.register("g", 0, 8, 30)
        s.close()
        s2 = open_store(self.tmp)
        v, created = s2.groups.register("g", 0, 8, 30)
        self.assertFalse(created)
        self.assertEqual(v["end"], 8)
        s2.close()


class ClaimSettleTest(GroupTestBase):
    def test_jia_redelivery_until_settle_then_next_batch(self):
        """甲: repeated claims return the original batch; the next batch
        appears only after settle; claiming never moves the checkpoint."""
        s = self.prep()
        s.groups.register("g", 0, None, 30)
        b1 = s.groups.claim("g", "w1", limit=5)
        self.assertEqual([m["offset"] for m in b1["messages"]], [0, 1, 2, 3, 4])
        self.assertEqual(b1["next_at"], 5)
        self.assertEqual(b1["epoch"], 1)
        self.assertFalse(b1["finished"])
        # the claim itself must not move the checkpoint
        self.assertEqual(s.groups.get_group("g")["checkpoint"], 0)
        for _ in range(3):
            b = s.groups.claim("g", "w1", limit=5)
            self.assertEqual(b["batch_key"], b1["batch_key"])
            self.assertEqual(b["lease_key"], b1["lease_key"])
            self.assertEqual(b["next_at"], 5)
            self.assertEqual([m["offset"] for m in b["messages"]],
                             [0, 1, 2, 3, 4])
        self.assertEqual(s.groups.get_group("g")["checkpoint"], 0)
        r = s.groups.settle("g", "w1", b1["lease_key"], b1["batch_key"], 5)
        self.assertFalse(r["idempotent"])
        self.assertEqual(s.groups.get_group("g")["checkpoint"], 5)
        b2 = s.groups.claim("g", "w1", limit=5)
        self.assertNotEqual(b2["batch_key"], b1["batch_key"])
        self.assertEqual([m["offset"] for m in b2["messages"]],
                         [5, 6, 7, 8, 9])
        self.assertEqual(b2["next_at"], 10)
        s.close()

    def test_ding_settle_guards(self):
        """丁: re-settle is a no-op; regression, cross-batch, fabricated
        keys and stale credentials are all rejected; checkpoint intact."""
        s = self.prep()
        s.groups.register("g", 0, None, 30)
        b1 = s.groups.claim("g", "w", limit=5)  # next_at = 5
        k1, lk = b1["batch_key"], b1["lease_key"]
        # regression / overshoot / fabricated / stale credentials -> 409
        with self.assertRaises(groupsmod.GroupConflict):
            s.groups.settle("g", "w", lk, k1, 3)          # 倒退
        with self.assertRaises(groupsmod.GroupConflict):
            s.groups.settle("g", "w", lk, k1, 7)          # 跨批(超前)
        with self.assertRaises(groupsmod.GroupConflict):
            s.groups.settle("g", "w", lk, "bt-forged", 5)  # 虚构批号
        with self.assertRaises(groupsmod.GroupConflict):
            s.groups.settle("g", "w", "lk-wrong", k1, 5)  # 失效凭证
        with self.assertRaises(groupsmod.GroupConflict):
            s.groups.settle("g", "intruder", lk, k1, 5)   # 非持有者
        self.assertEqual(s.groups.get_group("g")["checkpoint"], 0)

        r = s.groups.settle("g", "w", lk, k1, 5)
        self.assertEqual(r["checkpoint"], 5)
        # idempotent re-submit: done, no side effects
        r2 = s.groups.settle("g", "w", lk, k1, 5)
        self.assertTrue(r2["idempotent"])
        self.assertEqual(s.groups.get_group("g")["checkpoint"], 5)

        b2 = s.groups.claim("g", "w", limit=5)  # next_at = 10
        k2 = b2["batch_key"]
        # the old batch key is no longer the pending one
        with self.assertRaises(groupsmod.GroupConflict):
            s.groups.settle("g", "w", lk, k1, 10)         # 跨批(旧批)
        with self.assertRaises(groupsmod.GroupConflict):
            s.groups.settle("g", "w", lk, k2, 9)          # 倒退
        with self.assertRaises(groupsmod.GroupConflict):
            s.groups.settle("g", "w", lk, k2, 12)         # 超前
        self.assertEqual(s.groups.get_group("g")["checkpoint"], 5)
        r3 = s.groups.settle("g", "w", lk, k2, 10)
        self.assertEqual(r3["checkpoint"], 10)
        # an older-than-last-settled batch is rejected as cross-batch
        with self.assertRaises(groupsmod.GroupConflict):
            s.groups.settle("g", "w", lk, k1, 5)
        # but the most recent commit stays idempotent
        self.assertTrue(s.groups.settle("g", "w", lk, k2, 10)["idempotent"])
        self.assertEqual(s.groups.get_group("g")["checkpoint"], 10)
        s.close()


class StaticGroupTest(GroupTestBase):
    def test_yi_static_end_is_permanent_dynamic_follows(self):
        """乙: a static group finishes at its declared end and is isolated
        from later arrivals; a dynamic group keeps following new data."""
        s = self.prep(5)
        s.groups.register("gs", 0, 5, 30)
        b = s.groups.claim("gs", "w", limit=10)
        self.assertEqual([m["offset"] for m in b["messages"]],
                         [0, 1, 2, 3, 4])
        self.assertEqual(b["next_at"], 5)
        s.groups.settle("gs", "w", b["lease_key"], b["batch_key"], 5)
        g = s.groups.get_group("gs")
        self.assertEqual(g["status"], "finished")
        self.assertIsNone(g["gate"])  # finished groups hold no gate

        s.ingest([ev("d1", i) for i in range(5, 10)])  # later arrivals
        fin = s.groups.claim("gs", "w", limit=10)
        self.assertTrue(fin["finished"])
        self.assertEqual(fin["messages"], [])
        self.assertEqual(fin["next_at"], 5)
        self.assertEqual(s.groups.get_group("gs")["checkpoint"], 5)

        # a dynamic group over the same range follows the new data
        s.groups.register("gd", 0, None, 30)
        b2 = s.groups.claim("gd", "w", limit=100)
        self.assertEqual([m["offset"] for m in b2["messages"]],
                         list(range(10)))
        s.close()


class LeaseTest(GroupTestBase):
    def test_bing_contention_renew_takeover_blocks_old_holder(self):
        """丙: one active holder; renewal keeps the epoch; takeover after
        expiry increments it; the old holder is rejected everywhere."""
        s = self.prep()
        s.groups.register("g", 0, None, 0.4)
        b1 = s.groups.claim("g", "A", limit=5)
        self.assertEqual(b1["epoch"], 1)
        lk_a = b1["lease_key"]
        # renewal keeps the epoch
        r = s.groups.renew("g", "A", lk_a)
        self.assertEqual(r["epoch"], 1)
        # a second holder cannot claim while the lease is live
        with self.assertRaises(groupsmod.GroupConflict):
            s.groups.claim("g", "B", limit=5)
        time.sleep(0.5)  # let A's lease expire
        b2 = s.groups.claim("g", "B", limit=5)
        self.assertEqual(b2["epoch"], 2)            # incremented on takeover
        self.assertNotEqual(b2["lease_key"], lk_a)  # fresh credentials
        self.assertEqual(b2["batch_key"], b1["batch_key"])  # same batch
        self.assertEqual([m["offset"] for m in b2["messages"]],
                         [0, 1, 2, 3, 4])
        # the old holder is now blocked on every verb
        with self.assertRaises(groupsmod.GroupConflict):
            s.groups.claim("g", "A", limit=5)
        with self.assertRaises(groupsmod.GroupConflict):
            s.groups.renew("g", "A", lk_a)
        with self.assertRaises(groupsmod.GroupConflict):
            s.groups.settle("g", "A", lk_a, b1["batch_key"], 5)
        # the new holder renews (epoch unchanged) and settles
        self.assertEqual(s.groups.renew("g", "B", b2["lease_key"])["epoch"], 2)
        r = s.groups.settle("g", "B", b2["lease_key"], b2["batch_key"], 5)
        self.assertEqual(r["checkpoint"], 5)
        self.assertEqual(r["epoch"], 2)
        s.close()


class GateGCTest(GroupTestBase):
    def test_wu_gate_blocks_gc_until_settle_then_frees_prefix(self):
        """戊: pending/unread content blocks reclamation; after settle the
        consumed prefix can be evicted while the suffix stays gated."""
        s = self.prep()  # seg-0 [0..4], seg-5 [5..9], open [10,11]
        s.groups.register("g", 0, None, 30)
        # everything at/after the checkpoint is gated
        self.assertEqual(s.gc.create_plan(10)["items"], [])
        b1 = s.groups.claim("g", "w", limit=5)
        self.assertEqual(s.gc.create_plan(10)["items"], [])  # still gated
        s.groups.settle("g", "w", b1["lease_key"], b1["batch_key"], 5)

        # the consumed prefix is now reclaimable, the unread suffix is not
        plan = s.gc.create_plan(10)
        self.assertEqual([i["id"] for i in plan["items"]],
                         ["seg-00000000000000000000"])
        job, _ = s.gc.apply_plan(plan["plan_id"])
        done = wait_gc(s, job["id"])
        self.assertEqual(done["status"], "succeeded")
        self.assertEqual(s.gc.create_plan(10)["items"], [])

        # the group keeps reading the protected suffix undisturbed
        b2 = s.groups.claim("g", "w", limit=5)
        self.assertEqual([m["offset"] for m in b2["messages"]],
                         [5, 6, 7, 8, 9])
        s.groups.settle("g", "w", b2["lease_key"], b2["batch_key"], 10)
        plan2 = s.gc.create_plan(10)
        self.assertEqual([i["id"] for i in plan2["items"]],
                         ["seg-00000000000000000005"])
        s.close()

    def test_gate_moves_forward_as_checkpoint_advances(self):
        s = self.prep()
        s.groups.register("g", 0, None, 30)
        view = s.groups.get_group("g")
        self.assertEqual(view["gate"], {"checkpoint": 0, "boundary": 0})
        b = s.groups.claim("g", "w", limit=7)
        s.groups.settle("g", "w", b["lease_key"], b["batch_key"], 7)
        view = s.groups.get_group("g")
        # checkpoint 7 lives in seg-5 [5..9] -> segment-granular boundary 5
        self.assertEqual(view["gate"], {"checkpoint": 7, "boundary": 5})
        s.close()


class EvictedStartTest(GroupTestBase):
    def test_ji_register_inside_evicted_zone_is_410_with_exact_start(self):
        """己: a start inside the evicted zone gets 410 plus the exact
        first available offset; the precise start itself works."""
        s = self.prep()
        plan = s.gc.create_plan(10)
        job, _ = s.gc.apply_plan(plan["plan_id"])
        wait_gc(s, job["id"])  # [0..9] evicted as one contiguous run

        with self.assertRaises(groupsmod.GroupGone) as ctx:
            s.groups.register("g", 3, None, 30)
        self.assertEqual(ctx.exception.cursor, 10)
        self.assertEqual(ctx.exception.first_offset, 0)
        self.assertEqual(ctx.exception.last_offset, 9)
        # nothing was registered
        self.assertEqual(s.groups.list_groups(), [])

        v, created = s.groups.register("g", 10, None, 30)
        self.assertTrue(created)
        b = s.groups.claim("g", "w", limit=5)
        self.assertEqual([m["offset"] for m in b["messages"]], [10, 11])
        s.close()


class GroupCrashTest(unittest.TestCase):
    """庚: power loss at each persistence seam keeps batch_key, checkpoint,
    epoch and the gate mutually consistent after reboot."""
    PHASES = ("batch_persisted", "checkpoint_persisted", "gate_moved")

    def setUp(self):
        self.driver = os.path.join(
            os.path.dirname(__file__), "groups_crash_driver.py")

    def test_geng_crash_at_each_persistence_seam(self):
        for phase in self.PHASES:
            with self.subTest(phase=phase):
                self._crash_case(phase)

    def _crash_case(self, phase):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        ready = os.path.join(tmp, ".ready")
        proc = subprocess.Popen(
            [sys.executable, self.driver, tmp, phase, ready],
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
        proc.stdout.close()
        stderr_tail = proc.stderr.read().decode()[-1000:]
        proc.stderr.close()
        self.assertEqual(rc, 17, stderr_tail)

        s = open_store(tmp)
        try:
            g = s.groups.get_group("g1")
            if phase == "batch_persisted":
                # the pending batch survived with its original batch_key
                self.assertEqual(g["checkpoint"], 0)
                self.assertIsNotNone(g["pending"])
                k1 = g["pending"]["batch_key"]
                self.assertEqual(g["pending"]["next_at"], 5)
                b = s.groups.claim("g1", "w1", limit=5)
                self.assertEqual(b["batch_key"], k1)  # same batch
                self.assertEqual(b["epoch"], 1)       # lease survived
                self.assertEqual([m["offset"] for m in b["messages"]],
                                 [0, 1, 2, 3, 4])
                r = s.groups.settle("g1", "w1", b["lease_key"], k1, 5)
                self.assertEqual(r["checkpoint"], 5)
                b2 = s.groups.claim("g1", "w1", limit=5)
                self.assertNotEqual(b2["batch_key"], k1)
                self.assertEqual([m["offset"] for m in b2["messages"]],
                                 [5, 6, 7, 8, 9])
            else:
                # the settle committed the checkpoint before the crash
                self.assertEqual(g["checkpoint"], 5)
                self.assertIsNone(g["pending"])
                k1 = g["last_settled"]["batch_key"]
                # the gate ledger is aligned with the checkpoint after
                # recovery (it was stale/behind for checkpoint_persisted)
                with open(os.path.join(tmp, "state", "group_gates.json")) as fh:
                    ledger = json.load(fh)["gates"]
                self.assertEqual(ledger["g1"]["checkpoint"], 5)
                self.assertEqual(g["gate"],
                                 {"checkpoint": 5, "boundary": 5})
                # the settled batch never reappears
                b = s.groups.claim("g1", "w1", limit=5)
                self.assertEqual([m["offset"] for m in b["messages"]],
                                 [5, 6, 7, 8, 9])
                self.assertNotEqual(b["batch_key"], k1)
                self.assertEqual(b["epoch"], 1)  # epoch survived
                # re-settling the committed batch is a no-op: the
                # checkpoint must not be advanced twice
                r = s.groups.settle("g1", "w1", b["lease_key"], k1, 5)
                self.assertTrue(r["idempotent"])
                self.assertEqual(s.groups.get_group("g1")["checkpoint"], 5)
                # the gate protects only the unread suffix
                plan = s.gc.create_plan(10)
                self.assertEqual([i["id"] for i in plan["items"]],
                                 ["seg-00000000000000000000"])
        finally:
            s.close()

    def test_geng_orphan_gate_is_reconciled_away(self):
        """A gate ledger entry whose group is paused/gone must not survive
        recovery (no water-gate without an active holder)."""
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        s = open_store(tmp)
        s.ingest([ev("d1", i) for i in range(12)])
        s.groups.register("g1", 0, None, 30)
        s.groups.pause("g1")  # state journaled, gate removed
        # simulate a crash between the pause commit and the gate removal:
        # hand the ledger a stale entry for the paused group.
        gates_path = os.path.join(tmp, "state", "group_gates.json")
        with open(gates_path, "w") as fh:
            json.dump({"gates": {"g1": {"checkpoint": 0}}}, fh)
        s.close()
        s2 = open_store(tmp)
        try:
            g = s2.groups.get_group("g1")
            self.assertEqual(g["status"], "paused")
            self.assertIsNone(g["gate"])
            with open(gates_path) as fh:
                self.assertEqual(json.load(fh)["gates"], {})
            # and nothing is protected from reclamation
            self.assertEqual(len(s2.gc.create_plan(10)["items"]), 2)
        finally:
            s2.close()


class PauseResumeTest(GroupTestBase):
    def test_xin_pause_resume_restart_and_deregister(self):
        """辛: pause/deregister drop the gate; after a restart the paused
        group resumes subscribing from its original checkpoint."""
        s = self.prep()
        s.groups.register("g", 0, None, 30)
        b1 = s.groups.claim("g", "w", limit=5)
        s.groups.settle("g", "w", b1["lease_key"], b1["batch_key"], 5)
        # a pending batch also survives pause/resume with its batch_key
        b2 = s.groups.claim("g", "w", limit=5)
        k2 = b2["batch_key"]

        view = s.groups.pause("g")
        self.assertEqual(view["status"], "paused")
        self.assertIsNone(view["gate"])
        self.assertIsNone(view["holder"])  # lease released
        # the gate is gone: both sealed segments are reclaimable now
        self.assertEqual(
            {i["id"] for i in s.gc.create_plan(10)["items"]},
            {"seg-00000000000000000000", "seg-00000000000000000005"})
        with self.assertRaises(groupsmod.GroupConflict):
            s.groups.claim("g", "w", limit=5)
        s.close()

        # restart: the pause and the checkpoint are durable
        s2 = open_store(self.tmp)
        try:
            g = s2.groups.get_group("g")
            self.assertEqual(g["status"], "paused")
            self.assertEqual(g["checkpoint"], 5)
            self.assertIsNone(g["gate"])
            with self.assertRaises(groupsmod.GroupConflict):
                s2.groups.claim("g", "w", limit=5)
            s2.groups.resume("g")
            g = s2.groups.get_group("g")
            self.assertEqual(g["status"], "active")
            self.assertEqual(g["gate"], {"checkpoint": 5, "boundary": 5})
            # subscribing continues from the original checkpoint, and the
            # pending batch kept its batch_key across pause+restart
            b = s2.groups.claim("g", "w", limit=5)
            self.assertEqual(b["batch_key"], k2)
            self.assertEqual([m["offset"] for m in b["messages"]],
                             [5, 6, 7, 8, 9])
            s2.groups.settle("g", "w", b["lease_key"], b["batch_key"], 10)

            # deregister drops the gate and the declaration
            s2.groups.delete("g")
            self.assertEqual(s2.groups.list_groups(), [])
            with self.assertRaises(NotFound):
                s2.groups.get_group("g")
            self.assertEqual(
                {i["id"] for i in s2.gc.create_plan(10)["items"]},
                {"seg-00000000000000000000", "seg-00000000000000000005"})
        finally:
            s2.close()

        # the deregistration itself is durable
        s3 = open_store(self.tmp)
        try:
            self.assertEqual(s3.groups.list_groups(), [])
        finally:
            s3.close()


class ConcurrencyTest(GroupTestBase):
    def test_ren_subscription_never_blocks_foreground(self):
        """壬: claims/settles running concurrently with ingest, queries,
        freeze, repair, the janitor and GC leave every path responsive;
        cursor meaning and static horizons are unchanged."""
        s = open_store(self.tmp, segment_max_age_sec=0.3,
                       janitor_interval_sec=0.05)
        s.ingest([ev("d1", i) for i in range(25)])  # 5 sealed segments
        s.groups.register("g", 0, None, 60)

        stop = threading.Event()
        delivered = []
        errors = []

        def loop():
            while not stop.is_set():
                try:
                    b = s.groups.claim("g", "w", limit=4)
                    s.groups.settle("g", "w", b["lease_key"],
                                    b["batch_key"], b["next_at"])
                    delivered.extend(m["offset"] for m in b["messages"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)
                time.sleep(0.002)

        t = threading.Thread(target=loop, daemon=True)
        t.start()

        def timed(fn, budget=2.0):
            t0 = time.monotonic()
            out = fn()
            self.assertLess(time.monotonic() - t0, budget)
            return out

        try:
            timed(lambda: s.ingest([ev("d1", i) for i in range(25, 30)]))
            got = timed(lambda: s.device_events("d1", limit=500))
            self.assertTrue(got["events"])

            # let the subscription pass offset 10, then reclaim the prefix
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if s.groups.get_group("g")["checkpoint"] >= 10:
                    break
                time.sleep(0.01)
            self.assertGreaterEqual(
                s.groups.get_group("g")["checkpoint"], 10)
            plan = timed(lambda: s.gc.create_plan(10))
            self.assertEqual({i["id"] for i in plan["items"]},
                             {"seg-00000000000000000000",
                              "seg-00000000000000000005"})
            job, _ = timed(lambda: s.gc.apply_plan(plan["plan_id"]))
            timed(lambda: wait_gc(s, job["id"]), budget=10)

            # cursor semantics are unchanged: 410 with the exact cursor
            with self.assertRaises(gcmod.Gone) as ctx:
                s.replay(from_offset=0, limit=10)
            self.assertEqual(ctx.exception.cursor, 10)

            # static horizon: freeze pins the view; later data stays out
            frz = timed(lambda: s.freeze())
            end = frz["end_offset"]
            self.assertEqual(end, 30)
            timed(lambda: s.ingest([ev("d1", i) for i in range(30, 35)]))
            rep = timed(lambda: s.replay(freeze_id=frz["id"], limit=500))
            self.assertEqual(len(rep["events"]), end - 10)
            self.assertTrue(all(e["offset"] < end for e in rep["events"]))

            # maintenance: quarantine + background repair stay responsive
            victim = "seg-00000000000000000020"
            timed(lambda: s.quarantine(victim, "test"))
            rjob, _ = timed(lambda: s.start_repair(victim))
            done = timed(lambda: s.wait_repair(rjob["id"], timeout=10),
                         budget=15)
            self.assertEqual(done["status"], "succeeded")
            rep2 = timed(lambda: s.replay(freeze_id=frz["id"], limit=500))
            self.assertEqual(len(rep2["events"]), end - 10)  # horizon fixed

            # the janitor kept sealing by age throughout the run
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if s.list_segments()["open"]["count"] == 0:
                    break
                time.sleep(0.05)
            self.assertEqual(s.list_segments()["open"]["count"], 0)
        finally:
            stop.set()
            t.join(timeout=5)
            s.close()

        self.assertEqual(errors, [])
        # the delivered stream is monotonic and duplicate-free
        self.assertEqual(delivered, sorted(delivered))
        self.assertEqual(len(delivered), len(set(delivered)))
        self.assertIn(0, delivered)
        self.assertGreaterEqual(max(delivered), 29)


if __name__ == "__main__":
    unittest.main()

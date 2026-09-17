"""Capacity reclamation: GC plan rehearsal, reader holds, eviction jobs.

Design mirrors the background-repair machinery in store.py:

  plan   POST /v1/gc/plans is a *read-only rehearsal*.  It computes the
         sealed segments fully below ``cut`` that are not pinned by a
         snapshot (freeze), not under repair, and not covered by an
         unexpired reader hold.  Plans live in memory only, so rehearsal
         changes not a single byte on disk (not even state files).

  hold   POST /v1/holds {hold_id, pos, ttl_seconds} creates or renews a
         protection region anchored at the segment containing ``pos``:
         that segment and every segment at a greater position are
         protected.  DELETE releases it.  Holds are durable; expiry is
         wall-clock based.

  apply  POST /v1/gc/plans/{id}/apply revalidates the whole rehearsal.
         Any change to a candidate's stamp, snapshot references, repair
         state, the protection set, or the cut-derived selection rejects
         the *whole* order with 409 (sticky for that plan id) and touches
         nothing.  Acceptance enqueues one background gc_job (202 first,
         200 + same id forever after).

  evict  Every item is published in three crash-safe phases:
           1. directory swap  (live seg dir -> gcgrave-<job>/<seg>/)
           2. manifest commit (meta removed, evicted tombstone added)
           3. audit append    (one fsync'd JSONL line), then grave removal
         A crash in any window is reconciled at the next startup from the
         durable job journal: the interrupted item is either resumed to
         completion under the *same* gc_job or its old directory is
         restored -- a half-applied eviction is impossible.

Evicted offset ranges survive as tombstones in the manifest: reads that
reach them fail with HTTP 410 and an accurate resume ``cursor`` (the next
live offset after the contiguous evicted run), while every surviving
offset, business-order key and snapshot horizon keeps its original value.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import threading
import time
import uuid
from datetime import datetime, timezone
from queue import Queue
from typing import Dict, List, Optional, Tuple

from . import segments as segmod
from .models import fmt_ts, parse_ts, utcnow
from .util import atomic_write_json, fsync_dir, load_json

log = logging.getLogger("eventarch.gc")

GC_TERMINAL = ("succeeded", "failed")
GRAVE_PREFIX = "gcgrave-"


class Gone(Exception):
    """A read reached an evicted offset range."""

    def __init__(self, cursor: int, first_offset: int, last_offset: int,
                 seg_id: Optional[str] = None, reason: str = "evicted"):
        super().__init__(f"offsets [{first_offset}..{last_offset}] have been "
                         f"evicted; resume at cursor {cursor}")
        self.cursor = cursor
        self.first_offset = first_offset
        self.last_offset = last_offset
        self.seg_id = seg_id
        self.reason = reason


class PlanConflict(Exception):
    """A GC plan can no longer be applied as rehearsed (HTTP 409)."""

    def __init__(self, reasons: List[str]):
        super().__init__("gc plan conflict: " + "; ".join(reasons))
        self.reasons = reasons


class ReadRetry(Exception):
    """A read raced an eviction's directory swap (before manifest commit).

    The byte layout is momentarily absent (live dir moved to the graveyard,
    tombstone not yet published).  The caller should retry immediately; this
    never represents a permanent miss (that is Gone/410 after publish).
    """

    def __init__(self, seg_id: str):
        super().__init__(f"segment {seg_id} is being evicted; retry")
        self.seg_id = seg_id


def _item_size(seg_root: str, seg_id: str) -> int:
    total = 0
    d = segmod.seg_dir(seg_root, seg_id)
    for name in os.listdir(d):
        p = os.path.join(d, name)
        if os.path.isfile(p):
            total += os.path.getsize(p)
    return total


def _events_sha(seg_root: str, seg_id: str) -> str:
    """Actual byte digest of events.log -- catches any post-rehearsal tamper
    even when meta.json itself was not republished."""
    h = hashlib.sha256()
    with open(segmod.events_path(seg_root, seg_id), "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _item_stamp(meta: dict, size: int, content_sha: str) -> str:
    h = hashlib.sha256()
    h.update(f"{meta['id']}|{meta['first_offset']}|{meta['last_offset']}|"
             f"{meta['count']}|{meta.get('version', 1)}|{meta.get('sha256')}|"
             f"{size}|{content_sha}".encode())
    return h.hexdigest()


class GCManager:
    def __init__(self, store):
        self.s = store
        self.state_dir = store.state_dir
        self.seg_root = store.seg_root
        self._gc_path = os.path.join(self.state_dir, "gc.json")
        self._audit_path = os.path.join(self.state_dir, "gc_audit.log")
        self._holds_path = os.path.join(self.state_dir, "holds.json")

        self._holds: Dict[str, dict] = {}
        self._plans: Dict[str, dict] = {}   # in-memory rehearsal outcomes
        self._jobs: Dict[str, dict] = {}
        self._audit: List[dict] = []
        self._pending: Dict[str, str] = {}  # seg_id -> gc_job_id (accepted)
        self._q: "Queue[Optional[str]]" = Queue()
        self._workers: List[threading.Thread] = []
        self._stop = threading.Event()
        # Test/observability hook invoked OUTSIDE the lock:
        #   hook(job_view, seg_id, phase)
        # phase in {"swapped", "published", "audited"} -- each is a crash
        # injection point between the three publish phases.
        self._phase_hook = None

    # ------------------------------------------------------------------ #
    # recovery                                                            #
    # ------------------------------------------------------------------ #

    @property
    def tombstones(self) -> List[dict]:
        return self.s.manifest.setdefault("evicted", [])

    def tombstone(self, seg_id: str) -> Optional[dict]:
        for t in self.tombstones:
            if t["id"] == seg_id:
                return t
        return None

    def recover(self) -> None:
        """Load durable state and reconcile every interrupted eviction.

        Runs before segment verification / orphan sweeps in store.open(),
        so every on-disk decision below is based on reconciled layout.
        """
        self._load_holds()
        self._load_journal()
        self._audit = self._read_audit()

        manifest_changed = False
        for job in self._jobs.values():
            if job["status"] in GC_TERMINAL:
                continue
            plan_items = {it["id"]: it for it in job.get("plan", {}).get("items", [])}
            all_done = True
            for item in job["items"]:
                state = self._inspect_item(job["id"], item["id"])
                audited = any(a["job_id"] == job["id"] and a["id"] == item["id"]
                              for a in self._audit)
                if state in ("grave_only_evicted", "evicted") and audited:
                    # Phase 3 complete (or audit durable + grave already
                    # removed): only graveyard cleanup may remain.
                    self._cleanup_grave(job["id"], item["id"])
                    item["status"] = "done"
                    continue
                if state in ("grave_only_evicted", "evicted"):
                    # Phase 2 landed (manifest tombstone published), phase 3
                    # (audit append) was missed: complete the audit now.
                    with self.s._lock:
                        tomb = self.tombstone(item["id"])
                        if tomb is not None:
                            self._append_audit_entry(tomb)
                        self._pending.pop(item["id"], None)
                    self._cleanup_grave(job["id"], item["id"])
                    fsync_dir(self.seg_root)
                    item["status"] = "done"
                    manifest_changed = True
                    continue
                if state in ("live_missing", "grave_only_live_missing"):
                    # Swap landed, manifest did not (or live vanished while
                    # the durable intent said "evict"): resume the SAME job
                    # by completing publish + audit.
                    self._adopt_eviction(job, item, plan_items.get(item["id"]))
                    item["status"] = "done"
                    manifest_changed = True
                    continue
                if state == "live_and_grave":
                    # First rename never completed durably; restore layout
                    # by dropping the duplicate grave, then evict anew.
                    self._cleanup_grave(job["id"], item["id"])
                # "live": nothing happened yet -> run again.
                item["status"] = "pending"
                all_done = False

            # Recompute progress from the reconciled per-item states; a job
            # is only terminal when every item is genuinely done.
            done_items = [it for it in job["items"] if it["status"] == "done"]
            job["completed"] = len(done_items)
            job["size_freed"] = sum(it["size"] for it in done_items)
            all_done = job["completed"] == job["total"]
            job["status"] = "succeeded" if all_done else "queued"
            job["stage"] = job["status"]
            job["updated_at"] = fmt_ts(utcnow())

        self._reconcile_unknown_graves()
        self._rebuild_pending()
        if manifest_changed:
            self.s._persist_manifest()
        # Persist only when durable job state exists; an empty journal is
        # left untouched (and never created), so plain rehearsal never
        # touches disk.
        if self._jobs:
            self._persist_journal_locked()

        leftovers = [n for n in os.listdir(self.seg_root)
                     if n.startswith(GRAVE_PREFIX)]
        for n in leftovers:
            log.warning("removing unreconciled gc grave %s", n)
            shutil.rmtree(os.path.join(self.seg_root, n), ignore_errors=True)
        if leftovers:
            fsync_dir(self.seg_root)

    def _inspect_item(self, jid: str, seg_id: str) -> str:
        live = os.path.isdir(segmod.seg_dir(self.seg_root, seg_id))
        grave = os.path.isdir(os.path.join(self._grave_root(jid), seg_id))
        evicted = self.tombstone(seg_id) is not None
        if evicted:
            return "grave_only_evicted" if grave else "evicted"
        if live and grave:
            return "live_and_grave"
        if not live and grave:
            return "grave_only_live_missing"
        if not live:
            return "live_missing"
        return "grave" if grave else "live"

    def _adopt_eviction(self, job: dict, item: dict,
                        plan_item: Optional[dict]) -> None:
        """Finish an interrupted swap: publish tombstone + audit, drop grave."""
        seg_id = item["id"]
        grave_seg = os.path.join(self._grave_root(job["id"]), seg_id)
        compact_devices = None
        if plan_item is not None:
            compact_devices = plan_item.get("devices")
        if compact_devices is None and os.path.isdir(grave_seg):
            try:
                compact_devices = self._compact_index(grave_seg)
            except Exception:
                compact_devices = {}
        with self.s._lock:
            meta = self.s._seg_by_id.pop(seg_id, None)
            self.s.manifest["segments"] = [
                m for m in self.s.manifest["segments"] if m["id"] != seg_id]
            tomb = self._tombstone_from(job, item, plan_item, compact_devices)
            self.tombstones.append(tomb)
            self.tombstones.sort(key=lambda t: t["first_offset"])
            self.s._persist_manifest()
            self._append_audit_entry(tomb)
            self._pending.pop(seg_id, None)
        self._cleanup_grave(job["id"], seg_id)
        fsync_dir(self.seg_root)
        log.info("gc %s resumed eviction of %s at startup", job["id"], seg_id)

    def _reconcile_unknown_graves(self) -> None:
        """Graveyard roots with no journaled job: prefer restoring bytes."""
        known = set(self._jobs)
        for name in os.listdir(self.seg_root):
            if not name.startswith(GRAVE_PREFIX) or not os.path.isdir(
                    os.path.join(self.seg_root, name)):
                continue
            jid = name[len(GRAVE_PREFIX):]
            root = os.path.join(self.seg_root, name)
            if jid in known:
                continue
            for seg_id in list(os.listdir(root)):
                g = os.path.join(root, seg_id)
                if not os.path.isdir(g):
                    continue
                live = segmod.seg_dir(self.seg_root, seg_id)
                if self.tombstone(seg_id) is not None:
                    shutil.rmtree(g, ignore_errors=True)
                elif not os.path.exists(live):
                    # Intent lost, live missing: restore the old layout.
                    os.rename(g, live)
                    log.warning("restored %s from orphan gc grave", seg_id)
                else:
                    shutil.rmtree(g, ignore_errors=True)
            fsync_dir(self.seg_root)
            if not os.listdir(root):
                shutil.rmtree(root, ignore_errors=True)

    def _rebuild_pending(self) -> None:
        self._pending = {}
        for job in self._jobs.values():
            if job["status"] not in GC_TERMINAL:
                for item in job["items"]:
                    if item["status"] != "done":
                        self._pending[item["id"]] = job["id"]

    def attach_evicted_indexes(self) -> None:
        """Rebuild device-index markers for evicted items at startup.

        Mirrors store._apply_segment_index so business-order key reads keep
        reporting evicted gaps (and the dedup table keeps their event ids)
        after a restart.
        """
        from .store import DeviceState, Entry  # circular: lazy
        for tomb in self.tombstones:
            for dev_id, entries in (tomb.get("devices") or {}).items():
                dev = self.s._devices.setdefault(dev_id, DeviceState())
                for ie in entries:
                    dev.entries.append(Entry(
                        ie["seq"], ie["offset"], tomb["id"], 0, 0,
                        ie["event_id"], parse_ts(ie["device_ts"])))
                    dev.event_ids[ie["event_id"]] = ie["offset"]
                    dev.seqs.add(ie["seq"])
                dev.entries.sort(key=lambda e: (e.seq, e.offset))
                self.s._refresh_device_extremes(dev)

    # ------------------------------------------------------------------ #
    # holds (reader protection)                                           #
    # ------------------------------------------------------------------ #

    def create_hold(self, hold_id, pos, ttl_seconds) -> dict:
        if not isinstance(hold_id, str) or not hold_id or len(hold_id) > 256:
            raise ValueError("hold_id must be a non-empty string (<=256 chars)")
        if isinstance(pos, bool) or not isinstance(pos, int) or pos < 0:
            raise ValueError("pos must be a non-negative integer offset")
        if isinstance(ttl_seconds, bool) or not isinstance(
                ttl_seconds, (int, float)) or ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be a positive number")
        if ttl_seconds > 31_536_000:
            raise ValueError("ttl_seconds too large (max 1 year)")
        now = time.time()
        with self.s._lock:
            self._purge_holds_locked(now)
            existing = self._holds.get(hold_id)
            boundary = self._boundary_for_locked(pos)
            exp = now + float(ttl_seconds)
            if existing is not None:
                existing["pos"] = pos
                existing["boundary"] = boundary
                existing["expires_epoch"] = exp
                existing["expires_at"] = fmt_ts(
                    datetime.fromtimestamp(exp, timezone.utc))
                existing["renewals"] = existing.get("renewals", 0) + 1
                hold = existing
                renewed = True
            else:
                hold = {
                    "id": hold_id,
                    "pos": pos,
                    "boundary": boundary,
                    "created_at": fmt_ts(datetime.fromtimestamp(now, timezone.utc)),
                    "expires_epoch": exp,
                    "expires_at": fmt_ts(datetime.fromtimestamp(exp, timezone.utc)),
                    "renewals": 0,
                }
                self._holds[hold_id] = hold
                renewed = False
            self._persist_holds_locked()
            view = self._hold_view(hold)
            view["renewed"] = renewed
            return view

    def release_hold(self, hold_id: str) -> dict:
        with self.s._lock:
            self._purge_holds_locked()
            hold = self._holds.pop(hold_id, None)
            if hold is None:
                from .store import NotFound
                raise NotFound(f"hold {hold_id} not found")
            self._persist_holds_locked()
            return {"deleted": hold_id}

    def list_holds(self) -> List[dict]:
        with self.s._lock:
            self._purge_holds_locked()
            return [self._hold_view(h) for h in sorted(
                self._holds.values(), key=lambda h: h["id"])]

    def _boundary_for_locked(self, pos: int) -> int:
        """First protected offset: first_offset of the segment holding pos.

        Segments at first_offset >= boundary are protected.  If pos does
        not lie inside a live segment (open tail, pre-history, or an
        evicted range), pos itself is the boundary.
        """
        for m in self.s.manifest["segments"]:
            if m["first_offset"] <= pos <= m["last_offset"]:
                return m["first_offset"]
        for t in self.tombstones:
            if t["first_offset"] <= pos <= t["last_offset"]:
                return t["first_offset"]
        return pos

    def _purge_holds_locked(self, now: Optional[float] = None) -> bool:
        now = now if now is not None else time.time()
        dead = [hid for hid, h in self._holds.items()
                if h["expires_epoch"] <= now]
        for hid in dead:
            del self._holds[hid]
        if dead:
            self._persist_holds_locked()
        return bool(dead)

    def _is_held_locked(self, meta: dict, now: Optional[float] = None) -> bool:
        now = now if now is not None else time.time()
        for h in self._holds.values():
            if h["expires_epoch"] <= now:
                continue
            if meta["first_offset"] >= h["boundary"]:
                return True
        return False

    @staticmethod
    def _hold_view(h: dict) -> dict:
        return {k: h[k] for k in (
            "id", "pos", "boundary", "created_at", "expires_at", "renewals")}

    def _load_holds(self) -> None:
        if not os.path.exists(self._holds_path):
            return
        try:
            data = load_json(self._holds_path)
        except Exception as exc:
            log.error("cannot read holds journal (%s); starting empty", exc)
            return
        now = time.time()
        for h in data.get("holds", []):
            if h.get("expires_epoch", 0) > now:
                self._holds[h["id"]] = h

    def _persist_holds_locked(self) -> None:
        atomic_write_json(self._holds_path, {"holds": list(self._holds.values())})

    # ------------------------------------------------------------------ #
    # plan rehearsal                                                      #
    # ------------------------------------------------------------------ #

    def is_pending(self, seg_id: str) -> bool:
        return seg_id in self._pending

    def pending_ids(self) -> set:
        return set(self._pending)

    def _snapshot_ids_locked(self) -> set:
        out = set()
        for frz in self.s._freezes:
            out.update(frz.get("segments", []))
        return out

    def _eligible_metas_locked(self, cut: int, now: float) -> List[dict]:
        snap = self._snapshot_ids_locked()
        active_repairs = set(self.s._active_repairs)
        out = []
        for m in self.s.manifest["segments"]:
            if m["status"] != "sealed":
                continue  # quarantined / otherwise not reclaimable
            if m["last_offset"] >= cut:
                continue
            if m["id"] in snap or m["id"] in active_repairs \
                    or m["id"] in self._pending:
                continue
            if self._is_held_locked(m, now):
                continue
            out.append(m)
        out.sort(key=lambda m: m["first_offset"])
        return out

    def create_plan(self, cut) -> dict:
        if isinstance(cut, bool) or not isinstance(cut, int) or cut < 0:
            raise ValueError("cut must be a non-negative integer offset")
        now = time.time()
        with self.s._lock:
            self._purge_holds_locked(now)
            metas = [dict(m) for m in self._eligible_metas_locked(cut, now)]
        # File stats are I/O: gather them outside the global lock so even a
        # huge rehearsal never stalls foreground traffic.
        items = []
        for m in metas:
            size = _item_size(self.seg_root, m["id"])
            content_sha = _events_sha(self.seg_root, m["id"])
            items.append({
                "id": m["id"],
                "first_offset": m["first_offset"],
                "last_offset": m["last_offset"],
                "count": m["count"],
                "stamp": _item_stamp(m, size, content_sha),
                "size": size,
                "sha256": m.get("sha256"),
                "content_sha": content_sha,
                "version": m.get("version", 1),
            })
        h = hashlib.sha256()
        h.update(json.dumps(
            {"cut": cut, "items": [[i["id"], i["stamp"], i["size"]]
                                   for i in items]},
            separators=(",", ":"), sort_keys=True).encode())
        plan_id = (f"gcp-{time.strftime('%Y%m%dT%H%M%S', time.gmtime())}"
                   f"-{uuid.uuid4().hex[:12]}")
        plan = {
            "plan_id": plan_id,
            "stamp": h.hexdigest(),
            "cut": cut,
            "items": items,
            "size": sum(i["size"] for i in items),
            "created_at": fmt_ts(utcnow()),
            "outcome": None,
        }
        self._plans[plan_id] = plan
        return self._plan_view(plan)

    @staticmethod
    def _plan_view(plan: dict) -> dict:
        # Fixed response contract: exactly these four top-level keys.
        return {
            "plan_id": plan["plan_id"],
            "stamp": plan["stamp"],
            "items": [{k: it[k] for k in (
                "id", "first_offset", "last_offset", "count", "stamp", "size")}
                for it in plan["items"]],
            "size": plan["size"],
        }

    # ------------------------------------------------------------------ #
    # apply / validation                                                  #
    # ------------------------------------------------------------------ #

    def apply_plan(self, plan_id: str) -> Tuple[dict, bool]:
        """Returns (gc_job_view, accepted_now).  Raises PlanConflict (409)."""
        plan = self._plans.get(plan_id)
        if plan is None:
            from .store import NotFound
            raise NotFound(f"gc plan {plan_id} not found")

        # Sticky conclusion: the same plan always returns the same answer.
        outcome = plan.get("outcome")
        if outcome is not None:
            if outcome["type"] == "accepted":
                return self._job_view(self._jobs[outcome["job_id"]]), False
            raise PlanConflict(list(outcome.get("reasons", [])))

        now = time.time()
        with self.s._lock:
            self._purge_holds_locked(now)
            candidate_ids = {m["id"]
                             for m in self._eligible_metas_locked(plan["cut"], now)}
            snap = self._snapshot_ids_locked()
            active_repairs = set(self.s._active_repairs)
            pending_snapshot = dict(self._pending)
            metas = {m["id"]: dict(m) for m in self.s.manifest["segments"]}
            holds = [dict(h) for h in self._holds.values()
                     if h["expires_epoch"] > now]

        # Heavy I/O (size + content digest + per-device index capture for
        # the future tombstones) happens OUTSIDE the global lock so a large
        # order never stalls writes/key reads/snapshot builds.
        devices_by_id: Dict[str, dict] = {}
        reasons: List[str] = []
        plan_ids = {i["id"] for i in plan["items"]}
        if candidate_ids != plan_ids:
            gone = sorted(plan_ids - candidate_ids)
            new = sorted(candidate_ids - plan_ids)
            if gone:
                reasons.append("items no longer eligible: " + ",".join(gone))
            if new:
                reasons.append("newly eligible items below cut: " + ",".join(new))
        for it in plan["items"]:
            m = metas.get(it["id"])
            if m is None:
                tomb = self.tombstone(it["id"])
                reasons.append(f"{it['id']}: already evicted" if tomb
                               else f"{it['id']}: segment vanished")
                continue
            if m["status"] != "sealed":
                reasons.append(f"{it['id']}: repair state is {m['status']}")
            if m["id"] in snap:
                reasons.append(f"{it['id']}: now referenced by a snapshot")
            if m["id"] in active_repairs:
                reasons.append(f"{it['id']}: repair in progress")
            if m["id"] in pending_snapshot:
                reasons.append(f"{it['id']}: already being evicted by "
                               f"{pending_snapshot[m['id']]}")
            if any(m["first_offset"] >= h["boundary"] for h in holds):
                reasons.append(f"{it['id']}: covered by a reader hold")
            try:
                size = _item_size(self.seg_root, it["id"])
                content_sha = _events_sha(self.seg_root, it["id"])
                devices_by_id[it["id"]] = self._compact_index(
                    segmod.seg_dir(self.seg_root, it["id"]))
            except OSError as exc:
                reasons.append(f"{it['id']}: cannot stat segment ({exc})")
                continue
            stamp = _item_stamp(m, size, content_sha)
            if stamp != it["stamp"] or m.get("version", 1) != it["version"]:
                reasons.append(f"{it['id']}: stamp changed since rehearsal")

        with self.s._lock:
            outcome = plan.get("outcome")
            if outcome is not None:  # raced with another apply
                if outcome["type"] == "accepted":
                    return self._job_view(self._jobs[outcome["job_id"]]), False
                raise PlanConflict(list(outcome.get("reasons", [])))
            if reasons:
                plan["outcome"] = {
                    "type": "conflict", "reasons": reasons,
                    "at": fmt_ts(utcnow())}
                log.info("gc plan %s rejected: %s", plan_id, reasons)
                raise PlanConflict(reasons)

            job = self._new_job_locked(plan, devices_by_id)
            plan["outcome"] = {"type": "accepted", "job_id": job["id"],
                               "at": fmt_ts(utcnow())}
            for it in plan["items"]:
                self._pending[it["id"]] = job["id"]
            self._persist_journal_locked()

        self._q.put(job["id"])
        log.info("gc plan %s accepted as job %s (%d items, %d bytes)",
                 plan_id, job["id"], len(plan["items"]), plan["size"])
        return self._job_view(job), True

    def get_job(self, job_id: str) -> dict:
        with self.s._lock:
            job = self._jobs.get(job_id)
            if job is None:
                from .store import NotFound
                raise NotFound(f"gc job {job_id} not found")
            return self._job_view(job)

    def list_jobs(self, limit: int = 100) -> List[dict]:
        with self.s._lock:
            jobs = sorted(self._jobs.values(),
                          key=lambda j: j["created_at"], reverse=True)
            return [self._job_view(j) for j in jobs[:max(1, limit)]]

    def _new_job_locked(self, plan: dict,
                        devices_by_id: Optional[Dict[str, dict]] = None) -> dict:
        # Per-device compact indexes were already captured OUTSIDE the lock
        # during apply validation (files are immutable while the item is
        # pending); tombstones/audit stay self-describing.
        devices_by_id = devices_by_id or {}
        items = []
        for it in plan["items"]:
            jt = dict(it)
            jt["devices"] = devices_by_id.get(it["id"], {})
            jt["status"] = "pending"
            items.append(jt)
        job = {
            "id": f"gcj-{time.strftime('%Y%m%dT%H%M%S', time.gmtime())}"
                  f"-{uuid.uuid4().hex[:12]}",
            "plan_id": plan["plan_id"],
            "plan": {"cut": plan["cut"], "stamp": plan["stamp"],
                     "items": items},
            "status": "queued",
            "stage": "queued",
            "created_at": fmt_ts(utcnow()),
            "updated_at": fmt_ts(utcnow()),
            "total": len(items),
            "completed": 0,
            "size_freed": 0,
            "items": items,
            "error": None,
        }
        self._jobs[job["id"]] = job
        return job

    @staticmethod
    def _compact_index(seg_dir_path: str) -> dict:
        """Per-device seq/offset index kept in a tombstone (no byte positions)."""
        seg_id = os.path.basename(seg_dir_path.rstrip(os.sep))
        idx = segmod.load_index(os.path.dirname(seg_dir_path), seg_id)
        out = {}
        for dev_id, d in idx.get("devices", {}).items():
            out[dev_id] = [{
                "seq": ie["seq"], "offset": ie["offset"],
                "event_id": ie["event_id"], "device_ts": ie["device_ts"],
            } for ie in d.get("entries", [])]
        return out

    @staticmethod
    def _job_view(job: dict) -> dict:
        return {
            "id": job["id"],
            "plan_id": job["plan_id"],
            "status": job["status"],
            "stage": job["stage"],
            "created_at": job["created_at"],
            "updated_at": job["updated_at"],
            "total": job["total"],
            "completed": job["completed"],
            "size_freed": job["size_freed"],
            "items": [{"id": i["id"], "first_offset": i["first_offset"],
                       "last_offset": i["last_offset"], "status": i["status"]}
                      for i in job["items"]],
            "error": job.get("error"),
        }

    # ------------------------------------------------------------------ #
    # worker                                                              #
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        n = max(1, getattr(self.s.cfg, "gc_workers", 1))
        for job in self._jobs.values():
            if job["status"] not in GC_TERMINAL:
                job["status"] = "queued"
                self._q.put(job["id"])
        for i in range(n):
            t = threading.Thread(target=self._worker_loop,
                                 name=f"gc-{i}", daemon=True)
            t.start()
            self._workers.append(t)

    def close(self) -> None:
        self._stop.set()
        for _ in self._workers:
            self._q.put(None)
        for t in self._workers:
            t.join(timeout=5)

    def _worker_loop(self) -> None:
        while True:
            jid = self._q.get()
            try:
                if jid is None:
                    return
                self._run_job(jid)
            except Exception:
                log.exception("gc worker crashed running %s", jid)
                with self.s._lock:
                    job = self._jobs.get(jid)
                    if job is not None and job["status"] not in GC_TERMINAL:
                        job["status"] = "queued"
                        job["stage"] = "queued"
                        job["updated_at"] = fmt_ts(utcnow())
                        self._persist_journal_locked()
                        self._rebuild_pending()
            finally:
                self._q.task_done()

    def _run_job(self, jid: str) -> None:
        with self.s._lock:
            job = self._jobs.get(jid)
            if job is None or job["status"] in GC_TERMINAL:
                return
            job["status"] = "running"
            job["stage"] = "starting"
            self._persist_journal_locked()

        for item in job["items"]:
            if self._stop.is_set():
                self._park(jid)
                return
            if item["status"] == "done":
                continue
            self._evict_one(job, item)

        with self.s._lock:
            job = self._jobs.get(jid)
            if job is not None and job["status"] not in GC_TERMINAL:
                job["status"] = "succeeded"
                job["stage"] = "succeeded"
                job["completed"] = job["total"]
                job["updated_at"] = fmt_ts(utcnow())
                self._persist_journal_locked()
                self._rebuild_pending()
        # Evicted segments no longer pin their WAL coverage; re-run the
        # janitor so reclaimed capacity is returned promptly (safe off-lock:
        # only removes files wholly below the live retention horizon).
        try:
            self.s._collect_wal()
        except OSError:
            log.warning("post-gc WAL collection deferred; next seal will retry")
        log.info("gc job %s succeeded: %d items, %d bytes freed",
                 jid, job["total"], job["size_freed"])

    def _park(self, jid: str) -> None:
        with self.s._lock:
            job = self._jobs.get(jid)
            if job is None or job["status"] in GC_TERMINAL:
                return
            job["status"] = "queued"
            job["stage"] = "queued"
            job["updated_at"] = fmt_ts(utcnow())
            self._persist_journal_locked()

    def _evict_one(self, job: dict, item: dict) -> None:
        jid = job["id"]
        seg_id = item["id"]
        grave_root = self._grave_root(jid)
        grave_seg = os.path.join(grave_root, seg_id)

        # Phase 0: durable intent before touching the live directory.
        with self.s._lock:
            job["stage"] = f"swapping:{seg_id}"
            job["updated_at"] = fmt_ts(utcnow())
            self._persist_journal_locked()

        os.makedirs(grave_seg, exist_ok=True)
        live_dir = segmod.seg_dir(self.seg_root, seg_id)
        os.rename(live_dir, grave_seg)   # phase 1: atomic directory swap
        fsync_dir(self.seg_root)
        self._fire_hook(job, seg_id, "swapped")

        # Phase 2: publish the metadata master table (manifest).
        with self.s._lock:
            job["stage"] = f"publishing:{seg_id}"
            meta = self.s._seg_by_id.pop(seg_id, None)
            self.s.manifest["segments"] = [
                m for m in self.s.manifest["segments"] if m["id"] != seg_id]
            if meta is None:
                meta = item  # recovery-style adoption within a live process
            tomb = self._tombstone_from(job, item, item, item.get("devices"))
            self.tombstones.append(tomb)
            self.tombstones.sort(key=lambda t: t["first_offset"])
            self.s._persist_manifest()
            item["status"] = "published"
            job["completed"] += 1
            job["size_freed"] += item["size"]
            job["updated_at"] = fmt_ts(utcnow())
            self._persist_journal_locked()
            self._pending.pop(seg_id, None)
        self._fire_hook(job, seg_id, "published")

        # Phase 3: durable audit, then remove the bytes.
        with self.s._lock:
            self._append_audit_entry(tomb)
            job["stage"] = f"auditing:{seg_id}"
            item["status"] = "done"
            job["updated_at"] = fmt_ts(utcnow())
            self._persist_journal_locked()
        self._fire_hook(job, seg_id, "audited")

        shutil.rmtree(grave_seg, ignore_errors=True)
        try:
            if os.path.isdir(grave_root) and not os.listdir(grave_root):
                shutil.rmtree(grave_root, ignore_errors=True)
            fsync_dir(self.seg_root)
        except OSError:
            log.warning("grave cleanup for %s incomplete; startup will finish",
                        jid)
        log.info("gc %s evicted %s (offsets %d..%d, %d bytes)",
                 jid, seg_id, item["first_offset"], item["last_offset"],
                 item["size"])

    def _tombstone_from(self, job: dict, item: dict, plan_item: Optional[dict],
                        devices: Optional[dict]) -> dict:
        pi = plan_item or item
        return {
            "id": item["id"],
            "first_offset": item["first_offset"],
            "last_offset": item["last_offset"],
            "count": item["count"],
            "sha256": pi.get("sha256"),
            "size": item["size"],
            "stamp": pi.get("stamp"),
            "plan_id": job["plan_id"],
            "job_id": job["id"],
            "evicted_at": fmt_ts(utcnow()),
            "devices": devices or pi.get("devices") or {},
        }

    def _fire_hook(self, job: dict, seg_id: str, phase: str) -> None:
        hook = self._phase_hook
        if hook is not None:
            try:
                hook(self._job_view(job), seg_id, phase)
            except Exception:
                log.exception("gc phase hook raised")

    # ------------------------------------------------------------------ #
    # audit / cursors                                                     #
    # ------------------------------------------------------------------ #

    def _append_audit_entry(self, tomb: dict) -> None:
        entry = {k: tomb.get(k) for k in (
            "id", "first_offset", "last_offset", "count", "size", "stamp",
            "sha256", "plan_id", "job_id", "evicted_at")}
        line = (json.dumps(entry, separators=(",", ":"),
                           ensure_ascii=False) + "\n").encode("utf-8")
        with open(self._audit_path, "ab") as fh:
            fh.write(line)
            fh.flush()
            os.fsync(fh.fileno())
        fsync_dir(self.state_dir)
        self._audit.append(entry)

    def _read_audit(self) -> List[dict]:
        if not os.path.exists(self._audit_path):
            return []
        entries = []
        with open(self._audit_path, "rb") as fh:
            for line in fh:
                try:
                    entries.append(json.loads(line))
                except ValueError:
                    log.warning("ignoring torn audit tail: %r", line[:64])
        return entries

    def list_audit(self, limit: int = 100) -> dict:
        with self.s._lock:
            entries = list(self._audit)
        if limit is not None and len(entries) > limit:
            entries = entries[-limit:]
        return {"audit": entries}

    def cursor_after(self, offset: int) -> Optional[int]:
        """Resume cursor: next live offset after the evicted run covering it."""
        toms = sorted(self.tombstones, key=lambda t: t["first_offset"])
        run = next((t for t in toms
                    if t["first_offset"] <= offset <= t["last_offset"]), None)
        if run is None:
            return None
        end = run["last_offset"]
        # Merge adjacent evicted ranges into one run.
        while True:
            nxt = next((t for t in toms if t["first_offset"] == end + 1), None)
            if nxt is None:
                break
            end = nxt["last_offset"]
        return end + 1

    # ------------------------------------------------------------------ #
    # journals                                                            #
    # ------------------------------------------------------------------ #

    def _grave_root(self, jid: str) -> str:
        return os.path.join(self.seg_root, GRAVE_PREFIX + jid)

    def _cleanup_grave(self, jid: str, seg_id: str) -> None:
        root = self._grave_root(jid)
        shutil.rmtree(os.path.join(root, seg_id), ignore_errors=True)
        try:
            if os.path.isdir(root) and not os.listdir(root):
                shutil.rmtree(root, ignore_errors=True)
        except OSError:
            pass

    def _load_journal(self) -> None:
        if not os.path.exists(self._gc_path):
            return
        try:
            data = load_json(self._gc_path)
        except Exception as exc:
            log.error("cannot read gc journal (%s); starting empty", exc)
            return
        for job in data.get("jobs", []):
            self._jobs[job["id"]] = job
            plan = job.get("plan")
            if plan is not None:
                # Re-register the applied plan so repeat apply after a
                # restart yields the same conclusion (200, same job id).
                self._plans[job["plan_id"]] = {
                    "plan_id": job["plan_id"], "stamp": plan.get("stamp"),
                    "cut": plan.get("cut"), "items": plan.get("items", []),
                    "size": sum(i.get("size", 0) for i in plan.get("items", [])),
                    "outcome": {"type": "accepted", "job_id": job["id"]}}

    def _persist_journal_locked(self) -> None:
        active = [j for j in self._jobs.values()
                  if j["status"] not in GC_TERMINAL]
        done = sorted(
            (j for j in self._jobs.values() if j["status"] in GC_TERMINAL),
            key=lambda j: j["updated_at"] or "", reverse=True)
        keep = active + done[:max(0, getattr(self.s.cfg, "gc_history", 100))]
        keep.sort(key=lambda j: j["created_at"])
        atomic_write_json(self._gc_path, {"jobs": keep})

    def stats(self) -> dict:
        with self.s._lock:
            return self._stats_locked()

    def _stats_locked(self) -> dict:
        now = time.time()
        return {
            "plans": len(self._plans),
            "active_holds": sum(1 for h in self._holds.values()
                                if h["expires_epoch"] > now),
            "evicted_items": len(self.tombstones),
            "bytes_freed": sum(t["size"] for t in self.tombstones),
            "jobs": {
                "queued": sum(1 for j in self._jobs.values()
                              if j["status"] in ("queued", "running")),
                "succeeded": sum(1 for j in self._jobs.values()
                                 if j["status"] == "succeeded"),
                "failed": sum(1 for j in self._jobs.values()
                              if j["status"] == "failed"),
            },
        }

"""Persistent consumer groups for downstream operators.

A *group* is a durable subscription over the global offset log:

  declare   POST /v2/groups registers {name, start, end?, lease_seconds}.
            Re-registering the identical declaration returns the original
            object; a different declaration for an occupied name is
            rejected (409).  A start that already lies in an evicted range
            fails with 410 plus the exact first available offset -- the
            group never silently skips reclaimed data.

  claim     The single active holder receives one batch:
            {batch_key, lease_key, epoch, messages, gaps, next_at}.
            Claiming never moves the checkpoint; before the batch is
            settled, repeated claims redeliver the *same* batch (same
            batch_key) -- across lease takeovers and restarts alike.

  settle    Only the current holder (matching lease_key) may commit the
            batch's exact next_at as the new checkpoint.  Re-submitting
            the committed batch is an idempotent no-op; regressions,
            foreign/fabricated batch keys and stale credentials are
            rejected (409) and the checkpoint stays put.

  lease     One active holder per group.  Renewals keep the epoch; when
            the lease expires, another holder takes over with an
            incremented epoch, and the previous holder's claim/renew/
            settle are all rejected from then on.

  gate      A reclamation water-gate is derived from the checkpoint:
            GC cannot evict segments at or beyond it, so unsettled and
            unread messages are never removed.  Settling moves the gate
            forward; pausing or deregistering removes it.  Static groups
            (declared with `end`) finish permanently at the horizon and
            never see later arrivals; dynamic groups follow new data.

Durability
----------
Declarations, checkpoints, epochs, leases and pending batches live in
state/groups.json (atomic replace).  The gate ledger lives in
state/group_gates.json and is always written *after* the checkpoint
commit, so a crash can leave it stale-but-conservative at worst.  Startup
reconciles the ledger against the journal: no orphaned gates, gates
aligned with checkpoints, settled batches never reappear, pending batches
keep their batch_key, and the checkpoint can never be advanced twice.

Crash-injection hook: hook(group_name, phase) fires UNDER the store lock
at the persistence seams -- "batch_persisted" (claim journaled the
batch), "checkpoint_persisted" (settle committed the checkpoint, gate
ledger not yet moved) and "gate_moved" (gate ledger committed).
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from . import segments as segmod
from .models import fmt_ts, utcnow
from .util import atomic_write_json, load_json

log = logging.getLogger("eventarch.groups")

MAX_CLAIM_LIMIT = 1000
MAX_LEASE_SEC = 31_536_000  # 1 year, same ceiling as reader holds


class GroupConflict(Exception):
    """Declaration/lease/credential/batch validation failure (HTTP 409)."""


class GroupGone(Exception):
    """Requested start (or the checkpoint) lies in an evicted range (410).

    Carries the exact first available offset as ``cursor``; the group
    never silently skips reclaimed data.
    """

    def __init__(self, cursor: int, first_offset: Optional[int] = None,
                 last_offset: Optional[int] = None):
        super().__init__(
            f"requested range starts inside an evicted run; earliest "
            f"available offset is {cursor}")
        self.cursor = cursor
        self.first_offset = first_offset
        self.last_offset = last_offset


class _ScanRace(Exception):
    """Internal: segment layout changed mid-scan; replan the claim."""


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _validate_name(name, field: str = "name", path_safe: bool = True) -> None:
    if not isinstance(name, str) or not name or len(name) > 256:
        raise ValueError(f"{field} must be a non-empty string (<=256 chars)")
    if path_safe and "/" in name:
        raise ValueError(f"{field} must not contain '/'")


def _validate_offset(field: str, v) -> int:
    if isinstance(v, bool) or not isinstance(v, int) or v < 0:
        raise ValueError(f"{field} must be a non-negative integer offset")
    return v


def _validate_lease(v) -> float:
    if isinstance(v, bool) or not isinstance(v, (int, float)) or v <= 0:
        raise ValueError("lease_seconds must be a positive number")
    if v > MAX_LEASE_SEC:
        raise ValueError("lease_seconds too large (max 1 year)")
    return float(v)


def _cursor_after(tombs: List[dict], offset: int) -> Optional[int]:
    """Next live offset after the evicted run covering ``offset``."""
    run = next((t for t in tombs
                if t["first_offset"] <= offset <= t["last_offset"]), None)
    if run is None:
        return None
    end = run["last_offset"]
    while True:
        nxt = next((t for t in tombs if t["first_offset"] == end + 1), None)
        if nxt is None:
            break
        end = nxt["last_offset"]
    return end + 1


def _run_bounds(tombs: List[dict], offset: int) -> Tuple[Optional[int], Optional[int]]:
    """Bounds of the contiguous evicted run covering ``offset``."""
    ts = sorted(tombs, key=lambda t: t["first_offset"])
    run = next((t for t in ts
                if t["first_offset"] <= offset <= t["last_offset"]), None)
    if run is None:
        return None, None
    first, last = run["first_offset"], run["last_offset"]
    changed = True
    while changed:
        changed = False
        for t in ts:
            if t["last_offset"] == first - 1:
                first = t["first_offset"]
                changed = True
            if t["first_offset"] == last + 1:
                last = t["last_offset"]
                changed = True
    return first, last


class GroupManager:
    def __init__(self, store):
        self.s = store
        self.state_dir = store.state_dir
        self._groups_path = os.path.join(self.state_dir, "groups.json")
        self._gates_path = os.path.join(self.state_dir, "group_gates.json")
        self._groups: Dict[str, dict] = {}
        self._gates: Dict[str, int] = {}  # active group name -> gated checkpoint
        # Test/observability hook, invoked UNDER the store lock:
        #   hook(group_name, phase)
        # phase in {"batch_persisted", "checkpoint_persisted", "gate_moved"}
        self._phase_hook = None

    # ------------------------------------------------------------------ #
    # recovery                                                            #
    # ------------------------------------------------------------------ #

    def recover(self) -> None:
        """Load durable state and reconcile the gate ledger with it.

        groups.json is the authority; group_gates.json is a materialized
        copy of "which groups hold a reclamation gate at which checkpoint".
        Anything in the ledger that does not match an *active* group's
        checkpoint is rewritten, so a crash between the checkpoint commit
        and the gate move can leave neither a stale nor an orphaned gate.
        """
        if os.path.exists(self._groups_path):
            try:
                for name, g in load_json(self._groups_path).get("groups", {}).items():
                    self._groups[name] = g
            except Exception as exc:
                log.error("cannot read groups journal (%s); starting empty", exc)
        ledger: Dict[str, Optional[int]] = {}
        if os.path.exists(self._gates_path):
            try:
                for name, g in load_json(self._gates_path).get("gates", {}).items():
                    ledger[name] = g.get("checkpoint")
            except Exception as exc:
                log.error("cannot read group gate ledger (%s); rebuilding", exc)
        expected = {name: g["checkpoint"] for name, g in self._groups.items()
                    if g.get("status") == "active"}
        self._gates = dict(expected)
        if ledger != expected:
            log.warning("reconciling group gates with checkpoints: "
                        "ledger=%r expected=%r", ledger, expected)
            self._persist_gates_locked()

    # ------------------------------------------------------------------ #
    # gates (reclamation water-gate derived from checkpoints)             #
    # ------------------------------------------------------------------ #

    def gate_boundaries_locked(self) -> List[int]:
        """Segment-granularity protection boundaries for GC eligibility.

        Derived live from the gated checkpoints, so a segment that *grows
        into* a gated range (open tail sealing across a checkpoint) is
        protected from the moment it exists.
        """
        return [self._boundary_for_locked(cp) for cp in self._gates.values()]

    def _boundary_for_locked(self, checkpoint: int) -> int:
        """First protected offset: first_offset of the unit holding it."""
        for m in self.s.manifest["segments"]:
            if m["first_offset"] <= checkpoint <= m["last_offset"]:
                return m["first_offset"]
        for t in self.s.gc.tombstones:
            if t["first_offset"] <= checkpoint <= t["last_offset"]:
                return t["first_offset"]
        return checkpoint

    def _refresh_gate_locked(self, name: str) -> None:
        """Re-derive one group's gate ledger entry and persist the ledger.

        Only active groups hold a gate; paused/finished/deregistered
        groups leave nothing behind.
        """
        g = self._groups.get(name)
        if g is not None and g.get("status") == "active":
            self._gates[name] = g["checkpoint"]
        else:
            self._gates.pop(name, None)
        self._persist_gates_locked()

    # ------------------------------------------------------------------ #
    # registration                                                        #
    # ------------------------------------------------------------------ #

    def register(self, name, start, end, lease_seconds) -> Tuple[dict, bool]:
        """Returns (group_view, created).  409 on a diverging declaration,
        410 when ``start`` already lies in an evicted range."""
        _validate_name(name)
        start = _validate_offset("start", start)
        if end is not None:
            end = _validate_offset("end", end)
            if end <= start:
                raise ValueError("end must be greater than start")
        lease_seconds = _validate_lease(lease_seconds)
        with self.s._lock:
            existing = self._groups.get(name)
            if existing is not None:
                if (existing["start"] == start and existing["end"] == end
                        and existing["lease_seconds"] == lease_seconds):
                    # Identical re-declaration: the original object rules.
                    return self._view_locked(existing), False
                raise GroupConflict(
                    f"group {name!r} is already registered with a different "
                    f"declaration")
            cursor = self.s.gc.cursor_after(start)
            if cursor is not None:
                first, last = _run_bounds(self.s.gc.tombstones, start)
                raise GroupGone(cursor, first, last)
            self._check_gc_in_flight_locked(start)
            now = fmt_ts(utcnow())
            g = {
                "name": name,
                "start": start,
                "end": end,
                "lease_seconds": lease_seconds,
                "checkpoint": start,
                "epoch": 0,
                "status": "active",
                "holder": None,
                "pending": None,
                "last_settled": None,
                "created_at": now,
                "updated_at": now,
            }
            self._groups[name] = g
            self._persist_groups_locked()
            self._refresh_gate_locked(name)
            log.info("group %r registered: start=%d end=%r lease=%ss",
                     name, start, end, lease_seconds)
            return self._view_locked(g), True

    def _check_gc_in_flight_locked(self, start: int) -> None:
        """Refuse to subscribe over a range an accepted GC job is evicting.

        Gates only affect plans rehearsed *after* they exist; an eviction
        accepted just before registration must finish (and become a
        tombstone the caller can see) before the group subscribes blind.
        """
        for seg_id in self.s.gc.pending_ids():
            meta = self.s._seg_by_id.get(seg_id)
            if meta is not None and meta["last_offset"] >= start:
                raise GroupConflict(
                    f"capacity reclamation in flight over offset "
                    f"{meta['last_offset']}; retry shortly")

    # ------------------------------------------------------------------ #
    # claim                                                               #
    # ------------------------------------------------------------------ #

    def claim(self, name: str, holder: str, limit: int = 100) -> dict:
        """Produce (or redeliver) the group's current batch.

        Never moves the checkpoint.  Segment I/O happens outside the
        global lock; only the brief plan/commit sections take it.
        """
        _validate_name(holder, field="holder", path_safe=False)
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            limit = 100
        limit = max(1, min(limit, MAX_CLAIM_LIMIT))

        from .store import NotFound
        for _attempt in range(6):
            with self.s._lock:
                g = self._groups.get(name)
                if g is None:
                    raise NotFound(f"group {name!r} not found")
                if g["status"] == "paused":
                    raise GroupConflict(f"group {name!r} is paused")
                if self._is_finished(g):
                    return self._finished_view(g)
                cursor = self.s.gc.cursor_after(g["checkpoint"])
                if cursor is not None:
                    first, last = _run_bounds(self.s.gc.tombstones,
                                              g["checkpoint"])
                    raise GroupGone(cursor, first, last)
                now = time.time()
                h = g["holder"]
                if h is not None and h["expires_epoch"] > now \
                        and h["id"] != holder:
                    raise GroupConflict(
                        f"group {name!r} lease is held by {h['id']!r}")
                pend = g["pending"]
                if pend is not None:
                    spec = ("range", pend["from"], pend["next_at"])
                    planned_key = pend["batch_key"]
                    planned_from = None
                    next_at = None
                else:
                    spec = ("new", g["checkpoint"], limit)
                    planned_key = None
                    planned_from = g["checkpoint"]
                    next_at = None
                end = g["end"]
                snapshot = self._snapshot_locked()
            try:
                if spec[0] == "range":
                    messages, gaps, _ = self._scan(
                        snapshot, spec[1], None, end, spec[2])
                else:
                    messages, gaps, next_at = self._scan(
                        snapshot, spec[1], limit, end, None)
            except _ScanRace:
                continue  # layout changed mid-scan (repair/gc swap): replan
            with self.s._lock:
                g = self._groups.get(name)
                if g is None:
                    raise NotFound(f"group {name!r} not found")
                if g["status"] != "active":
                    continue  # paused/finished mid-claim: replan -> 409/view
                now = time.time()
                h = g["holder"]
                if h is not None and h["expires_epoch"] > now \
                        and h["id"] != holder:
                    raise GroupConflict(
                        f"group {name!r} lease is held by {h['id']!r}")
                if h is None or h["expires_epoch"] <= now:
                    # First claim, or a takeover after expiry: grant the
                    # lease with an incremented epoch and a fresh key.
                    self._grant_lease_locked(g, holder, now)
                    dirty = True
                else:
                    dirty = False
                if planned_key is not None:
                    cur = g["pending"]
                    if cur is None or cur["batch_key"] != planned_key:
                        continue  # settled/replaced while scanning: replan
                    if dirty:
                        g["updated_at"] = fmt_ts(utcnow())
                        self._persist_groups_locked()
                    return self._batch_view(g, cur, messages, gaps)
                if g["pending"] is not None or g["checkpoint"] != planned_from:
                    continue  # a concurrent claim won: serve its batch
                batch = {
                    "batch_key": _new_id("bt"),
                    "from": g["checkpoint"],
                    "next_at": next_at,
                    "limit": limit,
                    "epoch": g["epoch"],
                    "created_at": fmt_ts(utcnow()),
                }
                g["pending"] = batch
                g["updated_at"] = fmt_ts(utcnow())
                self._persist_groups_locked()
                self._fire_hook(name, "batch_persisted")
                return self._batch_view(g, batch, messages, gaps)
        raise GroupConflict("claim raced with concurrent changes; retry")

    def _grant_lease_locked(self, g: dict, holder: str, now: float) -> None:
        g["epoch"] += 1
        exp = now + g["lease_seconds"]
        g["holder"] = {
            "id": holder,
            "lease_key": _new_id("lk"),
            "expires_epoch": exp,
            "expires_at": fmt_ts(datetime.fromtimestamp(exp, timezone.utc)),
        }

    # ------------------------------------------------------------------ #
    # renew / settle                                                      #
    # ------------------------------------------------------------------ #

    def renew(self, name: str, holder: str, lease_key: str) -> dict:
        """Extend the current holder's lease; the epoch never changes."""
        _validate_name(holder, field="holder", path_safe=False)
        if not isinstance(lease_key, str) or not lease_key:
            raise ValueError("lease_key must be a non-empty string")
        from .store import NotFound
        with self.s._lock:
            g = self._groups.get(name)
            if g is None:
                raise NotFound(f"group {name!r} not found")
            if g["status"] != "active":
                raise GroupConflict(f"group {name!r} is {g['status']}")
            now = time.time()
            h = g["holder"]
            if (h is None or h["expires_epoch"] <= now
                    or h["id"] != holder or h["lease_key"] != lease_key):
                raise GroupConflict("stale or invalid lease credentials")
            exp = now + g["lease_seconds"]
            h["expires_epoch"] = exp
            h["expires_at"] = fmt_ts(datetime.fromtimestamp(exp, timezone.utc))
            g["updated_at"] = fmt_ts(utcnow())
            self._persist_groups_locked()
            return {"group": name, "epoch": g["epoch"], "holder": holder,
                    "lease_expires_at": h["expires_at"]}

    def settle(self, name: str, holder: str, lease_key: str,
               batch_key: str, next_at: int) -> dict:
        """Commit the pending batch's next_at as the new checkpoint.

        Only the current holder may submit, and only the exact next_at
        the pending batch carried.  Re-submitting the committed batch is
        an idempotent no-op; anything else is a 409 with the checkpoint
        untouched.
        """
        _validate_name(holder, field="holder", path_safe=False)
        if not isinstance(lease_key, str) or not lease_key:
            raise ValueError("lease_key must be a non-empty string")
        if not isinstance(batch_key, str) or not batch_key:
            raise ValueError("batch_key must be a non-empty string")
        next_at = _validate_offset("next_at", next_at)
        from .store import NotFound
        with self.s._lock:
            g = self._groups.get(name)
            if g is None:
                raise NotFound(f"group {name!r} not found")
            now = time.time()
            h = g["holder"]
            creds_ok = (h is not None and h["expires_epoch"] > now
                        and h["id"] == holder and h["lease_key"] == lease_key)
            ls = g["last_settled"]
            if (creds_ok and ls is not None
                    and ls["batch_key"] == batch_key
                    and ls["next_at"] == next_at):
                # Re-submitting the committed batch: done, no side effects.
                return self._settle_view(g, batch_key, idempotent=True)
            if g["status"] != "active":
                raise GroupConflict(
                    f"group {name!r} is {g['status']}; settle rejected")
            if not creds_ok:
                raise GroupConflict("stale or invalid lease credentials")
            pend = g["pending"]
            if pend is None:
                raise GroupConflict("no pending batch to settle")
            if pend["batch_key"] != batch_key:
                raise GroupConflict(
                    "batch_key does not match the pending batch")
            if pend["next_at"] != next_at:
                raise GroupConflict(
                    f"next_at {next_at} does not match the pending batch's "
                    f"{pend['next_at']}")
            # Commit point 1: checkpoint + cleared pending + settled record
            # in ONE atomic journal write, so the checkpoint can never be
            # advanced twice for the same batch.
            g["checkpoint"] = next_at
            g["pending"] = None
            g["last_settled"] = {
                "batch_key": batch_key,
                "next_at": next_at,
                "epoch": g["epoch"],
                "settled_at": fmt_ts(utcnow()),
            }
            if g["end"] is not None and next_at >= g["end"]:
                g["status"] = "finished"
            g["updated_at"] = fmt_ts(utcnow())
            self._persist_groups_locked()
            self._fire_hook(name, "checkpoint_persisted")
            # Commit point 2: the gate ledger follows the checkpoint
            # (stale-but-conservative if a crash lands in between).
            self._refresh_gate_locked(name)
            self._fire_hook(name, "gate_moved")
            return self._settle_view(g, batch_key, idempotent=False)

    # ------------------------------------------------------------------ #
    # pause / resume / deregister                                         #
    # ------------------------------------------------------------------ #

    def pause(self, name: str) -> dict:
        """Release the lease and drop the reclamation gate (idempotent)."""
        from .store import NotFound
        with self.s._lock:
            g = self._groups.get(name)
            if g is None:
                raise NotFound(f"group {name!r} not found")
            if g["status"] == "finished":
                raise GroupConflict(f"group {name!r} is finished")
            if g["status"] != "paused":
                g["status"] = "paused"
                g["holder"] = None
                g["updated_at"] = fmt_ts(utcnow())
                self._persist_groups_locked()
                self._refresh_gate_locked(name)  # gate removed
                log.info("group %r paused at checkpoint %d",
                         name, g["checkpoint"])
            return self._view_locked(g)

    def resume(self, name: str) -> dict:
        """Re-activate a paused group from its original checkpoint."""
        from .store import NotFound
        with self.s._lock:
            g = self._groups.get(name)
            if g is None:
                raise NotFound(f"group {name!r} not found")
            if g["status"] != "paused":
                raise GroupConflict(f"group {name!r} is not paused")
            cursor = self.s.gc.cursor_after(g["checkpoint"])
            if cursor is not None:
                # The unprotected checkpoint was reclaimed while paused;
                # report the exact available start, never skip silently.
                first, last = _run_bounds(self.s.gc.tombstones,
                                          g["checkpoint"])
                raise GroupGone(cursor, first, last)
            self._check_gc_in_flight_locked(g["checkpoint"])
            g["status"] = "active"
            g["updated_at"] = fmt_ts(utcnow())
            self._persist_groups_locked()
            self._refresh_gate_locked(name)  # gate re-established
            log.info("group %r resumed at checkpoint %d", name, g["checkpoint"])
            return self._view_locked(g)

    def delete(self, name: str) -> dict:
        """Deregister: drop the gate and remove the declaration."""
        from .store import NotFound
        with self.s._lock:
            g = self._groups.pop(name, None)
            if g is None:
                raise NotFound(f"group {name!r} not found")
            self._persist_groups_locked()
            self._refresh_gate_locked(name)  # gate removed
            log.info("group %r deregistered (last checkpoint %d)",
                     name, g["checkpoint"])
            return {"deleted": name}

    # ------------------------------------------------------------------ #
    # views                                                               #
    # ------------------------------------------------------------------ #

    def get_group(self, name: str) -> dict:
        from .store import NotFound
        with self.s._lock:
            g = self._groups.get(name)
            if g is None:
                raise NotFound(f"group {name!r} not found")
            return self._view_locked(g)

    def list_groups(self) -> List[dict]:
        with self.s._lock:
            return [self._view_locked(g) for g in sorted(
                self._groups.values(), key=lambda x: x["name"])]

    def _view_locked(self, g: dict) -> dict:
        h = g["holder"]
        holder_view = None
        if h is not None:
            holder_view = {
                "id": h["id"],
                "expires_at": h["expires_at"],
                "lease_live": h["expires_epoch"] > time.time(),
            }
        pend = g["pending"]
        gate_cp = self._gates.get(g["name"])
        return {
            "name": g["name"],
            "start": g["start"],
            "end": g["end"],
            "lease_seconds": g["lease_seconds"],
            "checkpoint": g["checkpoint"],
            "epoch": g["epoch"],
            "status": g["status"],
            "holder": holder_view,
            "pending": ({"batch_key": pend["batch_key"],
                         "from": pend["from"],
                         "next_at": pend["next_at"]}
                        if pend is not None else None),
            "gate": ({"checkpoint": gate_cp,
                      "boundary": self._boundary_for_locked(gate_cp)}
                     if gate_cp is not None else None),
            "last_settled": g["last_settled"],
            "created_at": g["created_at"],
            "updated_at": g["updated_at"],
        }

    @staticmethod
    def _is_finished(g: dict) -> bool:
        return g["status"] == "finished" or (
            g["end"] is not None and g["checkpoint"] >= g["end"])

    @staticmethod
    def _finished_view(g: dict) -> dict:
        return {
            "group": g["name"],
            "finished": True,
            "batch_key": None,
            "lease_key": None,
            "epoch": g["epoch"],
            "checkpoint": g["checkpoint"],
            "messages": [],
            "gaps": [],
            "next_at": g["checkpoint"],
        }

    @staticmethod
    def _batch_view(g: dict, batch: dict, messages, gaps) -> dict:
        h = g["holder"]
        return {
            "group": g["name"],
            "batch_key": batch["batch_key"],
            "lease_key": h["lease_key"] if h else None,
            "epoch": g["epoch"],
            "holder": h["id"] if h else None,
            "lease_expires_at": h["expires_at"] if h else None,
            "checkpoint": batch["from"],
            "messages": messages,
            "gaps": gaps,
            "next_at": batch["next_at"],
            "finished": False,
        }

    @staticmethod
    def _settle_view(g: dict, batch_key: str, idempotent: bool) -> dict:
        return {
            "group": g["name"],
            "checkpoint": g["checkpoint"],
            "settled": batch_key,
            "idempotent": idempotent,
            "epoch": g["epoch"],
            "status": g["status"],
        }

    # ------------------------------------------------------------------ #
    # scanning (segment I/O outside the global lock)                      #
    # ------------------------------------------------------------------ #

    def _snapshot_locked(self):
        return (
            [dict(m) for m in self.s.manifest["segments"]],
            [dict(t) for t in self.s.gc.tombstones],
            list(self.s._open_records),
        )

    def _scan(self, snapshot, from_offset: int, limit: Optional[int],
              horizon: Optional[int], until: Optional[int]):
        """Walk live/evicted units in offset order from ``from_offset``.

        ``horizon`` is the static end (offsets >= horizon are invisible);
        ``until`` re-scans exactly a persisted batch's [from, next_at).
        Returns (messages, gaps, next_at); quarantined and evicted ranges
        surface as explicit gaps, never silently skipped.
        """
        metas, tombs, open_recs = snapshot
        stop = horizon
        if until is not None:
            stop = until if stop is None else min(stop, until)
        units = ([(t["first_offset"], "tomb", t) for t in tombs]
                 + [(m["first_offset"], "seg", m) for m in metas])
        units.sort(key=lambda u: u[0])
        messages: List[dict] = []
        gaps: List[dict] = []
        scanned = from_offset - 1
        full = False

        def past_stop() -> bool:
            return stop is not None and scanned + 1 >= stop

        for _first, kind, unit in units:
            if full or past_stop():
                break
            if unit["last_offset"] <= scanned:
                continue
            if stop is not None and unit["first_offset"] >= stop:
                break
            if kind == "tomb":
                gaps.append({
                    "segment": unit["id"],
                    "reason": "evicted",
                    "resume_offset": _cursor_after(
                        tombs, max(from_offset, unit["first_offset"])),
                    "first_offset": unit["first_offset"],
                    "last_offset": unit["last_offset"],
                })
                scanned = max(scanned, unit["last_offset"])
                continue
            meta = unit
            if meta["status"] == "quarantined":
                gaps.append({
                    "segment": meta["id"],
                    "reason": meta.get("quarantine_reason", ""),
                    "resume_offset": meta["last_offset"] + 1,
                })
                scanned = max(scanned, meta["last_offset"])
                continue
            try:
                recs, _complete = segmod.scan_records(
                    self.s.seg_root, meta, scanned + 1)
            except segmod.SegmentCorrupt as c:
                self.s.quarantine(meta["id"], c.reason,
                                  expected_sha=meta.get("sha256"))
                gaps.append({"segment": meta["id"], "reason": c.reason,
                             "resume_offset": c.resume_offset})
                scanned = max(scanned, meta["last_offset"])
                continue
            except FileNotFoundError:
                # Directory swap (repair/gc) raced the read; replan.
                raise _ScanRace()
            for rec in recs:
                if rec["offset"] <= scanned:
                    continue
                if stop is not None and rec["offset"] >= stop:
                    break
                scanned = rec["offset"]
                messages.append(rec)
                if limit is not None and len(messages) >= limit:
                    full = True
                    break
        if not full and not past_stop():
            for rec in open_recs:
                if rec["offset"] <= scanned:
                    continue
                if stop is not None and rec["offset"] >= stop:
                    break
                scanned = rec["offset"]
                messages.append(rec)
                if limit is not None and len(messages) >= limit:
                    break
        next_at = scanned + 1
        if stop is not None and next_at > stop:
            next_at = stop
        return messages, gaps, next_at

    # ------------------------------------------------------------------ #
    # journals / stats                                                    #
    # ------------------------------------------------------------------ #

    def _persist_groups_locked(self) -> None:
        atomic_write_json(self._groups_path, {"groups": self._groups})

    def _persist_gates_locked(self) -> None:
        atomic_write_json(self._gates_path, {
            "gates": {n: {"checkpoint": cp, "updated_at": fmt_ts(utcnow())}
                      for n, cp in sorted(self._gates.items())}})

    def _fire_hook(self, name: str, phase: str) -> None:
        hook = self._phase_hook
        if hook is not None:
            try:
                hook(name, phase)
            except Exception:
                log.exception("group phase hook raised")

    def _stats_locked(self) -> dict:
        return {
            "total": len(self._groups),
            "active": sum(1 for g in self._groups.values()
                          if g["status"] == "active"),
            "paused": sum(1 for g in self._groups.values()
                          if g["status"] == "paused"),
            "finished": sum(1 for g in self._groups.values()
                            if g["status"] == "finished"),
            "gates": len(self._gates),
        }

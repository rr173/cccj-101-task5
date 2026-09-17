"""Persistent downstream consumer groups (durable subscriptions), API v2.

A group is a named downstream subscription with its own checkpoint.  Each
group has at most one *effective holder* at a time; holders work under
wall-clock leases and process data in explicitly settled batches.

Core objects
------------
declaration  POST /v2/groups registers {name, start, end?, lease_seconds}.
             ``end`` is an *exclusive* static horizon: a static group ends
             permanently once its checkpoint reaches ``end`` and never sees
             later messages; without ``end`` the group is dynamic and keeps
             following newly ingested messages.  Re-declaring the exact same
             name returns the original object; a different declaration that
             takes an existing name is rejected with 409.  A start offset that
             already lies inside an evicted range fails with 410 and the
             precise usable cursor -- never a silent skip.

lease        A successful claim makes the caller the holder with a fresh
             ``lease_key`` valid for ``lease_seconds``.  Renewal extends the
             same lease (epoch unchanged).  When the lease expires a new
             claimant takes over with a strictly greater epoch; every later
             claim / renew / settle carrying an old credential is rejected.

batch        ``claim`` returns one batch: batch_key, lease_key, epoch,
             messages, gaps and next_at.  Claiming never moves the checkpoint:
             while a batch is outstanding, repeated claims return the SAME
             batch (same batch_key).  ``settle`` accepts only the exact
             next_at of that batch from the current holder.  A repeat settle
             is an idempotent no-op; regressive next_at, cross/unknown batch
             keys and dead credentials fail with 409 and leave the checkpoint
             untouched.

watergate    Each active group automatically pins retention at a watergate
             derived from its checkpoint (the segment containing the
             checkpoint and everything at a greater position).  Messages a
             batch has been handed out but not yet settled therefore cannot
             be reclaimed.  The gate moves forward only after a settle; it is
             withdrawn when the group is paused, deregistered, or (for static
             groups) reaches its declared end.  No gate ever exists without a
             live owning group.

Durability / crash seams
------------------------
All declarations, checkpoints, epochs and outstanding batches are fsynced:

  state/v2_groups.json   declarations, checkpoint, epoch, owner lease,
                         embedded outstanding-batch descriptor
  state/v2_batches.json  outstanding-batch journal (batch_key -> descriptor)
  state/v2_gates.json    retention watergates (group -> boundary offset)

A settle publishes in order: (1) the batch was journaled at claim time;
(2) checkpoint persisted in v2_groups.json; (3) batch journal trimmed and the
watergate migrated in v2_gates.json.  A hard process kill after any of these
steps is reconciled at startup: settled batches never reappear, outstanding
batches keep their original batch_key, the gate realigns to the checkpoint,
no orphan gate survives, and a checkpoint is never advanced twice.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from . import gc as gcmod
from .models import fmt_ts, utcnow
from .util import atomic_write_json, load_json

log = logging.getLogger("eventarch.groups")

MAX_LEASE_SECONDS = 31_536_000  # one year
SETTLED_KEY_HISTORY = 8
_NAME_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}\Z")


class Conflict(Exception):
    """A group-state precondition failed (HTTP 409)."""

    def __init__(self, msg: str, reason: str = "conflict"):
        super().__init__(msg)
        self.reason = reason


def _iso(epoch: float) -> str:
    return fmt_ts(datetime.fromtimestamp(epoch, timezone.utc))


class GroupsManager:
    def __init__(self, store):
        self.s = store
        self.state_dir = store.state_dir
        self._groups_path = os.path.join(self.state_dir, "v2_groups.json")
        self._batches_path = os.path.join(self.state_dir, "v2_batches.json")
        self._gates_path = os.path.join(self.state_dir, "v2_gates.json")

        self._groups: Dict[str, dict] = {}
        self._batches: Dict[str, dict] = {}   # batch_key -> descriptor
        self._gates: Dict[str, int] = {}      # group name -> boundary offset
        self._lock = threading.RLock()
        # Test/observability hook invoked OUTSIDE manager state mutation but
        # while the store lock is held:
        #   hook(group_view, batch_view, phase)
        # phase in {"batch_durable", "checkpoint_durable", "gate_migrated"}.
        self._phase_hook = None

    # ------------------------------------------------------------------ #
    # recovery                                                            #
    # ------------------------------------------------------------------ #

    def recover(self) -> None:
        """Reconcile the three durable files into one consistent picture.

        Runs after manifest/tombstone load and GC recovery in store.open().
        The embedded outstanding descriptor in the group record is the
        authority: every crash window below is resolved from it.

          * checkpoint >= batch.next_at: the settle's checkpoint write landed
            (possibly before batch/gate writes) -> the batch is retired, never
            reissued, and the gate realigns to the checkpoint;
          * still outstanding: the journal entry is restored with the SAME
            batch_key and the gate keeps fencing the checkpoint;
          * gate for a missing/paused/finished group: dropped (no orphan
            watergate); missing gate for an active group: recreated.
        """
        if os.path.exists(self._groups_path):
            try:
                for g in load_json(self._groups_path).get("groups", []):
                    self._groups[g["name"]] = g
            except Exception as exc:
                log.error("cannot read v2 groups journal (%s); starting empty", exc)
        if os.path.exists(self._batches_path):
            try:
                for b in load_json(self._batches_path).get("batches", []):
                    self._batches[b["batch_key"]] = b
            except Exception as exc:
                log.error("cannot read v2 batch journal (%s); rebuilding", exc)
                self._batches = {}
        if os.path.exists(self._gates_path):
            try:
                self._gates = {g["group"]: g["boundary"]
                               for g in load_json(self._gates_path).get("gates", [])}
            except Exception as exc:
                log.error("cannot read v2 gates journal (%s); rebuilding", exc)
                self._gates = {}

        groups_changed = False
        journal: Dict[str, dict] = {}
        gates: Dict[str, int] = {}
        for name, g in self._groups.items():
            g.setdefault("settled_keys", [])
            desc = g.get("outstanding")
            if desc is not None:
                if g.get("checkpoint", 0) >= desc["next_at"]:
                    # Settle crash window: checkpoint durable, journal/gate
                    # steps were missed.  Retire the batch exactly once.  The
                    # committed lease is also retired (a fresh claim takes
                    # over with the same epoch), so the settled batch can
                    # never be reissued and no stale holder survives.
                    g["outstanding"] = None
                    g["owner"] = None
                    g["last_settled"] = {"batch_key": desc["batch_key"],
                                        "next_at": desc["next_at"],
                                        "lease_key": desc.get("lease_key")}
                    if desc["batch_key"] not in g["settled_keys"]:
                        g["settled_keys"].append(desc["batch_key"])
                        g["settled_keys"] = g["settled_keys"][-SETTLED_KEY_HISTORY:]
                    groups_changed = True
                    log.info("group %s: reconciled settled batch %s at checkpoint %d",
                             name, desc["batch_key"], g["checkpoint"])
                else:
                    # Outstanding batch survives with its original batch_key;
                    # repair the journal if its entry was torn/lost.
                    journal[desc["batch_key"]] = desc
                    old = self._batches.get(desc["batch_key"])
                    if old is None or old.get("next_at") != desc["next_at"]:
                        groups_changed = True

            if self._is_active_locked(g):
                gates[name] = self._boundary_for_locked(g["checkpoint"])
            elif name in self._gates:
                groups_changed = True  # gate must be withdrawn below

        if journal != self._batches:
            groups_changed = True
        self._batches = journal
        if gates != self._gates:
            groups_changed = True
        self._gates = gates

        if groups_changed:
            self._persist_groups_locked()
            self._persist_batches_locked()
            self._persist_gates_locked()
        log.info("v2 group recovery: %d groups, %d outstanding batches, %d gates",
                 len(self._groups), len(self._batches), len(self._gates))

    # ------------------------------------------------------------------ #
    # declaration                                                         #
    # ------------------------------------------------------------------ #

    def register(self, name, start, end, lease_seconds) -> dict:
        self._validate_declaration(name, start, end, lease_seconds)
        now = time.time()
        with self.s._lock:
            existing = self._groups.get(name)
            if existing is not None:
                same = (existing["start"] == start
                        and existing.get("end") == end
                        and existing.get("lease_seconds") == lease_seconds)
                if not same:
                    raise Conflict(
                        f"group {name!r} already declared with different "
                        f"start/end/lease_seconds", reason="name_taken")
                view = self.group_view(existing)
                view["redeclared"] = True
                return view

            # A start inside an evicted run is rejected with the exact usable
            # cursor -- the system must never jump over discarded data.
            self._raise_if_evicted_locked(start)

            finished = end is not None and start == end
            g = {
                "name": name,
                "start": start,
                "end": end,
                "lease_seconds": float(lease_seconds),
                "checkpoint": start,
                "epoch": 0,
                "status": "finished" if finished else "active",
                "owner": None,
                "outstanding": None,
                "last_settled": None,
                "settled_keys": [],
                "created_at": fmt_ts(utcnow()),
                "updated_at": fmt_ts(utcnow()),
            }
            self._groups[name] = g
            self._persist_groups_locked()
            if not finished:
                self._set_gate_locked(name, start)
            view = self.group_view(g)
            view["redeclared"] = False
            log.info("group %r registered: start=%d end=%s lease=%ss%s",
                     name, start, end, lease_seconds,
                     " (already at end -> finished)" if finished else "")
            return view

    def get_group(self, name: str) -> dict:
        with self.s._lock:
            g = self._require_group_locked(name)
            return self.group_view(g)

    def list_groups(self) -> List[dict]:
        with self.s._lock:
            return [self.group_view(g) for g in sorted(
                self._groups.values(), key=lambda g: g["created_at"])]

    @staticmethod
    def _validate_declaration(name, start, end, lease_seconds) -> None:
        if not isinstance(name, str) or not _NAME_RE.match(name):
            raise ValueError(
                "name must be 1..128 chars of [A-Za-z0-9_.:-]")
        if isinstance(start, bool) or not isinstance(start, int) or start < 0:
            raise ValueError("start must be a non-negative integer offset")
        if end is not None:
            if isinstance(end, bool) or not isinstance(end, int) or end < 0:
                raise ValueError("end must be a non-negative integer offset")
            if end < start:
                raise ValueError("end must be >= start")
        if isinstance(lease_seconds, bool) or not isinstance(
                lease_seconds, (int, float)) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be a positive number")
        if lease_seconds > MAX_LEASE_SECONDS:
            raise ValueError("lease_seconds too large (max 1 year)")

    def _raise_if_evicted_locked(self, offset: int) -> None:
        for t in self.s.gc.tombstones:
            if t["first_offset"] <= offset <= t["last_offset"]:
                cursor = self.s.gc.cursor_after(t["first_offset"])
                raise gcmod.Gone(cursor, t["first_offset"], t["last_offset"],
                                 seg_id=t["id"], reason="start_evicted")

    # ------------------------------------------------------------------ #
    # claim                                                               #
    # ------------------------------------------------------------------ #

    def claim(self, name: str, lease_key: Optional[str] = None,
              limit: Optional[int] = None) -> dict:
        """Hand out the outstanding batch, or a fresh batch at the checkpoint.

        Heavy segment reads happen OUTSIDE the global lock; ownership is
        decided under the lock, re-checked after the read, and the batch
        descriptor (with a fresh batch_key) is journaled before the response.
        """
        if limit is None:
            limit = getattr(self.s.cfg, "group_max_batch", 500)
        limit = max(1, min(int(limit), 5000))
        with self.s._lock:
            g = self._require_group_locked(name)
            if g["status"] == "finished":
                return self._finished_view(g)
            if g["status"] == "paused":
                raise Conflict(f"group {name!r} is paused; resume before claim",
                               reason="paused")
            now = time.time()
            granted = self._ensure_holder_locked(g, lease_key, now)
            my_key = g["owner"]["lease_key"]
            desc = g.get("outstanding")
            if desc is not None:
                # Same outstanding batch: deterministic re-read of exactly
                # the range first handed out, regardless of limit.
                read_from, horizon, read_limit = desc["from"], desc["next_at"], None
            else:
                read_from = g["checkpoint"]
                horizon = self._horizon_locked(g)
                read_limit = limit
            snapshot_epoch = g["epoch"]

        # File I/O outside the global lock.
        window = self.s.read_claim_window(read_from, horizon, read_limit)

        with self.s._lock:
            g = self._groups.get(name)
            if g is None or g["status"] != "active" or g["owner"] is None \
                    or g["owner"]["lease_key"] != my_key \
                    or g["epoch"] != snapshot_epoch:
                raise Conflict("group ownership changed while reading",
                               reason="concurrent_change")
            if desc is not None:
                cur = g.get("outstanding")
                if cur is None or cur["batch_key"] != desc["batch_key"]:
                    raise Conflict("batch superseded while re-reading",
                                   reason="cross_batch")
                return self._batch_view(g, desc, window)

            # Static group that has reached its end: finish permanently and
            # withdraw the gate.  No later message can ever become visible.
            if g["end"] is not None and g["checkpoint"] >= g["end"]:
                self._finish_locked(g)
                return self._finished_view(g)

            if window["next_at"] is None:
                # Dynamic drain / no data up to the horizon: keep the lease
                # but do not create a batch; checkpoint stays put.
                return self._empty_view(g)

            batch = {
                "batch_key": self._new_id("btk"),
                "lease_key": my_key,
                "epoch": g["epoch"],
                "group": name,
                "from": g["checkpoint"],
                "next_at": window["next_at"],
                "count": len(window["events"]),
                "created_at": fmt_ts(utcnow()),
            }
            g["outstanding"] = batch
            g["updated_at"] = fmt_ts(utcnow())
            # epoch/owner were persisted when the lease was granted; now make
            # the outstanding descriptor durable and journal the batch.
            self._persist_groups_locked()
            self._batches[batch["batch_key"]] = batch
            self._persist_batches_locked()      # batch durable (crash seam 1)
            self._fire_hook(g, batch, "batch_durable")
            return self._batch_view(g, batch, window)

    def renew(self, name: str, lease_key: str,
              lease_seconds: Optional[float] = None) -> dict:
        if lease_seconds is not None:
            self._validate_declaration("x:1", 0, None, lease_seconds)
        with self.s._lock:
            g = self._require_group_locked(name)
            self._require_holder_locked(g, lease_key)  # 409 if stale/expired
            seconds = float(lease_seconds) if lease_seconds is not None \
                else g["lease_seconds"]
            exp = time.time() + seconds
            g["owner"]["expires_epoch"] = exp
            g["owner"]["expires_at"] = _iso(exp)
            g["updated_at"] = fmt_ts(utcnow())
            self._persist_groups_locked()
            log.info("group %r lease renewed (epoch %d unchanged, until %s)",
                     name, g["epoch"], g["owner"]["expires_at"])
            return {"group": name, "renewed": True, "epoch": g["epoch"],
                    "lease_key": g["owner"]["lease_key"],
                    "lease_expires_at": g["owner"]["expires_at"]}

    # ------------------------------------------------------------------ #
    # settle                                                              #
    # ------------------------------------------------------------------ #

    def settle(self, name: str, lease_key: str, batch_key: str,
               next_at: int) -> dict:
        if isinstance(next_at, bool) or not isinstance(next_at, int):
            raise ValueError("next_at must be an integer offset")
        with self.s._lock:
            g = self._require_group_locked(name)
            # Only the current holder with a live lease may submit.  Settle is
            # a commit under that persistent lease: it does not release the
            # holder (lease expiry is the only hand-off point), so an
            # immediate identical resubmission still authenticates and is
            # treated as already done.
            self._require_holder_locked(g, lease_key)

            # A finished static group has no outstanding batch: an identical
            # resubmission of its final batch remains an idempotent no-op; a
            # different next_at is still rejected.
            if g["status"] == "finished" and batch_key in g.get("settled_keys", []):
                expected = g["checkpoint"]
                if next_at != expected:
                    raise Conflict(
                        f"next_at {next_at} does not match settled batch "
                        f"{batch_key} (expected {expected})",
                        reason="next_at_mismatch")
                view = self.group_view(g)
                view["settled"] = True
                view["idempotent"] = True
                view["next_at"] = expected
                return view

            desc = g.get("outstanding")
            if desc is not None:
                if desc["batch_key"] != batch_key:
                    if batch_key in g.get("settled_keys", []):
                        raise Conflict(
                            f"batch {batch_key} belongs to a different batch",
                            reason="cross_batch")
                    raise Conflict(f"unknown batch_key {batch_key}",
                                   reason="unknown_batch")
                if next_at != desc["next_at"]:
                    # Regressive (next_at below batch/checkpoint) or a forged
                    # value past the batch: reject, checkpoint untouched.
                    raise Conflict(
                        f"next_at {next_at} does not match batch "
                        f"{batch_key} (expected {desc['next_at']})",
                        reason="next_at_mismatch")
                return self._commit_settle_locked(g, desc)

            # No batch outstanding: a repeat submission of the batch this
            # holder just settled is a no-op (already done, checkpoint
            # unchanged) only when next_at matches; anything else is
            # cross/forged and the checkpoint stays put.
            if g.get("last_settled") and g["last_settled"]["batch_key"] == batch_key:
                expected = g["last_settled"]["next_at"]
                if next_at != expected:
                    raise Conflict(
                        f"next_at {next_at} does not match settled batch "
                        f"{batch_key} (expected {expected})",
                        reason="next_at_mismatch")
                view = self.group_view(g)
                view["settled"] = True
                view["idempotent"] = True
                view["next_at"] = expected
                return view
            if batch_key in g.get("settled_keys", []):
                raise Conflict(f"batch {batch_key} belongs to an older epoch",
                               reason="cross_batch")
            raise Conflict(f"unknown batch_key {batch_key}",
                           reason="unknown_batch")

    def _commit_settle_locked(self, g: dict, desc: dict) -> dict:
        name = g["name"]
        new_checkpoint = desc["next_at"]
        assert new_checkpoint >= g["checkpoint"]
        g["checkpoint"] = new_checkpoint
        g["outstanding"] = None  # holder/lease intentionally retained
        g["last_settled"] = {"batch_key": desc["batch_key"],
                             "next_at": new_checkpoint,
                             "lease_key": desc["lease_key"]}
        g["settled_keys"].append(desc["batch_key"])
        g["settled_keys"] = g["settled_keys"][-SETTLED_KEY_HISTORY:]
        g["updated_at"] = fmt_ts(utcnow())
        finished = g["end"] is not None and new_checkpoint >= g["end"]
        if finished:
            g["status"] = "finished"

        # Seam 2: checkpoint durable.  The batch journal still carries the
        # batch and the gate still fences the old checkpoint at this instant;
        # startup reconciliation retires the batch idempotently if we die now.
        self._persist_groups_locked()
        self._fire_hook(g, desc, "checkpoint_durable")

        self._batches.pop(desc["batch_key"], None)
        self._persist_batches_locked()

        # Seam 3: migrate (or withdraw) the retention watergate.
        if finished:
            self._gates.pop(name, None)
        else:
            self._set_gate_locked(name, new_checkpoint)
        self._persist_gates_locked()
        self._fire_hook(g, desc, "gate_migrated")

        view = self.group_view(g)
        view["settled"] = True
        view["idempotent"] = False
        view["next_at"] = new_checkpoint
        log.info("group %r settled batch %s: checkpoint -> %d%s",
                 name, desc["batch_key"], new_checkpoint,
                 " (finished)" if finished else "")
        return view

    # ------------------------------------------------------------------ #
    # pause / resume / delete                                             #
    # ------------------------------------------------------------------ #

    def pause(self, name: str) -> dict:
        with self.s._lock:
            g = self._require_group_locked(name)
            if g["status"] == "finished":
                raise Conflict(f"group {name!r} has finished", reason="finished")
            if g["status"] != "paused":
                g["status"] = "paused"
            g["owner"] = None
            old = g.pop("outstanding", None)
            g["updated_at"] = fmt_ts(utcnow())
            if old is not None:
                self._batches.pop(old["batch_key"], None)
            self._gates.pop(name, None)  # withdraw the watergate
            self._persist_groups_locked()
            self._persist_batches_locked()
            self._persist_gates_locked()
            return {"group": name, "paused": True,
                    "checkpoint": g["checkpoint"], "epoch": g["epoch"]}

    def resume(self, name: str) -> dict:
        with self.s._lock:
            g = self._require_group_locked(name)
            if g["status"] == "finished":
                raise Conflict(f"group {name!r} has finished", reason="finished")
            if g["status"] == "active":
                view = self.group_view(g)
                view["resumed"] = True
                view["already_active"] = True
                return view
            # Validate the checkpoint against evicted ranges BEFORE mutating
            # anything: a 410 must leave the group paused with its gate off.
            self._raise_if_evicted_locked(g["checkpoint"])
            g["status"] = "active"
            g["owner"] = None
            g["outstanding"] = None
            g["epoch"] += 1  # old leases/batches from before the pause are dead
            g["updated_at"] = fmt_ts(utcnow())
            self._set_gate_locked(name, g["checkpoint"])
            self._persist_groups_locked()
            self._persist_gates_locked()
            view = self.group_view(g)
            view["resumed"] = True
            view["already_active"] = False
            return view

    def delete_group(self, name: str) -> dict:
        with self.s._lock:
            g = self._groups.pop(name, None)
            if g is None:
                from .store import NotFound
                raise NotFound(f"group {name!r} not found")
            if g.get("outstanding"):
                self._batches.pop(g["outstanding"]["batch_key"], None)
            self._gates.pop(name, None)  # watergate withdrawn on deregistration
            self._persist_groups_locked()
            self._persist_batches_locked()
            self._persist_gates_locked()
            log.info("group %r deregistered", name)
            return {"deleted": name}

    # ------------------------------------------------------------------ #
    # GC integration (retention watergate)                                #
    # ------------------------------------------------------------------ #

    def active_gates_locked(self) -> Dict[str, int]:
        """group name -> protected boundary (only live, active groups)."""
        return {name: b for name, b in self._gates.items()
                if name in self._groups and self._is_active_locked(self._groups[name])}

    def is_protected_locked(self, meta: dict) -> Optional[str]:
        """Return the protecting group name when a segment is fenced."""
        for name, boundary in self.active_gates_locked().items():
            if meta["first_offset"] >= boundary:
                return name
        return None

    def _set_gate_locked(self, name: str, checkpoint: int) -> None:
        self._gates[name] = self._boundary_for_locked(checkpoint)

    def _boundary_for_locked(self, pos: int) -> int:
        """First protected offset: first_offset of the segment holding pos.

        Mirrors GCManager._boundary_for_locked: when pos does not lie inside
        a live segment (open tail / future / pre-history), pos itself fences
        everything at or beyond it.
        """
        for m in self.s.manifest["segments"]:
            if m["first_offset"] <= pos <= m["last_offset"]:
                return m["first_offset"]
        for t in self.s.gc.tombstones:
            if t["first_offset"] <= pos <= t["last_offset"]:
                return t["first_offset"]
        return pos

    # ------------------------------------------------------------------ #
    # lease / ownership helpers                                           #
    # ------------------------------------------------------------------ #

    def _require_group_locked(self, name: str) -> dict:
        g = self._groups.get(name)
        if g is None:
            from .store import NotFound
            raise NotFound(f"group {name!r} not found")
        return g

    def _ensure_holder_locked(self, g: dict, lease_key: Optional[str],
                              now: float) -> bool:
        """Validate/grant the holder lease.  Returns True on takeover.

        A valid owner presenting its own lease keeps it.  A foreign call while
        a valid lease exists is 409.  Once expired (or never granted), the
        claimant takes over: epoch increments and any old outstanding batch
        is superseded.
        """
        owner = g.get("owner")
        if owner is not None and owner["expires_epoch"] > now:
            if lease_key is not None and lease_key == owner["lease_key"]:
                return False
            if lease_key is not None:
                # A recognizable but foreign/stale credential (e.g. the
                # previous holder after a takeover) is an authentication
                # failure, not mere contention.
                raise Conflict(
                    "the presented lease is not the current holder's",
                    reason="bad_credential")
            raise Conflict(
                f"group {g['name']!r} already has an active holder "
                f"(epoch {g['epoch']})", reason="held")

        takeover = owner is not None  # expired owner -> new generation
        if takeover:
            g["epoch"] += 1
            old = g.pop("outstanding", None)
            if old is not None:
                self._batches.pop(old["batch_key"], None)
            log.info("group %r lease expired: epoch -> %d, old holder retired",
                     g["name"], g["epoch"])
        elif g["epoch"] == 0:
            g["epoch"] = 1

        exp = now + g["lease_seconds"]
        g["owner"] = {"lease_key": self._new_id("ltk"),
                      "expires_epoch": exp,
                      "expires_at": _iso(exp)}
        g["updated_at"] = fmt_ts(utcnow())
        self._persist_groups_locked()
        return takeover

    def _require_holder_locked(self, g: dict, lease_key: str) -> None:
        owner = g.get("owner")
        now = time.time()
        if owner is None or lease_key != owner["lease_key"]:
            raise Conflict("not the current holder of the group",
                           reason="bad_credential")
        if owner["expires_epoch"] <= now:
            raise Conflict("lease has expired", reason="lease_expired")

    @staticmethod
    def _is_active_locked(g: dict) -> bool:
        return g.get("status") == "active"

    def _horizon_locked(self, g: dict) -> int:
        head = self.s._next_offset
        if g["end"] is None:
            return head
        return min(head, g["end"])

    def _finish_locked(self, g: dict) -> None:
        g["status"] = "finished"
        g["owner"] = None
        g["updated_at"] = fmt_ts(utcnow())
        self._gates.pop(g["name"], None)
        self._persist_groups_locked()
        self._persist_gates_locked()

    # ------------------------------------------------------------------ #
    # views                                                               #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _new_id(prefix: str) -> str:
        return (f"{prefix}-{time.strftime('%Y%m%dT%H%M%S', time.gmtime())}"
                f"-{uuid.uuid4().hex[:16]}")

    def group_view(self, g: dict) -> dict:
        owner = g.get("owner")
        return {
            "name": g["name"],
            "kind": "static" if g.get("end") is not None else "dynamic",
            "start": g["start"],
            "end": g.get("end"),
            "lease_seconds": g["lease_seconds"],
            "checkpoint": g["checkpoint"],
            "epoch": g["epoch"],
            "status": g["status"],
            "outstanding": bool(g.get("outstanding")),
            "batch_key": (g.get("outstanding") or {}).get("batch_key"),
            "owner_expires_at": owner.get("expires_at") if owner else None,
            "gate": self._gates.get(g["name"]),
            "created_at": g.get("created_at"),
            "updated_at": g.get("updated_at"),
        }

    def _batch_view(self, g: dict, batch: dict, window: dict) -> dict:
        owner = g["owner"]
        return {
            "group": g["name"],
            "batch_key": batch["batch_key"],
            "lease_key": owner["lease_key"],
            "epoch": g["epoch"],
            "checkpoint": g["checkpoint"],
            "from_offset": batch["from"],
            "messages": window["events"],
            "gaps": window["gaps"],
            "next_at": batch["next_at"],
            "finished": False,
            "empty": False,
            "lease_expires_at": owner["expires_at"],
        }

    def _empty_view(self, g: dict) -> dict:
        owner = g["owner"]
        return {
            "group": g["name"],
            "batch_key": None,
            "lease_key": owner["lease_key"],
            "epoch": g["epoch"],
            "checkpoint": g["checkpoint"],
            "from_offset": g["checkpoint"],
            "messages": [],
            "gaps": [],
            "next_at": None,
            "finished": False,
            "empty": True,
            "lease_expires_at": owner["expires_at"],
        }

    def _finished_view(self, g: dict) -> dict:
        return {
            "group": g["name"],
            "batch_key": None,
            "lease_key": None,
            "epoch": g["epoch"],
            "checkpoint": g["checkpoint"],
            "from_offset": g["checkpoint"],
            "messages": [],
            "gaps": [],
            "next_at": None,
            "finished": True,
            "empty": True,
            "lease_expires_at": None,
        }

    # ------------------------------------------------------------------ #
    # journals / hooks / stats                                            #
    # ------------------------------------------------------------------ #

    def _persist_groups_locked(self) -> None:
        atomic_write_json(self._groups_path,
                          {"groups": list(self._groups.values())})

    def _persist_batches_locked(self) -> None:
        atomic_write_json(self._batches_path,
                          {"batches": list(self._batches.values())})

    def _persist_gates_locked(self) -> None:
        atomic_write_json(self._gates_path,
                          {"gates": [{"group": n, "boundary": b}
                                     for n, b in sorted(self._gates.items())]})

    def _fire_hook(self, g: dict, batch: Optional[dict], phase: str) -> None:
        hook = self._phase_hook
        if hook is None:
            return
        try:
            hook(self.group_view(g),
                 None if batch is None else dict(batch), phase)
        except Exception:
            log.exception("groups phase hook raised")

    def stats_locked(self) -> dict:
        now = time.time()
        active = held = 0
        outstanding = finished = paused = 0
        for g in self._groups.values():
            if g["status"] == "finished":
                finished += 1
                continue
            if g["status"] == "paused":
                paused += 1
                continue
            active += 1
            if g.get("outstanding"):
                outstanding += 1
            owner = g.get("owner")
            if owner and owner["expires_epoch"] > now:
                held += 1
        return {
            "groups": len(self._groups),
            "active": active,
            "held": held,
            "outstanding_batches": outstanding,
            "paused": paused,
            "finished": finished,
            "gates": len(self._gates),
        }

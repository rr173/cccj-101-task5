"""HTTP API (stdlib only).  See README.md for the full endpoint reference."""

from __future__ import annotations

import json
import logging
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

from . import gc as gcmod
from . import groups as grpmod
from . import store as storemod

log = logging.getLogger("eventarch.api")

MAX_BODY = 16 << 20  # 16 MiB


def _clamp_limit(raw, default, ceiling):
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return default
    return max(1, min(n, ceiling))


class Handler(BaseHTTPRequestHandler):
    server_version = "eventarch/0.1"
    protocol_version = "HTTP/1.1"

    # -- helpers -------------------------------------------------------- #

    @property
    def store(self) -> storemod.ArchiveStore:
        return self.server.store  # type: ignore[attr-defined]

    def log_message(self, fmt, *args):  # route through logging
        log.debug("%s - %s", self.address_string(), fmt % args)

    def _send_json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status, msg, **extra):
        self._send_json({"error": msg, **extra}, status=status)

    def _body_json(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n <= 0:
            return {}
        if n > MAX_BODY:
            raise ValueError("request body too large")
        return json.loads(self.rfile.read(n))

    def _dispatch(self, method):
        try:
            parsed = urlparse(self.path)
            parts = [p for p in parsed.path.split("/") if p]
            q = {k: v[0] for k, v in parse_qs(parsed.query).items()}
            return self._route(method, parts, q)
        except storemod.NotFound as exc:
            self._error(404, str(exc))
        except storemod.Quarantined as exc:
            self._error(410, str(exc), segment=exc.seg_id,
                        resume_offset=exc.resume_offset)
        except gcmod.Gone as exc:
            body = {"error": str(exc), "cursor": exc.cursor,
                    "first_offset": exc.first_offset,
                    "last_offset": exc.last_offset}
            if exc.seg_id is not None:
                body["segment"] = exc.seg_id
            if getattr(exc, "reason", None) is not None:
                body["reason"] = exc.reason
                if exc.reason == "start_evicted":
                    body["usable_start"] = exc.cursor
            self._send_json(body, status=410)
        except gcmod.PlanConflict as exc:
            self._error(409, str(exc), conflicts=exc.reasons)
        except grpmod.Conflict as exc:
            self._error(409, str(exc), reason=exc.reason)
        except gcmod.ReadRetry as exc:
            self._error(503, str(exc), segment=exc.seg_id, retry_after="0")
        except storemod.WalCoverageGone as exc:
            self._error(410, str(exc), segment=exc.seg_id,
                        resume_offset=exc.resume_offset)
        except storemod.RepairTimeout as exc:
            self._error(503, str(exc), job_id=exc.job_id)
        except (ValueError, json.JSONDecodeError) as exc:
            self._error(400, str(exc))
        except BrokenPipeError:
            pass
        except Exception:
            log.exception("unhandled error")
            self._error(500, "internal error")

    # -- routing -------------------------------------------------------- #

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def do_DELETE(self):
        self._dispatch("DELETE")

    def _route(self, method, parts, q):
        s = self.store

        if method == "GET" and parts == ["v1", "healthz"]:
            return self._send_json({"status": "ok"})

        if method == "GET" and parts == ["v1", "stats"]:
            return self._send_json(s.stats())

        if method == "POST" and parts == ["v1", "ingest"]:
            body = self._body_json()
            events = body.get("events") if isinstance(body, dict) else body
            if isinstance(body, dict) and "events" not in body and "event_id" in body:
                events = [body]  # single-event convenience form
            return self._send_json({"results": s.ingest(events)})

        if method == "GET" and parts == ["v1", "devices"]:
            return self._send_json({"devices": s.list_devices()})

        if method == "GET" and len(parts) == 4 and parts[:2] == ["v1", "devices"] \
                and parts[3] == "events":
            from_seq = q.get("from_seq")
            return self._send_json(s.device_events(
                parts[2],
                from_seq=int(from_seq) if from_seq is not None else None,
                from_offset=int(q.get("from_offset", 0)),
                limit=_clamp_limit(q.get("limit"), 100, 1000),
            ))

        if method == "GET" and parts == ["v1", "segments"]:
            return self._send_json(s.list_segments())

        if method == "GET" and len(parts) == 4 and parts[:2] == ["v1", "segments"] \
                and parts[3] == "events":
            return self._send_json(s.segment_events(
                parts[2],
                from_offset=int(q.get("from_offset", 0)),
                limit=_clamp_limit(q.get("limit"), 500, 5000),
            ))

        if method == "POST" and len(parts) == 4 and parts[:2] == ["v1", "segments"] \
                and parts[3] == "rebuild":
            # Maintenance is asynchronous: enqueue a background repair job
            # and return immediately so foreground traffic is never blocked.
            # Same segment while a job is active -> the existing job (200).
            job, created = s.start_repair(parts[2])
            return self._send_json({"job": job}, status=202 if created else 200)

        if method == "GET" and parts == ["v1", "repairs"]:
            limit = _clamp_limit(q.get("limit"), 100, 1000)
            return self._send_json({"jobs": s.list_repairs(limit=limit)})

        if method == "GET" and len(parts) == 3 and parts[:2] == ["v1", "repairs"]:
            return self._send_json({"job": s.get_repair(parts[2])})

        if method == "POST" and len(parts) == 3 and parts[:2] == ["v1", "repairs"]:
            body = self._body_json()
            timeout = body.get("timeout", 60.0) if isinstance(body, dict) else 60.0
            try:
                timeout = float(timeout)
            except (TypeError, ValueError):
                timeout = 60.0
            timeout = max(0.0, min(timeout, 3600.0))
            return self._send_json({"job": s.wait_repair(parts[2], timeout=timeout)})

        if method == "POST" and parts == ["v1", "freeze"]:
            body = self._body_json()
            note = body.get("note", "") if isinstance(body, dict) else ""
            return self._send_json(s.freeze(note))

        if method == "GET" and parts == ["v1", "freezes"]:
            return self._send_json({"freezes": s.list_freezes()})

        if method == "GET" and parts == ["v1", "replay"]:
            return self._send_json(s.replay(
                freeze_id=q.get("freeze_id"),
                from_offset=int(q.get("from_offset", 0)),
                device_id=q.get("device_id"),
                limit=_clamp_limit(q.get("limit"), 500, 5000),
            ))

        # -- capacity reclamation (gc) and reader protection ------------- #

        if method == "POST" and parts == ["v1", "gc", "plans"]:
            body = self._body_json()
            if not isinstance(body, dict) or "cut" not in body:
                raise ValueError("body must contain an integer 'cut'")
            return self._send_json(s.gc.create_plan(body["cut"]))

        if method == "POST" and len(parts) == 5 and parts[:3] == ["v1", "gc", "plans"] \
                and parts[4] == "apply":
            job, accepted = s.gc.apply_plan(parts[3])
            return self._send_json({"gc_job": job}, status=202 if accepted else 200)

        if method == "GET" and len(parts) == 4 and parts[:3] == ["v1", "gc", "jobs"]:
            return self._send_json({"gc_job": s.gc.get_job(parts[3])})

        if method == "GET" and parts == ["v1", "gc", "jobs"]:
            limit = _clamp_limit(q.get("limit"), 100, 1000)
            return self._send_json({"gc_jobs": s.gc.list_jobs(limit=limit)})

        if method == "GET" and parts == ["v1", "gc", "audit"]:
            limit = _clamp_limit(q.get("limit"), 100, 10000)
            return self._send_json(s.gc.list_audit(limit=limit))

        if method == "POST" and parts == ["v1", "holds"]:
            body = self._body_json()
            for key in ("hold_id", "pos", "ttl_seconds"):
                if not isinstance(body, dict) or key not in body:
                    raise ValueError(f"body must contain '{key}'")
            return self._send_json(s.gc.create_hold(
                body["hold_id"], body["pos"], body["ttl_seconds"]))

        if method == "GET" and parts == ["v1", "holds"]:
            return self._send_json({"holds": s.gc.list_holds()})

        if method == "DELETE" and len(parts) == 3 and parts[:2] == ["v1", "holds"]:
            return self._send_json(s.gc.release_hold(parts[2]))

        # -- v2: durable downstream consumer groups ---------------------- #

        if method == "POST" and parts == ["v2", "groups"]:
            body = self._body_json()
            if not isinstance(body, dict) or "name" not in body \
                    or "start" not in body:
                raise ValueError("body must contain 'name' and 'start' "
                                 "('end' and 'lease_seconds' optional)")
            if "lease_seconds" not in body:
                raise ValueError("body must contain 'lease_seconds'")
            status = None
            view = s.groups.register(
                body["name"], body["start"], body.get("end"),
                body["lease_seconds"])
            status = 200 if view.pop("redeclared", False) else 201
            return self._send_json(view, status=status)

        if method == "GET" and parts == ["v2", "groups"]:
            return self._send_json({"groups": s.groups.list_groups()})

        if method == "GET" and len(parts) == 3 and parts[:2] == ["v2", "groups"]:
            return self._send_json(s.groups.get_group(parts[2]))

        if method == "DELETE" and len(parts) == 3 and parts[:2] == ["v2", "groups"]:
            return self._send_json(s.groups.delete_group(parts[2]))

        if method == "POST" and len(parts) == 4 \
                and parts[:2] == ["v2", "groups"] and parts[3] == "claim":
            body = self._body_json()
            if not isinstance(body, dict):
                body = {}
            lease_key = body.get("lease_key")
            limit = body.get("limit")
            if limit is not None:
                limit = _clamp_limit(limit, 500, 5000)
            return self._send_json(s.groups.claim(
                parts[2], lease_key=lease_key, limit=limit))

        if method == "POST" and len(parts) == 4 \
                and parts[:2] == ["v2", "groups"] and parts[3] == "settle":
            body = self._body_json()
            for key in ("lease_key", "batch_key", "next_at"):
                if not isinstance(body, dict) or key not in body:
                    raise ValueError(f"body must contain '{key}'")
            return self._send_json(s.groups.settle(
                parts[2], body["lease_key"], body["batch_key"], body["next_at"]))

        if method == "POST" and len(parts) == 4 \
                and parts[:2] == ["v2", "groups"] and parts[3] == "renew":
            body = self._body_json()
            if not isinstance(body, dict) or "lease_key" not in body:
                raise ValueError("body must contain 'lease_key'")
            return self._send_json(s.groups.renew(
                parts[2], body["lease_key"], body.get("lease_seconds")))

        if method == "POST" and len(parts) == 4 \
                and parts[:2] == ["v2", "groups"] and parts[3] == "pause":
            return self._send_json(s.groups.pause(parts[2]))

        if method == "POST" and len(parts) == 4 \
                and parts[:2] == ["v2", "groups"] and parts[3] == "resume":
            return self._send_json(s.groups.resume(parts[2]))

        return self._error(404, "not found")

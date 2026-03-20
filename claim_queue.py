from __future__ import annotations

import secrets
import time
from typing import Any


def _now_ts() -> int:
    return int(time.time())


def _remaining_hourly_quota(conn, user_id: int, now: int, limit: int) -> int:
    cutoff = now - 3600
    row = conn.execute(
        """
        SELECT COUNT(*) as cnt
        FROM token_claims
        WHERE user_id = ? AND claimed_at_ts >= ?
        """,
        (user_id, cutoff),
    ).fetchone()
    used = int(row["cnt"]) if row else 0
    return max(0, limit - used)


def _remaining_minute_quota(conn, api_key_id: int, now: int, limit: int) -> int:
    cutoff = now - 60
    row = conn.execute(
        """
        SELECT COUNT(*) as cnt
        FROM token_claims
        WHERE api_key_id = ? AND claimed_at_ts >= ?
        """,
        (api_key_id, cutoff),
    ).fetchone()
    used = int(row["cnt"]) if row else 0
    return max(0, limit - used)


def has_pending_queue(db, *, conn=None) -> bool:
    def _check(target_conn):
        row = target_conn.execute(
            "SELECT 1 FROM claim_queue WHERE status = 'queued' LIMIT 1"
        ).fetchone()
        return row is not None

    if conn is None:
        with db.connect() as target_conn:
            return _check(target_conn)
    return _check(conn)


def enqueue_claim(
    db,
    user_id: int,
    api_key_id: int | None,
    requested: int,
    *,
    conn=None,
) -> dict[str, Any]:
    now = _now_ts()
    request_id = secrets.token_hex(8)
    requested = max(1, int(requested))

    def _enqueue(target_conn):
        existing = target_conn.execute(
            """
            SELECT id, requested, remaining, queue_rank, enqueued_at_ts, request_id
            FROM claim_queue
            WHERE user_id = ? AND status = 'queued' AND remaining > 0
            ORDER BY queue_rank ASC, id ASC
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()
        if existing:
            queue_rank = max(1, int(existing["queue_rank"]))
            return (
                int(existing["id"]),
                queue_rank,
                int(existing["requested"]),
                int(existing["remaining"]),
                int(existing["enqueued_at_ts"]),
                existing["request_id"],
                True,
            )

        runtime_row = target_conn.execute(
            "SELECT total_queued FROM queue_runtime WHERE id = 1"
        ).fetchone()
        total_queued = int(runtime_row["total_queued"]) if runtime_row else 0
        queue_rank = total_queued + 1
        cursor = target_conn.execute(
            """
            INSERT INTO claim_queue (
                user_id, api_key_id, requested, remaining, queue_rank, enqueued_at_ts, request_id, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued')
            """,
            (user_id, api_key_id, requested, requested, queue_rank, now, request_id),
        )
        target_conn.execute(
            """
            UPDATE queue_runtime
            SET total_queued = ?, updated_at_ts = ?
            WHERE id = 1
            """,
            (queue_rank, now),
        )
        return int(cursor.lastrowid), queue_rank, requested, requested, now, request_id, False

    if conn is None:
        with db._lock, db.connect() as target_conn:
            target_conn.execute("BEGIN IMMEDIATE")
            queue_id, queue_rank, requested_val, remaining_val, enqueued_ts, req_id, existing = _enqueue(target_conn)
    else:
        queue_id, queue_rank, requested_val, remaining_val, enqueued_ts, req_id, existing = _enqueue(conn)

    return {
        "queue_id": queue_id,
        "position": max(1, int(queue_rank)),
        "request_id": req_id,
        "requested": requested_val,
        "remaining": remaining_val,
        "enqueued_at_ts": enqueued_ts,
        "existing": bool(existing),
    }


def fulfill_queue(
    db,
    *,
    hourly_limit: int,
    apikey_rate_limit: int,
    max_batch: int | None = None,
) -> dict[str, Any]:
    now = _now_ts()
    fulfilled = 0
    updated = 0
    queue_removed = 0
    claim_events: dict[tuple[int, str], dict[str, Any]] = {}
    with db.connect() as conn:
        try:
            db.ensure_inventory_policy(conn=conn)
        except Exception:
            pass
        queue_rows = conn.execute(
            """
            SELECT id, user_id, api_key_id, remaining, request_id, queue_rank, requested, enqueued_at_ts
            FROM claim_queue
            WHERE status = 'queued' AND remaining > 0
            ORDER BY queue_rank ASC, id ASC
            """
        ).fetchall()

    if not queue_rows:
        return {"fulfilled": 0, "updated": 0, "queue_removed": 0, "events": []}

    for row in queue_rows:
        if max_batch is not None and fulfilled >= max_batch:
            break

        queue_id = int(row["id"])
        user_id = int(row["user_id"])
        api_key_id = row["api_key_id"]
        remaining = int(row["remaining"])
        if remaining <= 0:
            continue

        with db.connect() as conn:
            remaining_quota = _remaining_hourly_quota(conn, user_id, now, hourly_limit)
            if remaining_quota <= 0:
                continue
            allowed = min(remaining, remaining_quota)
            if api_key_id is not None and apikey_rate_limit > 0:
                remaining_minute = _remaining_minute_quota(conn, int(api_key_id), now, apikey_rate_limit)
                if remaining_minute <= 0:
                    continue
                allowed = min(allowed, remaining_minute)

        if allowed <= 0:
            continue

        granted = 0
        while granted < allowed:
            if max_batch is not None and fulfilled >= max_batch:
                break
            item = db.allocate_claimable_token(
                user_id,
                api_key_id,
                str(row["request_id"]),
                hourly_limit=hourly_limit,
                apikey_rate_limit=apikey_rate_limit,
            )
            if item is None:
                break
            fulfilled += 1
            granted += 1
            event_key = (user_id, str(row["request_id"]))
            event = claim_events.setdefault(
                event_key,
                {
                    "user_id": user_id,
                    "request_id": str(row["request_id"]),
                    "claimed_at_ts": now,
                    "queue_id": queue_id,
                    "queue_position": max(1, int(row["queue_rank"])),
                    "queue_requested": int(row["requested"]),
                    "queue_enqueued_at_ts": int(row["enqueued_at_ts"]),
                    "token_ids": [],
                    "first_claim_count": 0,
                    "granted": 0,
                },
            )
            event["token_ids"].append(int(item["token_id"]))
            event["granted"] += 1
            if bool(item.get("first_claim")):
                event["first_claim_count"] += 1

        if granted <= 0:
            continue
        queue_state = db.consume_queue_grant(queue_id, granted)
        event["queue_removed"] = bool(queue_state.get("removed"))
        event["queue_remaining"] = queue_state.get("remaining")
        if bool(queue_state.get("removed")):
            queue_removed += 1
        updated += 1

    return {
        "fulfilled": fulfilled,
        "updated": updated,
        "queue_removed": queue_removed,
        "events": list(claim_events.values()),
    }

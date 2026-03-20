from __future__ import annotations

import secrets
import sqlite3
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
            SELECT id, requested, remaining, enqueued_at_ts, request_id
            FROM claim_queue
            WHERE user_id = ? AND status = 'queued' AND remaining > 0
            ORDER BY enqueued_at_ts ASC, id ASC
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()
        if existing:
            queue_id = int(existing["id"])
            row = target_conn.execute(
                """
                SELECT COUNT(*) as cnt
                FROM claim_queue
                WHERE status = 'queued'
                  AND (enqueued_at_ts < ? OR (enqueued_at_ts = ? AND id < ?))
                """,
                (int(existing["enqueued_at_ts"]), int(existing["enqueued_at_ts"]), queue_id),
            ).fetchone()
            ahead = int(row["cnt"]) if row else 0
            return (
                queue_id,
                ahead,
                int(existing["requested"]),
                int(existing["remaining"]),
                int(existing["enqueued_at_ts"]),
                existing["request_id"],
                True,
            )

        cursor = target_conn.execute(
            """
            INSERT INTO claim_queue (
                user_id, api_key_id, requested, remaining, enqueued_at_ts, request_id, status
            ) VALUES (?, ?, ?, ?, ?, ?, 'queued')
            """,
            (user_id, api_key_id, requested, requested, now, request_id),
        )
        queue_id = int(cursor.lastrowid)
        row = target_conn.execute(
            """
            SELECT COUNT(*) as cnt
            FROM claim_queue
            WHERE status = 'queued'
              AND (enqueued_at_ts < ? OR (enqueued_at_ts = ? AND id < ?))
            """,
            (now, now, queue_id),
        ).fetchone()
        ahead = int(row["cnt"]) if row else 0
        return queue_id, ahead, requested, requested, now, request_id, False

    if conn is None:
        with db._lock, db.connect() as target_conn:
            target_conn.execute("BEGIN IMMEDIATE")
            queue_id, ahead, requested_val, remaining_val, enqueued_ts, req_id, existing = _enqueue(target_conn)
    else:
        queue_id, ahead, requested_val, remaining_val, enqueued_ts, req_id, existing = _enqueue(conn)

    return {
        "queue_id": queue_id,
        "position": ahead + 1,
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

    with db._lock, db.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            db.ensure_inventory_policy(conn=conn)
        except Exception:
            pass

        queue_rows = conn.execute(
            """
            SELECT id, user_id, api_key_id, remaining, request_id
            FROM claim_queue
            WHERE status = 'queued' AND remaining > 0
            ORDER BY enqueued_at_ts ASC, id ASC
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

            remaining_quota = _remaining_hourly_quota(conn, user_id, now, hourly_limit)
            if remaining_quota <= 0:
                continue

            allowed = min(remaining, remaining_quota)
            if api_key_id is not None and apikey_rate_limit > 0:
                remaining_minute = _remaining_minute_quota(
                    conn, int(api_key_id), now, apikey_rate_limit
                )
                if remaining_minute <= 0:
                    continue
                allowed = min(allowed, remaining_minute)

            if allowed <= 0:
                continue

            token_rows = db.list_claimable_tokens_for_user(conn, user_id, allowed)

            if not token_rows:
                continue

            granted = 0
            for token in token_rows:
                token_id = int(token["id"])
                first_claim = int(token["claim_count"]) <= 0
                new_count = int(token["claim_count"]) + 1
                new_available = 1 if new_count < int(token["max_claims"]) else 0
                savepoint = f"queue_claim_{queue_id}_{token_id}"
                try:
                    conn.execute(f"SAVEPOINT {savepoint}")
                    conn.execute(
                        """
                        UPDATE tokens
                        SET claim_count = ?,
                            is_available = ?,
                            updated_at_ts = ?
                        WHERE id = ?
                        """,
                        (new_count, new_available, now, token_id),
                    )
                    conn.execute(
                        """
                        INSERT INTO token_claims (
                            token_id, user_id, api_key_id, claimed_at_ts, is_hidden, request_id
                        ) VALUES (?, ?, ?, ?, 0, ?)
                        """,
                        (token_id, user_id, api_key_id, now, row["request_id"]),
                    )
                    claim_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                    conn.execute(
                        """
                        INSERT INTO user_token_claims (user_id, token_id, first_claim_id, created_at_ts)
                        VALUES (?, ?, ?, ?)
                        """,
                        (user_id, token_id, claim_id, now),
                    )
                    conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                except sqlite3.IntegrityError:
                    conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                    conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                    continue
                fulfilled += 1
                granted += 1
                event_key = (user_id, str(row["request_id"]))
                event = claim_events.setdefault(
                    event_key,
                    {
                        "user_id": user_id,
                        "request_id": str(row["request_id"]),
                        "claimed_at_ts": now,
                        "token_ids": [],
                        "first_claim_count": 0,
                        "granted": 0,
                    },
                )
                event["token_ids"].append(token_id)
                event["granted"] += 1
                if first_claim:
                    event["first_claim_count"] += 1

            remaining_after = remaining - granted
            if remaining_after <= 0:
                conn.execute(
                    "DELETE FROM claim_queue WHERE id = ?",
                    (queue_id,),
                )
                queue_removed += 1
            else:
                conn.execute(
                    """
                    UPDATE claim_queue
                    SET remaining = ?, status = 'queued'
                    WHERE id = ?
                    """,
                    (max(0, remaining_after), queue_id),
                )
            updated += 1

    return {
        "fulfilled": fulfilled,
        "updated": updated,
        "queue_removed": queue_removed,
        "events": list(claim_events.values()),
    }

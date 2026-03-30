"""
Worker process for the SQLite lockstep simulation.

Usage:
    python worker.py <worker_id>

The worker:
  1. Validates its ID exists in the registry.
  2. Sets status=READY and waits for the simulation to start.
  3. Runs a catch-up loop if the sim clock has advanced past its last event.
  4. Runs the normal task loop until sim reaches DONE or its next event
     is past SIM_END.
"""

import random
import sqlite3
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

from config import DB_PATH, POLL_INTERVAL_SECONDS, SIM_END


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def db_execute_with_retry(conn, sql, params=(), max_retries=5):
    """Retry on SQLite lock errors with exponential backoff."""
    for attempt in range(max_retries):
        try:
            return conn.execute(sql, params)
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < max_retries - 1:
                time.sleep(0.05 * (2 ** attempt))
            else:
                raise


# ---------------------------------------------------------------------------
# Business logic — PRIMARY CUSTOMIZATION POINT
# ---------------------------------------------------------------------------
#
# This is where real business logic goes for each worker.
#
# Rules:
#   - Must return a POSITIVE INTEGER: sim hours until the next task.
#   - The `is_catchup` flag MUST guard any side effects (API calls,
#     external writes, emails, etc.). During catch-up, only compute
#     and return — do NOT perform real-world actions.
#   - Different workers can branch on `worker_id` for completely different
#     schedules, data sources, or business rules.
#   - The function may query the DB, read files, or call APIs as needed,
#     provided side effects are skipped when is_catchup=True.
#
def get_next_event_hours(worker_id: str, sim_time: datetime,
                         is_catchup: bool = False) -> int:
    """
    Return the number of simulation hours until this worker's next task.

    Replace or extend this implementation with real business logic.
    Use is_catchup=True to skip any external side effects.
    """
    interval = random.choice([1, 2, 4, 8, 12, 24, 48])

    if is_catchup:
        return interval

    # Simulate variable real-world work duration
    real_sleep = random.uniform(1, 3)
    time.sleep(real_sleep)
    print(f"[{worker_id}] Task at {sim_time.isoformat()}: "
          f"will next act in {interval} sim hours")
    return interval


# ---------------------------------------------------------------------------
# Worker startup
# ---------------------------------------------------------------------------

def validate_worker(conn, worker_id: str):
    row = conn.execute(
        "SELECT worker_id FROM worker_registry WHERE worker_id = ?",
        (worker_id,),
    ).fetchone()
    if row is None:
        print(f"ERROR: worker_id '{worker_id}' not found in worker_registry.")
        print("Run db_setup.py first, and make sure the ID is listed in config.WORKERS.")
        sys.exit(1)


def set_status(conn, worker_id: str, status: str, extra_fields: dict | None = None):
    """Atomically update worker status plus any extra fields."""
    now_utc = _now_utc()
    fields = {"status": status, "last_seen": now_utc}
    if extra_fields:
        fields.update(extra_fields)

    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [worker_id]
    with conn:
        db_execute_with_retry(
            conn,
            f"UPDATE worker_registry SET {set_clause} WHERE worker_id = ?",
            values,
        )


def wait_for_running(conn, worker_id: str) -> tuple[str, datetime]:
    """
    Poll until sim_clock.status is RUNNING (or DONE).
    Returns (clock_status, sim_time).
    """
    while True:
        row = conn.execute("SELECT status, sim_time FROM sim_clock").fetchone()
        clock_status = row["status"]
        sim_time = datetime.fromisoformat(row["sim_time"])

        if clock_status in ("RUNNING", "DONE"):
            return clock_status, sim_time

        # Refresh last_seen while waiting
        with conn:
            db_execute_with_retry(
                conn,
                "UPDATE worker_registry SET last_seen = ? WHERE worker_id = ?",
                (_now_utc(), worker_id),
            )
        time.sleep(POLL_INTERVAL_SECONDS)


# ---------------------------------------------------------------------------
# Catch-up logic
# ---------------------------------------------------------------------------

def run_catchup(conn, worker_id: str, sim_time: datetime):
    """Replay all missed events from next_event_time up to sim_time."""
    row = conn.execute(
        "SELECT next_event_time, missed_events FROM worker_registry WHERE worker_id = ?",
        (worker_id,),
    ).fetchone()

    next_evt = datetime.fromisoformat(row["next_event_time"])
    total_missed = row["missed_events"]

    if next_evt >= sim_time:
        return  # Nothing to catch up

    set_status(conn, worker_id, "CATCHUP")
    print(
        f"[{worker_id}] CATCHUP: sim is at {sim_time.isoformat()}, "
        f"my last event was {row['next_event_time']}. Replaying missed events..."
    )

    replayed = 0
    while next_evt <= sim_time:
        hours = get_next_event_hours(worker_id, next_evt, is_catchup=True)
        total_missed += 1
        replayed += 1
        new_next = next_evt + timedelta(hours=hours)
        print(
            f"[{worker_id}] CATCHUP: replayed event at {next_evt.isoformat()}, "
            f"next at {new_next.isoformat()}"
        )
        next_evt = new_next

    # Persist final position
    with conn:
        db_execute_with_retry(
            conn,
            "UPDATE worker_registry SET status = 'WAITING', "
            "next_event_time = ?, missed_events = ?, last_seen = ? "
            "WHERE worker_id = ?",
            (next_evt.isoformat(), total_missed, _now_utc(), worker_id),
        )

    print(
        f"[{worker_id}] CATCHUP complete. Replayed {replayed} missed events. "
        f"Resuming normal operation at {next_evt.isoformat()}"
    )


# ---------------------------------------------------------------------------
# Normal task loop
# ---------------------------------------------------------------------------

def run_task_loop(conn, worker_id: str):
    while True:
        clock_row = conn.execute("SELECT status, sim_time FROM sim_clock").fetchone()
        clock_status = clock_row["status"]
        sim_time = datetime.fromisoformat(clock_row["sim_time"])

        if clock_status == "DONE":
            row = conn.execute(
                "SELECT status, last_ack_time, next_event_time, missed_events "
                "FROM worker_registry WHERE worker_id = ?",
                (worker_id,),
            ).fetchone()
            print(
                f"[{worker_id}] Simulation DONE. "
                f"last_ack={row['last_ack_time']}, "
                f"next_event={row['next_event_time']}, "
                f"missed_events={row['missed_events']}"
            )
            return

        my_row = conn.execute(
            "SELECT status, next_event_time FROM worker_registry WHERE worker_id = ?",
            (worker_id,),
        ).fetchone()

        # Refresh last_seen
        with conn:
            db_execute_with_retry(
                conn,
                "UPDATE worker_registry SET last_seen = ? WHERE worker_id = ?",
                (_now_utc(), worker_id),
            )

        my_status = my_row["status"]
        next_evt = datetime.fromisoformat(my_row["next_event_time"])

        if sim_time >= next_evt and my_status == "WAITING":
            # Set WORKING, execute task, then atomically write ACK + next_event_time
            with conn:
                db_execute_with_retry(
                    conn,
                    "UPDATE worker_registry SET status = 'WORKING', last_seen = ? "
                    "WHERE worker_id = ?",
                    (_now_utc(), worker_id),
                )

            hours = get_next_event_hours(worker_id, sim_time, is_catchup=False)
            new_next = sim_time + timedelta(hours=hours)

            if new_next > SIM_END:
                with conn:
                    db_execute_with_retry(
                        conn,
                        "UPDATE worker_registry SET status = 'DONE', "
                        "next_event_time = ?, last_ack_time = ?, last_seen = ? "
                        "WHERE worker_id = ?",
                        (new_next.isoformat(), sim_time.isoformat(),
                         _now_utc(), worker_id),
                    )
                print(f"[{worker_id}] No more events before SIM_END. Done.")
                return

            with conn:
                db_execute_with_retry(
                    conn,
                    "UPDATE worker_registry SET status = 'ACK', "
                    "next_event_time = ?, last_ack_time = ?, last_seen = ? "
                    "WHERE worker_id = ?",
                    (new_next.isoformat(), sim_time.isoformat(),
                     _now_utc(), worker_id),
                )

            print(
                f"[{worker_id}] ACK at sim_time {sim_time.isoformat()}, "
                f"next task scheduled at {new_next.isoformat()} "
                f"({hours} sim hours from now)"
            )
        else:
            time.sleep(POLL_INTERVAL_SECONDS)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(worker_id: str):
    conn = open_db()
    try:
        validate_worker(conn, worker_id)

        clock_row = conn.execute("SELECT status, sim_time FROM sim_clock").fetchone()
        print(f"[{worker_id}] Online. Waiting for simulation to start...")

        set_status(conn, worker_id, "READY")

        # Wait for RUNNING (handles case where sim is already RUNNING on restart)
        clock_status, sim_time = wait_for_running(conn, worker_id)

        if clock_status == "DONE":
            print(f"[{worker_id}] Simulation is already DONE. Nothing to do.")
            return

        # Catch-up check — always end in WAITING so the task loop can run
        run_catchup(conn, worker_id, sim_time)
        # run_catchup sets WAITING when it actually replays events; if no
        # catch-up was needed, the worker is still READY — set WAITING now.
        my_status = conn.execute(
            "SELECT status FROM worker_registry WHERE worker_id = ?",
            (worker_id,),
        ).fetchone()["status"]
        if my_status == "READY":
            set_status(conn, worker_id, "WAITING")

        # Normal loop wrapped in error handler
        try:
            run_task_loop(conn, worker_id)
        except Exception:
            traceback.print_exc()
            try:
                set_status(conn, worker_id, "ERROR")
            except Exception:
                pass
            sys.exit(1)

    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python worker.py <worker_id>")
        sys.exit(1)
    main(sys.argv[1])

"""
Worker process for the SQLite lockstep simulation.

Usage:
    python worker.py <worker_id>
"""

import random
import sqlite3
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone

from config import DB_PATH, POLL_INTERVAL_SECONDS, SIM_END, WORKERS


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

def get_connection():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def db_execute(conn, sql, params=()):
    for attempt in range(5):
        try:
            return conn.execute(sql, params)
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < 4:
                time.sleep(0.05 * (2 ** attempt))
            else:
                raise


# ---------------------------------------------------------------------------
# Fail fast helper
# ---------------------------------------------------------------------------

def fatal(worker_id, message):
    print(f"[{worker_id}] FATAL: {message}")
    print(f"[{worker_id}] Run db_setup.py to reset and start over.")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Business logic — PRIMARY CUSTOMIZATION POINT
# ---------------------------------------------------------------------------
#
# This is the ONLY function that needs to change for real business logic.
# No catchup guard needed — this is only ever called for live tasks.
# Branch on worker_id for per-worker behavior.
# Can open its own DB connection to read simulation state if needed.
#
def get_next_event_hours(worker_id: str, sim_time: datetime) -> int:
    """
    Return the number of simulation hours until this worker's next task.
    Replace or extend this implementation with real business logic.
    """
    hours = random.choice([1, 2, 4, 8, 12, 24, 48])
    time.sleep(random.uniform(1, 3))
    print(f"[{worker_id}] Task at {sim_time}: next in {hours} sim hours")
    return hours


# ---------------------------------------------------------------------------
# Task loop
# ---------------------------------------------------------------------------

def _run_task_loop(conn, worker_id: str):
    while True:
        # 1. Read sim_clock
        clock_row = conn.execute("SELECT status, sim_time FROM sim_clock").fetchone()
        if clock_row["status"] == "DONE":
            my_row = conn.execute(
                "SELECT last_ack_time, next_event_time FROM worker_registry "
                "WHERE worker_id = ?",
                (worker_id,),
            ).fetchone()
            print(
                f"[{worker_id}] Simulation DONE. "
                f"last_ack={my_row['last_ack_time']}, "
                f"next_event={my_row['next_event_time']}"
            )
            return

        sim_time = datetime.fromisoformat(clock_row["sim_time"])

        # 2. Read own row
        my_row = conn.execute(
            "SELECT status, next_event_time FROM worker_registry WHERE worker_id = ?",
            (worker_id,),
        ).fetchone()

        # 3. Update last_seen on every poll
        with conn:
            db_execute(
                conn,
                "UPDATE worker_registry SET last_seen = ? WHERE worker_id = ?",
                (_now_utc(), worker_id),
            )

        my_status = my_row["status"]
        next_evt = datetime.fromisoformat(my_row["next_event_time"])

        # 4. Execute task if due and WAITING
        if sim_time >= next_evt and my_status == "WAITING":
            _execute_task(conn, worker_id, sim_time)
        else:
            time.sleep(POLL_INTERVAL_SECONDS)


def _execute_task(conn, worker_id: str, sim_time: datetime):
    # 1. Set WORKING
    with conn:
        db_execute(
            conn,
            "UPDATE worker_registry SET status = 'WORKING', last_seen = ? "
            "WHERE worker_id = ?",
            (_now_utc(), worker_id),
        )

    # 2. Compute next event
    hours = get_next_event_hours(worker_id, sim_time)
    new_next = sim_time + timedelta(hours=hours)

    # 3. If new_next > SIM_END: mark DONE and wait for sim to finish
    if new_next > SIM_END:
        with conn:
            db_execute(
                conn,
                "UPDATE worker_registry "
                "SET status = 'DONE', last_ack_time = ?, next_event_time = ? "
                "WHERE worker_id = ?",
                (sim_time.isoformat(), new_next.isoformat(), worker_id),
            )
        print(
            f"[{worker_id}] Final task at {sim_time.isoformat()}. "
            f"No more events before SIM_END. Waiting for DONE signal."
        )
        while True:
            row = conn.execute("SELECT status FROM sim_clock").fetchone()
            if row["status"] == "DONE":
                return
            time.sleep(POLL_INTERVAL_SECONDS)

    # 4. ACK — write next_event_time and status atomically
    with conn:
        db_execute(
            conn,
            "UPDATE worker_registry "
            "SET status = 'ACK', last_ack_time = ?, next_event_time = ? "
            "WHERE worker_id = ?",
            (sim_time.isoformat(), new_next.isoformat(), worker_id),
        )

    print(
        f"[{worker_id}] ACK at {sim_time.isoformat()}, "
        f"next task at {new_next.isoformat()} ({hours} sim hours from now)"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(worker_id: str):
    # 1. Validate worker_id is in config.WORKERS
    if worker_id not in WORKERS:
        print(f"ERROR: '{worker_id}' is not a registered worker. Valid IDs: {WORKERS}")
        sys.exit(1)

    # 2. Open DB connection
    conn = get_connection()

    try:
        # 3. Verify sim_clock status is INIT or WAITING_FOR_WORKERS
        row = conn.execute("SELECT status FROM sim_clock").fetchone()
        status = row["status"]
        if status in ("RUNNING", "DONE"):
            fatal(
                worker_id,
                "Simulation already started. All workers must be present before "
                "simulation begins. Run db_setup.py and restart.",
            )

        # 4. Check own row exists
        row = conn.execute(
            "SELECT worker_id FROM worker_registry WHERE worker_id = ?",
            (worker_id,),
        ).fetchone()
        if row is None:
            fatal(worker_id, "Worker not found in registry. Run db_setup.py to initialize.")

        # 5. Register
        with conn:
            db_execute(
                conn,
                "UPDATE worker_registry SET status = 'READY', last_seen = ? "
                "WHERE worker_id = ?",
                (_now_utc(), worker_id),
            )

        # 6. Print registered
        print(f"[{worker_id}] Registered. Waiting for all workers and simulation start...")

        # 7. Poll until sim_clock status == RUNNING
        while True:
            row = conn.execute("SELECT status FROM sim_clock").fetchone()
            status = row["status"]
            if status == "RUNNING":
                break
            if status == "DONE":
                fatal(worker_id, "Simulation ended before this worker could start.")
            time.sleep(POLL_INTERVAL_SECONDS)

        # 8. Print and transition to WAITING
        print(f"[{worker_id}] Simulation is running. Entering task loop.")
        with conn:
            db_execute(
                conn,
                "UPDATE worker_registry SET status = 'WAITING', last_seen = ? "
                "WHERE worker_id = ?",
                (_now_utc(), worker_id),
            )

        # Main task loop — wrapped in try/except
        try:
            _run_task_loop(conn, worker_id)
        except Exception:
            traceback.print_exc()
            try:
                with conn:
                    db_execute(
                        conn,
                        "UPDATE worker_registry SET status = 'ERROR' WHERE worker_id = ?",
                        (worker_id,),
                    )
            except Exception:
                pass
            print(f"[{worker_id}] Worker failed. Run db_setup.py to reset.")
            sys.exit(1)

    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python worker.py <worker_id>")
        sys.exit(1)
    main(sys.argv[1])

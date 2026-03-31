import sqlite3
import sys
import time
from datetime import datetime

from config import (
    DB_PATH,
    POLL_INTERVAL_SECONDS,
    READY_TIMEOUT_SECONDS,
    SIM_END,
    SIM_START,
    WORKER_TIMEOUT_SECONDS,
    WORKERS,
)


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

def fatal(message):
    print(f"[CONTROLLER] FATAL: {message}")
    print("[CONTROLLER] Run db_setup.py to reset and start over.")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Ready gate
# ---------------------------------------------------------------------------

def run_ready_gate(conn):
    # 1. Verify INIT
    row = conn.execute("SELECT status FROM sim_clock").fetchone()
    if row["status"] != "INIT":
        fatal("Simulation already started or not reset. Run db_setup.py before starting.")

    # 2. Set WAITING_FOR_WORKERS
    with conn:
        db_execute(conn, "UPDATE sim_clock SET status = 'WAITING_FOR_WORKERS'")

    # 3. Print
    print(f"[CONTROLLER] Waiting for workers. Timeout: {READY_TIMEOUT_SECONDS} seconds.")

    # 4. Poll loop
    elapsed = 0
    while elapsed < READY_TIMEOUT_SECONDS:
        rows = conn.execute(
            "SELECT worker_id, status FROM worker_registry WHERE status != 'READY'"
        ).fetchall()
        missing = [r["worker_id"] for r in rows]
        if not missing:
            break
        if elapsed % 5 == 0:
            print(f"[CONTROLLER] Waiting for: {missing}")
        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS

    # 5. After loop — check for missing workers
    rows = conn.execute(
        "SELECT worker_id FROM worker_registry WHERE status != 'READY'"
    ).fetchall()
    missing = [r["worker_id"] for r in rows]

    if len(missing) == len(WORKERS):
        fatal("No workers checked in.")
    if missing:
        fatal(
            f"Workers never checked in: {missing} "
            f"All workers must be present before simulation can start. "
            f"Run db_setup.py and restart everything."
        )

    # 6. Set RUNNING
    with conn:
        db_execute(conn, "UPDATE sim_clock SET status = 'RUNNING'")

    # 7. Print
    print(f"[CONTROLLER] All workers ready. Simulation starting at {SIM_START.isoformat()}.")


# ---------------------------------------------------------------------------
# Main simulation loop
# ---------------------------------------------------------------------------

def _print_final_summary(conn):
    print("\n[CONTROLLER] === Final Worker Summary ===")
    rows = conn.execute(
        "SELECT worker_id, last_ack_time FROM worker_registry ORDER BY worker_id"
    ).fetchall()
    for r in rows:
        print(f"  {r['worker_id']}: last_ack={r['last_ack_time']}")


def run_main_loop(conn):
    last_advance_time = time.time()

    while True:
        # 1. Read sim_clock
        clock_row = conn.execute("SELECT sim_time, status FROM sim_clock").fetchone()
        if clock_row["status"] == "DONE":
            _print_final_summary(conn)
            print("[CONTROLLER] Simulation complete.")
            return

        sim_time = datetime.fromisoformat(clock_row["sim_time"])

        # 2. Identify due workers
        due_rows = conn.execute(
            "SELECT worker_id, status FROM worker_registry "
            "WHERE next_event_time <= ? "
            "AND status NOT IN ('DONE', 'ERROR', 'OFFLINE')",
            (sim_time.isoformat(),),
        ).fetchall()
        due_ids = [r["worker_id"] for r in due_rows]

        # 3. Check for ERROR workers
        error_rows = conn.execute(
            "SELECT worker_id FROM worker_registry WHERE status = 'ERROR'"
        ).fetchall()
        if error_rows:
            fatal(f"Worker {error_rows[0]['worker_id']} is in ERROR state.")

        # 4. Wait for all due workers to ACK
        last_warn_time = last_advance_time
        while due_ids:
            pending_rows = conn.execute(
                "SELECT worker_id, status FROM worker_registry "
                "WHERE worker_id IN ({}) AND status NOT IN ('ACK', 'DONE')".format(
                    ",".join("?" * len(due_ids))
                ),
                due_ids,
            ).fetchall()

            if not pending_rows:
                break

            # Check for ERROR workers on each poll
            error_rows = conn.execute(
                "SELECT worker_id FROM worker_registry WHERE status = 'ERROR'"
            ).fetchall()
            if error_rows:
                fatal(f"Worker {error_rows[0]['worker_id']} is in ERROR state.")

            # Stall warnings every WORKER_TIMEOUT_SECONDS
            now = time.time()
            if (now - last_advance_time > WORKER_TIMEOUT_SECONDS
                    and now - last_warn_time > WORKER_TIMEOUT_SECONDS):
                for row in pending_rows:
                    print(
                        f"[CONTROLLER] WARNING: {row['worker_id']} stalled "
                        f"(status={row['status']})"
                    )
                last_warn_time = now

            time.sleep(POLL_INTERVAL_SECONDS)

        # 5a. Reset ACK'd workers to WAITING
        with conn:
            db_execute(
                conn,
                "UPDATE worker_registry SET status = 'WAITING' WHERE status = 'ACK'",
            )

        # 5b. Find minimum next_event_time across all active workers
        row = conn.execute(
            "SELECT MIN(next_event_time) AS min_next FROM worker_registry "
            "WHERE status NOT IN ('DONE', 'ERROR', 'OFFLINE')"
        ).fetchone()
        min_next_str = row["min_next"]

        # 5c. If no result or minimum > SIM_END: done
        if min_next_str is None:
            with conn:
                db_execute(conn, "UPDATE sim_clock SET status = 'DONE'")
            _print_final_summary(conn)
            print("[CONTROLLER] Simulation complete — no active workers remain.")
            return

        min_next = datetime.fromisoformat(min_next_str)
        if min_next > SIM_END:
            with conn:
                db_execute(conn, "UPDATE sim_clock SET status = 'DONE'")
            _print_final_summary(conn)
            print("[CONTROLLER] Simulation complete — all events past SIM_END.")
            return

        # 5d. Advance clock
        with conn:
            db_execute(
                conn,
                "UPDATE sim_clock SET sim_time = ?",
                (min_next.isoformat(),),
            )

        # 5e. Print
        print(f"[CONTROLLER] Sim time -> {min_next.isoformat()}")
        last_advance_time = time.time()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    conn = get_connection()
    try:
        run_ready_gate(conn)
        run_main_loop(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()


# ---------------------------------------------------------------------------
# Validation trace — DB state at each step
# ---------------------------------------------------------------------------
#
# Step 0 — db_setup.py run:
#   sim_clock:
#     sim_time = "2026-01-01T00:00:00"
#     status   = "INIT"
#   worker_registry:
#     worker_a | OFFLINE | next_event_time="2026-01-01T00:00:00" | last_ack_time=NULL
#     worker_b | OFFLINE | next_event_time="2026-01-01T00:00:00" | last_ack_time=NULL
#     worker_c | OFFLINE | next_event_time="2026-01-01T00:00:00" | last_ack_time=NULL
#
# Step 1 — Controller starts ready gate:
#   sim_clock status -> "WAITING_FOR_WORKERS"
#   worker_a starts: UPDATE worker_registry SET status='READY' WHERE worker_id='worker_a'
#   worker_b starts: UPDATE worker_registry SET status='READY' WHERE worker_id='worker_b'
#   worker_c starts: UPDATE worker_registry SET status='READY' WHERE worker_id='worker_c'
#   SELECT worker_id FROM worker_registry WHERE status != 'READY' -> (empty)
#   sim_clock status -> "RUNNING"
#   worker_registry:
#     worker_a | READY | next_event_time="2026-01-01T00:00:00"
#     worker_b | READY | next_event_time="2026-01-01T00:00:00"
#     worker_c | READY | next_event_time="2026-01-01T00:00:00"
#
# Step 2 — First cycle (sim_time = "2026-01-01T00:00:00"):
#   Due workers:
#     SELECT worker_id FROM worker_registry
#     WHERE next_event_time <= "2026-01-01T00:00:00"
#     AND status NOT IN ('DONE','ERROR','OFFLINE')
#     -> [worker_a, worker_b, worker_c]
#
#   worker_a: status -> WORKING
#             get_next_event_hours -> 8
#             next_event_time -> "2026-01-01T08:00:00"
#             status -> ACK, last_ack_time -> "2026-01-01T00:00:00"
#
#   worker_b: status -> WORKING
#             get_next_event_hours -> 24
#             next_event_time -> "2026-01-02T00:00:00"
#             status -> ACK, last_ack_time -> "2026-01-01T00:00:00"
#
#   worker_c: status -> WORKING
#             get_next_event_hours -> 12
#             next_event_time -> "2026-01-01T12:00:00"
#             status -> ACK, last_ack_time -> "2026-01-01T00:00:00"
#
#   All status == ACK. Controller resets:
#     UPDATE worker_registry SET status='WAITING' WHERE status='ACK'
#
#   SELECT MIN(next_event_time) FROM worker_registry
#   WHERE status NOT IN ('DONE','ERROR','OFFLINE')
#   -> "2026-01-01T08:00:00"  (worker_a)
#
#   UPDATE sim_clock SET sim_time = "2026-01-01T08:00:00"
#
#   worker_registry after reset:
#     worker_a | WAITING | next_event_time="2026-01-01T08:00:00"
#     worker_b | WAITING | next_event_time="2026-01-02T00:00:00"
#     worker_c | WAITING | next_event_time="2026-01-01T12:00:00"
#
# Step 3 — Second cycle (sim_time = "2026-01-01T08:00:00"):
#   Due workers:
#     SELECT ... WHERE next_event_time <= "2026-01-01T08:00:00"
#     -> [worker_a] only
#
#   worker_b not due (next = Jan 2), worker_c not due (next = 12:00)
#
#   worker_a: status -> WORKING
#             get_next_event_hours -> 4
#             next_event_time -> "2026-01-01T12:00:00"
#             status -> ACK
#
#   Controller resets worker_a to WAITING.
#
#   SELECT MIN(next_event_time) FROM worker_registry
#   WHERE status NOT IN ('DONE','ERROR','OFFLINE')
#   -> "2026-01-01T12:00:00"  (tie: worker_a and worker_c both at 12:00)
#
#   UPDATE sim_clock SET sim_time = "2026-01-01T12:00:00"
#
#   Due workers at 12:00:
#     -> [worker_a, worker_c]  (both have next_event_time = "2026-01-01T12:00:00")
#
#   Both execute tasks, both ACK, controller resets both.
#   Next minimum next_event_time determines next clock advance.

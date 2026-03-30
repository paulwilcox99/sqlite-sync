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
# Ready gate
# ---------------------------------------------------------------------------

def run_ready_gate(conn) -> list[str]:
    """
    Wait for all configured workers to reach READY status.
    Returns the list of active worker IDs that successfully checked in.
    """
    with conn:
        db_execute_with_retry(
            conn,
            "UPDATE sim_clock SET status = 'WAITING_FOR_WORKERS'",
        )

    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    last_warn = time.monotonic()
    known_statuses: dict[str, str] = {w: "OFFLINE" for w in WORKERS}

    while True:
        rows = conn.execute(
            "SELECT worker_id, status FROM worker_registry WHERE worker_id IN ({})".format(
                ",".join("?" * len(WORKERS))
            ),
            WORKERS,
        ).fetchall()

        for row in rows:
            known_statuses[row["worker_id"]] = row["status"]

        not_ready = [wid for wid, st in known_statuses.items() if st != "READY"]

        if not not_ready:
            break

        now = time.monotonic()
        if now >= deadline:
            absent = [wid for wid in not_ready]
            print(f"[CONTROLLER] WARNING: workers never checked in: {absent}")
            print("[CONTROLLER] Marking absent workers OFFLINE and proceeding.")
            with conn:
                for wid in absent:
                    db_execute_with_retry(
                        conn,
                        "UPDATE worker_registry SET status = 'OFFLINE' WHERE worker_id = ?",
                        (wid,),
                    )
            break

        if now - last_warn >= 5:
            print(f"[CONTROLLER] Waiting for workers: {not_ready}")
            last_warn = now

        time.sleep(POLL_INTERVAL_SECONDS)

    active = conn.execute(
        "SELECT worker_id FROM worker_registry WHERE status = 'READY'"
    ).fetchall()
    active_ids = [r["worker_id"] for r in active]

    if not active_ids:
        print("[CONTROLLER] ERROR: No workers checked in. Exiting.")
        sys.exit(1)

    with conn:
        db_execute_with_retry(conn, "UPDATE sim_clock SET status = 'RUNNING'")

    print(f"[CONTROLLER] All workers ready. Simulation starting at {SIM_START.isoformat()}")
    return active_ids


# ---------------------------------------------------------------------------
# Main loop helpers
# ---------------------------------------------------------------------------

def get_sim_time(conn) -> datetime:
    row = conn.execute("SELECT sim_time FROM sim_clock").fetchone()
    return datetime.fromisoformat(row["sim_time"])


def get_active_workers(conn) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM worker_registry "
        "WHERE status NOT IN ('OFFLINE', 'DONE', 'ERROR')"
    ).fetchall()


def get_due_workers(conn, sim_time: datetime) -> list[sqlite3.Row]:
    """Active workers whose next_event_time <= sim_time and not in CATCHUP."""
    return conn.execute(
        "SELECT * FROM worker_registry "
        "WHERE status NOT IN ('OFFLINE', 'DONE', 'ERROR', 'CATCHUP') "
        "  AND next_event_time <= ?",
        (sim_time.isoformat(),),
    ).fetchall()


# ---------------------------------------------------------------------------
# Main simulation loop
# ---------------------------------------------------------------------------

def run_main_loop(conn):
    # Track previous statuses for re-registration detection.
    # A worker is "rejoined" when it transitions from an inactive state
    # (OFFLINE/ERROR) to any active state. We track which ones we've already
    # announced so we don't repeat the message across multiple poll cycles.
    prev_statuses: dict[str, str] = {}
    rejoined_announced: set[str] = set()

    while True:
        sim_time = get_sim_time(conn)
        active = get_active_workers(conn)

        if not active:
            print("[CONTROLLER] No active workers remain. Ending simulation.")
            with conn:
                db_execute_with_retry(conn, "UPDATE sim_clock SET status = 'DONE'")
            break

        # --- Re-registration detection ---
        # Detect any worker that was inactive and is now in any active state.
        # We can't rely on catching the READY transition specifically because
        # a restarting worker may move OFFLINE->READY->CATCHUP faster than our
        # poll interval. Any active status after an inactive one counts.
        _active_statuses = {"READY", "WAITING", "WORKING", "ACK", "CATCHUP"}
        for row in conn.execute("SELECT worker_id, status FROM worker_registry").fetchall():
            wid, st = row["worker_id"], row["status"]
            prev = prev_statuses.get(wid)
            if (prev in ("OFFLINE", "ERROR") and st in _active_statuses
                    and wid not in rejoined_announced):
                print(f"[CONTROLLER] Worker {wid} has rejoined at sim_time {sim_time.isoformat()}")
                rejoined_announced.add(wid)
            elif st in ("OFFLINE", "ERROR"):
                # Reset so a future re-join can be detected again
                rejoined_announced.discard(wid)
            prev_statuses[wid] = st

        # --- Wait for due workers to ACK ---
        due = get_due_workers(conn, sim_time)
        stall_start: dict[str, float] = {r["worker_id"]: time.monotonic() for r in due}
        last_warn: dict[str, float] = {}

        while True:
            pending = conn.execute(
                "SELECT * FROM worker_registry "
                "WHERE status NOT IN ('OFFLINE', 'DONE', 'ERROR', 'ACK', 'CATCHUP') "
                "  AND next_event_time <= ?",
                (sim_time.isoformat(),),
            ).fetchall()

            if not pending:
                break

            now = time.monotonic()
            for row in pending:
                wid = row["worker_id"]
                if row["status"] == "ERROR":
                    print(f"[CONTROLLER] WARNING: Worker {wid} is in ERROR — skipping this cycle.")
                    continue
                elapsed = now - stall_start.get(wid, now)
                last = last_warn.get(wid, 0)
                if elapsed >= WORKER_TIMEOUT_SECONDS and now - last >= WORKER_TIMEOUT_SECONDS:
                    print(
                        f"[CONTROLLER] WARNING: Worker {wid} has been in "
                        f"'{row['status']}' for {elapsed:.0f}s without ACK."
                    )
                    last_warn[wid] = now

            time.sleep(POLL_INTERVAL_SECONDS)

        # --- Reset ACK'd workers and advance clock ---
        acked = conn.execute(
            "SELECT worker_id FROM worker_registry WHERE status = 'ACK'"
        ).fetchall()
        with conn:
            for row in acked:
                db_execute_with_retry(
                    conn,
                    "UPDATE worker_registry SET status = 'WAITING' WHERE worker_id = ?",
                    (row["worker_id"],),
                )

        # Find minimum next_event_time across all active workers
        active = get_active_workers(conn)
        if not active:
            print("[CONTROLLER] No active workers remain after ACK reset.")
            with conn:
                db_execute_with_retry(conn, "UPDATE sim_clock SET status = 'DONE'")
            break

        min_next = min(
            datetime.fromisoformat(r["next_event_time"])
            for r in active
            if r["next_event_time"] is not None
        )

        if min_next > SIM_END:
            print("[CONTROLLER] Simulation complete — all events past SIM_END.")
            with conn:
                db_execute_with_retry(conn, "UPDATE sim_clock SET status = 'DONE'")
            _print_final_summary(conn)
            break

        with conn:
            db_execute_with_retry(
                conn,
                "UPDATE sim_clock SET sim_time = ?",
                (min_next.isoformat(),),
            )

        print(f"[CONTROLLER] Sim time -> {min_next.isoformat()}")


def _print_final_summary(conn):
    print("\n[CONTROLLER] === Final Worker Summary ===")
    rows = conn.execute(
        "SELECT worker_id, status, last_ack_time, next_event_time, missed_events "
        "FROM worker_registry ORDER BY worker_id"
    ).fetchall()
    for r in rows:
        print(
            f"  {r['worker_id']}: status={r['status']}, "
            f"last_ack={r['last_ack_time']}, "
            f"next_event={r['next_event_time']}, "
            f"missed_events={r['missed_events']}"
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    conn = open_db()
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
# Step 0 — db_setup run:
#   sim_clock: sim_time=2026-01-01T00:00:00, status=INIT
#   worker_a:  status=OFFLINE, next_event_time=2026-01-01T00:00:00,
#              last_ack_time=None, missed_events=0
#   worker_b:  status=OFFLINE, next_event_time=2026-01-01T00:00:00,
#              last_ack_time=None, missed_events=0
#   worker_c:  status=OFFLINE, next_event_time=2026-01-01T00:00:00,
#              last_ack_time=None, missed_events=0
#
# Step 1 — controller starts, sets WAITING_FOR_WORKERS:
#   sim_clock: sim_time=2026-01-01T00:00:00, status=WAITING_FOR_WORKERS
#   worker_a sets status=READY  -> worker_registry.status=READY
#   worker_b sets status=READY  -> worker_registry.status=READY
#   worker_c never checks in within READY_TIMEOUT_SECONDS
#   controller writes: worker_c.status=OFFLINE
#   sim_clock: status -> RUNNING
#
#   DB state after Step 1:
#   sim_clock: sim_time=2026-01-01T00:00:00, status=RUNNING
#   worker_a:  status=READY,    next_event_time=2026-01-01T00:00:00
#   worker_b:  status=READY,    next_event_time=2026-01-01T00:00:00
#   worker_c:  status=OFFLINE,  next_event_time=2026-01-01T00:00:00
#
# Step 2 — First cycle (sim_time=2026-01-01T00:00:00):
#   Due workers: worker_a, worker_b  (next_event_time == sim_time)
#   worker_a executes task, returns 8 hours:
#     next_event_time = 2026-01-01T08:00:00, status = ACK
#   worker_b executes task, returns 24 hours:
#     next_event_time = 2026-01-02T00:00:00, status = ACK
#   All due workers ACK'd.
#   Controller resets both to WAITING.
#   min(next_event_time) = 2026-01-01T08:00:00 (worker_a)
#   sim_clock.sim_time -> 2026-01-01T08:00:00
#
#   DB state after Step 2:
#   sim_clock: sim_time=2026-01-01T08:00:00, status=RUNNING
#   worker_a:  status=WAITING, next_event_time=2026-01-01T08:00:00
#   worker_b:  status=WAITING, next_event_time=2026-01-02T00:00:00
#   worker_c:  status=OFFLINE, next_event_time=2026-01-01T00:00:00
#
# Step 3 — worker_c comes online late (sim is now 2026-01-01T08:00:00):
#   worker_c sets status=READY
#   sim is already RUNNING, so worker_c enters catch-up check:
#     own next_event_time = 2026-01-01T00:00:00 < sim_time (08:00)
#   worker_c sets status=CATCHUP
#   Catch-up loop iteration 1:
#     get_next_event_hours(worker_c, 2026-01-01T00:00:00, is_catchup=True) -> 12
#     new next_event_time = 2026-01-01T12:00:00
#     missed_events incremented to 1
#     2026-01-01T12:00:00 > sim_time (08:00) -> exit loop
#   worker_c writes next_event_time=2026-01-01T12:00:00, status=WAITING
#
#   DB state after Step 3:
#   sim_clock: sim_time=2026-01-01T08:00:00, status=RUNNING
#   worker_a:  status=WAITING, next_event_time=2026-01-01T08:00:00
#   worker_b:  status=WAITING, next_event_time=2026-01-02T00:00:00
#   worker_c:  status=WAITING, next_event_time=2026-01-01T12:00:00,
#              missed_events=1
#
# Step 4 — Second cycle (sim_time=2026-01-01T08:00:00):
#   Due workers: worker_a only (next_event_time=08:00 == sim_time)
#   worker_b: WAITING, next=Jan 2  -> not due
#   worker_c: WAITING, next=12:00  -> not due
#   worker_a executes task, returns 12 hours:
#     next_event_time=2026-01-01T20:00:00, status=ACK
#   All due workers ACK'd.
#   Controller resets worker_a to WAITING.
#   Active workers: worker_a (20:00), worker_b (Jan 2), worker_c (12:00)
#   min(next_event_time) = 2026-01-01T12:00:00 (worker_c)
#   sim_clock.sim_time -> 2026-01-01T12:00:00
#
#   DB state after Step 4:
#   sim_clock: sim_time=2026-01-01T12:00:00, status=RUNNING
#   worker_a:  status=WAITING, next_event_time=2026-01-01T20:00:00
#   worker_b:  status=WAITING, next_event_time=2026-01-02T00:00:00
#   worker_c:  status=WAITING, next_event_time=2026-01-01T12:00:00

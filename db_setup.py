import sqlite3
from config import DB_PATH, SIM_START, WORKERS


def setup():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    with conn:
        conn.execute("DROP TABLE IF EXISTS sim_clock")
        conn.execute("DROP TABLE IF EXISTS worker_registry")

        conn.execute("""
            CREATE TABLE sim_clock (
                sim_time TEXT NOT NULL,
                status   TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE worker_registry (
                worker_id       TEXT PRIMARY KEY,
                status          TEXT    NOT NULL DEFAULT 'OFFLINE',
                last_seen       TEXT,
                last_ack_time   TEXT,
                next_event_time TEXT,
                missed_events   INTEGER NOT NULL DEFAULT 0
            )
        """)

        conn.execute(
            "INSERT INTO sim_clock (sim_time, status) VALUES (?, ?)",
            (SIM_START.isoformat(), "INIT"),
        )

        for wid in WORKERS:
            conn.execute(
                """INSERT INTO worker_registry
                   (worker_id, status, last_seen, last_ack_time, next_event_time, missed_events)
                   VALUES (?, 'OFFLINE', NULL, NULL, ?, 0)""",
                (wid, SIM_START.isoformat()),
            )

    print("Database initialised.")
    print()

    row = conn.execute("SELECT sim_time, status FROM sim_clock").fetchone()
    print(f"  sim_clock: sim_time={row[0]}, status={row[1]}")

    workers = conn.execute(
        "SELECT worker_id, status, next_event_time, last_ack_time, missed_events "
        "FROM worker_registry ORDER BY worker_id"
    ).fetchall()
    for w in workers:
        print(f"  {w[0]}: status={w[1]}, next_event_time={w[2]}, "
              f"last_ack_time={w[3]}, missed_events={w[4]}")

    conn.close()


if __name__ == "__main__":
    setup()

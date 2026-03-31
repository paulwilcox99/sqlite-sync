import sqlite3
import config


def get_connection():
    conn = sqlite3.connect(config.DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def setup():
    conn = get_connection()

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
                status          TEXT NOT NULL DEFAULT 'OFFLINE',
                last_seen       TEXT,
                last_ack_time   TEXT,
                next_event_time TEXT
            )
        """)

        conn.execute(
            "INSERT INTO sim_clock (sim_time, status) VALUES (?, ?)",
            (config.SIM_START.isoformat(), "INIT"),
        )

        for wid in config.WORKERS:
            conn.execute(
                "INSERT INTO worker_registry "
                "(worker_id, status, last_seen, last_ack_time, next_event_time) "
                "VALUES (?, 'OFFLINE', NULL, NULL, ?)",
                (wid, config.SIM_START.isoformat()),
            )

    print("Database initialized.")
    print()

    row = conn.execute("SELECT sim_time, status FROM sim_clock").fetchone()
    print(f"  sim_clock: sim_time={row['sim_time']}, status={row['status']}")
    print()

    workers = conn.execute(
        "SELECT worker_id, status, next_event_time, last_ack_time "
        "FROM worker_registry ORDER BY worker_id"
    ).fetchall()
    for w in workers:
        print(
            f"  {w['worker_id']}: status={w['status']}, "
            f"next_event_time={w['next_event_time']}, "
            f"last_ack_time={w['last_ack_time']}"
        )

    conn.close()

    print()
    print(
        f"Database initialized. Start controller.py then start all workers "
        f"within {config.READY_TIMEOUT_SECONDS} seconds."
    )


if __name__ == "__main__":
    setup()

# SQLite Lockstep Simulation

A simulated time coordination system using SQLite and a lockstep clock pattern.
Workers have dynamic task schedules. All workers must be present before the
simulation starts (enforced by a ready gate). If any worker or the controller
crashes, the simulation errors out and must be restarted fresh from db_setup.py.

## Prerequisites

- Python 3.9 or higher
- No external packages required (stdlib only)

## Setup (run once per simulation)

**Unix/Mac:**
```bash
bash setup.sh
source venv/bin/activate
```

**Windows:**
```bat
setup.bat
venv\Scripts\activate
```

## Running the Simulation

**Step 1 — Initialize the database** (required before every run):
```bash
python db_setup.py
```

**Step 2 — Start the controller** in its own terminal:
```bash
source venv/bin/activate
python controller.py
```
The controller waits up to `READY_TIMEOUT_SECONDS` for all workers.
Do not let this timeout expire — start all workers promptly.

**Step 3 — Start all workers** within the timeout window:
```bash
# Terminal 2
source venv/bin/activate && python worker.py worker_a

# Terminal 3
source venv/bin/activate && python worker.py worker_b

# Terminal 4
source venv/bin/activate && python worker.py worker_c
```

ALL workers must be running before `READY_TIMEOUT_SECONDS` elapses.
The controller will not start until every registered worker checks in.

## Operational Rules

- Always run `db_setup.py` before starting a new simulation
- Start all workers before the ready timeout expires
- If any process crashes, the simulation is over
- Do not restart a crashed worker and expect it to rejoin —
  it will be rejected because the simulation is already RUNNING

## If Something Goes Wrong

Any crash means the simulation state is invalid. Recovery is always:
1. `python db_setup.py`
2. Restart controller and all workers

## Live Inspection

You can query the SQLite database directly while the simulation runs:
```bash
sqlite3 simulation.db "SELECT * FROM sim_clock;"
sqlite3 simulation.db "SELECT * FROM worker_registry;"
sqlite3 simulation.db "SELECT worker_id, status, next_event_time, last_ack_time FROM worker_registry ORDER BY next_event_time;"
```

## Adding a New Worker

1. Add the worker ID to `config.WORKERS`
2. Run `db_setup.py` (resets the simulation)
3. Start: `python worker.py <new_worker_id>`

## Customizing Task Logic

Edit `get_next_event_hours()` in `worker.py`.
Return a positive integer: sim hours until next task.
Branch on `worker_id` for completely different behavior per worker.
No catchup flag — this function is only ever called for live tasks.

## Resetting the Simulation

```bash
python db_setup.py
```

Drops and recreates all tables, returns to clean INIT state.

## Architecture Summary

| Component | Role |
|-----------|------|
| `sim_clock` table | Single row — owns the current simulation time and overall status. Controller is the only writer. |
| `worker_registry` | One row per worker. Workers write their own `next_event_time` and status. Controller reads `next_event_time` to find the minimum (next clock target) and resets status from ACK to WAITING each cycle. |
| Polling | Workers and controller poll the DB on `POLL_INTERVAL_SECONDS`. WAL mode allows concurrent reads without blocking. |
| Ready gate | Controller stays in WAITING_FOR_WORKERS until all registered workers have `status=READY`. Fatal if any worker missing after timeout. |
| Clock advance | Next-event driven — jumps to `MIN(next_event_time)` across all active workers. Never wastes cycles on empty sim time. |

## Worker Status Reference

| Status | Meaning |
|--------|---------|
| OFFLINE | Not yet started; set by db_setup.py |
| READY | Worker checked in, waiting for RUNNING |
| WAITING | Sim is RUNNING, worker idle until `next_event_time` |
| WORKING | Worker actively executing task |
| ACK | Task complete, `next_event_time` written, awaiting controller reset |
| DONE | Worker's `next_event_time` has passed SIM_END |
| ERROR | Worker encountered unhandled exception |

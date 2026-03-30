# SQLite Lockstep Simulation

A simulated time coordination system using SQLite and a lockstep clock pattern.
Workers operate on dynamic task schedules. The system handles late-joining workers,
worker restarts, and ensures no events are missed at startup or during recovery.

## Prerequisites

- Python 3.9 or higher
- No external packages required (stdlib only)

## Setup (do this once)

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

**Step 1 — Initialize the database** (run once, or to reset):
```bash
python db_setup.py
```

**Step 2 — Start the controller** in its own terminal:
```bash
source venv/bin/activate   # or venv\Scripts\activate on Windows
python controller.py
```
The controller waits up to `READY_TIMEOUT_SECONDS` for all workers.

**Step 3 — Start each worker** in its own terminal:
```bash
# Terminal 2
source venv/bin/activate && python worker.py worker_a

# Terminal 3
source venv/bin/activate && python worker.py worker_b

# Terminal 4
source venv/bin/activate && python worker.py worker_c
```

Start workers within the `READY_TIMEOUT` window for a clean synchronized start.
Workers started after the timeout will catch up automatically.

## What to Expect

- Controller prints each sim time advance
- Workers print task execution and ACK messages
- Late-joining workers print CATCHUP progress
- Simulation ends when `sim_time` passes `SIM_END`

## Worker Status Reference

| Status   | Meaning                                              |
|----------|------------------------------------------------------|
| OFFLINE  | Not yet started or never checked in                  |
| READY    | Started, waiting for sim to begin                    |
| WAITING  | Idle, waiting for `next_event_time`                  |
| WORKING  | Executing current task                               |
| ACK      | Task done, waiting for controller to advance clock   |
| CATCHUP  | Replaying missed events after late start or restart  |
| DONE     | No more events before `SIM_END`                      |
| ERROR    | Crashed — check terminal output for traceback        |

## Adding a New Worker

1. Add the worker ID to `config.WORKERS`
2. Run `db_setup.py` again (this resets the simulation)
3. Start the new worker: `python worker.py <new_worker_id>`

## Customizing Task Logic

Edit `get_next_event_hours()` in `worker.py`.

```python
def get_next_event_hours(worker_id: str, sim_time: datetime,
                          is_catchup: bool = False) -> int:
```

- Returns a **positive integer**: sim hours until the next task
- Use `is_catchup=True` to skip side effects during catch-up replay
- Branch on `worker_id` for completely different per-worker behavior
- May query the DB, read files, or call APIs — but guard all side effects with `is_catchup`

## Restarting a Crashed Worker

Simply re-run:
```bash
python worker.py <worker_id>
```

The worker detects missed events, runs catch-up automatically, and rejoins
the simulation without any manual intervention.

## Resetting the Simulation

```bash
python db_setup.py
```

This wipes all state and starts fresh from `SIM_START`.

## Configuration (`config.py`)

| Constant                | Default              | Description                                   |
|-------------------------|----------------------|-----------------------------------------------|
| `SIM_START`             | `2026-01-01`         | Simulation start datetime                     |
| `SIM_END`               | `2026-03-31`         | Simulation end datetime                       |
| `DB_PATH`               | `simulation.db`      | SQLite database file path                     |
| `POLL_INTERVAL_SECONDS` | `1`                  | How often workers/controller poll the DB      |
| `WORKER_TIMEOUT_SECONDS`| `30`                 | Stall warning threshold (seconds)             |
| `READY_TIMEOUT_SECONDS` | `120`                | How long controller waits for workers at startup |
| `WORKERS`               | `[worker_a, b, c]`   | Registered worker IDs                         |

## Architecture

```
controller.py          worker_a     worker_b     worker_c
     |                    |            |            |
     |-- WAITING_FOR_WORKERS                        |
     |<-- READY ----------|<-- READY --|            |
     |-- RUNNING                                    |
     |                    |-- WORKING  |            |
     |                    |-- ACK      |            |
     |                                |-- WORKING   |
     |                                |-- ACK       |
     |-- advance sim_time                           |
     |                                              |-- READY (late join)
     |                                              |-- CATCHUP
     |                                              |-- WAITING
     ...
```

### SQLite Concurrency

- WAL journal mode on all connections for concurrent reads during writes
- Exponential backoff retry (up to 5 attempts) on `SQLITE_BUSY` errors
- `PRAGMA busy_timeout=5000` as a coarser backstop
- All multi-field writes in explicit transactions (`BEGIN IMMEDIATE`)

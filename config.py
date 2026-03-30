from datetime import datetime

SIM_START = datetime(2026, 1, 1)
SIM_END   = datetime(2026, 3, 31)
DB_PATH   = "simulation.db"

POLL_INTERVAL_SECONDS  = 1
WORKER_TIMEOUT_SECONDS = 30
READY_TIMEOUT_SECONDS  = 120

WORKERS = ["worker_a", "worker_b", "worker_c"]

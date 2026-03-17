# Services — ordered structure

All services under `services/` follow the same order. Keep this layout when adding or changing code.

---

## Standard service layout (order): routers / models / services

Use this order in every service folder:

| Order | Layer        | Contents |
|-------|--------------|----------|
| 1     | **Entry**    | `main.py` — app, lifespan, include_router only |
| 2     | **Routers**  | `routers/` — HTTP route handlers; delegate to services (no business logic) |
| 3     | **Models**   | `models.py` (or `models/`) — request/response DTOs |
| 4     | **Services** | Subpackages for business logic (e.g. `orchestrator/`, `planner/`, `lifecycle/`, `loaders/`, `connectors/`, `workers/`) |
| 5     | **Utils**    | `utils/` — shared helpers (remap, temp tables, etc.) |
| 6     | **Meta**     | `README.md` (this layout + service-specific flow), `requirements.txt` if any |

**Do not** put docs (`.md`) or tests (`test_*.py`) inside a service; use project `docs/` and `tests/`.

---

## Services in this repo

| Service              | Purpose                          | Entry        |
|----------------------|-----------------------------------|-------------|
| **migration_service**| SQL pushdown ETL orchestration    | `main.py`   |
| **extraction_service**| Data extraction from sources     | `main.py`   |
| **websocket_service** | WebSocket server                  | `main.py`   |

Each service has its own `README.md` describing its internal flow (see e.g. `migration_service/README.md`).

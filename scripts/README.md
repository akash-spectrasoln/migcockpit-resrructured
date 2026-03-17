# MigCockpit — Scripts

## Starting everything (recommended)

All start scripts now run a pre-flight sequence first:

```
Lint (Ruff + ESLint) → Type-check → Unit tests → Start services
```

If lint or tests fail, you are prompted whether to continue or abort.
This ensures you never start a broken build.

### Windows (PowerShell — recommended)
```powershell
.\scripts\start_all_services.ps1
```

### Windows (batch)
```batch
scripts\start_all_services.bat
```

### Linux / Mac
```bash
chmod +x scripts/start_services.sh
./scripts/start_services.sh
```

---

## Pre-flight sequence explained

| Step | What runs | Fails on |
|---|---|---|
| 1 | Prerequisites check | Python/Node not installed |
| 2 | `ruff check .` | Python lint errors |
| 3 | `npm run type-check` + `npm run lint` | TypeScript errors, ESLint errors |
| 4 | `pytest tests/unit/ -q` | Any failing unit test |
| 5 | Django, Celery, Extraction, Migration, WebSocket | — |
| 6 | `npm install && npm run dev` | — |

Steps 2–4 will **prompt you** if they fail — you can choose to continue or abort.
Tools that aren't installed (ruff, pytest) are skipped with a notice.

---

## Services and ports

| Service | Port | Started by |
|---|---|---|
| Django API | 8000 | `python manage.py runserver 8000` |
| Celery worker | — | `celery -A datamigrationapi worker` |
| Extraction Service | 8001 | `services/extraction_service/main.py` |
| Migration Service | 8003 | `services/migration_service/main.py` |
| WebSocket Service | 8004 | `services/websocket_service/main.py` |
| React frontend | 5173 | `npm install && npm run dev` |

> **Note on Celery:** `-A datamigrationapi` is the Django project package name — do not change it.

---

## Stopping services

### Windows
```batch
scripts\stop_all_services.bat
```
```powershell
.\scripts\stop_all_services.ps1
```

### Linux / Mac
Press `Ctrl+C` in the terminal running `start_services.sh`.

---

## Running individually

### Python unit tests only
```bash
python -m pytest tests/unit/ -v      # Linux/Mac
scripts\run_tests.bat                 # Windows
./scripts/run_tests.sh               # Linux/Mac
```

### Python lint only
```bash
ruff check .                          # check
ruff check . --fix                    # auto-fix safe issues
./scripts/lint.sh                     # Linux/Mac wrapper
scripts\lint.bat                      # Windows wrapper
```

### Frontend checks only
```bash
cd frontend
npm run type-check    # TypeScript errors
npm run lint          # ESLint errors
npm run lint:fix      # auto-fix ESLint
npm run check-all     # both together
```

### Extraction service only (for debugging)
```batch
scripts\start_extraction_service.bat
```

---

## Debug scripts
One-off diagnostic scripts live in `scripts/debug/`.
They are not part of the normal workflow.

# CLAUDE.md — grandcru-value-api (MinMax Wine)

## Run and test

```bash
pip install -r requirements.txt pytest
uvicorn app.main:app --reload            # SQLite at data/wines.db unless DATABASE_URL is set
PYTHONPATH=. pytest
pre-commit run --all-files               # secrets scan, deploy-command check, py_compile
```

CI (`.github/workflows/ci.yml`) runs the same checks plus pytest on Python 3.11.

## Deploy

- Railway project `zonal-purpose`; services `web`, `daily-ingest`, `weekly-ingest`
  (cron) and `Postgres`. Every push to `main` auto-deploys from the `Dockerfile`.
- `web` starts with `import_wine_data.py --skip-if-fresh 20`: a redeploy re-imports
  the committed `seed/` CSVs only if no ingestion ran in the last 20 hours
  (`IMPORT_SKIP_IF_FRESH_HOURS`). Real refreshes pass `--skip-if-fresh 0`.
- Cron start commands live in the Railway UI. `deploy/*-command.txt` are checked-in
  copies; change both together. `scripts/check_deploy_commands.py` validates the
  flags, and `--live` diffs against Railway (needs a Railway user token).

## Gotchas

- Railway app storage is ephemeral: resolver writes to `seed/vivino_overrides.csv`
  at runtime are lost on redeploy. Postgres is the only durable store.
- `import_wine_data.py` deletes and rebuilds `wine_deals` on every import. Fields
  missing from the CSVs (e.g. `vivino_description`) survive only via its DB-fallback
  logic; keep that fallback when touching the importer.
- `DATABASE_URL` is rewritten to the psycopg 3 driver by
  `app.config.normalize_database_url`; any new engine must go through it.
- `scripts/data_quality_rules.py` is the allowlist for accepted data gaps. Rows in
  `vivino_overrides.csv` without a URL deliberately block wrong fuzzy matches.

## Safety

- `/ops/*` requires the `X-Ops-Key` header (`OPS_API_KEY`); keep public routes read-only.
- Keys (`GEMINI_API_KEY`, `BRAVE_API_KEY`, ...) go in `.env`, never in commits;
  `scripts/check_no_plaintext_secrets.py` gates CI.

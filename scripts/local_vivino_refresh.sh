#!/bin/bash
# Local Vivino price refresh — runs on your Mac (residential IP)
# Scrapes fresh Vivino data, commits overrides, pushes to main.
# Schedule via launchd or cron.

set -euo pipefail

REPO_DIR="/Users/koohaoming/dev/grandcru-value-api"
LOG_FILE="$REPO_DIR/data/local_vivino_refresh.log"
BRANCH="main"
PYTHON_BIN="$REPO_DIR/.venv/bin/python"
OPS_BASE_URL="${OPS_BASE_URL:-https://wine.kooexperience.com}"
UNRESOLVED_CSV="$REPO_DIR/data/live_vivino_unresolved.csv"
RESOLVED_CSV="$REPO_DIR/data/live_llm_resolved.csv"

if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$LOG_FILE"; }

json_field() {
  local field="$1"
  "$PYTHON_BIN" -c '
import json
import sys

field = sys.argv[1]
payload = json.load(sys.stdin)
value = payload
for part in field.split("."):
    if isinstance(value, dict):
        value = value.get(part)
    else:
        value = None
        break
print("" if value is None else value)
' "$field"
}

cd "$REPO_DIR"
if [[ -f .env ]]; then
  set -a
  source .env >/dev/null 2>&1
  set +a
fi

if [[ -z "${OPS_API_KEY:-}" ]]; then
  log "OPS_API_KEY is required"
  exit 1
fi

log "Starting Vivino refresh"

if ! git diff --quiet || ! git diff --cached --quiet || [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
  log "Worktree must be clean before autonomous refresh."
  exit 1
fi

# Ensure clean state on main
git checkout "$BRANCH" 2>/dev/null
git pull --ff-only origin "$BRANCH"

# Pull the live unresolved export from production so the local resolver
# works against the current catalog, not a stale committed snapshot.
log "Fetching unresolved live rows..."
curl -fsS "$OPS_BASE_URL/ops/vivino/unresolved.csv" \
  -H "X-Ops-Key: $OPS_API_KEY" \
  -o "$UNRESOLVED_CSV"

unresolved_count=$("$PYTHON_BIN" - <<'PY'
import csv
from pathlib import Path

path = Path("data/live_vivino_unresolved.csv")
if not path.exists():
    print(0)
else:
    with path.open("r", encoding="utf-8", newline="") as handle:
        print(sum(1 for _ in csv.DictReader(handle)))
PY
)

if [[ "$unresolved_count" == "0" ]]; then
  log "No unresolved live Vivino rows. Done."
  exit 0
fi

# Run the resolver locally on a residential IP.
log "Running resolver for $unresolved_count live rows..."
"$PYTHON_BIN" scripts/llm_vivino_resolver.py \
  --comparison "$UNRESOLVED_CSV" \
  --vivino seed/vivino_results.csv \
  --vivino-overrides seed/vivino_overrides.csv \
  --output "$RESOLVED_CSV" \
  --all \
  --auto-apply \
  --sleep 3 \
  2>&1 | tee -a "$LOG_FILE"

# Check if overrides changed
if git diff --quiet seed/vivino_overrides.csv; then
  log "No changes to overrides. Done."
  exit 0
fi

# Commit and push
log "Overrides changed — committing..."
git add seed/vivino_overrides.csv
git commit -m "chore: refresh Vivino overrides (local cron $(date '+%Y-%m-%d'))"
git push origin "$BRANCH"

target_commit=$(git rev-parse HEAD)
log "Waiting for deploy of $target_commit ..."

for _ in $(seq 1 40); do
  deployed_commit=$(
    curl -fsS "$OPS_BASE_URL/ops/diagnostics" \
      -H "X-Ops-Key: $OPS_API_KEY" \
      | json_field git_commit
  )
  if [[ -n "$deployed_commit" && "$deployed_commit" == "$target_commit" ]]; then
    log "Deploy live on production."
    break
  fi
  sleep 15
done

if [[ -z "${deployed_commit:-}" || "$deployed_commit" != "$target_commit" ]]; then
  log "Deploy did not reach $target_commit in time."
  exit 1
fi

log "Triggering production daily refresh..."
curl -fsS "$OPS_BASE_URL/ops/refresh/trigger" \
  -H "X-Ops-Key: $OPS_API_KEY" \
  -H "Content-Type: application/json" \
  --data '{"mode":"daily","strict_health":false}' \
  2>&1 | tee -a "$LOG_FILE"

log "Waiting for production refresh to finish..."
for _ in $(seq 1 80); do
  refresh_status=$(
    curl -fsS "$OPS_BASE_URL/ops/refresh/status" \
      -H "X-Ops-Key: $OPS_API_KEY"
  )
  status_value=$(printf '%s' "$refresh_status" | json_field status)
  exit_code=$(printf '%s' "$refresh_status" | json_field exit_code)
  if [[ "$status_value" == "success" ]]; then
    break
  fi
  if [[ "$status_value" == "failed" ]]; then
    log "Production refresh failed with exit code ${exit_code:-unknown}."
    exit 1
  fi
  sleep 15
done

if [[ "${status_value:-}" != "success" ]]; then
  log "Production refresh did not finish in time."
  exit 1
fi

health_payload=$(
  curl -fsS "$OPS_BASE_URL/health"
)
total_deals=$(printf '%s' "$health_payload" | json_field total_deals)
total_unrated=$(printf '%s' "$health_payload" | json_field latest_ingestion.details.unrated_rows)
log "Completed autonomous refresh flow. total_deals=${total_deals:-unknown} unrated_rows=${total_unrated:-unknown}"

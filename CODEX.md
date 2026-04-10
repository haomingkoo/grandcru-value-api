# Codex Review Brief — `vivino_description` Persistence

## Goal

Review the tasting-notes persistence fix and confirm we no longer lose
`vivino_description` after Railway refreshes or deploys.

Your review should focus on bugs, regressions, and remaining persistence gaps.
Lead with findings, ordered by severity, with file references.

## Confirmed Behavior To Review

### 1. Railway writes happen on ephemeral app storage

- `app/ops.py` builds the scheduled refresh command against repo-local files:
  `seed/comparison_summary.csv`, `seed/vivino_results.csv`, `seed/vivino_overrides.csv`.
- `scripts/refresh_pipeline.py` runs the resolver and then re-runs
  `scripts/import_wine_data.py` against those same paths.
- `scripts/llm_vivino_resolver.py --auto-apply` writes back into
  `seed/vivino_overrides.csv`.

That means runtime CSV edits can affect the next import, but they do not survive
the next Railway redeploy unless they are also persisted somewhere durable.

### 2. The importer rebuilds `wine_deals` from scratch

`scripts/import_wine_data.py` still does a full `delete(WineDeal)` before
re-inserting the current records.

Without a fallback, blank CSV descriptions would wipe previously stored DB
descriptions on every import.

### 3. Current committed seed state

- `seed/vivino_results.csv` currently does not have a `vivino_description` column.
- `seed/vivino_overrides.csv` currently does carry `vivino_description`.

Important nuance:
`scripts/enrich_vivino_results.py` is capable of writing `vivino_description`
into `vivino_results.csv`, but that script is not the durability mechanism for
Railway deploys. Runtime file writes on Railway are still ephemeral.

## Fix That Landed

Review the new logic in `scripts/import_wine_data.py`:

- `_load_existing_vivino_descriptions()` snapshots current DB descriptions before
  the table rebuild.
- `_resolve_vivino_description()` uses this precedence:
  1. non-empty CSV description
  2. existing DB description matched by normalized Vivino URL
  3. existing DB description matched by canonicalized wine name
- The importer still allows real CSV data to replace the preserved fallback.

This is the intended durable behavior:

- first successful import with a description stores it in Postgres
- later imports keep it even if Railway resets the runtime CSVs
- committed/manual override descriptions still win when present

## Tests Added

Inspect and trust these regression tests unless you find a concrete flaw:

- `tests/test_import_wine_data.py`
  `test_import_preserves_existing_description_when_csv_loses_it`
- `tests/test_import_wine_data.py`
  `test_csv_description_overrides_preserved_database_value`

These cover the exact failure mode:

1. import once with a description present
2. remove the description from CSV inputs
3. import again
4. confirm the DB still serves the existing description

## What To Verify

1. The DB fallback cannot overwrite a non-empty CSV description.
2. The fallback is scoped tightly enough that it will not attach an unrelated
   description to the wrong wine.
3. The Railway refresh path still imports after resolver writes and therefore
   depends on DB preservation for deploy durability.
4. No existing override-lock behavior was weakened.
5. No obvious edge case remains where notes are still silently dropped during a
   normal daily or weekly refresh.

## Acceptance Criteria

1. A wine that already has `vivino_description` in Postgres keeps it after a new
   import even when the CSV inputs are blank for that field.
2. A non-empty description from `vivino_overrides.csv` or any future base CSV
   column takes precedence over the preserved DB value.
3. After a Railway redeploy, the next import does not erase previously stored
   descriptions solely because runtime CSV edits were lost.
4. A wine with no description in CSV and no description in DB still resolves to
   `None`.

## Commands

Run at minimum:

```bash
python3 -m pytest tests/test_import_wine_data.py -q
python3 -m pytest tests/test_vivino_ops.py -q
```

## Review Output Format

- Findings first, highest severity first.
- Use file references and explain the concrete failure mode.
- If there are no findings, say that explicitly and call out only residual risk
  that is still real.

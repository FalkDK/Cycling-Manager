# Giro Workspace

Separate workspace for Giro ingestion, mapping, and optimization.

The point of this folder is to keep Giro-specific rules and scripts isolated from the
existing classics pipeline while still using the same PostgreSQL database.

## Current Games

Checked against the live Holdet payloads on 2026-04-17:

- `giro-d-italia-manager-2026`
  - Display name: `Giro Manager`
  - Holdet game id: `613`
  - Ruleset id: `118`
  - Rule summary: no substitutions between stages
  - Positions: `Kategori 1` / `Kategori 2` / `Kategori 3` / `Kategori 4`
  - Pricing: all riders currently priced at `0`

- `giro-d-italia-2026`
  - Display name: `Girospillet`
  - Holdet game id: `612`
  - Ruleset id: `117`
  - Rule summary: substitutions between stages
  - Positions: one generic `Rytter`
  - Pricing: salary cap game with rider prices

## Important Identity Rule

Do not use Holdet `player id` as the cross-game rider identity.

For Giro, the same rider appears with:

- a stable `personId`
- a game-specific `id`

On 2026-04-17 the two Giro games had the same 91 riders, but all 91 had different
`id` values between the two games. That means:

- map `holdet_person_id -> pcs_rider_id`
- store `holdet_player_id` per game and per snapshot

## Recommended Folder Scope

This folder should own:

- Giro-only schema
- Giro-only ingestion scripts
- Giro-only mapping workflow
- Giro-only exports
- Giro-only optimization logic for manager vs trading game

This folder should not change the classics workflow unless we later extract small,
stable shared helpers.

## First Ingestion Command

Run both Giro Holdet games into the Giro tables:

```bash
python3 -m giro.ingest_holdet
```

Run just one cartridge:

```bash
python3 -m giro.ingest_holdet --cartridge giro-d-italia-manager-2026
python3 -m giro.ingest_holdet --cartridge giro-d-italia-2026
```

## Mapping Commands

Generate Giro mapping suggestions and auto-approve the clean matches:

```bash
python3 -m giro.mapping suggest
```

Check overall mapping status:

```bash
python3 -m giro.mapping status
```

Approve one rider manually:

```bash
python3 -m giro.mapping approve --holdet-person-id 4196 --pcs-rider-id jonas-vingegaard
```

## PCS Rider History Import

Import:

- all 2026 race rows for mapped Giro riders
- 2025 Giro, Tour, and Vuelta stage/classification rows

Command:

```bash
python3 -m giro.pcs_history
```

Useful options:

```bash
python3 -m giro.pcs_history --limit 3
python3 -m giro.pcs_history --pcs-rider-id jonas-vingegaard
python3 -m giro.pcs_history --profile-dir ~/.cache/fantasy-cycling/giro-playwright
python3 -m giro.pcs_history --force
```

Notes:

- The importer uses Playwright with a persistent browser profile.
- Default is `--no-headless` because PCS challenge handling is browser-sensitive.
- On first run, let the browser settle if PCS shows a security or consent screen.
- Successful imports are marked in `giro_pcs_history_import_status`.
- Re-running without `--force` skips riders already imported successfully for the same `season` and `grand-tour-season` window.
- Failed riders are recorded as `FAILED` and the importer continues with the next rider.

## PCS History Status Check

Show riders that still need attention because they are:

- marked `FAILED`
- marked `SUCCESS` with `0` rows
- missing actual history rows for the import window

Command:

```bash
python3 -m giro.pcs_history_status
```

## Snapshot Export For Public Streamlit

If you want to deploy the Giro app publicly without a database connection, export a read-only
snapshot and let the app read files instead of Postgres.

Export command:

```bash
python3 -m giro.snapshot --out data/giro_snapshot_latest
```

The snapshot contains:

- `metadata.json`
- `giro_rider_browser.csv`
- `giro_rider_results.csv`

The Streamlit Giro page will prefer `GIRO_SNAPSHOT_DIR` or `data/giro_snapshot_latest` if it exists.
Only if no snapshot directory is present will it fall back to `FANTASY_CYCLING_DB_URL`.

## Public GitHub/Streamlit Deployment

For a public Streamlit deployment, put these on GitHub:

- `streamlit_app.py`
- `giro/`
- `pyproject.toml`
- `Makefile`
- `data/giro_snapshot_latest/`

Do not put these on GitHub:

- database URLs
- local browser profiles
- `.env` files
- raw private DB dumps

Useful helper targets:

```bash
make giro-snapshot
make giro-run
make giro-publish MSG="refresh giro snapshot"
```

`giro-publish` stages only the public app files plus the snapshot directory, then commits and pushes.

## PCS Slug Fixes

When PCS uses a different rider slug than the one stored locally, use:

```bash
python3 -m giro.update_pcs_rider_id --from-id einer-augusto-rubio --to-id einer-rubio
python3 -m giro.update_pcs_rider_id --from-id enric-mas-nicolau --to-id enric-mas
python3 -m giro.update_pcs_rider_id --from-id igor-arrieta --to-id igor-arrieta-lizarraga
```

Preview the generated SQL without executing it:

```bash
python3 -m giro.update_pcs_rider_id --from-id old-slug --to-id new-slug --sql-only
```

## Initial Database Design

Use separate Giro tables in the same database:

- `giro_game`
- `giro_game_position`
- `giro_holdet_person`
- `giro_raw_payload_archive`
- `giro_player_pool_snapshot`
- `giro_player_pool_entry`
- `giro_person_pcs_map`
- `giro_person_pcs_map_suggestion`

The SQL draft lives in [schema.sql](/Users/steffenfalkjensen/Python/Cycling/giro/schema.sql).

## Workflow Direction

1. Ingest Holdet cartridge metadata for both Giro games.
2. Ingest latest player pools for both games into snapshots.
3. Map `holdet_person_id` to PCS rider id once.
4. Store PCS rider history:
   - 2026 races already done by mapped Giro riders
   - 2025 grand-tour stage and classification results for mapped Giro riders
5. Build two optimizers:
   - manager: one-and-done roster
   - trading: day-by-day substitutions
6. Re-run pool ingestion close to lock because more riders can be added later.

## Rider History Model

The Giro schema now includes `giro_pcs_rider_result` for rider-level historical rows.

The intended use is:

- `SEASON_RACE`
  - any 2026 race result already done by a mapped Giro rider
- `GRAND_TOUR_STAGE`
  - a 2025 Giro, Tour, or Vuelta stage result row
- `GRAND_TOUR_GC`
  - a 2025 general classification row
- `GRAND_TOUR_POINTS`
  - a 2025 points classification row
- `GRAND_TOUR_KOM`
  - a 2025 mountains classification row
- `GRAND_TOUR_YOUTH`
  - a 2025 youth classification row

Useful views:

- [v_giro_mapped_rider_results](/Users/steffenfalkjensen/Python/Cycling/giro/schema.sql)
- [v_giro_mapped_rider_results_2026](/Users/steffenfalkjensen/Python/Cycling/giro/schema.sql)
- [v_giro_mapped_grand_tour_results_2025](/Users/steffenfalkjensen/Python/Cycling/giro/schema.sql)

## Current PCS Blocker

As of 2026-04-17, direct non-browser fetches to PCS rider and race pages from this runtime are
being challenged by Cloudflare. That means the data model is ready, but the live history ingester
still depends on either:

- a fetch path that can pass PCS anti-bot checks
- or a local/exported source file workflow

## Why This Stays Separate For Now

The existing classics schema assumes:

- one cartridge-centric rider pool
- `holdet_player_id` as the mapping key
- one dominant game model

That is acceptable for the current classics flow, but it is the wrong abstraction for
these Giro games. Keeping Giro separate is safer than refactoring a working classics
pipeline before the Giro logic is proven.

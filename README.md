<<<<<<< Updated upstream
# Cycling-Manager
=======
# Fantasy Cycling (Local Workspace)

Local toolkit for fantasy cycling decisions using:
- ProCyclingStats (PCS) startlists/results ingestion
- Holdet manager rider pool ingestion + manager->PCS mapping
- Snapshot export for a shareable Streamlit app

This repo is now maintained **locally** (no GitHub remote required).

## What You Use Day-to-Day

1. Refresh PCS startlists
2. Refresh results for races that have finished
3. Refresh Holdet manager riders + mapping
4. Export snapshot files
5. Open Streamlit app (read-only, in-memory)

## Requirements

- Python 3.10+
- PostgreSQL + `psql`
- Internet access for ingestion commands

Install dependencies:

```bash
cd /Users/steffenfalkjensen/Python/Cycling
pip install -e ".[ui]"
```

Set DB connection (example):

```bash
export FANTASY_CYCLING_DB_URL="postgresql://<user>:<password>@localhost:5432/fantasy_cycling"
```

## Core Workflow (Recommended)

### 1) Initialize/upgrade DB schema

```bash
python3 -m fantasy_cycling init-db
```

### 2) Update current startlists (2026)

```bash
python3 -m fantasy_cycling ingest startlists --season 2026
```

### 3) Update historical results

For full backfill:

```bash
python3 -m fantasy_cycling ingest results --seasons 2025,2024
```

For current season results as races complete (safer race-by-race):

```bash
python3 -m fantasy_cycling ingest results --seasons 2026 --race "Omloop Nieuwsblad"
```

### 4) Update Holdet manager riders + mapping

```bash
python3 -m fantasy_cycling ingest manager-riders --cartridge classics-manager-2026
python3 -m fantasy_cycling mapping suggest --cartridge classics-manager-2026 --auto-approve
python3 -m fantasy_cycling mapping status --cartridge classics-manager-2026
```

### 5) Export snapshot for app

```bash
python3 -m fantasy_cycling export snapshot \
  --out data/snapshot_latest \
  --season 2026 \
  --history-seasons 2025,2024 \
  --cartridge classics-manager-2026
```

### 6) Run Streamlit app

```bash
export FANTASY_CYCLING_SNAPSHOT_DIR="/Users/steffenfalkjensen/Python/Cycling/data/snapshot_latest"
streamlit run /Users/steffenfalkjensen/Python/Cycling/streamlit_app.py
```

## CLI Reference

### Top-level

```bash
python3 -m fantasy_cycling --help
```

Main command groups:
- `init-db`
- `ingest`
- `mapping`
- `strategy` (optional/advanced)
- `export`

### Ingest

```bash
python3 -m fantasy_cycling ingest startlists --season 2026 [--race "Strade Bianche"]
python3 -m fantasy_cycling ingest results --seasons 2025,2024 [--race "Milano-Sanremo"]
python3 -m fantasy_cycling ingest all --season 2026 --history 2025,2024 [--race "Omloop Nieuwsblad"]
python3 -m fantasy_cycling ingest race --race "Paris-Roubaix" --season 2026 --history 2025,2024
python3 -m fantasy_cycling ingest manager-riders --cartridge classics-manager-2026
```

### Mapping

```bash
python3 -m fantasy_cycling mapping suggest --cartridge classics-manager-2026 --auto-approve
python3 -m fantasy_cycling mapping approve --holdet-player-id 43219 --pcs-rider-id thomas-pidcock --note "manual"
python3 -m fantasy_cycling mapping reject --cartridge classics-manager-2026 --holdet-player-id 43219
python3 -m fantasy_cycling mapping status --cartridge classics-manager-2026
```

### Export

```bash
python3 -m fantasy_cycling export snapshot --help
```

## Holdet API + Cartridge Discovery

The manager-riders ingestion uses Holdet APIs in `fantasy_cycling/ingest.py`.

### How cartridge is identified

- Cartridge slug is the tournament slug (example: `classics-manager-2026`).
- You can read it from the Holdet URL path, e.g.:
  - `https://.../da/classics-manager-2026/rules` -> slug is `classics-manager-2026`

### How game/player endpoint is resolved

1. Fetch cartridge details:

`GET /api/cartridges/{cartridge_slug}`

2. Read `gameId` from that payload.

3. Fetch player pool:

`GET /api/games/{gameId}/players`

In code:
- `build_holdet_cartridge_url(cartridge_slug)`
- `extract_holdet_game_id(cartridge_payload, cartridge_slug)`
- `build_holdet_players_url(game_id)`

All are defined in [`fantasy_cycling/ingest.py`](/Users/steffenfalkjensen/Python/Cycling/fantasy_cycling/ingest.py).

Quick manual check with `curl`:

```bash
curl -s "https://nexus-app-fantasy-fargate.holdet.dk/api/cartridges/classics-manager-2026"
# find gameId
curl -s "https://nexus-app-fantasy-fargate.holdet.dk/api/games/<gameId>/players"
```

## Snapshot Files Used by Streamlit

Export writes:
- `data/snapshot_latest/current_startlist.csv`
- `data/snapshot_latest/startlist_changes.csv`
- `data/snapshot_latest/race_history_rankings.csv`
- `data/snapshot_latest/manager_riders_enriched.csv`
- `data/snapshot_latest/metadata.json`

The Streamlit app reads only these files (in memory). It does not require live DB access.

## Public App Publish Flow

For the public GitHub/Streamlit app, use the wrapper script instead of pushing files by hand.

Giro only:

```bash
./publish_public_app.sh --workspace giro --commit-message "refresh giro app"
```

Classics + Giro:

```bash
./publish_public_app.sh --workspace both --commit-message "refresh public app"
```

Useful shortcuts:

```bash
make public-giro MSG="refresh giro app"
make public-both MSG="refresh public app"
```

What this script does:

- refreshes the selected snapshot directories
- stages the public app files and snapshot files
- commits them
- pushes to `origin/main`

Streamlit Cloud should use [requirements.txt](/Users/steffenfalkjensen/Python/Cycling/requirements.txt) plus the
snapshot directories committed in git. No database URL is needed for the public app when snapshots exist.

## Useful SQL Views

- `v_current_startlist`
- `v_startlist_changes`
- `v_race_history_rankings`
- `v_manager_riders`
- `v_manager_riders_enriched`
- `v_manager_rider_map_suggestions`

## Local Helpers (not required)

You may keep local helper scripts (`run.sh`, `suggest.sh`) for your own workflow.
They are not required by the snapshot Streamlit app.

## Tests

```bash
python3 -m unittest discover -s tests -p "test_*.py"
```
>>>>>>> Stashed changes

#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REMOTE_URL_DEFAULT="https://github.com/FalkDK/Cycling-Manager.git"
BRANCH="main"
WORKSPACE="both"
CLASSICS_SNAPSHOT_DIR="data/snapshot_latest"
GIRO_SNAPSHOT_DIR="data/giro_snapshot_latest"
PUSH_ENABLED=1
REFRESH_CLASSICS=1
REFRESH_GIRO=1
COMMIT_MESSAGE=""

SEASON=2026
BASE_HISTORY_SEASONS="2025,2024"
EXPORT_HISTORY_SEASONS="2026,2025,2024"
CARTRIDGE_SLUG="classics-manager-2026"
RESULT_RACES_2026=()
DB_URL="${FANTASY_CYCLING_DB_URL:-}"
REMOTE_URL="$REMOTE_URL_DEFAULT"

usage() {
  cat <<'EOF'
Usage: ./publish_public_app.sh [options]

Publishes the public Streamlit app repo by refreshing snapshot files and staging the
app code + selected snapshot directories in one commit.

Options:
  --workspace <classics|giro|both>  Which app data to refresh and publish. Default: both
  --db-url <url>                    Override FANTASY_CYCLING_DB_URL for this run.
  --remote-url <url>                Git remote URL. Default: FalkDK/Cycling-Manager
  --branch <name>                   Remote branch to push. Default: main
  --commit-message <text>           Explicit git commit message.
  --classics-snapshot-dir <path>    Default: data/snapshot_latest
  --giro-snapshot-dir <path>        Default: data/giro_snapshot_latest
  --no-refresh-classics             Skip classics snapshot refresh.
  --no-refresh-giro                 Skip Giro snapshot refresh.
  --no-push                         Refresh/stage only. Do not commit or push.

  Classics refresh options:
  --season <year>                   Startlist season. Default: 2026
  --history-seasons <csv>           Base history refresh. Default: 2025,2024
  --export-history <csv>            Exported seasons. Default: 2026,2025,2024
  --result-race <name>              Completed 2026 race to ingest results for. Repeatable.
  --cartridge <slug>                Holdet cartridge slug. Default: classics-manager-2026

  -h, --help                        Show this help text.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workspace)
      WORKSPACE="${2:-}"
      shift 2
      ;;
    --db-url)
      DB_URL="${2:-}"
      shift 2
      ;;
    --remote-url)
      REMOTE_URL="${2:-}"
      shift 2
      ;;
    --branch)
      BRANCH="${2:-}"
      shift 2
      ;;
    --commit-message)
      COMMIT_MESSAGE="${2:-}"
      shift 2
      ;;
    --classics-snapshot-dir)
      CLASSICS_SNAPSHOT_DIR="${2:-}"
      shift 2
      ;;
    --giro-snapshot-dir)
      GIRO_SNAPSHOT_DIR="${2:-}"
      shift 2
      ;;
    --no-refresh-classics)
      REFRESH_CLASSICS=0
      shift
      ;;
    --no-refresh-giro)
      REFRESH_GIRO=0
      shift
      ;;
    --no-push)
      PUSH_ENABLED=0
      shift
      ;;
    --season)
      SEASON="${2:-}"
      shift 2
      ;;
    --history-seasons)
      BASE_HISTORY_SEASONS="${2:-}"
      shift 2
      ;;
    --export-history)
      EXPORT_HISTORY_SEASONS="${2:-}"
      shift 2
      ;;
    --result-race)
      RESULT_RACES_2026+=("${2:-}")
      shift 2
      ;;
    --cartridge)
      CARTRIDGE_SLUG="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ "$WORKSPACE" != "classics" && "$WORKSPACE" != "giro" && "$WORKSPACE" != "both" ]]; then
  echo "--workspace must be one of: classics, giro, both"
  exit 1
fi

cd "$REPO_ROOT"

if [[ -z "$DB_URL" ]]; then
  echo "FANTASY_CYCLING_DB_URL is not set."
  echo "Set it in the environment or pass --db-url."
  exit 1
fi

export FANTASY_CYCLING_DB_URL="$DB_URL"

echo "Checking database connection..."
psql "$FANTASY_CYCLING_DB_URL" -X -q -c "SELECT 1;" >/dev/null

if [[ "$WORKSPACE" == "classics" || "$WORKSPACE" == "both" ]]; then
  if [[ "$REFRESH_CLASSICS" -eq 1 ]]; then
    echo "Refreshing classics snapshot..."
    classics_args=(
      "--db-url" "$DB_URL"
      "--season" "$SEASON"
      "--history-seasons" "$BASE_HISTORY_SEASONS"
      "--export-history" "$EXPORT_HISTORY_SEASONS"
      "--cartridge" "$CARTRIDGE_SLUG"
      "--snapshot-dir" "$CLASSICS_SNAPSHOT_DIR"
      "--remote-url" "$REMOTE_URL"
      "--branch" "$BRANCH"
      "--no-push"
    )
    for race_name in "${RESULT_RACES_2026[@]}"; do
      classics_args+=("--result-race" "$race_name")
    done
    ./publish_snapshot.sh "${classics_args[@]}"
  else
    echo "Skipping classics refresh."
  fi
fi

if [[ "$WORKSPACE" == "giro" || "$WORKSPACE" == "both" ]]; then
  if [[ "$REFRESH_GIRO" -eq 1 ]]; then
    echo "Refreshing Giro snapshot..."
    python3 -m giro.snapshot --out "$GIRO_SNAPSHOT_DIR"
  else
    echo "Skipping Giro refresh."
  fi
fi

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Not inside a git repository. Cannot publish public app."
  exit 1
fi

if git remote get-url origin >/dev/null 2>&1; then
  current_remote="$(git remote get-url origin)"
  if [[ "$current_remote" != "$REMOTE_URL" ]]; then
    echo "Updating git remote origin to $REMOTE_URL"
    git remote set-url origin "$REMOTE_URL"
  fi
else
  echo "Adding git remote origin -> $REMOTE_URL"
  git remote add origin "$REMOTE_URL"
fi

paths_to_add=(
  "README.md"
  "pyproject.toml"
  "requirements.txt"
  "streamlit_app.py"
  "Makefile"
  "publish_snapshot.sh"
  "publish_public_app.sh"
)

if [[ "$WORKSPACE" == "classics" || "$WORKSPACE" == "both" ]]; then
  paths_to_add+=("fantasy_cycling" "$CLASSICS_SNAPSHOT_DIR")
fi

if [[ "$WORKSPACE" == "giro" || "$WORKSPACE" == "both" ]]; then
  paths_to_add+=("giro" "$GIRO_SNAPSHOT_DIR")
fi

echo "Staging public app files..."
git add "${paths_to_add[@]}"

if git diff --cached --quiet; then
  echo "No staged public app changes to commit."
  exit 0
fi

if [[ "$PUSH_ENABLED" -ne 1 ]]; then
  echo "Public app refresh complete. Commit/push skipped (--no-push)."
  exit 0
fi

if [[ -z "$COMMIT_MESSAGE" ]]; then
  COMMIT_MESSAGE="Update public app $(date '+%Y-%m-%d %H:%M:%S')"
fi

git commit -m "$COMMIT_MESSAGE"
git push origin "HEAD:$BRANCH"

echo "Public app pushed to $REMOTE_URL ($BRANCH)."

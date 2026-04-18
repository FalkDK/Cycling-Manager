from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from fantasy_cycling.config import Settings, load_settings
from fantasy_cycling.db import DatabaseError, PostgresClient, Repository


BROWSER_COLUMNS = [
    "holdet_person_id",
    "holdet_rider_name",
    "pcs_rider_id",
    "pcs_rider_name",
    "holdet_team_name",
    "manager_holdet_player_id",
    "manager_category",
    "manager_is_out",
    "trading_holdet_player_id",
    "trading_start_price",
    "trading_price",
    "trading_points",
    "trading_popularity",
    "trading_is_out",
    "results_2026_rows",
    "race_days_2026",
    "distinct_races_2026",
    "stage_rows_2026",
    "classification_rows_2026",
    "wins_2026",
    "podiums_2026",
    "top10s_2026",
    "pcs_points_2026_total",
    "uci_points_2026_total",
    "last_result_date_2026",
    "gt_stage_rows_2025",
    "gt_stage_days_2025",
    "gt_count_2025",
    "giro_stage_rows_2025",
    "tour_stage_rows_2025",
    "vuelta_stage_rows_2025",
    "best_gt_stage_result_2025",
    "best_gt_gc_result_2025",
    "best_gt_points_result_2025",
    "best_gt_kom_result_2025",
    "best_gt_youth_result_2025",
    "gt_stage_wins_2025",
    "gt_stage_top5s_2025",
    "gt_stage_top10s_2025",
]

RESULT_COLUMNS = [
    "holdet_person_id",
    "holdet_rider_name",
    "pcs_rider_id",
    "pcs_rider_name",
    "season",
    "result_date",
    "race_name",
    "result_label",
    "result_scope",
    "grand_tour_slug",
    "race_class",
    "rank_position",
    "raw_result",
    "kms",
    "pcs_points",
    "uci_points",
    "vertical_meters",
    "source_url",
    "fetched_at",
]

NUMERIC_COLUMNS = [
    "holdet_person_id",
    "manager_holdet_player_id",
    "trading_holdet_player_id",
    "trading_start_price",
    "trading_price",
    "trading_points",
    "trading_popularity",
    "results_2026_rows",
    "race_days_2026",
    "distinct_races_2026",
    "stage_rows_2026",
    "classification_rows_2026",
    "wins_2026",
    "podiums_2026",
    "top10s_2026",
    "pcs_points_2026_total",
    "uci_points_2026_total",
    "gt_stage_rows_2025",
    "gt_stage_days_2025",
    "gt_count_2025",
    "giro_stage_rows_2025",
    "tour_stage_rows_2025",
    "vuelta_stage_rows_2025",
    "best_gt_stage_result_2025",
    "best_gt_gc_result_2025",
    "best_gt_points_result_2025",
    "best_gt_kom_result_2025",
    "best_gt_youth_result_2025",
    "gt_stage_wins_2025",
    "gt_stage_top5s_2025",
    "gt_stage_top10s_2025",
    "season",
    "rank_position",
    "kms",
    "pcs_points",
    "uci_points",
    "vertical_meters",
]

BOOL_COLUMNS = ["manager_is_out", "trading_is_out"]

GIRO_SNAPSHOT_FILES = {
    "rider_browser": "giro_rider_browser.csv",
    "rider_results": "giro_rider_results.csv",
}


@dataclass
class GiroSnapshotSummary:
    output_dir: str
    rider_browser_rows: int
    rider_results_rows: int
    files_written: int


def _schema_path() -> Path:
    return Path(__file__).resolve().with_name("schema.sql")


def _sql_literal(value: str | int) -> str:
    if isinstance(value, int):
        return str(value)
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _normalize_giro_frame(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in [col for col in columns if col in NUMERIC_COLUMNS and col in frame.columns]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in [col for col in columns if col in BOOL_COLUMNS and col in frame.columns]:
        frame[column] = (
            frame[column]
            .map({"t": True, "f": False, "True": True, "False": False, True: True, False: False})
            .fillna(False)
        )
    for column in ["last_result_date_2026", "result_date", "fetched_at"]:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce", utc=(column == "fetched_at"))
    return frame


def load_giro_snapshot(snapshot_dir: str) -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    base = Path(snapshot_dir).expanduser().resolve()
    if not base.exists():
        raise RuntimeError(f"Giro snapshot directory not found: {base}")

    metadata_path = base / "metadata.json"
    if metadata_path.exists():
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        files = metadata.get("files", GIRO_SNAPSHOT_FILES)
    else:
        metadata = {}
        files = GIRO_SNAPSHOT_FILES

    browser_file = base / files.get("rider_browser", GIRO_SNAPSHOT_FILES["rider_browser"])
    results_file = base / files.get("rider_results", GIRO_SNAPSHOT_FILES["rider_results"])
    for path in [browser_file, results_file]:
        if not path.exists():
            raise RuntimeError(f"Missing Giro snapshot file: {path}")

    browser_df = pd.read_csv(browser_file)
    result_df = pd.read_csv(results_file)
    browser_df = _normalize_giro_frame(browser_df, BROWSER_COLUMNS)
    result_df = _normalize_giro_frame(result_df, RESULT_COLUMNS)
    return metadata, browser_df, result_df


class GiroSnapshotService:
    def __init__(self, repository: Repository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings

    def _copy_query_to_csv(self, query_sql: str, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        escaped_path = str(output_path).replace("'", "''")
        copy_sql = f"\\copy ({query_sql}) TO '{escaped_path}' CSV HEADER"

        command = ["psql", "-X", "-v", "ON_ERROR_STOP=1"]
        if self.settings.db_url:
            command.extend(["-d", self.settings.db_url])
        command.extend(["-c", copy_sql])

        completed = subprocess.run(command, capture_output=True, text=True)
        if completed.returncode != 0:
            message = completed.stderr.strip() or completed.stdout.strip() or "Giro snapshot export failed."
            raise DatabaseError(message)

    def export_snapshot(self, output_dir: str) -> GiroSnapshotSummary:
        out_path = Path(output_dir).expanduser().resolve()
        out_path.mkdir(parents=True, exist_ok=True)

        rider_browser_query = """
            SELECT
                holdet_person_id,
                holdet_rider_name,
                pcs_rider_id,
                pcs_rider_name,
                holdet_team_name,
                manager_holdet_player_id,
                manager_category,
                manager_is_out,
                trading_holdet_player_id,
                trading_start_price,
                trading_price,
                trading_points,
                trading_popularity,
                trading_is_out,
                results_2026_rows,
                race_days_2026,
                distinct_races_2026,
                stage_rows_2026,
                classification_rows_2026,
                wins_2026,
                podiums_2026,
                top10s_2026,
                pcs_points_2026_total,
                uci_points_2026_total,
                last_result_date_2026,
                gt_stage_rows_2025,
                gt_stage_days_2025,
                gt_count_2025,
                giro_stage_rows_2025,
                tour_stage_rows_2025,
                vuelta_stage_rows_2025,
                best_gt_stage_result_2025,
                best_gt_gc_result_2025,
                best_gt_points_result_2025,
                best_gt_kom_result_2025,
                best_gt_youth_result_2025,
                gt_stage_wins_2025,
                gt_stage_top5s_2025,
                gt_stage_top10s_2025
            FROM v_giro_rider_browser
            ORDER BY holdet_team_name, manager_category, holdet_rider_name
        """
        rider_results_query = """
            SELECT
                holdet_person_id,
                holdet_rider_name,
                pcs_rider_id,
                pcs_rider_name,
                season,
                result_date,
                race_name,
                result_label,
                result_scope,
                grand_tour_slug,
                race_class,
                rank_position,
                raw_result,
                kms,
                pcs_points,
                uci_points,
                vertical_meters,
                source_url,
                fetched_at
            FROM v_giro_mapped_rider_results
            ORDER BY holdet_rider_name, result_date DESC, race_name, result_label
        """

        self.repository.init_schema(_schema_path())
        self._copy_query_to_csv(rider_browser_query, out_path / GIRO_SNAPSHOT_FILES["rider_browser"])
        self._copy_query_to_csv(rider_results_query, out_path / GIRO_SNAPSHOT_FILES["rider_results"])

        counts_client = PostgresClient(db_url=self.settings.db_url)
        rider_browser_rows = int(
            counts_client.query_scalar(f"SELECT COUNT(*) FROM ({rider_browser_query}) t;") or "0"
        )
        rider_results_rows = int(
            counts_client.query_scalar(f"SELECT COUNT(*) FROM ({rider_results_query}) t;") or "0"
        )

        metadata = {
            "generated_at": datetime.now(self.settings.timezone).isoformat(),
            "files": GIRO_SNAPSHOT_FILES,
            "row_counts": {
                "rider_browser": rider_browser_rows,
                "rider_results": rider_results_rows,
            },
        }
        (out_path / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")

        return GiroSnapshotSummary(
            output_dir=str(out_path),
            rider_browser_rows=rider_browser_rows,
            rider_results_rows=rider_results_rows,
            files_written=len(GIRO_SNAPSHOT_FILES) + 1,
        )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python3 -m giro.snapshot")
    parser.add_argument("--db-url", help="PostgreSQL connection URL. Falls back to env vars.")
    parser.add_argument("--out", required=True, help="Output directory for Giro snapshot files.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    settings = load_settings(db_url=args.db_url)
    client = PostgresClient(db_url=settings.db_url)
    repository = Repository(client=client)
    service = GiroSnapshotService(repository=repository, settings=settings)
    summary = service.export_snapshot(output_dir=args.out)

    print("Giro snapshot export complete.")
    print(f"  output_dir: {summary.output_dir}")
    print(f"  rider_browser_rows: {summary.rider_browser_rows}")
    print(f"  rider_results_rows: {summary.rider_results_rows}")
    print(f"  files_written: {summary.files_written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

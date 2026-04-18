from __future__ import annotations

import argparse

from fantasy_cycling.config import load_settings
from fantasy_cycling.db import PostgresClient
from giro.ingest_holdet import GiroRepository, _sql_literal


class GiroPcsHistoryStatusRepository(GiroRepository):
    def list_attention_rows(self, target_season: int, grand_tour_season: int) -> list[dict[str, object]]:
        sql = f"""
            WITH mapped AS (
                SELECT gp.holdet_person_id, hp.rider_name AS holdet_rider_name, gp.pcs_rider_id
                FROM giro_person_pcs_map gp
                JOIN giro_holdet_person hp ON hp.holdet_person_id = gp.holdet_person_id
                WHERE gp.status = 'APPROVED'
            ),
            history_counts AS (
                SELECT pcs_rider_id, COUNT(*) AS history_rows
                FROM giro_pcs_rider_result
                WHERE season = {_sql_literal(target_season)}
                   OR (
                        season = {_sql_literal(grand_tour_season)}
                    AND grand_tour_slug IN ('giro-d-italia', 'tour-de-france', 'vuelta-a-espana')
                    AND result_scope IN (
                        'GRAND_TOUR_STAGE',
                        'GRAND_TOUR_GC',
                        'GRAND_TOUR_POINTS',
                        'GRAND_TOUR_KOM',
                        'GRAND_TOUR_YOUTH'
                    )
                   )
                GROUP BY pcs_rider_id
            ),
            latest_status AS (
                SELECT DISTINCT ON (pcs_rider_id)
                    pcs_rider_id,
                    status,
                    row_count,
                    error_message,
                    updated_at
                FROM giro_pcs_history_import_status
                WHERE target_season = {_sql_literal(target_season)}
                  AND grand_tour_season = {_sql_literal(grand_tour_season)}
                ORDER BY pcs_rider_id, updated_at DESC
            )
            SELECT
                m.holdet_person_id,
                m.holdet_rider_name,
                m.pcs_rider_id,
                COALESCE(ls.status, 'MISSING') AS import_status,
                COALESCE(ls.row_count, 0) AS status_row_count,
                COALESCE(hc.history_rows, 0) AS actual_history_rows,
                ls.updated_at,
                ls.error_message
            FROM mapped m
            LEFT JOIN latest_status ls ON ls.pcs_rider_id = m.pcs_rider_id
            LEFT JOIN history_counts hc ON hc.pcs_rider_id = m.pcs_rider_id
            WHERE COALESCE(ls.status, 'MISSING') = 'FAILED'
               OR COALESCE(ls.row_count, 0) = 0
               OR COALESCE(hc.history_rows, 0) = 0
            ORDER BY m.holdet_rider_name;
        """
        rows = self.client.query_rows(sql)
        return [
            {
                "holdet_person_id": int(row[0]),
                "holdet_rider_name": row[1],
                "pcs_rider_id": row[2],
                "import_status": row[3],
                "status_row_count": int(row[4]),
                "actual_history_rows": int(row[5]),
                "updated_at": row[6] if len(row) > 6 else None,
                "error_message": row[7] if len(row) > 7 else None,
            }
            for row in rows
        ]

    def get_summary(self, target_season: int, grand_tour_season: int) -> dict[str, int]:
        sql = f"""
            WITH mapped AS (
                SELECT DISTINCT gp.pcs_rider_id
                FROM giro_person_pcs_map gp
                WHERE gp.status = 'APPROVED'
            ),
            history_counts AS (
                SELECT pcs_rider_id, COUNT(*) AS history_rows
                FROM giro_pcs_rider_result
                WHERE season = {_sql_literal(target_season)}
                   OR (
                        season = {_sql_literal(grand_tour_season)}
                    AND grand_tour_slug IN ('giro-d-italia', 'tour-de-france', 'vuelta-a-espana')
                    AND result_scope IN (
                        'GRAND_TOUR_STAGE',
                        'GRAND_TOUR_GC',
                        'GRAND_TOUR_POINTS',
                        'GRAND_TOUR_KOM',
                        'GRAND_TOUR_YOUTH'
                    )
                   )
                GROUP BY pcs_rider_id
            ),
            latest_status AS (
                SELECT DISTINCT ON (pcs_rider_id)
                    pcs_rider_id,
                    status,
                    row_count
                FROM giro_pcs_history_import_status
                WHERE target_season = {_sql_literal(target_season)}
                  AND grand_tour_season = {_sql_literal(grand_tour_season)}
                ORDER BY pcs_rider_id, updated_at DESC
            )
            SELECT
                (SELECT COUNT(*) FROM mapped) AS mapped_riders,
                (SELECT COUNT(*) FROM latest_status WHERE status = 'SUCCESS') AS status_success,
                (SELECT COUNT(*) FROM latest_status WHERE status = 'FAILED') AS status_failed,
                (SELECT COUNT(*) FROM latest_status WHERE status = 'SUCCESS' AND row_count = 0) AS status_zero_rows,
                (SELECT COUNT(*) FROM mapped m
                  LEFT JOIN history_counts hc ON hc.pcs_rider_id = m.pcs_rider_id
                  WHERE COALESCE(hc.history_rows, 0) = 0) AS riders_without_history_rows;
        """
        row = self.client.query_rows(sql)[0]
        return {
            "mapped_riders": int(row[0]),
            "status_success": int(row[1]),
            "status_failed": int(row[2]),
            "status_zero_rows": int(row[3]),
            "riders_without_history_rows": int(row[4]),
        }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="giro-pcs-history-status")
    parser.add_argument("--db-url", help="PostgreSQL connection URL. Falls back to env vars.")
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--grand-tour-season", type=int, default=2025)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    settings = load_settings(db_url=args.db_url)
    repository = GiroPcsHistoryStatusRepository(client=PostgresClient(db_url=settings.db_url))
    repository.init_schema()

    summary = repository.get_summary(target_season=args.season, grand_tour_season=args.grand_tour_season)
    print("Giro PCS history status.")
    for key, value in summary.items():
        print(f"  {key}: {value}")

    rows = repository.list_attention_rows(target_season=args.season, grand_tour_season=args.grand_tour_season)
    if not rows:
        print("  attention_rows: 0")
        return 0

    print("Attention rows:")
    for row in rows:
        error_message = row["error_message"] or ""
        if len(error_message) > 140:
            error_message = error_message[:137] + "..."
        print(
            f"  holdet_person_id={row['holdet_person_id']} "
            f"rider={row['holdet_rider_name']} "
            f"pcs={row['pcs_rider_id']} "
            f"status={row['import_status']} "
            f"status_rows={row['status_row_count']} "
            f"actual_rows={row['actual_history_rows']} "
            f"updated_at={row['updated_at'] or '-'} "
            f"error={error_message}"
        )
    print(f"  attention_rows: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

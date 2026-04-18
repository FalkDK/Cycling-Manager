from __future__ import annotations

import argparse

from fantasy_cycling.config import load_settings
from fantasy_cycling.db import PostgresClient
from giro.ingest_holdet import _sql_literal

REFERENCE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("giro_pcs_history_import_status", "pcs_rider_id"),
    ("giro_pcs_rider_result", "pcs_rider_id"),
    ("giro_person_pcs_map_suggestion", "pcs_rider_id"),
    ("giro_person_pcs_map", "pcs_rider_id"),
    ("manager_rider_map_suggestion", "pcs_rider_id"),
    ("manager_rider_map", "pcs_rider_id"),
    ("strategy_shortlist_option", "captain_pcs_rider_id"),
    ("strategy_decision_log", "captain_pcs_rider_id"),
    ("strategy_round_lock", "pcs_rider_id"),
    ("strategy_shortlist_option_member", "pcs_rider_id"),
)


def build_slug_migration_sql(old_id: str, new_id: str, delete_old_rider: bool = True) -> str:
    old_sql = _sql_literal(old_id)
    new_sql = _sql_literal(new_id)
    statements: list[str] = [
        "BEGIN",
        f"""
        INSERT INTO rider (pcs_rider_id, rider_name, nationality)
        SELECT {new_sql}, rider_name, nationality
        FROM rider
        WHERE pcs_rider_id = {old_sql}
        ON CONFLICT (pcs_rider_id) DO NOTHING
        """.strip(),
    ]

    for table_name, column_name in REFERENCE_COLUMNS:
        statements.append(
            f"""
            UPDATE {table_name}
            SET {column_name} = {new_sql}
            WHERE {column_name} = {old_sql}
            """.strip()
        )

    if delete_old_rider:
        statements.append(
            f"""
            DELETE FROM rider
            WHERE pcs_rider_id = {old_sql}
            """.strip()
        )

    statements.append("COMMIT")
    return ";\n\n".join(statements) + ";\n"


def _count_rows(client: PostgresClient, table_name: str, column_name: str, pcs_rider_id: str) -> int:
    sql = f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE {column_name} = {_sql_literal(pcs_rider_id)};
    """
    result = client.query_scalar(sql)
    return int(result or "0")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="giro-update-pcs-rider-id")
    parser.add_argument("--db-url", help="PostgreSQL connection URL. Falls back to env vars.")
    parser.add_argument("--from-id", required=True, help="Existing PCS rider slug to migrate away from.")
    parser.add_argument("--to-id", required=True, help="Replacement PCS rider slug.")
    parser.add_argument(
        "--keep-old-rider",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Keep the old rider row instead of deleting it after references are migrated.",
    )
    parser.add_argument(
        "--sql-only",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Print the generated migration SQL without executing it.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.from_id == args.to_id:
        parser.error("--from-id and --to-id must be different.")

    sql = build_slug_migration_sql(
        old_id=args.from_id,
        new_id=args.to_id,
        delete_old_rider=not args.keep_old_rider,
    )

    if args.sql_only:
        print(sql)
        return 0

    settings = load_settings(db_url=args.db_url)
    client = PostgresClient(db_url=settings.db_url)

    before_counts = {
        f"{table}.{column}": _count_rows(client, table, column, args.from_id)
        for table, column in REFERENCE_COLUMNS
    }
    before_old_rider = _count_rows(client, "rider", "pcs_rider_id", args.from_id)
    before_new_rider = _count_rows(client, "rider", "pcs_rider_id", args.to_id)

    client.execute(sql)

    after_counts = {
        f"{table}.{column}": _count_rows(client, table, column, args.to_id)
        for table, column in REFERENCE_COLUMNS
    }
    after_old_rider = _count_rows(client, "rider", "pcs_rider_id", args.from_id)
    after_new_rider = _count_rows(client, "rider", "pcs_rider_id", args.to_id)

    print("PCS rider slug migration complete.")
    print(f"  from_id: {args.from_id}")
    print(f"  to_id: {args.to_id}")
    print(f"  rider_old_before: {before_old_rider}")
    print(f"  rider_new_before: {before_new_rider}")
    print(f"  rider_old_after: {after_old_rider}")
    print(f"  rider_new_after: {after_new_rider}")
    for key in sorted(before_counts):
        before = before_counts[key]
        after = after_counts[key]
        if before or after:
            print(f"  {key}: {before} -> {after}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
from dataclasses import dataclass
from difflib import SequenceMatcher

from fantasy_cycling.config import load_settings
from fantasy_cycling.db import PostgresClient, Repository
from fantasy_cycling.manager_mapping import PcsRiderProfile, normalize_text, normalized_name_signature
from giro.ingest_holdet import GiroRepository, _sql_literal


@dataclass(frozen=True)
class GiroPersonProfile:
    game_id: int
    cartridge_slug: str
    holdet_player_id: int
    holdet_person_id: int
    rider_name: str
    position_title: str
    team_name: str


@dataclass(frozen=True)
class GiroMappingSuggestion:
    game_id: int
    holdet_person_id: int
    holdet_player_id: int
    holdet_rider_name: str
    holdet_team_name: str
    position_title: str
    pcs_rider_id: str
    pcs_rider_name: str
    score: float
    suggestion_rank: int
    status: str
    mapping_source: str


def _normalized_team_set(values: tuple[str, ...]) -> set[str]:
    return {normalize_text(value) for value in values if normalize_text(value)}


def _base_name_score(giro_name: str, pcs_name: str) -> tuple[float, str]:
    giro_norm = normalize_text(giro_name)
    pcs_norm = normalize_text(pcs_name)
    if giro_norm and giro_norm == pcs_norm:
        return 0.95, "auto_exact_name"

    giro_sig = normalized_name_signature(giro_name)
    pcs_sig = normalized_name_signature(pcs_name)
    if giro_sig and giro_sig == pcs_sig:
        return 0.90, "auto_name_signature"

    ratio = SequenceMatcher(None, giro_norm, pcs_norm).ratio()
    if ratio >= 0.90:
        return 0.78, "auto_fuzzy_high"
    if ratio >= 0.84:
        return 0.70, "auto_fuzzy_medium"
    return 0.0, "auto_fuzzy_low"


def score_giro_mapping_candidate(giro: GiroPersonProfile, pcs: PcsRiderProfile) -> tuple[float, str]:
    base_score, source = _base_name_score(giro.rider_name, pcs.rider_name)
    if base_score == 0:
        return 0.0, source

    score = base_score
    giro_team_norm = normalize_text(giro.team_name)
    pcs_teams_norm = _normalized_team_set(pcs.team_names)
    if giro_team_norm and giro_team_norm in pcs_teams_norm:
        score += 0.07
        source = f"{source}+team"
    return min(score, 0.9999), source


def _suggestion_status(score: float, score_gap: float) -> str:
    if score >= 0.95 and score_gap >= 0.05:
        return "APPROVED"
    if score >= 0.70:
        return "PENDING"
    return "REJECTED"


def build_giro_mapping_suggestions(
    giro_rows: list[GiroPersonProfile],
    pcs_rows: list[PcsRiderProfile],
    max_candidates_per_person: int = 3,
) -> list[GiroMappingSuggestion]:
    if max_candidates_per_person < 1:
        raise ValueError("max_candidates_per_person must be >= 1")

    suggestions: list[GiroMappingSuggestion] = []
    for giro in giro_rows:
        candidates: list[tuple[float, str, PcsRiderProfile]] = []
        for pcs in pcs_rows:
            score, source = score_giro_mapping_candidate(giro, pcs)
            if score <= 0:
                continue
            candidates.append((score, source, pcs))

        if not candidates:
            continue

        candidates.sort(key=lambda item: (-item[0], item[2].pcs_rider_id))
        top = candidates[:max_candidates_per_person]
        second_score = top[1][0] if len(top) > 1 else 0.0
        for rank, (score, source, pcs) in enumerate(top, start=1):
            status = _suggestion_status(score, score - second_score if rank == 1 else 0.0)
            suggestions.append(
                GiroMappingSuggestion(
                    game_id=giro.game_id,
                    holdet_person_id=giro.holdet_person_id,
                    holdet_player_id=giro.holdet_player_id,
                    holdet_rider_name=giro.rider_name,
                    holdet_team_name=giro.team_name,
                    position_title=giro.position_title,
                    pcs_rider_id=pcs.pcs_rider_id,
                    pcs_rider_name=pcs.rider_name,
                    score=round(score, 4),
                    suggestion_rank=rank,
                    status=status,
                    mapping_source=source,
                )
            )

    suggestions.sort(
        key=lambda row: (row.holdet_person_id, row.suggestion_rank, -row.score, row.pcs_rider_id)
    )
    return suggestions


class GiroMappingRepository(GiroRepository):
    def __init__(self, client: PostgresClient) -> None:
        super().__init__(client)
        self.core_repository = Repository(client=client)

    def list_latest_giro_person_profiles(self, approved_only_unmapped: bool = True) -> list[GiroPersonProfile]:
        unmapped_filter = ""
        if approved_only_unmapped:
            unmapped_filter = "WHERE gpm.holdet_person_id IS NULL"

        sql = f"""
            WITH latest_snapshot AS (
                SELECT DISTINCT ON (game_id)
                    game_id,
                    snapshot_id,
                    fetched_at
                FROM giro_player_pool_snapshot
                ORDER BY game_id, fetched_at DESC, snapshot_id DESC
            ),
            latest_rows AS (
                SELECT
                    gg.game_id,
                    gg.cartridge_slug,
                    gg.game_mode,
                    e.holdet_player_id,
                    e.holdet_person_id,
                    hp.rider_name,
                    e.position_title,
                    e.holdet_team_name
                FROM latest_snapshot ls
                JOIN giro_game gg ON gg.game_id = ls.game_id
                JOIN giro_player_pool_entry e
                  ON e.snapshot_id = ls.snapshot_id
                 AND e.game_id = ls.game_id
                JOIN giro_holdet_person hp ON hp.holdet_person_id = e.holdet_person_id
            )
            SELECT DISTINCT ON (lr.holdet_person_id)
                lr.game_id,
                lr.cartridge_slug,
                lr.holdet_player_id,
                lr.holdet_person_id,
                lr.rider_name,
                lr.position_title,
                lr.holdet_team_name
            FROM latest_rows lr
            LEFT JOIN giro_person_pcs_map gpm ON gpm.holdet_person_id = lr.holdet_person_id
            {unmapped_filter}
            ORDER BY
                lr.holdet_person_id,
                CASE WHEN lr.game_mode = 'MANAGER' THEN 0 ELSE 1 END,
                lr.cartridge_slug,
                lr.holdet_player_id;
        """
        rows = self.client.query_rows(sql)
        return [
            GiroPersonProfile(
                game_id=int(row[0]),
                cartridge_slug=row[1],
                holdet_player_id=int(row[2]),
                holdet_person_id=int(row[3]),
                rider_name=row[4],
                position_title=row[5],
                team_name=row[6],
            )
            for row in rows
        ]

    def list_pcs_rider_profiles(self) -> list[PcsRiderProfile]:
        return self.core_repository.list_pcs_rider_profiles()

    def replace_giro_mapping_suggestions(self, suggestions: list[GiroMappingSuggestion]) -> int:
        self.client.execute("DELETE FROM giro_person_pcs_map_suggestion;")
        inserted = 0
        for suggestion in suggestions:
            sql = f"""
                INSERT INTO giro_person_pcs_map_suggestion (
                    game_id,
                    holdet_person_id,
                    holdet_player_id,
                    holdet_rider_name,
                    holdet_team_name,
                    position_title,
                    pcs_rider_id,
                    pcs_rider_name,
                    score,
                    suggestion_rank,
                    status,
                    mapping_source
                )
                VALUES (
                    {_sql_literal(suggestion.game_id)},
                    {_sql_literal(suggestion.holdet_person_id)},
                    {_sql_literal(suggestion.holdet_player_id)},
                    {_sql_literal(suggestion.holdet_rider_name)},
                    {_sql_literal(suggestion.holdet_team_name)},
                    {_sql_literal(suggestion.position_title)},
                    {_sql_literal(suggestion.pcs_rider_id)},
                    {_sql_literal(suggestion.pcs_rider_name)},
                    {_sql_literal(suggestion.score)},
                    {_sql_literal(suggestion.suggestion_rank)},
                    {_sql_literal(suggestion.status)},
                    {_sql_literal(suggestion.mapping_source)}
                )
                ON CONFLICT (game_id, holdet_person_id, pcs_rider_id)
                DO UPDATE SET
                    holdet_player_id = EXCLUDED.holdet_player_id,
                    holdet_rider_name = EXCLUDED.holdet_rider_name,
                    holdet_team_name = EXCLUDED.holdet_team_name,
                    position_title = EXCLUDED.position_title,
                    pcs_rider_name = EXCLUDED.pcs_rider_name,
                    score = EXCLUDED.score,
                    suggestion_rank = EXCLUDED.suggestion_rank,
                    status = EXCLUDED.status,
                    mapping_source = EXCLUDED.mapping_source,
                    created_at = now();
            """
            self.client.execute(sql)
            inserted += 1
        return inserted

    def upsert_giro_person_map(
        self,
        holdet_person_id: int,
        pcs_rider_id: str,
        status: str,
        confidence: float | None,
        mapping_source: str,
        note: str | None = None,
    ) -> None:
        sql = f"""
            INSERT INTO giro_person_pcs_map (
                holdet_person_id,
                pcs_rider_id,
                status,
                confidence,
                mapping_source,
                note,
                mapped_at
            )
            VALUES (
                {_sql_literal(holdet_person_id)},
                {_sql_literal(pcs_rider_id)},
                {_sql_literal(status)},
                {_sql_literal(confidence)},
                {_sql_literal(mapping_source)},
                {_sql_literal(note)},
                now()
            )
            ON CONFLICT (holdet_person_id)
            DO UPDATE SET
                pcs_rider_id = EXCLUDED.pcs_rider_id,
                status = EXCLUDED.status,
                confidence = EXCLUDED.confidence,
                mapping_source = EXCLUDED.mapping_source,
                note = EXCLUDED.note,
                mapped_at = now();
        """
        self.client.execute(sql)

    def get_top_giro_mapping_suggestion(self, holdet_person_id: int) -> GiroMappingSuggestion | None:
        sql = f"""
            SELECT
                game_id,
                holdet_person_id,
                holdet_player_id,
                holdet_rider_name,
                holdet_team_name,
                position_title,
                pcs_rider_id,
                pcs_rider_name,
                score,
                suggestion_rank,
                status,
                mapping_source
            FROM giro_person_pcs_map_suggestion
            WHERE holdet_person_id = {holdet_person_id}
            ORDER BY suggestion_rank ASC, score DESC, game_id ASC
            LIMIT 1;
        """
        rows = self.client.query_rows(sql)
        if not rows:
            return None
        row = rows[0]
        return GiroMappingSuggestion(
            game_id=int(row[0]),
            holdet_person_id=int(row[1]),
            holdet_player_id=int(row[2]),
            holdet_rider_name=row[3],
            holdet_team_name=row[4],
            position_title=row[5],
            pcs_rider_id=row[6],
            pcs_rider_name=row[7],
            score=float(row[8]),
            suggestion_rank=int(row[9]),
            status=row[10],
            mapping_source=row[11],
        )

    def apply_auto_approved_giro_mapping_suggestions(self) -> int:
        sql = """
            SELECT DISTINCT ON (holdet_person_id)
                holdet_person_id,
                pcs_rider_id,
                score,
                mapping_source
            FROM giro_person_pcs_map_suggestion
            WHERE suggestion_rank = 1
              AND status = 'APPROVED'
            ORDER BY holdet_person_id, game_id ASC;
        """
        rows = self.client.query_rows(sql)
        for row in rows:
            self.upsert_giro_person_map(
                holdet_person_id=int(row[0]),
                pcs_rider_id=row[1],
                status="APPROVED",
                confidence=float(row[2]),
                mapping_source=row[3],
                note="auto-approved from suggestion",
            )
        return len(rows)

    def get_giro_mapping_summary(self) -> dict[str, int]:
        sql = """
            WITH latest_snapshot AS (
                SELECT DISTINCT ON (game_id)
                    game_id,
                    snapshot_id
                FROM giro_player_pool_snapshot
                ORDER BY game_id, fetched_at DESC, snapshot_id DESC
            ),
            latest_person_scope AS (
                SELECT DISTINCT e.holdet_person_id
                FROM latest_snapshot ls
                JOIN giro_player_pool_entry e
                  ON e.snapshot_id = ls.snapshot_id
                 AND e.game_id = ls.game_id
            )
            SELECT
                (SELECT COUNT(*) FROM latest_person_scope) AS latest_person_rows,
                (SELECT COUNT(*) FROM latest_person_scope lps
                   JOIN giro_person_pcs_map gpm ON gpm.holdet_person_id = lps.holdet_person_id
                  WHERE gpm.status = 'APPROVED') AS approved_mapped,
                (SELECT COUNT(*) FROM latest_person_scope lps
                   JOIN giro_person_pcs_map gpm ON gpm.holdet_person_id = lps.holdet_person_id
                  WHERE gpm.status = 'PENDING') AS pending_mapped,
                (SELECT COUNT(*) FROM latest_person_scope lps
                   JOIN giro_person_pcs_map gpm ON gpm.holdet_person_id = lps.holdet_person_id
                  WHERE gpm.status = 'REJECTED') AS rejected_mapped,
                (SELECT COUNT(DISTINCT holdet_person_id)
                   FROM giro_person_pcs_map_suggestion
                  WHERE suggestion_rank = 1) AS top_suggestions;
        """
        row = self.client.query_rows(sql)[0]
        return {
            "latest_person_rows": int(row[0]),
            "approved_mapped": int(row[1]),
            "pending_mapped": int(row[2]),
            "rejected_mapped": int(row[3]),
            "top_suggestions": int(row[4]),
        }


class GiroMappingService:
    def __init__(self, repository: GiroMappingRepository) -> None:
        self.repository = repository

    def suggest(self, auto_approve: bool = True, max_candidates_per_person: int = 3) -> dict[str, int]:
        self.repository.init_schema()
        giro_profiles = self.repository.list_latest_giro_person_profiles(approved_only_unmapped=True)
        pcs_profiles = self.repository.list_pcs_rider_profiles()
        suggestions = build_giro_mapping_suggestions(
            giro_rows=giro_profiles,
            pcs_rows=pcs_profiles,
            max_candidates_per_person=max_candidates_per_person,
        )
        created = self.repository.replace_giro_mapping_suggestions(suggestions)
        auto_approved = 0
        if auto_approve:
            auto_approved = self.repository.apply_auto_approved_giro_mapping_suggestions()
        summary = self.repository.get_giro_mapping_summary()
        summary["mapping_suggestions_created"] = created
        summary["manager_mappings_auto_approved"] = auto_approved
        return summary

    def approve(
        self,
        holdet_person_id: int,
        pcs_rider_id: str,
        confidence: float | None = None,
        note: str | None = None,
    ) -> None:
        self.repository.init_schema()
        self.repository.upsert_giro_person_map(
            holdet_person_id=holdet_person_id,
            pcs_rider_id=pcs_rider_id,
            status="APPROVED",
            confidence=confidence,
            mapping_source="manual",
            note=note,
        )

    def reject(self, holdet_person_id: int, note: str | None = None) -> None:
        self.repository.init_schema()
        top = self.repository.get_top_giro_mapping_suggestion(holdet_person_id=holdet_person_id)
        if top is None:
            raise ValueError(f"No Giro mapping suggestion found for holdet_person_id={holdet_person_id}")
        self.repository.upsert_giro_person_map(
            holdet_person_id=holdet_person_id,
            pcs_rider_id=top.pcs_rider_id,
            status="REJECTED",
            confidence=top.score,
            mapping_source="manual_reject",
            note=note or f"Rejected suggested mapping to {top.pcs_rider_id}",
        )

    def status(self) -> dict[str, int]:
        self.repository.init_schema()
        return self.repository.get_giro_mapping_summary()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="giro-mapping")
    parser.add_argument("--db-url", help="PostgreSQL connection URL. Falls back to env vars.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    suggest_parser = subparsers.add_parser("suggest", help="Generate Giro person->PCS mapping suggestions.")
    suggest_parser.add_argument("--max-candidates", type=int, default=3)
    suggest_parser.add_argument(
        "--auto-approve",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Auto-approve only high-confidence suggestions.",
    )

    approve_parser = subparsers.add_parser("approve", help="Approve a Giro mapping manually.")
    approve_parser.add_argument("--holdet-person-id", type=int, required=True)
    approve_parser.add_argument("--pcs-rider-id", required=True)
    approve_parser.add_argument("--confidence", type=float)
    approve_parser.add_argument("--note")

    reject_parser = subparsers.add_parser("reject", help="Reject top suggestion for a Giro rider.")
    reject_parser.add_argument("--holdet-person-id", type=int, required=True)
    reject_parser.add_argument("--note")

    subparsers.add_parser("status", help="Show Giro mapping status.")
    return parser


def _print_summary(summary: dict[str, int]) -> None:
    for key, value in summary.items():
        print(f"  {key}: {value}")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    settings = load_settings(db_url=args.db_url)
    repository = GiroMappingRepository(client=PostgresClient(db_url=settings.db_url))
    service = GiroMappingService(repository=repository)

    if args.command == "suggest":
        summary = service.suggest(
            auto_approve=args.auto_approve,
            max_candidates_per_person=args.max_candidates,
        )
        print("Giro mapping suggestions generated.")
        _print_summary(summary)
        return 0

    if args.command == "approve":
        service.approve(
            holdet_person_id=args.holdet_person_id,
            pcs_rider_id=args.pcs_rider_id,
            confidence=args.confidence,
            note=args.note,
        )
        print(
            f"Approved Giro mapping holdet_person_id={args.holdet_person_id} -> "
            f"pcs_rider_id={args.pcs_rider_id}"
        )
        return 0

    if args.command == "reject":
        service.reject(holdet_person_id=args.holdet_person_id, note=args.note)
        print(f"Rejected top suggestion for holdet_person_id={args.holdet_person_id}")
        return 0

    if args.command == "status":
        print("Giro mapping status.")
        _print_summary(service.status())
        return 0

    parser.error("Unknown command.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

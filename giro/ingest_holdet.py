from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from fantasy_cycling.config import load_settings
from fantasy_cycling.db import PostgresClient, Repository
from fantasy_cycling.fetcher import fetch_json

HOLDET_API_BASE_URL = "https://nexus-app-fantasy-fargate.holdet.dk"


@dataclass(frozen=True)
class GiroGameConfig:
    cartridge_slug: str
    game_mode: str
    allows_stage_substitutions: bool


@dataclass(frozen=True)
class GiroPlayerRow:
    holdet_player_id: int
    holdet_person_id: int
    first_name: str
    last_name: str
    rider_name: str
    holdet_team_id: int | None
    holdet_team_name: str
    position_id: int
    position_name: str
    position_title: str
    start_price: int | None
    price: int | None
    points: float | None
    popularity: float | None
    is_out: bool


@dataclass(frozen=True)
class GiroIngestSummary:
    cartridge_slug: str
    game_id: int
    snapshot_id: int
    player_rows: int


GIRO_GAMES: tuple[GiroGameConfig, ...] = (
    GiroGameConfig(
        cartridge_slug="giro-d-italia-manager-2026",
        game_mode="MANAGER",
        allows_stage_substitutions=False,
    ),
    GiroGameConfig(
        cartridge_slug="giro-d-italia-2026",
        game_mode="TRADING",
        allows_stage_substitutions=True,
    ),
)


def build_holdet_cartridge_url(cartridge_slug: str) -> str:
    return f"{HOLDET_API_BASE_URL}/api/cartridges/{cartridge_slug}"


def build_holdet_game_url(game_id: int) -> str:
    return f"{HOLDET_API_BASE_URL}/api/games/{game_id}"


def build_holdet_players_url(game_id: int) -> str:
    return f"{HOLDET_API_BASE_URL}/api/games/{game_id}/players"


def _sql_literal(value: str | int | float | bool | None) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return format(value, ".12g")
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _sql_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("Timestamp must be timezone aware.")
    return _sql_literal(value.isoformat())


def _sql_jsonb(value: dict[str, object]) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return f"{_sql_literal(payload)}::jsonb"


def _schema_paths() -> tuple[Path, Path]:
    root = Path(__file__).resolve().parent.parent
    return root / "fantasy_cycling" / "schema.sql", root / "giro" / "schema.sql"


def _require_dict(parent: dict[str, object], key: str) -> dict[str, object]:
    value = parent.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"Expected object for key '{key}'.")
    return value


def _require_int(parent: dict[str, object], key: str) -> int:
    value = parent.get(key)
    if isinstance(value, bool):
        raise ValueError(f"Invalid boolean value for key '{key}'.")
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as error:
        raise ValueError(f"Expected integer for key '{key}'.") from error


def _coerce_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_rider_name(person_payload: dict[str, object]) -> tuple[str, str, str]:
    first_name = str(person_payload.get("firstName") or "").strip()
    last_name = str(person_payload.get("lastName") or "").strip()
    rider_name = " ".join(piece for piece in (first_name, last_name) if piece).strip()
    if not rider_name:
        raise ValueError("Missing rider name in person payload.")
    return first_name, last_name, rider_name


def parse_giro_player_pool_payload(payload: dict[str, object]) -> list[GiroPlayerRow]:
    items = payload.get("items")
    embedded = payload.get("_embedded")
    if not isinstance(items, list):
        raise ValueError("Expected payload['items'] to be a list.")
    if not isinstance(embedded, dict):
        raise ValueError("Expected payload['_embedded'] to be an object.")

    persons = embedded.get("persons")
    teams = embedded.get("teams")
    positions = embedded.get("positions")
    if not isinstance(persons, dict) or not isinstance(teams, dict) or not isinstance(positions, dict):
        raise ValueError("Expected persons, teams, and positions maps in payload['_embedded'].")

    rows: list[GiroPlayerRow] = []
    seen_player_ids: set[int] = set()
    for item in items:
        if not isinstance(item, dict):
            continue

        holdet_player_id = _require_int(item, "id")
        holdet_person_id = _require_int(item, "personId")
        if holdet_player_id in seen_player_ids:
            continue

        team_key = str(item.get("teamId") or "").strip()
        position_key = str(item.get("positionId") or "").strip()
        if not team_key or not position_key:
            continue

        person_payload = persons.get(str(holdet_person_id))
        team_payload = teams.get(team_key)
        position_payload = positions.get(position_key)
        if not isinstance(person_payload, dict) or not isinstance(team_payload, dict) or not isinstance(
            position_payload, dict
        ):
            continue

        first_name, last_name, rider_name = _build_rider_name(person_payload)
        holdet_team_name = str(team_payload.get("name") or "").strip()
        position_name = str(position_payload.get("name") or "").strip()
        position_title = str(position_payload.get("title") or "").strip()
        if not holdet_team_name or not position_name or not position_title:
            continue

        rows.append(
            GiroPlayerRow(
                holdet_player_id=holdet_player_id,
                holdet_person_id=holdet_person_id,
                first_name=first_name,
                last_name=last_name,
                rider_name=rider_name,
                holdet_team_id=_coerce_int(item.get("teamId")),
                holdet_team_name=holdet_team_name,
                position_id=_require_int(position_payload, "id"),
                position_name=position_name,
                position_title=position_title,
                start_price=_coerce_int(item.get("startPrice")),
                price=_coerce_int(item.get("price")),
                points=_coerce_float(item.get("points")),
                popularity=_coerce_float(item.get("popularity")),
                is_out=bool(item.get("isOut", False)),
            )
        )
        seen_player_ids.add(holdet_player_id)

    rows.sort(key=lambda row: (row.position_title, row.holdet_team_name, row.rider_name))
    return rows


class GiroRepository:
    def __init__(self, client: PostgresClient) -> None:
        self.client = client

    def init_schema(self) -> None:
        core_schema_path, giro_schema_path = _schema_paths()
        repository = Repository(client=self.client)
        repository.init_schema(core_schema_path)
        repository.init_schema(giro_schema_path)

    def store_raw_payload(
        self,
        source_url: str,
        payload: dict[str, object],
        fetched_at: datetime,
        parser_version: str,
    ) -> int:
        canonical_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        payload_sha256 = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
        sql = f"""
            INSERT INTO giro_raw_payload_archive (
                source_url,
                payload_sha256,
                payload_json,
                fetched_at,
                parser_version
            )
            VALUES (
                {_sql_literal(source_url)},
                {_sql_literal(payload_sha256)},
                {_sql_jsonb(payload)},
                {_sql_timestamp(fetched_at)},
                {_sql_literal(parser_version)}
            )
            ON CONFLICT (source_url, payload_sha256)
            DO UPDATE SET
                fetched_at = EXCLUDED.fetched_at,
                parser_version = EXCLUDED.parser_version
            RETURNING raw_payload_id;
        """
        result = self.client.query_scalar(sql)
        if result is None:
            raise RuntimeError(f"Could not archive raw payload for {source_url}")
        return int(result)

    def upsert_game(
        self,
        config: GiroGameConfig,
        source_url: str,
        cartridge_payload: dict[str, object],
        game_payload: dict[str, object],
        fetched_at: datetime,
    ) -> int:
        game_id = _require_int(cartridge_payload, "gameId")
        cartridge_id = _coerce_int(cartridge_payload.get("id"))
        game_name = str(cartridge_payload.get("name") or config.cartridge_slug).strip()
        edition_id = _coerce_int(game_payload.get("editionId"))
        stream_id = _coerce_int(game_payload.get("streamId"))

        embedded = _require_dict(cartridge_payload, "_embedded")
        games = _require_dict(embedded, "games")
        rulesets = _require_dict(embedded, "rulesets")
        game_info = _require_dict(games, str(game_id))
        ruleset_id = _require_int(game_info, "rulesetId")
        ruleset = _require_dict(rulesets, str(ruleset_id))
        ruleset_name = str(ruleset.get("name") or "").strip()
        if not ruleset_name:
            raise ValueError(f"Missing ruleset name for game_id={game_id}")

        sql = f"""
            INSERT INTO giro_game (
                game_id,
                cartridge_slug,
                cartridge_id,
                game_name,
                edition_id,
                stream_id,
                ruleset_id,
                ruleset_name,
                game_mode,
                allows_stage_substitutions,
                salary_cap,
                transfer_fee,
                interest_rate,
                captain_bonus_assets,
                captain_bonus_points,
                source_url,
                fetched_at,
                updated_at
            )
            VALUES (
                {_sql_literal(game_id)},
                {_sql_literal(config.cartridge_slug)},
                {_sql_literal(cartridge_id)},
                {_sql_literal(game_name)},
                {_sql_literal(edition_id)},
                {_sql_literal(stream_id)},
                {_sql_literal(ruleset_id)},
                {_sql_literal(ruleset_name)},
                {_sql_literal(config.game_mode)},
                {_sql_literal(config.allows_stage_substitutions)},
                {_sql_literal(_coerce_int(ruleset.get("salaryCap")))},
                {_sql_literal(_coerce_float(ruleset.get("transferFee")))},
                {_sql_literal(_coerce_float(ruleset.get("interestRate")))},
                {_sql_literal(_coerce_int(ruleset.get("captainBonusAssets")))},
                {_sql_literal(_coerce_int(ruleset.get("captainBonusPoints")))},
                {_sql_literal(source_url)},
                {_sql_timestamp(fetched_at)},
                {_sql_timestamp(fetched_at)}
            )
            ON CONFLICT (game_id)
            DO UPDATE SET
                cartridge_slug = EXCLUDED.cartridge_slug,
                cartridge_id = EXCLUDED.cartridge_id,
                game_name = EXCLUDED.game_name,
                edition_id = EXCLUDED.edition_id,
                stream_id = EXCLUDED.stream_id,
                ruleset_id = EXCLUDED.ruleset_id,
                ruleset_name = EXCLUDED.ruleset_name,
                game_mode = EXCLUDED.game_mode,
                allows_stage_substitutions = EXCLUDED.allows_stage_substitutions,
                salary_cap = EXCLUDED.salary_cap,
                transfer_fee = EXCLUDED.transfer_fee,
                interest_rate = EXCLUDED.interest_rate,
                captain_bonus_assets = EXCLUDED.captain_bonus_assets,
                captain_bonus_points = EXCLUDED.captain_bonus_points,
                source_url = EXCLUDED.source_url,
                fetched_at = EXCLUDED.fetched_at,
                updated_at = EXCLUDED.updated_at;
        """
        self.client.execute(sql)
        self.replace_game_positions(game_id=game_id, ruleset=ruleset)
        return game_id

    def replace_game_positions(self, game_id: int, ruleset: dict[str, object]) -> None:
        positions = ruleset.get("positions")
        if not isinstance(positions, list):
            raise ValueError(f"Expected positions list for game_id={game_id}")

        delete_sql = f"DELETE FROM giro_game_position WHERE game_id = {_sql_literal(game_id)};"
        self.client.execute(delete_sql)

        for item in positions:
            if not isinstance(item, dict):
                continue
            sql = f"""
                INSERT INTO giro_game_position (
                    game_id,
                    position_id,
                    position_name,
                    position_title,
                    position_order
                )
                VALUES (
                    {_sql_literal(game_id)},
                    {_sql_literal(_require_int(item, "id"))},
                    {_sql_literal(str(item.get("name") or "").strip())},
                    {_sql_literal(str(item.get("title") or "").strip())},
                    {_sql_literal(_require_int(item, "order"))}
                );
            """
            self.client.execute(sql)

    def store_player_pool_snapshot(
        self,
        game_id: int,
        source_url: str,
        raw_payload_id: int,
        fetched_at: datetime,
    ) -> int:
        sql = f"""
            INSERT INTO giro_player_pool_snapshot (
                game_id,
                raw_payload_id,
                source_url,
                fetched_at
            )
            VALUES (
                {_sql_literal(game_id)},
                {_sql_literal(raw_payload_id)},
                {_sql_literal(source_url)},
                {_sql_timestamp(fetched_at)}
            )
            ON CONFLICT (game_id, raw_payload_id)
            DO UPDATE SET
                source_url = EXCLUDED.source_url,
                fetched_at = EXCLUDED.fetched_at
            RETURNING snapshot_id;
        """
        result = self.client.query_scalar(sql)
        if result is None:
            raise RuntimeError(f"Could not store Giro player pool snapshot for game_id={game_id}")
        return int(result)

    def upsert_holdet_person(self, row: GiroPlayerRow) -> None:
        sql = f"""
            INSERT INTO giro_holdet_person (
                holdet_person_id,
                first_name,
                last_name,
                rider_name,
                updated_at
            )
            VALUES (
                {_sql_literal(row.holdet_person_id)},
                {_sql_literal(row.first_name)},
                {_sql_literal(row.last_name)},
                {_sql_literal(row.rider_name)},
                now()
            )
            ON CONFLICT (holdet_person_id)
            DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                rider_name = EXCLUDED.rider_name,
                updated_at = now();
        """
        self.client.execute(sql)

    def upsert_player_pool_entry(self, snapshot_id: int, game_id: int, row: GiroPlayerRow) -> None:
        sql = f"""
            INSERT INTO giro_player_pool_entry (
                snapshot_id,
                game_id,
                holdet_player_id,
                holdet_person_id,
                holdet_team_id,
                holdet_team_name,
                position_id,
                position_name,
                position_title,
                start_price,
                price,
                points,
                popularity,
                is_out
            )
            VALUES (
                {_sql_literal(snapshot_id)},
                {_sql_literal(game_id)},
                {_sql_literal(row.holdet_player_id)},
                {_sql_literal(row.holdet_person_id)},
                {_sql_literal(row.holdet_team_id)},
                {_sql_literal(row.holdet_team_name)},
                {_sql_literal(row.position_id)},
                {_sql_literal(row.position_name)},
                {_sql_literal(row.position_title)},
                {_sql_literal(row.start_price)},
                {_sql_literal(row.price)},
                {_sql_literal(row.points)},
                {_sql_literal(row.popularity)},
                {_sql_literal(row.is_out)}
            )
            ON CONFLICT (snapshot_id, holdet_player_id)
            DO UPDATE SET
                game_id = EXCLUDED.game_id,
                holdet_person_id = EXCLUDED.holdet_person_id,
                holdet_team_id = EXCLUDED.holdet_team_id,
                holdet_team_name = EXCLUDED.holdet_team_name,
                position_id = EXCLUDED.position_id,
                position_name = EXCLUDED.position_name,
                position_title = EXCLUDED.position_title,
                start_price = EXCLUDED.start_price,
                price = EXCLUDED.price,
                points = EXCLUDED.points,
                popularity = EXCLUDED.popularity,
                is_out = EXCLUDED.is_out;
        """
        self.client.execute(sql)


class GiroHoldetIngestionService:
    def __init__(
        self,
        repository: GiroRepository,
        parser_version: str,
        timeout_seconds: int,
        user_agent: str,
        timezone,
    ) -> None:
        self.repository = repository
        self.parser_version = parser_version
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent
        self.timezone = timezone

    def ingest_games(self, configs: list[GiroGameConfig]) -> list[GiroIngestSummary]:
        summaries: list[GiroIngestSummary] = []
        self.repository.init_schema()

        for config in configs:
            fetched_at = datetime.now(self.timezone)
            cartridge_url = build_holdet_cartridge_url(config.cartridge_slug)
            cartridge_payload = fetch_json(
                url=cartridge_url,
                timeout_seconds=self.timeout_seconds,
                user_agent=self.user_agent,
            )
            game_id = _require_int(cartridge_payload, "gameId")

            game_url = build_holdet_game_url(game_id)
            game_payload = fetch_json(
                url=game_url,
                timeout_seconds=self.timeout_seconds,
                user_agent=self.user_agent,
            )
            players_url = build_holdet_players_url(game_id)
            players_payload = fetch_json(
                url=players_url,
                timeout_seconds=self.timeout_seconds,
                user_agent=self.user_agent,
            )
            rows = parse_giro_player_pool_payload(players_payload)

            self.repository.store_raw_payload(
                source_url=cartridge_url,
                payload=cartridge_payload,
                fetched_at=fetched_at,
                parser_version=self.parser_version,
            )
            self.repository.store_raw_payload(
                source_url=game_url,
                payload=game_payload,
                fetched_at=fetched_at,
                parser_version=self.parser_version,
            )
            raw_payload_id = self.repository.store_raw_payload(
                source_url=players_url,
                payload=players_payload,
                fetched_at=fetched_at,
                parser_version=self.parser_version,
            )

            self.repository.upsert_game(
                config=config,
                source_url=cartridge_url,
                cartridge_payload=cartridge_payload,
                game_payload=game_payload,
                fetched_at=fetched_at,
            )
            snapshot_id = self.repository.store_player_pool_snapshot(
                game_id=game_id,
                source_url=players_url,
                raw_payload_id=raw_payload_id,
                fetched_at=fetched_at,
            )
            for row in rows:
                self.repository.upsert_holdet_person(row)
                self.repository.upsert_player_pool_entry(snapshot_id=snapshot_id, game_id=game_id, row=row)

            summaries.append(
                GiroIngestSummary(
                    cartridge_slug=config.cartridge_slug,
                    game_id=game_id,
                    snapshot_id=snapshot_id,
                    player_rows=len(rows),
                )
            )
        return summaries


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="giro-holdet-ingest")
    parser.add_argument("--db-url", help="PostgreSQL connection URL. Falls back to env vars.")
    parser.add_argument(
        "--cartridge",
        action="append",
        dest="cartridges",
        help="Specific Giro cartridge slug to ingest. Defaults to both Giro games.",
    )
    return parser


def _resolve_configs(cartridges: list[str] | None) -> list[GiroGameConfig]:
    if not cartridges:
        return list(GIRO_GAMES)
    by_slug = {config.cartridge_slug: config for config in GIRO_GAMES}
    resolved: list[GiroGameConfig] = []
    for slug in cartridges:
        config = by_slug.get(slug)
        if config is None:
            available = ", ".join(sorted(by_slug))
            raise ValueError(f"Unknown Giro cartridge '{slug}'. Available: {available}")
        resolved.append(config)
    return resolved


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    settings = load_settings(db_url=args.db_url)
    client = PostgresClient(db_url=settings.db_url)
    service = GiroHoldetIngestionService(
        repository=GiroRepository(client=client),
        parser_version=settings.parser_version,
        timeout_seconds=settings.request_timeout_seconds,
        user_agent=settings.user_agent,
        timezone=settings.timezone,
    )
    summaries = service.ingest_games(_resolve_configs(args.cartridges))

    print("Giro Holdet ingestion complete.")
    for summary in summaries:
        print(
            f"  cartridge={summary.cartridge_slug} "
            f"game_id={summary.game_id} "
            f"snapshot_id={summary.snapshot_id} "
            f"player_rows={summary.player_rows}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
import re

from fantasy_cycling.config import load_settings
from fantasy_cycling.db import PostgresClient
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, sync_playwright

from giro.ingest_holdet import GiroRepository, _sql_literal

PCS_BASE_URL = "https://www.procyclingstats.com"
PCS_RESULTS_URL_TEMPLATE = PCS_BASE_URL + "/rider/{pcs_rider_id}/results/all"
GRAND_TOUR_SLUGS = ("giro-d-italia", "tour-de-france", "vuelta-a-espana")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class MappedGiroRider:
    holdet_person_id: int
    holdet_rider_name: str
    pcs_rider_id: str
    pcs_rider_name: str


@dataclass(frozen=True)
class ExtractedResultRow:
    season: int
    result_date: date
    race_name: str
    result_label: str
    result_scope: str
    grand_tour_slug: str | None
    race_class: str | None
    rank_position: int | None
    raw_result: str
    kms: float | None
    pcs_points: float | None
    uci_points: float | None
    vertical_meters: int | None
    source_url: str | None
    source_rider_results_url: str


def _default_profile_dir() -> Path:
    return Path.home() / ".cache" / "fantasy-cycling" / "giro-playwright"


def _parse_date(value: str) -> date:
    year, month, day = value.strip().split("-")
    return date(int(year), int(month), int(day))


def _parse_int(value: str) -> int | None:
    normalized = value.strip()
    if not normalized or normalized == "-":
        return None
    if normalized.isdigit():
        return int(normalized)
    return None


def _parse_float(value: str) -> float | None:
    normalized = value.strip().replace(",", ".")
    if not normalized or normalized == "-":
        return None
    try:
        return float(normalized)
    except ValueError:
        return None


def _split_race_title(title: str) -> tuple[str, str]:
    if " | " in title:
        race_name, result_label = title.split(" | ", 1)
        return race_name.strip(), result_label.strip()
    normalized = title.strip()
    return normalized, normalized


def _grand_tour_slug_from_url(source_url: str | None) -> str | None:
    if not source_url:
        return None
    lower = source_url.lower()
    for slug in GRAND_TOUR_SLUGS:
        if f"/race/{slug}/" in lower:
            return slug
    return None


def _classify_scope(
    season: int,
    race_name: str,
    result_label: str,
    source_url: str | None,
    target_season: int,
    grand_tour_season: int,
) -> str:
    grand_tour_slug = _grand_tour_slug_from_url(source_url)
    label_lower = result_label.lower()

    if season == grand_tour_season and grand_tour_slug:
        if label_lower.startswith("stage "):
            return "GRAND_TOUR_STAGE"
        if label_lower == "general classification":
            return "GRAND_TOUR_GC"
        if label_lower == "points classification":
            return "GRAND_TOUR_POINTS"
        if label_lower == "mountains classification":
            return "GRAND_TOUR_KOM"
        if label_lower == "youth classification":
            return "GRAND_TOUR_YOUTH"
        return "OTHER"

    if season == target_season:
        return "SEASON_RACE"

    return "OTHER"


def _row_should_be_stored(row: ExtractedResultRow, target_season: int, grand_tour_season: int) -> bool:
    if row.season == target_season:
        return True
    if row.season == grand_tour_season and row.grand_tour_slug in GRAND_TOUR_SLUGS:
        return row.result_scope in {
            "GRAND_TOUR_STAGE",
            "GRAND_TOUR_GC",
            "GRAND_TOUR_POINTS",
            "GRAND_TOUR_KOM",
            "GRAND_TOUR_YOUTH",
        }
    return False


def parse_extracted_results(
    raw_rows: list[dict[str, object]],
    source_rider_results_url: str,
    target_season: int,
    grand_tour_season: int,
) -> list[ExtractedResultRow]:
    rows: list[ExtractedResultRow] = []
    for raw in raw_rows:
        cells = raw.get("cells")
        if not isinstance(cells, list) or len(cells) < 9:
            continue
        text_cells = [str(cell).strip() for cell in cells[:9]]
        if not DATE_RE.match(text_cells[1]):
            continue
        result_date = _parse_date(text_cells[1])
        season = result_date.year
        race_name, result_label = _split_race_title(text_cells[3])
        source_url = str(raw.get("href") or "").strip() or None
        grand_tour_slug = _grand_tour_slug_from_url(source_url)
        result_scope = _classify_scope(
            season=season,
            race_name=race_name,
            result_label=result_label,
            source_url=source_url,
            target_season=target_season,
            grand_tour_season=grand_tour_season,
        )
        row = ExtractedResultRow(
            season=season,
            result_date=result_date,
            race_name=race_name,
            result_label=result_label,
            result_scope=result_scope,
            grand_tour_slug=grand_tour_slug,
            race_class=text_cells[4] or None,
            rank_position=_parse_int(text_cells[2]),
            raw_result=text_cells[2],
            kms=_parse_float(text_cells[5]),
            pcs_points=_parse_float(text_cells[6]),
            uci_points=_parse_float(text_cells[7]),
            vertical_meters=_parse_int(text_cells[8]),
            source_url=source_url,
            source_rider_results_url=source_rider_results_url,
        )
        if _row_should_be_stored(row, target_season=target_season, grand_tour_season=grand_tour_season):
            rows.append(row)
    return rows


class GiroPcsHistoryRepository(GiroRepository):
    def list_mapped_giro_riders(self, pcs_rider_ids: list[str] | None = None) -> list[MappedGiroRider]:
        where_clause = "WHERE gp.status = 'APPROVED'"
        if pcs_rider_ids:
            ids = ", ".join(_sql_literal(value) for value in sorted(set(pcs_rider_ids)))
            where_clause += f" AND gp.pcs_rider_id IN ({ids})"

        sql = f"""
            SELECT
                gp.holdet_person_id,
                hp.rider_name,
                gp.pcs_rider_id,
                r.rider_name
            FROM giro_person_pcs_map gp
            JOIN giro_holdet_person hp ON hp.holdet_person_id = gp.holdet_person_id
            JOIN rider r ON r.pcs_rider_id = gp.pcs_rider_id
            {where_clause}
            ORDER BY hp.rider_name;
        """
        rows = self.client.query_rows(sql)
        return [
            MappedGiroRider(
                holdet_person_id=int(row[0]),
                holdet_rider_name=row[1],
                pcs_rider_id=row[2],
                pcs_rider_name=row[3],
            )
            for row in rows
        ]

    def list_successfully_imported_rider_ids(
        self,
        target_season: int,
        grand_tour_season: int,
    ) -> set[str]:
        sql = f"""
            SELECT pcs_rider_id
            FROM giro_pcs_history_import_status
            WHERE target_season = {_sql_literal(target_season)}
              AND grand_tour_season = {_sql_literal(grand_tour_season)}
              AND status = 'SUCCESS';
        """
        return {row[0] for row in self.client.query_rows(sql)}

    def list_rider_ids_with_existing_history(
        self,
        target_season: int,
        grand_tour_season: int,
    ) -> set[str]:
        sql = f"""
            SELECT DISTINCT pcs_rider_id
            FROM giro_pcs_rider_result
            WHERE season = {_sql_literal(target_season)}
               OR (
                    season = {_sql_literal(grand_tour_season)}
                AND grand_tour_slug IN ({", ".join(_sql_literal(slug) for slug in GRAND_TOUR_SLUGS)})
                AND result_scope IN (
                    'GRAND_TOUR_STAGE',
                    'GRAND_TOUR_GC',
                    'GRAND_TOUR_POINTS',
                    'GRAND_TOUR_KOM',
                    'GRAND_TOUR_YOUTH'
                )
               );
        """
        return {row[0] for row in self.client.query_rows(sql)}

    def upsert_import_status(
        self,
        pcs_rider_id: str,
        target_season: int,
        grand_tour_season: int,
        status: str,
        row_count: int,
        updated_at: datetime,
        source_rider_results_url: str | None = None,
        error_message: str | None = None,
    ) -> None:
        sql = f"""
            INSERT INTO giro_pcs_history_import_status (
                pcs_rider_id,
                target_season,
                grand_tour_season,
                status,
                row_count,
                source_rider_results_url,
                error_message,
                updated_at
            )
            VALUES (
                {_sql_literal(pcs_rider_id)},
                {_sql_literal(target_season)},
                {_sql_literal(grand_tour_season)},
                {_sql_literal(status)},
                {_sql_literal(row_count)},
                {_sql_literal(source_rider_results_url)},
                {_sql_literal(error_message)},
                {_sql_literal(updated_at.isoformat())}
            )
            ON CONFLICT (pcs_rider_id, target_season, grand_tour_season)
            DO UPDATE SET
                status = EXCLUDED.status,
                row_count = EXCLUDED.row_count,
                source_rider_results_url = EXCLUDED.source_rider_results_url,
                error_message = EXCLUDED.error_message,
                updated_at = EXCLUDED.updated_at;
        """
        self.client.execute(sql)

    def replace_rider_history(
        self,
        pcs_rider_id: str,
        fetched_at: datetime,
        target_season: int,
        grand_tour_season: int,
        rows: list[ExtractedResultRow],
    ) -> int:
        delete_sql = f"""
            DELETE FROM giro_pcs_rider_result
            WHERE pcs_rider_id = {_sql_literal(pcs_rider_id)}
              AND (
                    season = {_sql_literal(target_season)}
                 OR (
                        season = {_sql_literal(grand_tour_season)}
                    AND grand_tour_slug IN ({", ".join(_sql_literal(slug) for slug in GRAND_TOUR_SLUGS)})
                    AND result_scope IN (
                        'GRAND_TOUR_STAGE',
                        'GRAND_TOUR_GC',
                        'GRAND_TOUR_POINTS',
                        'GRAND_TOUR_KOM',
                        'GRAND_TOUR_YOUTH'
                    )
                 )
              );
        """
        self.client.execute(delete_sql)

        inserted = 0
        for row in rows:
            sql = f"""
                INSERT INTO giro_pcs_rider_result (
                    pcs_rider_id,
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
                    source_rider_results_url,
                    fetched_at
                )
                VALUES (
                    {_sql_literal(pcs_rider_id)},
                    {_sql_literal(row.season)},
                    {_sql_literal(row.result_date.isoformat())},
                    {_sql_literal(row.race_name)},
                    {_sql_literal(row.result_label)},
                    {_sql_literal(row.result_scope)},
                    {_sql_literal(row.grand_tour_slug)},
                    {_sql_literal(row.race_class)},
                    {_sql_literal(row.rank_position)},
                    {_sql_literal(row.raw_result)},
                    {_sql_literal(row.kms)},
                    {_sql_literal(row.pcs_points)},
                    {_sql_literal(row.uci_points)},
                    {_sql_literal(row.vertical_meters)},
                    {_sql_literal(row.source_url)},
                    {_sql_literal(row.source_rider_results_url)},
                    {_sql_literal(fetched_at.isoformat())}
                );
            """
            self.client.execute(sql)
            inserted += 1
        return inserted


class PcsRiderHistoryBrowser:
    def __init__(self, profile_dir: Path, headless: bool, timeout_ms: int = 60000) -> None:
        self.profile_dir = profile_dir.expanduser().resolve()
        self.headless = headless
        self.timeout_ms = timeout_ms

    def __enter__(self) -> "PcsRiderHistoryBrowser":
        self.profile_dir.mkdir(parents=True, exist_ok=True)
        self._storage_state_path = self.profile_dir / "storage_state.json"
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=self.headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context_kwargs = {"viewport": {"width": 1440, "height": 1200}}
        if self._storage_state_path.exists():
            context_kwargs["storage_state"] = str(self._storage_state_path)
        self._context = self._browser.new_context(**context_kwargs)
        self._page = self._context.new_page()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._context.storage_state(path=str(self._storage_state_path))
        self._context.close()
        self._browser.close()
        self._playwright.stop()

    def _dismiss_cookie_banner(self, page: Page) -> None:
        for label in ("Accept All", "Accept all", "I agree"):
            try:
                page.get_by_text(label, exact=True).click(timeout=2000)
                page.wait_for_timeout(1000)
                return
            except PlaywrightTimeoutError:
                continue

    def _has_unlock_gate(self, page: Page) -> bool:
        try:
            body_text = page.locator("body").inner_text(timeout=2000)
        except PlaywrightTimeoutError:
            return False
        markers = (
            "Unlock This Content!",
            "Watch an Ad & Continue",
            "Support us by watching a short ad",
            "Or Subscribe to PCS Pro",
        )
        return any(marker in body_text for marker in markers)

    def _wait_for_results_table(self, page: Page, pcs_rider_id: str) -> None:
        if page.locator("table.basic").count():
            return

        gate_notice_printed = False
        deadline_ms = self.timeout_ms + 240000
        waited_ms = 0
        while waited_ms <= deadline_ms:
            if page.locator("table.basic").count():
                return

            if self._has_unlock_gate(page) and not gate_notice_printed:
                print(
                    f"PCS unlock gate detected for {pcs_rider_id}. "
                    "Use the visible browser to press 'Watch an Ad & Continue' "
                    "or otherwise unlock the page. Import will resume automatically."
                )
                gate_notice_printed = True

            page.wait_for_timeout(2000)
            waited_ms += 2000

        title = page.title()
        body = page.locator("body").inner_text()[:1000]
        raise RuntimeError(
            f"PCS results table did not load for {pcs_rider_id}. title={title!r} body={body!r}"
        )

    def fetch_rider_results(self, pcs_rider_id: str) -> tuple[str, list[dict[str, object]]]:
        url = PCS_RESULTS_URL_TEMPLATE.format(pcs_rider_id=pcs_rider_id)
        page = self._page
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
        except PlaywrightTimeoutError:
            # PCS sometimes pauses on anti-bot or ad-unlock interstitials before
            # firing domcontentloaded. In visible-browser mode we can still keep
            # the partially loaded page and wait for the user/browser to clear it.
            pass
        self._dismiss_cookie_banner(page)
        page.wait_for_timeout(5000)
        self._wait_for_results_table(page, pcs_rider_id=pcs_rider_id)

        page.wait_for_timeout(1500)
        raw_rows = page.evaluate(
            """() => Array.from(document.querySelectorAll('table.basic tr')).slice(1).map((tr) => {
                const cells = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim())
                const link = tr.querySelector('a')
                return {
                    cells,
                    href: link ? link.href : null,
                }
            }).filter(row => row.cells.length >= 9)"""
        )
        if not isinstance(raw_rows, list):
            raise RuntimeError(f"Unexpected row payload for {pcs_rider_id}")
        return url, raw_rows


class GiroPcsHistoryImportService:
    def __init__(
        self,
        repository: GiroPcsHistoryRepository,
        timezone,
        profile_dir: Path,
        headless: bool,
    ) -> None:
        self.repository = repository
        self.timezone = timezone
        self.profile_dir = profile_dir
        self.headless = headless

    def import_history(
        self,
        target_season: int,
        grand_tour_season: int,
        pcs_rider_ids: list[str] | None = None,
        limit: int | None = None,
        force: bool = False,
    ) -> dict[str, int]:
        self.repository.init_schema()
        riders = self.repository.list_mapped_giro_riders(pcs_rider_ids=pcs_rider_ids)
        if not force:
            imported_ids = self.repository.list_successfully_imported_rider_ids(
                target_season=target_season,
                grand_tour_season=grand_tour_season,
            )
            imported_ids |= self.repository.list_rider_ids_with_existing_history(
                target_season=target_season,
                grand_tour_season=grand_tour_season,
            )
            riders = [rider for rider in riders if rider.pcs_rider_id not in imported_ids]
        if limit is not None:
            riders = riders[:limit]

        totals = {
            "riders_selected": len(riders),
            "riders_imported": 0,
            "rows_inserted": 0,
            "riders_failed": 0,
        }
        with PcsRiderHistoryBrowser(profile_dir=self.profile_dir, headless=self.headless) as browser:
            for rider in riders:
                fetched_at = datetime.now(self.timezone)
                try:
                    source_rider_results_url, raw_rows = browser.fetch_rider_results(rider.pcs_rider_id)
                    parsed_rows = parse_extracted_results(
                        raw_rows=raw_rows,
                        source_rider_results_url=source_rider_results_url,
                        target_season=target_season,
                        grand_tour_season=grand_tour_season,
                    )
                    inserted = self.repository.replace_rider_history(
                        pcs_rider_id=rider.pcs_rider_id,
                        fetched_at=fetched_at,
                        target_season=target_season,
                        grand_tour_season=grand_tour_season,
                        rows=parsed_rows,
                    )
                    self.repository.upsert_import_status(
                        pcs_rider_id=rider.pcs_rider_id,
                        target_season=target_season,
                        grand_tour_season=grand_tour_season,
                        status="SUCCESS",
                        row_count=inserted,
                        updated_at=fetched_at,
                        source_rider_results_url=source_rider_results_url,
                        error_message=None,
                    )
                    totals["riders_imported"] += 1
                    totals["rows_inserted"] += inserted
                    print(
                        f"Imported PCS history for {rider.holdet_rider_name} "
                        f"({rider.pcs_rider_id}) rows={inserted}"
                    )
                except Exception as error:
                    self.repository.upsert_import_status(
                        pcs_rider_id=rider.pcs_rider_id,
                        target_season=target_season,
                        grand_tour_season=grand_tour_season,
                        status="FAILED",
                        row_count=0,
                        updated_at=fetched_at,
                        source_rider_results_url=PCS_RESULTS_URL_TEMPLATE.format(
                            pcs_rider_id=rider.pcs_rider_id
                        ),
                        error_message=str(error)[:4000],
                    )
                    totals["riders_failed"] += 1
                    print(
                        f"Failed PCS history for {rider.holdet_rider_name} "
                        f"({rider.pcs_rider_id}): {error}"
                    )
        return totals


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="giro-pcs-history")
    parser.add_argument("--db-url", help="PostgreSQL connection URL. Falls back to env vars.")
    parser.add_argument("--season", type=int, default=2026, help="Season to import current rider race activity for.")
    parser.add_argument(
        "--grand-tour-season",
        type=int,
        default=2025,
        help="Season to import grand-tour stages and classifications from.",
    )
    parser.add_argument(
        "--pcs-rider-id",
        action="append",
        dest="pcs_rider_ids",
        help="Limit import to one or more mapped PCS rider ids.",
    )
    parser.add_argument("--limit", type=int, help="Limit the number of mapped riders imported.")
    parser.add_argument(
        "--profile-dir",
        default=str(_default_profile_dir()),
        help="Persistent Playwright browser profile directory.",
    )
    parser.add_argument(
        "--headless",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Run the Playwright browser headless. Default is false because PCS challenge handling is browser-sensitive.",
    )
    parser.add_argument(
        "--force",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Re-import riders even if a successful marker already exists for this season window.",
    )
    return parser


def _print_summary(summary: dict[str, int]) -> None:
    print("Giro PCS history import complete.")
    for key, value in summary.items():
        print(f"  {key}: {value}")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    settings = load_settings(db_url=args.db_url)
    service = GiroPcsHistoryImportService(
        repository=GiroPcsHistoryRepository(client=PostgresClient(db_url=settings.db_url)),
        timezone=settings.timezone,
        profile_dir=Path(args.profile_dir),
        headless=args.headless,
    )
    summary = service.import_history(
        target_season=args.season,
        grand_tour_season=args.grand_tour_season,
        pcs_rider_ids=args.pcs_rider_ids,
        limit=args.limit,
        force=args.force,
    )
    _print_summary(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

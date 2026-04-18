from __future__ import annotations

import json
import os
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd
import streamlit as st

RACES = [
    "Omloop Nieuwsblad",
    "Strade Bianche",
    "Milano-Sanremo",
    "Ronde Van Brugge - Tour of Bruges",
    "E3 Saxo Classic",
    "In Flanders Fields",
    "Dwars door Vlaanderen",
    "Ronde van Vlaanderen",
    "Paris-Roubaix",
    "Amstel Gold Race",
    "La Flèche Wallonne",
    "Liège-Bastogne-Liège",
    "Eschborn-Frankfurt",
]

CATEGORY_SLOTS = {
    "Kategori 1": 2,
    "Kategori 2": 3,
    "Kategori 3": 3,
    "Kategori 4": 4,
}

RACE_WEATHER_POINTS = {
    "Omloop Nieuwsblad": (
        {"label": "Start", "name": "Ghent", "query": "Ghent", "country_code": "BE"},
        {"label": "Finish", "name": "Ninove", "query": "Ninove", "country_code": "BE"},
    ),
    "Strade Bianche": (
        {"label": "Race hub", "name": "Siena", "query": "Siena", "country_code": "IT"},
    ),
    "Milano-Sanremo": (
        {"label": "Start", "name": "Milan", "query": "Milan", "country_code": "IT"},
        {"label": "Finish", "name": "Sanremo", "query": "Sanremo", "country_code": "IT"},
    ),
    "Ronde Van Brugge - Tour of Bruges": (
        {"label": "Race hub", "name": "Bruges", "query": "Bruges", "country_code": "BE"},
    ),
    "E3 Saxo Classic": (
        {"label": "Race hub", "name": "Harelbeke", "query": "Harelbeke", "country_code": "BE"},
    ),
    "In Flanders Fields": (
        {"label": "Race hub", "name": "Ypres", "query": "Ypres", "country_code": "BE"},
    ),
    "Dwars door Vlaanderen": (
        {"label": "Start", "name": "Roeselare", "query": "Roeselare", "country_code": "BE"},
        {"label": "Finish", "name": "Waregem", "query": "Waregem", "country_code": "BE"},
    ),
    "Ronde van Vlaanderen": (
        {"label": "Start", "name": "Bruges", "query": "Bruges", "country_code": "BE"},
        {"label": "Finish", "name": "Oudenaarde", "query": "Oudenaarde", "country_code": "BE"},
    ),
    "Paris-Roubaix": (
        {"label": "Start", "name": "Compiegne", "query": "Compiegne", "country_code": "FR"},
        {"label": "Mid-route", "name": "Orchies", "query": "Orchies", "country_code": "FR"},
        {"label": "Finish", "name": "Roubaix", "query": "Roubaix", "country_code": "FR"},
    ),
    "Amstel Gold Race": (
        {"label": "Race hub", "name": "Valkenburg", "query": "Valkenburg", "country_code": "NL"},
    ),
    "La Flèche Wallonne": (
        {"label": "Finish", "name": "Huy", "query": "Huy", "country_code": "BE"},
    ),
    "Liège-Bastogne-Liège": (
        {"label": "Start", "name": "Liege", "query": "Liege", "country_code": "BE"},
        {"label": "Turn", "name": "Bastogne", "query": "Bastogne", "country_code": "BE"},
        {"label": "Finish", "name": "Liege", "query": "Liege", "country_code": "BE"},
    ),
    "Eschborn-Frankfurt": (
        {"label": "Start", "name": "Eschborn", "query": "Eschborn", "country_code": "DE"},
        {"label": "Finish", "name": "Frankfurt", "query": "Frankfurt", "country_code": "DE"},
    ),
}

WEATHER_CODE_LABELS = {
    0: "Clear",
    1: "Mostly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Rime fog",
    51: "Light drizzle",
    53: "Drizzle",
    55: "Heavy drizzle",
    56: "Freezing drizzle",
    57: "Heavy freezing drizzle",
    61: "Light rain",
    63: "Rain",
    65: "Heavy rain",
    66: "Freezing rain",
    67: "Heavy freezing rain",
    71: "Light snow",
    73: "Snow",
    75: "Heavy snow",
    77: "Snow grains",
    80: "Rain showers",
    81: "Heavy showers",
    82: "Violent showers",
    85: "Snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm hail",
    99: "Severe thunderstorm hail",
}


@dataclass
class LockMatch:
    input_name: str
    pcs_rider_id: str
    rider_name: str
    kategori: str
    method: str
    score: float


@dataclass(frozen=True)
class WeatherPointForecast:
    label: str
    name: str
    current_temp_c: float | None
    current_precip_mm: float | None
    current_wind_kmh: float | None
    current_wind_direction_deg: float | None
    current_weather_label: str
    today_max_c: float | None
    today_min_c: float | None
    today_precip_mm: float | None
    today_wind_max_kmh: float | None


@st.cache_data(show_spinner=False)
def load_snapshot(snapshot_dir: str) -> tuple[dict[str, str], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    base = Path(snapshot_dir).expanduser().resolve()
    if not base.exists():
        raise RuntimeError(f"Snapshot directory not found: {base}")

    metadata_path = base / "metadata.json"
    if metadata_path.exists():
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        files = metadata.get("files", {})
    else:
        metadata = {}
        files = {}

    startlist_file = base / files.get("current_startlist", "current_startlist.csv")
    changes_file = base / files.get("startlist_changes", "startlist_changes.csv")
    history_file = base / files.get("race_history_rankings", "race_history_rankings.csv")
    manager_file = base / files.get("manager_riders_enriched", "manager_riders_enriched.csv")

    for path in [startlist_file, changes_file, history_file, manager_file]:
        if not path.exists():
            raise RuntimeError(f"Missing snapshot file: {path}")

    startlist_df = pd.read_csv(startlist_file)
    changes_df = pd.read_csv(changes_file)
    history_df = pd.read_csv(history_file)
    manager_df = pd.read_csv(manager_file)

    for col in ["season"]:
        if col in startlist_df.columns:
            startlist_df[col] = pd.to_numeric(startlist_df[col], errors="coerce")
        if col in changes_df.columns:
            changes_df[col] = pd.to_numeric(changes_df[col], errors="coerce")
        if col in history_df.columns:
            history_df[col] = pd.to_numeric(history_df[col], errors="coerce")

    if "rank_position" in history_df.columns:
        history_df["rank_position"] = pd.to_numeric(history_df["rank_position"], errors="coerce")

    for time_col in ["fetched_at"]:
        if time_col in startlist_df.columns:
            startlist_df[time_col] = pd.to_datetime(startlist_df[time_col], errors="coerce", utc=True)
    for time_col in ["event_at", "from_fetched_at", "to_fetched_at"]:
        if time_col in changes_df.columns:
            changes_df[time_col] = pd.to_datetime(changes_df[time_col], errors="coerce", utc=True)

    return metadata, startlist_df, changes_df, history_df, manager_df


def race_block(target_race: str, lookahead: int) -> list[str]:
    idx = RACES.index(target_race)
    return RACES[idx : min(idx + lookahead + 1, len(RACES))]


def _format_rank(rank: float | int | None) -> str:
    if rank is None or pd.isna(rank):
        return "-"
    return str(int(rank))


def _best_finish_rank(history_df: pd.DataFrame, pcs_rider_id: str, race_name: str, season: int) -> float | None:
    rows = history_df[
        (history_df["pcs_rider_id"] == pcs_rider_id)
        & (history_df["canonical_name"] == race_name)
        & (history_df["season"] == season)
        & (history_df["status"] == "FINISH")
    ]
    if rows.empty:
        return None
    return rows["rank_position"].min()


def _best_status(history_df: pd.DataFrame, pcs_rider_id: str, race_name: str, season: int) -> str | None:
    rows = history_df[
        (history_df["pcs_rider_id"] == pcs_rider_id)
        & (history_df["canonical_name"] == race_name)
        & (history_df["season"] == season)
    ]
    if rows.empty:
        return None

    finish_rows = rows[rows["status"] == "FINISH"].sort_values("rank_position", na_position="last")
    if not finish_rows.empty:
        return str(finish_rows.iloc[0]["status"])

    first_status = rows["status"].dropna()
    if first_status.empty:
        return None
    return str(first_status.iloc[0])


def _history_score(rank: float | None, weight: float) -> float:
    if rank is None or pd.isna(rank):
        return 0.0
    return max(121 - int(rank), 0) * weight


def _recent_form_score(history_df: pd.DataFrame, pcs_rider_id: str, target_race: str) -> float:
    target_index = RACES.index(target_race)
    prior_races = RACES[:target_index]
    if not prior_races:
        return 0.0

    score = 0.0
    race_count = len(prior_races)
    for idx, race_name in enumerate(prior_races):
        rank = _best_finish_rank(history_df, pcs_rider_id, race_name, 2026)
        if rank is None or int(rank) > 25:
            continue

        recency_weight = 0.6 + (0.4 * ((idx + 1) / race_count))
        top25_boost = 50 + max(26 - int(rank), 0) * 2
        score += top25_boost * recency_weight

    return score


def resolve_locks(manager_df: pd.DataFrame, lock_text: str) -> tuple[list[LockMatch], list[str]]:
    lock_tokens = [token.strip() for token in lock_text.split(",") if token.strip()]
    if not lock_tokens:
        return [], []

    candidates = []
    for _, row in manager_df.iterrows():
        rider_name = str(row.get("rider_name", ""))
        pcs_rider_id = str(row.get("pcs_rider_id", ""))
        kategori = str(row.get("kategori", ""))
        if not rider_name or not pcs_rider_id or pcs_rider_id == "nan":
            continue
        candidates.append(
            {
                "rider_name": rider_name,
                "pcs_rider_id": pcs_rider_id,
                "kategori": kategori,
                "name_key": rider_name.lower(),
                "pcs_key": pcs_rider_id.replace("-", " ").lower(),
            }
        )

    alias = {
        "pidcock": "thomas-pidcock",
        "mvdp": "mathieu-van-der-poel",
    }

    matched: list[LockMatch] = []
    unresolved: list[str] = []
    seen: set[str] = set()

    for token in lock_tokens:
        token_key = token.lower().replace("-", " ").strip()
        if not token_key:
            continue

        if token_key in alias:
            wanted = alias[token_key]
            match = next((c for c in candidates if c["pcs_rider_id"] == wanted), None)
            if match and wanted not in seen:
                matched.append(
                    LockMatch(
                        input_name=token,
                        pcs_rider_id=match["pcs_rider_id"],
                        rider_name=match["rider_name"],
                        kategori=match["kategori"],
                        method="alias",
                        score=1.0,
                    )
                )
                seen.add(wanted)
                continue

        best = None
        best_score = -1.0
        best_method = "fuzzy"
        for cand in candidates:
            if token_key == cand["name_key"] or token_key == cand["pcs_key"]:
                score = 1.0
                method = "exact"
            elif token_key in cand["name_key"] or token_key in cand["pcs_key"]:
                score = 0.94
                method = "contains"
            else:
                score = max(
                    SequenceMatcher(None, token_key, cand["name_key"]).ratio(),
                    SequenceMatcher(None, token_key, cand["pcs_key"]).ratio(),
                )
                method = "fuzzy"
            if score > best_score:
                best = cand
                best_score = score
                best_method = method

        if best is None or best_score < 0.62:
            unresolved.append(token)
            continue
        if best["pcs_rider_id"] in seen:
            continue

        matched.append(
            LockMatch(
                input_name=token,
                pcs_rider_id=best["pcs_rider_id"],
                rider_name=best["rider_name"],
                kategori=best["kategori"],
                method=best_method,
                score=best_score,
            )
        )
        seen.add(best["pcs_rider_id"])

    return matched, unresolved


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 1.5rem;
            padding-bottom: 2rem;
            max-width: 1400px;
        }
        .app-shell {
            padding: 1.25rem 1.25rem 0.5rem 1.25rem;
            border: 1px solid rgba(49, 51, 63, 0.15);
            border-radius: 1rem;
            background:
                radial-gradient(circle at top right, rgba(255, 75, 75, 0.08), transparent 28%),
                linear-gradient(180deg, rgba(248, 249, 251, 0.95), rgba(255, 255, 255, 1));
            margin-bottom: 1rem;
        }
        .app-kicker {
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 700;
            color: #ff4b4b;
            margin-bottom: 0.35rem;
        }
        .app-title {
            font-size: 2.2rem;
            line-height: 1.1;
            font-weight: 800;
            margin-bottom: 0.35rem;
            color: #111827;
        }
        .app-subtitle {
            font-size: 0.98rem;
            color: #4b5563;
            max-width: 60rem;
        }
        .section-label {
            font-size: 0.82rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 700;
            color: #6b7280;
            margin-bottom: 0.4rem;
        }
        div[data-testid="stMetric"] {
            background: linear-gradient(180deg, #1f2937, #111827);
            border: 1px solid rgba(15, 23, 42, 0.65);
            border-radius: 0.9rem;
            padding: 0.85rem 1rem;
            min-height: 6.2rem;
            box-shadow: 0 12px 28px rgba(15, 23, 42, 0.16);
        }
        div[data-testid="stMetricLabel"] p {
            font-weight: 700;
            color: #cbd5e1 !important;
            font-size: 0.82rem !important;
            letter-spacing: 0.01em;
        }
        div[data-testid="stMetricValue"] {
            color: #f8fafc !important;
        }
        div[data-testid="stMetricValue"] > div {
            color: #f8fafc !important;
        }
        div[data-testid="stMetricValue"] p {
            color: #f8fafc !important;
            font-size: 1.7rem !important;
            line-height: 1.05 !important;
            font-weight: 700 !important;
        }
        div[data-testid="stMetricDelta"] {
            color: #94a3b8 !important;
        }
        div[data-testid="stMetricDelta"] p {
            color: #94a3b8 !important;
            font-size: 0.78rem !important;
        }
        div[data-testid="stDataFrame"] {
            border: 1px solid rgba(49, 51, 63, 0.12);
            border-radius: 0.8rem;
            overflow: hidden;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def section_label(text: str) -> None:
    st.markdown(f"<div class='section-label'>{text}</div>", unsafe_allow_html=True)


def format_timestamp(value: object) -> str:
    if value is None or pd.isna(value):
        return "-"
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return "-"
    return ts.strftime("%Y-%m-%d %H:%M UTC")


def build_kategori_map(manager_df: pd.DataFrame) -> dict[str, str]:
    return (
        manager_df[["pcs_rider_id", "kategori"]]
        .drop_duplicates(subset=["pcs_rider_id"])
        .set_index("pcs_rider_id")["kategori"]
        .to_dict()
    )


def summarize_race_changes(changes_df: pd.DataFrame, race: str, season: int) -> tuple[int, int]:
    rows = changes_df[
        (changes_df["season"] == season)
        & (changes_df["canonical_name"] == race)
    ]
    if rows.empty or "event_type" not in rows.columns:
        return 0, 0
    entered = int((rows["event_type"] == "ENTERED").sum())
    left = int((rows["event_type"] == "LEFT").sum())
    return entered, left


def build_history_summary(
    history_df: pd.DataFrame,
    manager_df: pd.DataFrame,
    race_name: str,
    seasons: list[int],
    kategori_filter: list[str],
) -> pd.DataFrame:
    rows = history_df[
        (history_df["canonical_name"] == race_name)
        & (history_df["season"].isin(seasons))
    ].copy()
    rows = rows.merge(
        manager_df[["pcs_rider_id", "rider_name", "kategori"]].drop_duplicates("pcs_rider_id"),
        on="pcs_rider_id",
        how="inner",
        suffixes=("_history", "_manager"),
    )
    rows = rows[rows["kategori"].isin(kategori_filter)].copy()
    return rows.sort_values(
        ["kategori", "rank_position", "rider_name_manager", "season"],
        ascending=[True, True, True, False],
    )


def render_resolved_matches(title: str, items: list[LockMatch]) -> None:
    if not items:
        return
    section_label(title)
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "input": item.input_name,
                    "rider_name": item.rider_name,
                    "pcs_rider_id": item.pcs_rider_id,
                    "kategori": item.kategori,
                    "method": item.method,
                    "score": round(item.score, 3),
                }
                for item in items
            ]
        ),
        use_container_width=True,
        hide_index=True,
    )


def build_available_lineup_pool(
    startlist_df: pd.DataFrame,
    manager_df: pd.DataFrame,
    season: int,
    race: str,
) -> pd.DataFrame:
    race_rows = startlist_df[
        (startlist_df["season"] == season)
        & (startlist_df["canonical_name"] == race)
        & (startlist_df["pcs_rider_id"].notna())
    ].copy()
    manager_rows = manager_df[
        ["pcs_rider_id", "rider_name", "kategori", "holdet_player_id"]
    ].drop_duplicates(subset=["pcs_rider_id"])
    lineup_pool = race_rows.merge(manager_rows, on="pcs_rider_id", how="inner", suffixes=("_startlist", "_manager"))
    lineup_pool = lineup_pool.rename(
        columns={
            "rider_name_startlist": "startlist_rider_name",
            "rider_name_manager": "manager_rider_name",
        }
    )
    lineup_pool["display_name"] = lineup_pool["manager_rider_name"].fillna(lineup_pool["startlist_rider_name"])
    return lineup_pool.sort_values(["kategori", "display_name", "team_name"]).reset_index(drop=True)


def build_lineup_option_labels(lineup_pool: pd.DataFrame) -> dict[str, str]:
    return {
        str(row.pcs_rider_id): f"{row.display_name} | {row.team_name} | {row.pcs_rider_id}"
        for row in lineup_pool.itertuples(index=False)
    }


def build_lineup_summary(
    selected_rows: list[dict[str, object]],
    history_df: pd.DataFrame,
    target_race: str,
) -> pd.DataFrame:
    summary_rows: list[dict[str, object]] = []
    for row in selected_rows:
        pcs_rider_id = str(row["pcs_rider_id"])
        rank_2025 = _best_finish_rank(history_df, pcs_rider_id, target_race, 2025)
        rank_2024 = _best_finish_rank(history_df, pcs_rider_id, target_race, 2024)
        summary_rows.append(
            {
                "kategori": row["kategori"],
                "slot": row["slot"],
                "rider_name": row["display_name"],
                "team_name": row["team_name"],
                "pcs_rider_id": pcs_rider_id,
                "rank_2025": _format_rank(rank_2025),
                "rank_2024": _format_rank(rank_2024),
                "form_2026_score": round(_recent_form_score(history_df, pcs_rider_id, target_race), 2),
            }
        )
    return pd.DataFrame(summary_rows)


def _weather_label_from_code(code: object) -> str:
    try:
        numeric_code = int(code)
    except (TypeError, ValueError):
        return "Unknown"
    return WEATHER_CODE_LABELS.get(numeric_code, f"Code {numeric_code}")


@st.cache_data(show_spinner=False, ttl=86400)
def geocode_location(query: str, country_code: str | None = None) -> dict[str, object]:
    params = {"name": query, "count": 1, "language": "en", "format": "json"}
    if country_code:
        params["countryCode"] = country_code
    url = f"https://geocoding-api.open-meteo.com/v1/search?{urlencode(params)}"
    with urlopen(url, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))
    results = payload.get("results") or []
    if not results:
        raise ValueError(f"No geocoding result for {query}.")
    return dict(results[0])


@st.cache_data(show_spinner=False, ttl=1800)
def fetch_race_weather(race_name: str) -> list[WeatherPointForecast]:
    points = RACE_WEATHER_POINTS.get(race_name, ())
    forecasts: list[WeatherPointForecast] = []
    for point in points:
        location = geocode_location(str(point["query"]), str(point.get("country_code") or ""))
        params = urlencode(
            {
                "latitude": location["latitude"],
                "longitude": location["longitude"],
                "timezone": "auto",
                "forecast_days": 2,
                "current": "temperature_2m,precipitation,weather_code,wind_speed_10m,wind_direction_10m",
                "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max",
            }
        )
        url = f"https://api.open-meteo.com/v1/forecast?{params}"
        with urlopen(url, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))

        current = payload.get("current", {})
        daily = payload.get("daily", {})
        forecasts.append(
            WeatherPointForecast(
                label=str(point["label"]),
                name=str(point["name"]),
                current_temp_c=current.get("temperature_2m"),
                current_precip_mm=current.get("precipitation"),
                current_wind_kmh=current.get("wind_speed_10m"),
                current_wind_direction_deg=current.get("wind_direction_10m"),
                current_weather_label=_weather_label_from_code(current.get("weather_code")),
                today_max_c=(daily.get("temperature_2m_max") or [None])[0],
                today_min_c=(daily.get("temperature_2m_min") or [None])[0],
                today_precip_mm=(daily.get("precipitation_sum") or [None])[0],
                today_wind_max_kmh=(daily.get("wind_speed_10m_max") or [None])[0],
            )
        )
    return forecasts


def build_candidate_score_frame(
    candidate_pool: pd.DataFrame,
    history_df: pd.DataFrame,
    startlist_df: pd.DataFrame,
    season: int,
    target_race: str,
    lookahead: int,
) -> pd.DataFrame:
    block = race_block(target_race, lookahead)
    block_start = startlist_df[
        (startlist_df["season"] == season) & (startlist_df["canonical_name"].isin(block))
    ]
    races_in_block = block_start.groupby("pcs_rider_id")["canonical_name"].nunique().to_dict()

    candidate_rows: list[dict[str, object]] = []
    for row in candidate_pool.itertuples(index=False):
        pcs = str(row.pcs_rider_id)
        availability_score = float(races_in_block.get(pcs, 0)) * 100.0

        target_2025 = _best_finish_rank(history_df, pcs, target_race, 2025)
        target_2024 = _best_finish_rank(history_df, pcs, target_race, 2024)
        target_history_score = _history_score(target_2025, 1.0) + _history_score(target_2024, 0.7)

        lookahead_history_score = 0.0
        for race_name in block[1:]:
            lookahead_2025 = _best_finish_rank(history_df, pcs, race_name, 2025)
            lookahead_2024 = _best_finish_rank(history_df, pcs, race_name, 2024)
            lookahead_history_score += _history_score(lookahead_2025, 0.45)
            lookahead_history_score += _history_score(lookahead_2024, 0.30)

        form_2026_score = _recent_form_score(history_df, pcs, target_race)
        score = availability_score + target_history_score + lookahead_history_score + form_2026_score

        candidate_rows.append(
            {
                "kategori": row.kategori,
                "holdet_player_id": row.holdet_player_id,
                "manager_rider_name": getattr(row, "rider_name", getattr(row, "display_name", pcs)),
                "display_name": getattr(row, "display_name", getattr(row, "rider_name", pcs)),
                "team_name": row.team_name,
                "pcs_rider_id": pcs,
                "races_in_block": int(races_in_block.get(pcs, 0)),
                "score": round(score, 2),
                "availability_score": round(availability_score, 2),
                "target_history_score": round(target_history_score, 2),
                "lookahead_history_score": round(lookahead_history_score, 2),
                "form_2026_score": round(form_2026_score, 2),
            }
        )

    return pd.DataFrame(candidate_rows)


def app() -> None:
    st.set_page_config(page_title="Fantasy Cycling Snapshot", layout="wide")
    inject_styles()
    st.markdown(
        """
        <div class="app-shell">
            <div class="app-kicker">Snapshot Workspace</div>
            <div class="app-title">Fantasy Cycling Race Desk</div>
            <div class="app-subtitle">
                Read-only race intelligence for startlists, manager mappings, historical results,
                and lineup simulation. The data still comes from the same snapshot export, but the
                layout is now race-first and easier to scan.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    default_snapshot = os.getenv("FANTASY_CYCLING_SNAPSHOT_DIR", "data/snapshot_latest")

    with st.sidebar:
        st.header("Controls")
        section_label("Snapshot")
        snapshot_dir = st.text_input("Snapshot directory", value=default_snapshot)
        cartridge_slug = st.text_input("Cartridge", value="classics-manager-2026")
        season = int(st.number_input("Season", min_value=2024, max_value=2030, value=2026))

    try:
        metadata, startlist_df, changes_df, history_df, manager_df = load_snapshot(snapshot_dir)
    except RuntimeError as err:
        st.error(str(err))
        st.info(
            "Generate files first: `python3 -m fantasy_cycling export snapshot --out data/snapshot_latest --season 2026 --history-seasons 2025,2024 --cartridge classics-manager-2026`"
        )
        st.stop()

    mgr = manager_df[
        (manager_df["cartridge_slug"] == cartridge_slug)
        & (manager_df["mapping_status"] == "APPROVED")
        & (manager_df["pcs_rider_id"].notna())
        & (manager_df["pcs_rider_id"] != "")
    ].copy()

    if metadata:
        top_stats = st.columns(4)
        top_stats[0].metric("Snapshot Generated", format_timestamp(metadata.get("generated_at")))
        top_stats[1].metric(
            "History Seasons",
            ", ".join(str(item) for item in metadata.get("history_seasons", [])) or "-",
        )
        top_stats[2].metric("Tracked Races", len(RACES))
        top_stats[3].metric("Approved Mapped Riders", int(len(mgr)))

    with st.sidebar:
        section_label("Snapshot Health")
        st.metric("Unique mapped PCS riders", int(mgr["pcs_rider_id"].nunique()))
        st.metric("Startlist rows", int(len(startlist_df)))
        st.metric("Change rows", int(len(changes_df)))
        st.metric("History rows", int(len(history_df)))

    tabs = st.tabs(["Race Desk", "Change Feed", "History Lab", "Rider Profile", "Simulation", "Lineup Builder"])

    with tabs[0]:
        section_label("Race Selection")
        race = st.selectbox("Race", options=RACES, index=0, key="dash_race")
        race_rows = startlist_df[
            (startlist_df["canonical_name"] == race)
            & (startlist_df["season"] == season)
        ].copy()
        kategori_map = build_kategori_map(mgr)
        race_rows["kategori"] = race_rows["pcs_rider_id"].map(kategori_map).fillna("Unmapped")

        filters_left, filters_right = st.columns([2, 3])
        kategori_filter = filters_left.multiselect(
            "Kategori filter",
            options=["Kategori 1", "Kategori 2", "Kategori 3", "Kategori 4", "Unmapped"],
            default=["Kategori 1", "Kategori 2", "Kategori 3", "Kategori 4", "Unmapped"],
            key="dash_kategori_filter",
        )
        team_filter = filters_right.multiselect(
            "Team filter",
            options=sorted(race_rows["team_name"].dropna().unique().tolist()),
            default=[],
            key="dash_team_filter",
        )

        filtered_race_rows = race_rows[race_rows["kategori"].isin(kategori_filter)].copy()
        if team_filter:
            filtered_race_rows = filtered_race_rows[filtered_race_rows["team_name"].isin(team_filter)]

        entered_count, left_count = summarize_race_changes(changes_df, race, season)
        metric_cols = st.columns(5)
        metric_cols[0].metric("Riders", int(len(filtered_race_rows)))
        metric_cols[1].metric("Teams", int(filtered_race_rows["team_name"].nunique()))
        metric_cols[2].metric("Mapped riders", int((filtered_race_rows["kategori"] != "Unmapped").sum()))
        metric_cols[3].metric("Entered events", entered_count)
        metric_cols[4].metric("Left events", left_count)

        if race in RACE_WEATHER_POINTS:
            section_label("Live Weather")
            try:
                weather_points = fetch_race_weather(race)
            except Exception as error:
                st.warning(f"Weather unavailable right now: {error}")
                weather_points = []
            if weather_points:
                weather_cols = st.columns(len(weather_points))
                for column, forecast in zip(weather_cols, weather_points):
                    column.metric(
                        f"{forecast.label}: {forecast.name}",
                        (
                            f"{forecast.current_temp_c:.1f} C"
                            if forecast.current_temp_c is not None
                            else "-"
                        ),
                        (
                            f"{forecast.current_weather_label}, wind {forecast.current_wind_kmh:.0f} km/h"
                            if forecast.current_wind_kmh is not None
                            else forecast.current_weather_label
                        ),
                    )
                    column.caption(
                        " | ".join(
                            [
                                f"Today {forecast.today_min_c:.0f}-{forecast.today_max_c:.0f} C"
                                if forecast.today_min_c is not None and forecast.today_max_c is not None
                                else "Today -",
                                f"Rain {forecast.today_precip_mm:.1f} mm"
                                if forecast.today_precip_mm is not None
                                else "Rain -",
                                f"Wind max {forecast.today_wind_max_kmh:.0f} km/h"
                                if forecast.today_wind_max_kmh is not None
                                else "Wind -",
                            ]
                        )
                    )

        category_summary = (
            filtered_race_rows.groupby("kategori", dropna=False)["pcs_rider_id"]
            .count()
            .reset_index(name="rider_count")
            .sort_values(["kategori"])
        )
        team_counts = (
            filtered_race_rows.groupby("team_name", dropna=False)["pcs_rider_id"]
            .count()
            .reset_index(name="rider_count")
            .sort_values(["rider_count", "team_name"], ascending=[False, True])
        )
        latest_changes = changes_df[
            (changes_df["season"] == season)
            & (changes_df["canonical_name"] == race)
        ].sort_values("event_at", ascending=False).head(15)

        top_left, top_right = st.columns([2.4, 1.1])
        with top_left:
            section_label("Race Startlist")
            st.dataframe(
                filtered_race_rows[["rider_name", "team_name", "pcs_rider_id", "kategori"]]
                .sort_values(["kategori", "team_name", "rider_name"]),
                use_container_width=True,
                hide_index=True,
            )
        with top_right:
            section_label("Kategori Mix")
            st.dataframe(category_summary, use_container_width=True, hide_index=True)
            section_label("Largest Teams")
            st.dataframe(team_counts.head(12), use_container_width=True, hide_index=True)

        bottom_left, bottom_right = st.columns([1.3, 1.7])
        with bottom_left:
            section_label("Coverage Notes")
            unmapped = filtered_race_rows[filtered_race_rows["kategori"] == "Unmapped"]
            if unmapped.empty:
                st.success("All visible riders are mapped to a Holdet kategori.")
            else:
                st.warning(f"{len(unmapped)} visible riders are unmapped.")
                st.dataframe(
                    unmapped[["rider_name", "team_name", "pcs_rider_id"]].sort_values(["team_name", "rider_name"]),
                    use_container_width=True,
                    hide_index=True,
                )
        with bottom_right:
            section_label("Latest Changes For This Race")
            if latest_changes.empty:
                st.info("No recorded change events for this race and season.")
            else:
                st.dataframe(
                    latest_changes[["event_at", "event_type", "rider_name", "team_name", "pcs_rider_id"]],
                    use_container_width=True,
                    hide_index=True,
                )

    with tabs[1]:
        section_label("Change Feed")
        filters = st.columns([1.6, 1, 1])
        race_filter = filters[0].selectbox("Race filter", options=["All"] + RACES, index=0, key="chg_race")
        event_filter = filters[1].selectbox("Event type", options=["All", "ENTERED", "LEFT"], index=0)
        limit = int(filters[2].slider("Rows", min_value=10, max_value=200, value=50, step=10))

        rows = changes_df[changes_df["season"] == season].copy()
        if race_filter != "All":
            rows = rows[rows["canonical_name"] == race_filter]
        if event_filter != "All" and "event_type" in rows.columns:
            rows = rows[rows["event_type"] == event_filter]
        rows = rows.sort_values("event_at", ascending=False).head(limit)

        summary_cols = st.columns(4)
        summary_cols[0].metric("Visible rows", int(len(rows)))
        summary_cols[1].metric("Races", int(rows["canonical_name"].nunique()) if not rows.empty else 0)
        summary_cols[2].metric("Entered", int((rows["event_type"] == "ENTERED").sum()) if "event_type" in rows.columns else 0)
        summary_cols[3].metric("Left", int((rows["event_type"] == "LEFT").sum()) if "event_type" in rows.columns else 0)
        st.dataframe(rows, use_container_width=True, hide_index=True)

    with tabs[2]:
        section_label("History Lab")
        hist_controls = st.columns([1.5, 2.2])
        hist_race = hist_controls[0].selectbox("History race", options=RACES, index=1, key="hist_race")
        kategori_filter = hist_controls[1].multiselect(
            "Kategori filter",
            options=["Kategori 1", "Kategori 2", "Kategori 3", "Kategori 4"],
            default=["Kategori 1", "Kategori 2", "Kategori 3", "Kategori 4"],
        )

        hist = build_history_summary(history_df, mgr, hist_race, [2025, 2024], kategori_filter)
        hist_summary = (
            hist.groupby(["kategori", "season"], dropna=False)["pcs_rider_id"]
            .count()
            .reset_index(name="results_rows")
            .sort_values(["kategori", "season"], ascending=[True, False])
        )

        left, right = st.columns([2.4, 1.2])
        with left:
            section_label("Historical Results")
            st.dataframe(
                hist[["kategori", "rider_name_manager", "season", "rank_position", "status"]]
                .rename(columns={"rider_name_manager": "rider_name"}),
                use_container_width=True,
                hide_index=True,
            )
        with right:
            section_label("Coverage Summary")
            st.dataframe(hist_summary, use_container_width=True, hide_index=True)
            top_finishers = hist[hist["status"] == "FINISH"].sort_values(["rank_position", "season"]).head(12)
            section_label("Best Recorded Finishes")
            st.dataframe(
                top_finishers[["rider_name_manager", "season", "rank_position", "kategori"]]
                .rename(columns={"rider_name_manager": "rider_name"}),
                use_container_width=True,
                hide_index=True,
            )

    with tabs[3]:
        section_label("Rider Profile")
        st.markdown("Pick a rider and view history across all tracked races.")

        search = st.text_input("Search rider (name or PCS slug)", value="", key="rider_profile_search").strip().lower()
        rider_options = mgr[["rider_name", "kategori", "pcs_rider_id"]].drop_duplicates().copy()
        if search:
            rider_options = rider_options[
                rider_options["rider_name"].str.lower().str.contains(search, na=False)
                | rider_options["pcs_rider_id"].str.lower().str.contains(search, na=False)
            ]

        if rider_options.empty:
            st.warning("No riders match this search.")
        else:
            labels = [
                f"{row.rider_name} | {row.kategori} | {row.pcs_rider_id}"
                for row in rider_options.itertuples(index=False)
            ]
            selected_label = st.selectbox("Rider", options=labels, index=0, key="rider_profile_select")
            selected_row = rider_options.iloc[labels.index(selected_label)]
            selected_pcs = str(selected_row["pcs_rider_id"])

            rider_start = startlist_df[
                (startlist_df["season"] == season) & (startlist_df["pcs_rider_id"] == selected_pcs)
            ]
            entered_races = sorted(set(rider_start["canonical_name"].tolist()), key=RACES.index)
            rider_changes = changes_df[
                (changes_df["season"] == season) & (changes_df["pcs_rider_id"] == selected_pcs)
            ].sort_values("event_at", ascending=False)

            profile_metrics = st.columns(4)
            profile_metrics[0].metric("Selected Rider", selected_row["rider_name"])
            profile_metrics[1].metric("Kategori", selected_row["kategori"])
            profile_metrics[2].metric(f"Entered Races ({season})", len(entered_races))
            profile_metrics[3].metric("Change Events", int(len(rider_changes)))

            top_left, top_right = st.columns([1.2, 2.2])
            with top_left:
                section_label("Current Race Coverage")
                if entered_races:
                    st.write("\n".join(f"- {race_name}" for race_name in entered_races))
                else:
                    st.caption("Not currently on any tracked race startlist.")

                section_label("Recent Startlist Changes")
                if rider_changes.empty:
                    st.caption("No recorded change events for this rider in current season.")
                else:
                    st.dataframe(
                        rider_changes[["canonical_name", "event_type", "event_at"]],
                        use_container_width=True,
                        hide_index=True,
                    )
            with top_right:
                rows = []
                for race_name in RACES:
                    rank_2025 = _best_finish_rank(history_df, selected_pcs, race_name, 2025)
                    rank_2024 = _best_finish_rank(history_df, selected_pcs, race_name, 2024)
                    rows.append(
                        {
                            "canonical_name": race_name,
                            f"on_startlist_{season}": "Yes" if race_name in entered_races else "No",
                            "rank_2025": _format_rank(rank_2025),
                            "status_2025": _best_status(history_df, selected_pcs, race_name, 2025) or "",
                            "rank_2024": _format_rank(rank_2024),
                            "status_2024": _best_status(history_df, selected_pcs, race_name, 2024) or "",
                        }
                    )

                section_label("Full Race History")
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    with tabs[4]:
        section_label("Simulation")
        st.markdown("Use locks and race blocks to simulate category suggestions.")
        st.caption(
            "Score = availability in race block + target-race history + lookahead-race history + 2026 form."
        )

        controls_top = st.columns([1.3, 1, 1])
        target = controls_top[0].selectbox("Target race", options=RACES, index=1, key="sim_target")
        lookahead = int(controls_top[1].slider("Lookahead rounds", min_value=0, max_value=5, value=2))
        extra_suggestions = int(
            controls_top[2].slider("Extra suggestions per kategori", min_value=0, max_value=3, value=2)
        )
        locks_text = st.text_input("Locks (comma-separated names or slugs)", value="tadej pogacar,thomas pidcock")
        excluded_text = st.text_input("Excluded riders (comma-separated names or slugs)", value="")
        run_sim = st.button("Run Simulation", type="primary")

        if run_sim:
            lock_matches, unresolved = resolve_locks(mgr, locks_text)
            excluded_matches, unresolved_excluded = resolve_locks(mgr, excluded_text)
            block = race_block(target, lookahead)

            block_metrics = st.columns(3)
            block_metrics[0].metric("Target race", target)
            block_metrics[1].metric("Race block size", len(block))
            block_metrics[2].metric("Active locks", len(lock_matches))
            st.caption("Race block: " + " -> ".join(block))

            resolved_left, resolved_right = st.columns(2)
            with resolved_left:
                render_resolved_matches("Resolved Locks", lock_matches)
                if unresolved:
                    st.warning("Unresolved locks: " + ", ".join(unresolved))
            with resolved_right:
                render_resolved_matches("Resolved Exclusions", excluded_matches)
                if unresolved_excluded:
                    st.warning("Unresolved exclusions: " + ", ".join(unresolved_excluded))

            lock_ids = {item.pcs_rider_id for item in lock_matches}
            excluded_ids = {item.pcs_rider_id for item in excluded_matches}
            lock_counts: dict[str, int] = {}
            for item in lock_matches:
                lock_counts[item.kategori] = lock_counts.get(item.kategori, 0) + 1

            needs_rows = []
            for kategori, slots in CATEGORY_SLOTS.items():
                locked = lock_counts.get(kategori, 0)
                needs_rows.append(
                    {
                        "kategori": kategori,
                        "slot_count": slots,
                        "locked": locked,
                        "needed": max(slots - locked, 0),
                    }
                )

            section_label("Slot Needs")
            st.dataframe(pd.DataFrame(needs_rows), use_container_width=True, hide_index=True)

            start_target = startlist_df[
                (startlist_df["season"] == season) & (startlist_df["canonical_name"] == target)
            ]
            starters = set(start_target["pcs_rider_id"].dropna().astype(str).tolist())

            block_start = startlist_df[
                (startlist_df["season"] == season) & (startlist_df["canonical_name"].isin(block))
            ]
            races_in_block = (
                block_start.groupby("pcs_rider_id")["canonical_name"]
                .nunique()
                .to_dict()
            )

            cand = mgr[mgr["pcs_rider_id"].astype(str).isin(starters)].copy()
            cand = cand[~cand["pcs_rider_id"].astype(str).isin(lock_ids)]
            cand = cand[~cand["pcs_rider_id"].astype(str).isin(excluded_ids)]

            if cand.empty:
                st.info("No candidates found for this race with current mapping and snapshot.")
            else:
                cand_rows = []
                for row in cand.itertuples(index=False):
                    pcs = str(row.pcs_rider_id)
                    availability_score = float(races_in_block.get(pcs, 0)) * 100.0

                    target_2025 = _best_finish_rank(history_df, pcs, target, 2025)
                    target_2024 = _best_finish_rank(history_df, pcs, target, 2024)
                    target_history_score = _history_score(target_2025, 1.0) + _history_score(target_2024, 0.7)

                    lookahead_history_score = 0.0
                    for race_name in block[1:]:
                        lookahead_2025 = _best_finish_rank(history_df, pcs, race_name, 2025)
                        lookahead_2024 = _best_finish_rank(history_df, pcs, race_name, 2024)
                        lookahead_history_score += _history_score(lookahead_2025, 0.45)
                        lookahead_history_score += _history_score(lookahead_2024, 0.30)

                    form_2026_score = _recent_form_score(history_df, pcs, target)
                    score = (
                        availability_score
                        + target_history_score
                        + lookahead_history_score
                        + form_2026_score
                    )

                    out = {
                        "kategori": row.kategori,
                        "holdet_player_id": row.holdet_player_id,
                        "manager_rider_name": row.rider_name,
                        "pcs_rider_id": pcs,
                        "races_in_block": int(races_in_block.get(pcs, 0)),
                        "score": round(score, 2),
                    }
                    for race_name in block:
                        r25 = _best_finish_rank(history_df, pcs, race_name, 2025)
                        r24 = _best_finish_rank(history_df, pcs, race_name, 2024)
                        out[f"{race_name} 25/24"] = f"{_format_rank(r25)}/{_format_rank(r24)}"
                    out["availability_score"] = round(availability_score, 2)
                    out["target_history_score"] = round(target_history_score, 2)
                    out["lookahead_history_score"] = round(lookahead_history_score, 2)
                    out["form_2026_score"] = round(form_2026_score, 2)
                    cand_rows.append(out)

                cand_df = pd.DataFrame(cand_rows)

                summary_left, summary_right = st.columns([2.2, 1.2])
                with summary_left:
                    section_label("Best Overall Candidates")
                    st.dataframe(
                        cand_df.sort_values(["score", "manager_rider_name"], ascending=[False, True]).head(15),
                        use_container_width=True,
                        hide_index=True,
                    )
                with summary_right:
                    section_label("Score Components")
                    top_example = cand_df.sort_values(
                        ["score", "manager_rider_name"], ascending=[False, True]
                    ).iloc[0]
                    st.markdown(
                        "\n".join(
                            [
                                f"**Example:** {top_example['manager_rider_name']} (`{top_example['pcs_rider_id']}`)",
                                f"- races_in_block = {top_example['races_in_block']}",
                                f"- total score = {top_example['score']}",
                                f"- availability_score = {top_example['availability_score']}",
                                f"- target_history_score = {top_example['target_history_score']}",
                                f"- lookahead_history_score = {top_example['lookahead_history_score']}",
                                f"- form_2026_score = {top_example['form_2026_score']}",
                            ]
                        )
                    )

                for kategori in CATEGORY_SLOTS:
                    needed = next((n["needed"] for n in needs_rows if n["kategori"] == kategori), 0)
                    display_count = needed + extra_suggestions
                    if display_count <= 0:
                        continue
                    top = cand_df[cand_df["kategori"] == kategori].sort_values(
                        ["score", "manager_rider_name"],
                        ascending=[False, True],
                    )
                    if top.empty:
                        continue
                    section_label(f"Suggestions: {kategori} ({needed} needed + {extra_suggestions} backups)")
                    st.dataframe(top.head(display_count), use_container_width=True, hide_index=True)

    with tabs[5]:
        section_label("Lineup Builder")
        st.markdown("Build a 12-rider team interactively from the selected race startlist.")

        builder_controls = st.columns([1.4, 1])
        builder_race = builder_controls[0].selectbox("Builder race", options=RACES, index=8, key="builder_race")
        builder_lookahead = int(
            builder_controls[1].slider("Builder lookahead", min_value=0, max_value=5, value=2, key="builder_lookahead")
        )
        lineup_pool = build_available_lineup_pool(startlist_df, mgr, season, builder_race)

        if lineup_pool.empty:
            st.info("No mapped riders are available for this race in the current snapshot.")
        else:
            option_labels = build_lineup_option_labels(lineup_pool)
            selected_rows: list[dict[str, object]] = []

            availability_summary = (
                lineup_pool.groupby("kategori", dropna=False)["pcs_rider_id"]
                .count()
                .reset_index(name="available_riders")
                .sort_values("kategori")
            )

            summary_cols = st.columns(4)
            summary_cols[0].metric("Available mapped riders", int(len(lineup_pool)))
            summary_cols[1].metric("Available teams", int(lineup_pool["team_name"].nunique()))
            summary_cols[2].metric("Paris-Roubaix style slots", sum(CATEGORY_SLOTS.values()))
            summary_cols[3].metric("Categories covered", int(availability_summary["kategori"].nunique()))

            top_left, top_right = st.columns([2.3, 1.2])
            with top_left:
                for kategori, slot_count in CATEGORY_SLOTS.items():
                    section_label(f"{kategori} Picks")
                    category_rows = lineup_pool[lineup_pool["kategori"] == kategori].copy()
                    if category_rows.empty:
                        st.warning(f"No available riders mapped to {kategori} for {builder_race}.")
                        continue

                    category_options = [""] + category_rows["pcs_rider_id"].astype(str).tolist()
                    category_cols = st.columns(slot_count)
                    for slot_index in range(slot_count):
                        selected_id = category_cols[slot_index].selectbox(
                            f"{kategori} #{slot_index + 1}",
                            options=category_options,
                            format_func=lambda value, labels=option_labels: "Select rider" if value == "" else labels.get(value, value),
                            key=f"lineup_{builder_race}_{kategori}_{slot_index + 1}",
                        )
                        if selected_id == "":
                            continue
                        selected_row = category_rows[category_rows["pcs_rider_id"].astype(str) == selected_id].iloc[0]
                        selected_rows.append(
                            {
                                "kategori": kategori,
                                "slot": slot_index + 1,
                                "display_name": selected_row["display_name"],
                                "team_name": selected_row["team_name"],
                                "pcs_rider_id": str(selected_row["pcs_rider_id"]),
                            }
                        )

            with top_right:
                section_label("Availability By Category")
                st.dataframe(availability_summary, use_container_width=True, hide_index=True)
                section_label("Builder Notes")
                st.caption("Selections are restricted to mapped riders present on the selected race startlist.")
                st.caption("You can leave slots empty while sketching different versions of the team.")

            selected_ids = [str(row["pcs_rider_id"]) for row in selected_rows]
            duplicate_ids = sorted({pcs_id for pcs_id in selected_ids if selected_ids.count(pcs_id) > 1})
            filled_slots = len(selected_rows)
            total_slots = sum(CATEGORY_SLOTS.values())

            status_cols = st.columns(4)
            status_cols[0].metric("Filled slots", filled_slots)
            status_cols[1].metric("Open slots", max(total_slots - filled_slots, 0))
            status_cols[2].metric("Unique riders", len(set(selected_ids)))
            status_cols[3].metric("Duplicates", len(duplicate_ids))

            if duplicate_ids:
                st.error("A rider has been selected more than once. Remove duplicate picks before treating this as a valid team.")

            lineup_summary = build_lineup_summary(selected_rows, history_df, builder_race)
            if lineup_summary.empty:
                st.info("No riders selected yet.")
            else:
                section_label("Current Team")
                st.dataframe(
                    lineup_summary.sort_values(["kategori", "slot"]),
                    use_container_width=True,
                    hide_index=True,
                )

                team_mix = (
                    lineup_summary.groupby("team_name", dropna=False)["pcs_rider_id"]
                    .count()
                    .reset_index(name="selected_riders")
                    .sort_values(["selected_riders", "team_name"], ascending=[False, True])
                )
                lower_left, lower_right = st.columns([2.2, 1.2])
                with lower_left:
                    section_label("Selected Team Mix")
                    st.dataframe(team_mix, use_container_width=True, hide_index=True)
                with lower_right:
                    section_label("Selection Notes")
                    st.caption(f"Target race history columns show best finish in 2025 and 2024 for {builder_race}.")
                    st.caption("`form_2026_score` reuses the same recent-form logic as the simulation tab.")

            open_slot_rows = []
            selected_counts_by_category: dict[str, int] = {}
            for row in selected_rows:
                kategori = str(row["kategori"])
                selected_counts_by_category[kategori] = selected_counts_by_category.get(kategori, 0) + 1
            for kategori, slot_count in CATEGORY_SLOTS.items():
                selected_count = selected_counts_by_category.get(kategori, 0)
                open_count = max(slot_count - selected_count, 0)
                if open_count > 0:
                    open_slot_rows.append(
                        {"kategori": kategori, "selected": selected_count, "open_slots": open_count}
                    )

            if open_slot_rows:
                section_label("Fill Suggestions")
                st.caption(
                    "Open slots use the same scoring logic as the simulation tab, excluding riders already selected."
                )
                open_slots_df = pd.DataFrame(open_slot_rows)
                st.dataframe(open_slots_df, use_container_width=True, hide_index=True)

                suggestion_pool = lineup_pool[
                    ~lineup_pool["pcs_rider_id"].astype(str).isin(selected_ids)
                ].copy()
                suggestion_df = build_candidate_score_frame(
                    candidate_pool=suggestion_pool,
                    history_df=history_df,
                    startlist_df=startlist_df,
                    season=season,
                    target_race=builder_race,
                    lookahead=builder_lookahead,
                )

                for row in open_slot_rows:
                    kategori = str(row["kategori"])
                    open_count = int(row["open_slots"])
                    top = suggestion_df[suggestion_df["kategori"] == kategori].sort_values(
                        ["score", "display_name"],
                        ascending=[False, True],
                    )
                    if top.empty:
                        continue
                    section_label(f"Suggested fills: {kategori} ({open_count} open)")
                    st.dataframe(
                        top.head(open_count + 2)[
                            [
                                "display_name",
                                "team_name",
                                "pcs_rider_id",
                                "score",
                                "races_in_block",
                                "target_history_score",
                                "lookahead_history_score",
                                "form_2026_score",
                            ]
                        ],
                        use_container_width=True,
                        hide_index=True,
                    )


if __name__ == "__main__":
    app()

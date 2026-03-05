from __future__ import annotations

import json
import os
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path

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


@dataclass
class LockMatch:
    input_name: str
    pcs_rider_id: str
    rider_name: str
    kategori: str
    method: str
    score: float


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


def app() -> None:
    st.set_page_config(page_title="Fantasy Cycling Snapshot", layout="wide")
    st.title("Fantasy Cycling Snapshot UI")
    st.caption("Read-only app for sharing startlists, changes, history, and lineup simulation.")

    default_snapshot = os.getenv("FANTASY_CYCLING_SNAPSHOT_DIR", "data/snapshot_latest")

    with st.sidebar:
        st.header("Snapshot")
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

    if metadata:
        st.caption(
            f"Snapshot generated: {metadata.get('generated_at', '-')}, "
            f"history seasons: {metadata.get('history_seasons', [])}"
        )

    mgr = manager_df[
        (manager_df["cartridge_slug"] == cartridge_slug)
        & (manager_df["mapping_status"] == "APPROVED")
        & (manager_df["pcs_rider_id"].notna())
        & (manager_df["pcs_rider_id"] != "")
    ].copy()

    tabs = st.tabs(["Race Dashboard", "Change Feed", "History", "Rider Profile", "Simulation"])

    with tabs[0]:
        race = st.selectbox("Race", options=RACES, index=0, key="dash_race")
        race_rows = startlist_df[
            (startlist_df["canonical_name"] == race)
            & (startlist_df["season"] == season)
        ].copy()

        kategori_map = (
            mgr[["pcs_rider_id", "kategori"]]
            .drop_duplicates(subset=["pcs_rider_id"])
            .set_index("pcs_rider_id")["kategori"]
            .to_dict()
        )
        race_rows["kategori"] = race_rows["pcs_rider_id"].map(kategori_map).fillna("Unmapped")

        kategori_filter = st.multiselect(
            "Kategori filter",
            options=["Kategori 1", "Kategori 2", "Kategori 3", "Kategori 4", "Unmapped"],
            default=["Kategori 1", "Kategori 2", "Kategori 3", "Kategori 4", "Unmapped"],
            key="dash_kategori_filter",
        )
        filtered_race_rows = race_rows[race_rows["kategori"].isin(kategori_filter)].copy()

        c1, c2 = st.columns(2)
        c1.metric("Riders", int(len(filtered_race_rows)))
        c2.metric("Teams", int(filtered_race_rows["team_name"].nunique()))

        st.dataframe(
            filtered_race_rows[["rider_name", "team_name", "pcs_rider_id", "kategori"]]
            .sort_values(["team_name", "rider_name"]),
            use_container_width=True,
            hide_index=True,
        )

        team_counts = (
            filtered_race_rows.groupby("team_name", dropna=False)["pcs_rider_id"]
            .count()
            .reset_index(name="rider_count")
            .sort_values(["rider_count", "team_name"], ascending=[False, True])
        )
        st.subheader("Team Counts")
        st.dataframe(team_counts, use_container_width=True, hide_index=True)

    with tabs[1]:
        race_filter = st.selectbox("Race filter", options=["All"] + RACES, index=0, key="chg_race")
        limit = int(st.slider("Rows", min_value=10, max_value=200, value=50, step=10))

        rows = changes_df[changes_df["season"] == season].copy()
        if race_filter != "All":
            rows = rows[rows["canonical_name"] == race_filter]
        rows = rows.sort_values("event_at", ascending=False).head(limit)
        st.dataframe(rows, use_container_width=True, hide_index=True)

    with tabs[2]:
        hist_race = st.selectbox("History race", options=RACES, index=1, key="hist_race")
        kategori_filter = st.multiselect(
            "Kategori filter",
            options=["Kategori 1", "Kategori 2", "Kategori 3", "Kategori 4"],
            default=["Kategori 1", "Kategori 2", "Kategori 3", "Kategori 4"],
        )

        hist = history_df[
            (history_df["canonical_name"] == hist_race)
            & (history_df["season"].isin([2025, 2024]))
        ].copy()

        hist = hist.merge(
            mgr[["pcs_rider_id", "rider_name", "kategori"]].drop_duplicates("pcs_rider_id"),
            on="pcs_rider_id",
            how="inner",
            suffixes=("_history", "_manager"),
        )
        hist = hist[hist["kategori"].isin(kategori_filter)]
        hist = hist.sort_values(["kategori", "rank_position", "rider_name_manager", "season"], ascending=[True, True, True, False])

        st.dataframe(
            hist[["kategori", "rider_name_manager", "season", "rank_position", "status"]]
            .rename(columns={"rider_name_manager": "rider_name"}),
            use_container_width=True,
            hide_index=True,
        )

    with tabs[3]:
        st.markdown("Pick a rider and view history across all 13 races.")

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

            st.write(
                f"Selected: **{selected_row['rider_name']}** ({selected_row['kategori']}) - `{selected_pcs}`"
            )

            rider_start = startlist_df[
                (startlist_df["season"] == season) & (startlist_df["pcs_rider_id"] == selected_pcs)
            ]
            entered_races = sorted(set(rider_start["canonical_name"].tolist()), key=RACES.index)

            c1, c2 = st.columns(2)
            c1.metric(f"Entered Races ({season})", len(entered_races))
            c2.metric("Not Entered", max(len(RACES) - len(entered_races), 0))

            st.subheader(f"Currently Entered ({season})")
            if entered_races:
                st.write(", ".join(entered_races))
            else:
                st.caption("Not currently on any tracked race startlist.")

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

            st.subheader("All Races History")
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

            rider_changes = changes_df[
                (changes_df["season"] == season) & (changes_df["pcs_rider_id"] == selected_pcs)
            ].sort_values("event_at", ascending=False)
            st.subheader("Recent Startlist Changes")
            if rider_changes.empty:
                st.caption("No recorded change events for this rider in current season.")
            else:
                st.dataframe(rider_changes[["canonical_name", "event_type", "event_at"]], use_container_width=True, hide_index=True)

    with tabs[4]:
        st.markdown("Use locks + race block to simulate remaining slot suggestions.")
        target = st.selectbox("Target race", options=RACES, index=1, key="sim_target")
        lookahead = int(st.slider("Lookahead rounds", min_value=0, max_value=5, value=2))
        locks_text = st.text_input("Locks (comma-separated names or slugs)", value="tadej pogacar,thomas pidcock")
        run_sim = st.button("Run Simulation", type="primary")

        if run_sim:
            lock_matches, unresolved = resolve_locks(mgr, locks_text)
            block = race_block(target, lookahead)
            st.write("Race block:", ", ".join(block))

            if lock_matches:
                st.subheader("Resolved Locks")
                lock_df = pd.DataFrame(
                    [
                        {
                            "input": item.input_name,
                            "rider_name": item.rider_name,
                            "pcs_rider_id": item.pcs_rider_id,
                            "kategori": item.kategori,
                            "method": item.method,
                            "score": round(item.score, 3),
                        }
                        for item in lock_matches
                    ]
                )
                st.dataframe(lock_df, use_container_width=True, hide_index=True)

            if unresolved:
                st.warning("Unresolved locks: " + ", ".join(unresolved))

            lock_ids = {item.pcs_rider_id for item in lock_matches}
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
            st.subheader("Slot Needs")
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

            if cand.empty:
                st.info("No candidates found for this race with current mapping and snapshot.")
            else:
                cand_rows = []
                for row in cand.itertuples(index=False):
                    pcs = str(row.pcs_rider_id)
                    t25 = _best_finish_rank(history_df, pcs, target, 2025)
                    t24 = _best_finish_rank(history_df, pcs, target, 2024)
                    score = float(races_in_block.get(pcs, 0)) * 100.0
                    if t25 is not None:
                        score += max(121 - int(t25), 0)
                    if t24 is not None:
                        score += max(121 - int(t24), 0) * 0.7

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
                    cand_rows.append(out)

                cand_df = pd.DataFrame(cand_rows)

                for kategori in CATEGORY_SLOTS:
                    needed = next((n["needed"] for n in needs_rows if n["kategori"] == kategori), 0)
                    if needed <= 0:
                        continue
                    top = cand_df[cand_df["kategori"] == kategori].sort_values(
                        ["score", "manager_rider_name"],
                        ascending=[False, True],
                    )
                    st.subheader(f"Suggestions: {kategori} (top {needed})")
                    st.dataframe(top.head(needed), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    app()

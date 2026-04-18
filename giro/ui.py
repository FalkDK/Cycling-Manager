from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st

from giro.snapshot import (
    BROWSER_COLUMNS,
    RESULT_COLUMNS,
    BOOL_COLUMNS,
    NUMERIC_COLUMNS,
    load_giro_snapshot,
)


class DatabaseError(RuntimeError):
    pass


def _schema_path() -> str:
    return os.fspath(Path(__file__).resolve().with_name("schema.sql"))


@st.cache_resource(show_spinner=False)
def _get_repository(db_url: str):
    try:
        from fantasy_cycling.db import PostgresClient, Repository
    except ModuleNotFoundError as err:
        raise DatabaseError(
            "Database-backed Giro mode is unavailable because the fantasy_cycling package is not installed."
        ) from err
    client = PostgresClient(db_url=db_url or None)
    repo = Repository(client)
    repo.init_schema(_schema_path())
    return repo


def _query_df(client, sql: str, columns: list[str]) -> pd.DataFrame:
    rows = client.query_rows(sql)
    frame = pd.DataFrame(rows, columns=columns)
    for column in [col for col in columns if col in NUMERIC_COLUMNS]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in [col for col in columns if col in BOOL_COLUMNS]:
        frame[column] = frame[column].map({"t": True, "f": False}).fillna(False)
    for column in ["last_result_date_2026", "result_date", "fetched_at"]:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce", utc=(column == "fetched_at"))
    return frame


@st.cache_data(show_spinner=False, ttl=120)
def load_giro_frames(db_url: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    repo = _get_repository(db_url)
    browser_df = _query_df(
        repo.client,
        """
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
        ORDER BY holdet_team_name, manager_category, holdet_rider_name;
        """,
        BROWSER_COLUMNS,
    )
    result_df = _query_df(
        repo.client,
        """
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
        ORDER BY holdet_rider_name, result_date DESC, race_name, result_label;
        """,
        RESULT_COLUMNS,
    )
    return browser_df, result_df


def _format_rank(value: object) -> str:
    if value is None or pd.isna(value):
        return "-"
    return str(int(value))


def _format_number(value: object, decimals: int = 0) -> str:
    if value is None or pd.isna(value):
        return "-"
    if decimals == 0:
        return str(int(round(float(value))))
    return f"{float(value):.{decimals}f}"


def _set_selected_rider(state_key: str, holdet_person_id: int | None) -> None:
    if holdet_person_id is not None:
        st.session_state[state_key] = int(holdet_person_id)


def _render_selectable_table(rows: pd.DataFrame, display_columns: list[str], key: str) -> int | None:
    selectable_rows = rows.reset_index(drop=True).copy()
    display_df = selectable_rows[display_columns].copy()
    event = None
    try:
        event = st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            key=key,
            on_select="rerun",
            selection_mode="single-row",
        )
    except TypeError:
        st.dataframe(display_df, use_container_width=True, hide_index=True, key=key)
        return None

    selection = getattr(event, "selection", None)
    selected_rows = getattr(selection, "rows", []) if selection is not None else []
    if not selected_rows:
        return None
    return int(selectable_rows.iloc[selected_rows[0]]["holdet_person_id"])


def _render_rider_selector(rows: pd.DataFrame, label: str, key: str, state_key: str) -> None:
    if rows.empty:
        return
    options = rows[["holdet_person_id", "holdet_rider_name", "holdet_team_name", "pcs_rider_id"]].copy()
    options["label"] = options.apply(
        lambda row: f"{row['holdet_rider_name']} | {row['holdet_team_name']} | {row['pcs_rider_id']}",
        axis=1,
    )
    option_labels = options["label"].tolist()
    option_ids = options["holdet_person_id"].astype(int).tolist()
    current_id = st.session_state.get(state_key)
    default_index = option_ids.index(current_id) if current_id in option_ids else 0
    selected_label = st.selectbox(label, options=option_labels, index=default_index, key=key)
    selected_id = int(options.iloc[option_labels.index(selected_label)]["holdet_person_id"])
    _set_selected_rider(state_key, selected_id)


def _render_grouped_browser(
    rows: pd.DataFrame,
    group_column: str | None,
    display_columns: list[str],
    key_prefix: str,
    state_key: str,
) -> None:
    if rows.empty:
        st.info("No riders match the current filters.")
        return

    if not group_column:
        selected_id = _render_selectable_table(rows, display_columns, key=f"{key_prefix}_all")
        _set_selected_rider(state_key, selected_id)
        return

    group_summary = (
        rows.groupby(group_column, dropna=False)["holdet_person_id"]
        .count()
        .reset_index(name="riders")
        .sort_values(["riders", group_column], ascending=[False, True])
    )
    layout_left, layout_right = st.columns([2.4, 1.0])
    with layout_right:
        st.caption("Visible groups")
        st.dataframe(group_summary, use_container_width=True, hide_index=True)

    with layout_left:
        for group_value, group_rows in rows.groupby(group_column, dropna=False, sort=True):
            title = group_value if pd.notna(group_value) and str(group_value).strip() else "Unspecified"
            st.markdown(f"#### {title} ({len(group_rows)})")
            selected_id = _render_selectable_table(
                group_rows.sort_values(display_columns).reset_index(drop=True),
                display_columns,
                key=f"{key_prefix}_{str(title).lower().replace(' ', '_')}",
            )
            _set_selected_rider(state_key, selected_id)


def _prepare_manager_rows(browser_df: pd.DataFrame) -> pd.DataFrame:
    rows = browser_df[browser_df["manager_category"].notna()].copy()
    rows["manager_category"] = rows["manager_category"].fillna("Unspecified")
    return rows.sort_values(["manager_category", "holdet_team_name", "holdet_rider_name"]).reset_index(drop=True)


def _prepare_trading_rows(browser_df: pd.DataFrame) -> pd.DataFrame:
    rows = browser_df[browser_df["trading_price"].notna()].copy()
    rows["trading_price"] = pd.to_numeric(rows["trading_price"], errors="coerce")
    return rows.sort_values(["holdet_team_name", "trading_price", "holdet_rider_name"]).reset_index(drop=True)


def _render_manager_tab(browser_df: pd.DataFrame, result_df: pd.DataFrame) -> None:
    manager_rows = _prepare_manager_rows(browser_df)
    controls = st.columns([1.0, 1.3, 1.7])
    group_by_label = controls[0].selectbox(
        "Group by",
        options=["Kategori", "Team", "None"],
        index=0,
        key="giro_manager_group_by",
    )
    selected_categories = controls[1].multiselect(
        "Kategori",
        options=sorted(manager_rows["manager_category"].dropna().unique().tolist()),
        default=sorted(manager_rows["manager_category"].dropna().unique().tolist()),
        key="giro_manager_categories",
    )
    search = controls[2].text_input("Search rider or PCS slug", key="giro_manager_search").strip().lower()

    filtered = manager_rows[manager_rows["manager_category"].isin(selected_categories)].copy()
    if search:
        filtered = filtered[
            filtered["holdet_rider_name"].str.lower().str.contains(search, na=False)
            | filtered["pcs_rider_id"].str.lower().str.contains(search, na=False)
            | filtered["holdet_team_name"].str.lower().str.contains(search, na=False)
        ]

    metrics = st.columns(4)
    metrics[0].metric("Visible riders", int(len(filtered)))
    metrics[1].metric("Teams", int(filtered["holdet_team_name"].nunique()) if not filtered.empty else 0)
    metrics[2].metric("Kategori 1", int((filtered["manager_category"] == "Kategori 1").sum()))
    metrics[3].metric("Out", int(filtered["manager_is_out"].sum()))

    st.caption("Click a row to open rider detail. If your Streamlit version does not support row clicks, use the selector below.")
    group_column = {"Kategori": "manager_category", "Team": "holdet_team_name", "None": None}[group_by_label]
    _render_grouped_browser(
        filtered,
        group_column=group_column,
        display_columns=[
            "holdet_rider_name",
            "holdet_team_name",
            "manager_category",
            "race_days_2026",
            "gt_stage_days_2025",
            "best_gt_gc_result_2025",
            "pcs_rider_id",
        ],
        key_prefix="giro_manager",
        state_key="giro_manager_selected_person_id",
    )
    _render_rider_selector(
        filtered,
        "Open manager rider detail",
        key="giro_manager_detail_select",
        state_key="giro_manager_selected_person_id",
    )
    _render_rider_detail(
        browser_df,
        result_df,
        selected_id=st.session_state.get("giro_manager_selected_person_id"),
        header="Manager rider detail",
    )


def _render_trading_tab(browser_df: pd.DataFrame, result_df: pd.DataFrame) -> None:
    trading_rows = _prepare_trading_rows(browser_df)
    min_price = int(trading_rows["trading_price"].min()) if not trading_rows.empty else 0
    max_price = int(trading_rows["trading_price"].max()) if not trading_rows.empty else 0

    controls = st.columns([1.0, 1.3, 1.7])
    group_by_label = controls[0].selectbox(
        "Group by",
        options=["Team", "None"],
        index=0,
        key="giro_trading_group_by",
    )
    include_out = controls[1].checkbox("Include riders marked out", value=True, key="giro_trading_include_out")
    search = controls[2].text_input("Search rider or PCS slug", key="giro_trading_search").strip().lower()

    price_range = st.slider(
        "Price range",
        min_value=min_price,
        max_value=max_price,
        value=(min_price, max_price),
        key="giro_trading_price_range",
    )

    filtered = trading_rows[
        trading_rows["trading_price"].between(price_range[0], price_range[1], inclusive="both")
    ].copy()
    if not include_out:
        filtered = filtered[~filtered["trading_is_out"]]
    if search:
        filtered = filtered[
            filtered["holdet_rider_name"].str.lower().str.contains(search, na=False)
            | filtered["pcs_rider_id"].str.lower().str.contains(search, na=False)
            | filtered["holdet_team_name"].str.lower().str.contains(search, na=False)
        ]

    metrics = st.columns(4)
    metrics[0].metric("Visible riders", int(len(filtered)))
    metrics[1].metric("Teams", int(filtered["holdet_team_name"].nunique()) if not filtered.empty else 0)
    metrics[2].metric("Avg price", _format_number(filtered["trading_price"].mean()) if not filtered.empty else "-")
    metrics[3].metric("Out", int(filtered["trading_is_out"].sum()))

    st.caption("Click a row to open rider detail. Team grouping is usually the most useful view for the trading game.")
    group_column = {"Team": "holdet_team_name", "None": None}[group_by_label]
    _render_grouped_browser(
        filtered,
        group_column=group_column,
        display_columns=[
            "holdet_rider_name",
            "holdet_team_name",
            "trading_price",
            "trading_points",
            "race_days_2026",
            "gt_stage_days_2025",
            "pcs_rider_id",
        ],
        key_prefix="giro_trading",
        state_key="giro_trading_selected_person_id",
    )
    _render_rider_selector(
        filtered,
        "Open trading rider detail",
        key="giro_trading_detail_select",
        state_key="giro_trading_selected_person_id",
    )
    _render_rider_detail(
        browser_df,
        result_df,
        selected_id=st.session_state.get("giro_trading_selected_person_id"),
        header="Trading rider detail",
    )


def _render_rider_overview(row: pd.Series) -> None:
    metrics = st.columns(6)
    metrics[0].metric("Rider", str(row["holdet_rider_name"]))
    metrics[1].metric("Team", str(row["holdet_team_name"]))
    metrics[2].metric("Manager kategori", str(row["manager_category"] or "-"))
    metrics[3].metric("Trading price", _format_number(row["trading_price"]))
    metrics[4].metric("2026 race days", _format_number(row["race_days_2026"]))
    metrics[5].metric("2025 GT stage days", _format_number(row["gt_stage_days_2025"]))

    left, right = st.columns([1.3, 1.7])
    with left:
        st.caption("Game context")
        st.dataframe(
            pd.DataFrame(
                [
                    {"field": "PCS rider", "value": row["pcs_rider_name"]},
                    {"field": "PCS slug", "value": row["pcs_rider_id"]},
                    {"field": "Manager out", "value": "Yes" if row["manager_is_out"] else "No"},
                    {"field": "Trading out", "value": "Yes" if row["trading_is_out"] else "No"},
                    {"field": "Trading points", "value": _format_number(row["trading_points"], decimals=2)},
                    {"field": "Trading popularity", "value": _format_number(row["trading_popularity"], decimals=3)},
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )
    with right:
        st.caption("History summary")
        st.dataframe(
            pd.DataFrame(
                [
                    {"metric": "2026 results rows", "value": _format_number(row["results_2026_rows"])},
                    {"metric": "2026 distinct races", "value": _format_number(row["distinct_races_2026"])},
                    {"metric": "2026 wins / podiums / top10s", "value": f"{_format_number(row['wins_2026'])} / {_format_number(row['podiums_2026'])} / {_format_number(row['top10s_2026'])}"},
                    {"metric": "2026 PCS / UCI points", "value": f"{_format_number(row['pcs_points_2026_total'], 2)} / {_format_number(row['uci_points_2026_total'], 2)}"},
                    {"metric": "2025 GT count", "value": _format_number(row["gt_count_2025"])},
                    {"metric": "Best 2025 GT GC", "value": _format_rank(row["best_gt_gc_result_2025"])},
                    {"metric": "Best 2025 GT stage", "value": _format_rank(row["best_gt_stage_result_2025"])},
                    {"metric": "Best 2025 GT KOM / points / youth", "value": f"{_format_rank(row['best_gt_kom_result_2025'])} / {_format_rank(row['best_gt_points_result_2025'])} / {_format_rank(row['best_gt_youth_result_2025'])}"},
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )


def _render_history_table(rows: pd.DataFrame, empty_message: str) -> None:
    if rows.empty:
        st.info(empty_message)
        return
    display = rows[
        [
            "result_date",
            "race_name",
            "result_label",
            "result_scope",
            "grand_tour_slug",
            "rank_position",
            "pcs_points",
            "uci_points",
            "raw_result",
        ]
    ].copy()
    display["result_date"] = display["result_date"].dt.strftime("%Y-%m-%d")
    display["rank_position"] = display["rank_position"].map(_format_rank)
    st.dataframe(display, use_container_width=True, hide_index=True)


def _render_rider_detail(
    browser_df: pd.DataFrame,
    result_df: pd.DataFrame,
    selected_id: int | None,
    header: str,
) -> None:
    if selected_id is None:
        st.info("Select a rider above to inspect full Giro history and game context.")
        return

    selected_rows = browser_df[browser_df["holdet_person_id"] == int(selected_id)]
    if selected_rows.empty:
        st.info("The selected rider is not present in the current Giro browser dataset.")
        return

    row = selected_rows.iloc[0]
    rider_results = result_df[result_df["holdet_person_id"] == int(selected_id)].copy()
    grand_tour_2025 = rider_results[
        (rider_results["season"] == 2025)
        & (rider_results["grand_tour_slug"].isin(["giro-d-italia", "tour-de-france", "vuelta-a-espana"]))
    ].copy()
    results_2026 = rider_results[rider_results["season"] == 2026].copy()

    st.markdown("---")
    st.subheader(f"{header}: {row['holdet_rider_name']}")
    detail_tabs = st.tabs(["Overview", "2026 Results", "2025 Grand Tours", "All Imported"])

    with detail_tabs[0]:
        _render_rider_overview(row)
    with detail_tabs[1]:
        _render_history_table(results_2026, "No 2026 results imported for this rider yet.")
    with detail_tabs[2]:
        _render_history_table(grand_tour_2025, "No 2025 grand-tour results imported for this rider yet.")
    with detail_tabs[3]:
        _render_history_table(rider_results, "No history rows imported for this rider yet.")


def render_giro_workspace() -> None:
    st.markdown(
        """
        <div class="app-shell">
            <div class="app-kicker">Giro Workspace</div>
            <div class="app-title">Giro Rider Browser</div>
            <div class="app-subtitle">
                Explore the two Giro games from the same mapped rider universe. Start with the browser,
                then drill into imported PCS history before adding simulations and scoring layers.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.header("Giro Controls")
        default_snapshot_dir = os.getenv("GIRO_SNAPSHOT_DIR", "data/giro_snapshot_latest")
        snapshot_dir = st.text_input("Snapshot directory", value=default_snapshot_dir)
        st.caption("Snapshot files are preferred for public/read-only deployment. Database access is only a fallback.")

    db_url = os.getenv("FANTASY_CYCLING_DB_URL", "")
    snapshot_path = Path(snapshot_dir).expanduser()

    data_source = ""
    metadata: dict[str, object] = {}
    if snapshot_path.exists():
        try:
            metadata, browser_df, result_df = load_giro_snapshot(str(snapshot_path))
            data_source = f"Snapshot: {snapshot_path}"
        except RuntimeError as err:
            st.error(str(err))
            st.stop()
    else:
        if not db_url:
            st.error(
                "No Giro snapshot directory found and no database URL is available. "
                "Generate a snapshot or start Streamlit with FANTASY_CYCLING_DB_URL set."
            )
            st.stop()
        try:
            browser_df, result_df = load_giro_frames(db_url)
            data_source = "Database fallback"
        except DatabaseError as err:
            st.error(str(err))
            st.info("Make sure the Giro schema is reachable from this database and the Streamlit process can run `psql`.")
            st.stop()

    top = st.columns(5)
    top[0].metric("Mapped riders", int(len(browser_df)))
    top[1].metric("Manager riders", int(browser_df["manager_category"].notna().sum()))
    top[2].metric("Trading riders", int(browser_df["trading_price"].notna().sum()))
    top[3].metric("Imported history rows", int(len(result_df)))
    top[4].metric("2026 race days total", int(browser_df["race_days_2026"].sum()))
    st.caption(data_source)
    if metadata.get("generated_at"):
        st.caption(f"Snapshot generated: {metadata['generated_at']}")

    game_tabs = st.tabs(["Manager", "Trading"])
    with game_tabs[0]:
        _render_manager_tab(browser_df, result_df)
    with game_tabs[1]:
        _render_trading_tab(browser_df, result_df)

CREATE TABLE IF NOT EXISTS giro_game (
    game_id BIGINT PRIMARY KEY,
    cartridge_slug TEXT NOT NULL UNIQUE,
    cartridge_id BIGINT,
    game_name TEXT NOT NULL,
    edition_id BIGINT,
    stream_id BIGINT,
    ruleset_id BIGINT NOT NULL,
    ruleset_name TEXT NOT NULL,
    game_mode TEXT NOT NULL CHECK (game_mode IN ('MANAGER', 'TRADING', 'UNKNOWN')),
    allows_stage_substitutions BOOLEAN NOT NULL DEFAULT FALSE,
    salary_cap BIGINT,
    transfer_fee NUMERIC(12,6),
    interest_rate NUMERIC(12,6),
    captain_bonus_assets INTEGER,
    captain_bonus_points INTEGER,
    source_url TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS giro_game_position (
    game_id BIGINT NOT NULL REFERENCES giro_game(game_id) ON DELETE CASCADE,
    position_id BIGINT NOT NULL,
    position_name TEXT NOT NULL,
    position_title TEXT NOT NULL,
    position_order INTEGER NOT NULL,
    PRIMARY KEY (game_id, position_id)
);

CREATE TABLE IF NOT EXISTS giro_holdet_person (
    holdet_person_id BIGINT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    rider_name TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS giro_raw_payload_archive (
    raw_payload_id BIGSERIAL PRIMARY KEY,
    source_url TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    parser_version TEXT NOT NULL,
    UNIQUE (source_url, payload_sha256)
);

CREATE TABLE IF NOT EXISTS giro_player_pool_snapshot (
    snapshot_id BIGSERIAL PRIMARY KEY,
    game_id BIGINT NOT NULL REFERENCES giro_game(game_id) ON DELETE CASCADE,
    raw_payload_id BIGINT NOT NULL REFERENCES giro_raw_payload_archive(raw_payload_id),
    source_url TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    UNIQUE (game_id, raw_payload_id)
);

CREATE TABLE IF NOT EXISTS giro_player_pool_entry (
    snapshot_id BIGINT NOT NULL REFERENCES giro_player_pool_snapshot(snapshot_id) ON DELETE CASCADE,
    game_id BIGINT NOT NULL REFERENCES giro_game(game_id) ON DELETE CASCADE,
    holdet_player_id BIGINT NOT NULL,
    holdet_person_id BIGINT NOT NULL REFERENCES giro_holdet_person(holdet_person_id),
    holdet_team_id BIGINT,
    holdet_team_name TEXT NOT NULL,
    position_id BIGINT NOT NULL,
    position_name TEXT NOT NULL,
    position_title TEXT NOT NULL,
    start_price BIGINT,
    price BIGINT,
    points NUMERIC(12,2),
    popularity NUMERIC(12,6),
    is_out BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (snapshot_id, holdet_player_id),
    UNIQUE (snapshot_id, holdet_person_id)
);

CREATE TABLE IF NOT EXISTS giro_person_pcs_map (
    holdet_person_id BIGINT PRIMARY KEY REFERENCES giro_holdet_person(holdet_person_id) ON DELETE CASCADE,
    pcs_rider_id TEXT NOT NULL REFERENCES rider(pcs_rider_id),
    status TEXT NOT NULL DEFAULT 'APPROVED'
        CHECK (status IN ('APPROVED', 'REJECTED', 'PENDING')),
    confidence NUMERIC(5,4),
    mapping_source TEXT NOT NULL,
    mapped_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    note TEXT
);

CREATE TABLE IF NOT EXISTS giro_person_pcs_map_suggestion (
    suggestion_id BIGSERIAL PRIMARY KEY,
    game_id BIGINT NOT NULL REFERENCES giro_game(game_id) ON DELETE CASCADE,
    holdet_person_id BIGINT NOT NULL REFERENCES giro_holdet_person(holdet_person_id) ON DELETE CASCADE,
    holdet_player_id BIGINT NOT NULL,
    holdet_rider_name TEXT NOT NULL,
    holdet_team_name TEXT NOT NULL,
    position_title TEXT NOT NULL,
    pcs_rider_id TEXT NOT NULL REFERENCES rider(pcs_rider_id),
    pcs_rider_name TEXT NOT NULL,
    score NUMERIC(5,4) NOT NULL,
    suggestion_rank INTEGER NOT NULL CHECK (suggestion_rank >= 1),
    status TEXT NOT NULL CHECK (status IN ('APPROVED', 'REJECTED', 'PENDING')),
    mapping_source TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (game_id, holdet_person_id, pcs_rider_id)
);

CREATE TABLE IF NOT EXISTS giro_pcs_rider_result (
    giro_pcs_rider_result_id BIGSERIAL PRIMARY KEY,
    pcs_rider_id TEXT NOT NULL REFERENCES rider(pcs_rider_id) ON DELETE CASCADE,
    season INTEGER NOT NULL CHECK (season >= 1900),
    result_date DATE NOT NULL,
    race_name TEXT NOT NULL,
    result_label TEXT NOT NULL,
    result_scope TEXT NOT NULL CHECK (
        result_scope IN (
            'SEASON_RACE',
            'GRAND_TOUR_STAGE',
            'GRAND_TOUR_GC',
            'GRAND_TOUR_POINTS',
            'GRAND_TOUR_KOM',
            'GRAND_TOUR_YOUTH',
            'OTHER'
        )
    ),
    grand_tour_slug TEXT,
    race_class TEXT,
    rank_position INTEGER CHECK (rank_position IS NULL OR rank_position > 0),
    raw_result TEXT NOT NULL,
    kms NUMERIC(8,1),
    pcs_points NUMERIC(10,2),
    uci_points NUMERIC(10,2),
    vertical_meters INTEGER,
    source_url TEXT,
    source_rider_results_url TEXT,
    fetched_at TIMESTAMPTZ NOT NULL,
    UNIQUE (pcs_rider_id, result_date, race_name, result_label, raw_result)
);

CREATE TABLE IF NOT EXISTS giro_pcs_history_import_status (
    pcs_rider_id TEXT NOT NULL REFERENCES rider(pcs_rider_id) ON DELETE CASCADE,
    target_season INTEGER NOT NULL CHECK (target_season >= 1900),
    grand_tour_season INTEGER NOT NULL CHECK (grand_tour_season >= 1900),
    status TEXT NOT NULL CHECK (status IN ('SUCCESS', 'FAILED')),
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    source_rider_results_url TEXT,
    error_message TEXT,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (pcs_rider_id, target_season, grand_tour_season)
);

CREATE INDEX IF NOT EXISTS idx_giro_game_mode
    ON giro_game (game_mode, cartridge_slug);

CREATE INDEX IF NOT EXISTS idx_giro_snapshot_lookup
    ON giro_player_pool_snapshot (game_id, fetched_at DESC, snapshot_id DESC);

CREATE INDEX IF NOT EXISTS idx_giro_entry_person
    ON giro_player_pool_entry (game_id, holdet_person_id, snapshot_id);

CREATE INDEX IF NOT EXISTS idx_giro_entry_price
    ON giro_player_pool_entry (game_id, price, holdet_team_name);

CREATE INDEX IF NOT EXISTS idx_giro_map_pcs
    ON giro_person_pcs_map (pcs_rider_id, status);

CREATE INDEX IF NOT EXISTS idx_giro_map_suggestion_lookup
    ON giro_person_pcs_map_suggestion (game_id, holdet_person_id, suggestion_rank);

CREATE INDEX IF NOT EXISTS idx_giro_pcs_rider_result_lookup
    ON giro_pcs_rider_result (pcs_rider_id, season, result_date DESC);

CREATE INDEX IF NOT EXISTS idx_giro_pcs_rider_result_scope
    ON giro_pcs_rider_result (result_scope, grand_tour_slug, result_date DESC);

CREATE INDEX IF NOT EXISTS idx_giro_pcs_history_import_status_lookup
    ON giro_pcs_history_import_status (target_season, grand_tour_season, status, updated_at DESC);

CREATE OR REPLACE VIEW v_giro_latest_player_pool AS
WITH latest_snapshot AS (
    SELECT DISTINCT ON (game_id)
        game_id,
        snapshot_id,
        fetched_at
    FROM giro_player_pool_snapshot
    ORDER BY game_id, fetched_at DESC, snapshot_id DESC
)
SELECT
    gg.cartridge_slug,
    gg.game_name,
    gg.game_mode,
    gg.allows_stage_substitutions,
    ls.fetched_at,
    e.holdet_player_id,
    e.holdet_person_id,
    hp.rider_name AS holdet_rider_name,
    e.holdet_team_name,
    e.position_title,
    e.start_price,
    e.price,
    e.points,
    e.popularity,
    e.is_out,
    m.pcs_rider_id,
    r.rider_name AS pcs_rider_name,
    m.status AS mapping_status,
    m.confidence,
    m.mapping_source
FROM latest_snapshot ls
JOIN giro_game gg ON gg.game_id = ls.game_id
JOIN giro_player_pool_entry e
  ON e.snapshot_id = ls.snapshot_id
 AND e.game_id = ls.game_id
JOIN giro_holdet_person hp ON hp.holdet_person_id = e.holdet_person_id
LEFT JOIN giro_person_pcs_map m ON m.holdet_person_id = e.holdet_person_id
LEFT JOIN rider r ON r.pcs_rider_id = m.pcs_rider_id
ORDER BY gg.cartridge_slug, e.position_title, hp.rider_name;

CREATE OR REPLACE VIEW v_giro_person_pcs_map_suggestions AS
SELECT
    s.game_id,
    g.cartridge_slug,
    s.holdet_person_id,
    s.holdet_player_id,
    s.holdet_rider_name,
    s.holdet_team_name,
    s.position_title,
    s.pcs_rider_id,
    s.pcs_rider_name,
    s.score,
    s.suggestion_rank,
    s.status AS suggestion_status,
    s.mapping_source,
    s.created_at
FROM giro_person_pcs_map_suggestion s
JOIN giro_game g ON g.game_id = s.game_id
ORDER BY s.holdet_person_id, s.suggestion_rank, s.score DESC, s.game_id;

CREATE OR REPLACE VIEW v_giro_mapped_rider_results AS
SELECT
    gp.holdet_person_id,
    hp.rider_name AS holdet_rider_name,
    gp.pcs_rider_id,
    r.rider_name AS pcs_rider_name,
    pr.season,
    pr.result_date,
    pr.race_name,
    pr.result_label,
    pr.result_scope,
    pr.grand_tour_slug,
    pr.race_class,
    pr.rank_position,
    pr.raw_result,
    pr.kms,
    pr.pcs_points,
    pr.uci_points,
    pr.vertical_meters,
    pr.source_url,
    pr.fetched_at
FROM giro_person_pcs_map gp
JOIN giro_holdet_person hp ON hp.holdet_person_id = gp.holdet_person_id
JOIN rider r ON r.pcs_rider_id = gp.pcs_rider_id
JOIN giro_pcs_rider_result pr ON pr.pcs_rider_id = gp.pcs_rider_id
WHERE gp.status = 'APPROVED'
ORDER BY hp.rider_name, pr.result_date DESC, pr.race_name, pr.result_label;

CREATE OR REPLACE VIEW v_giro_mapped_rider_results_2026 AS
SELECT *
FROM v_giro_mapped_rider_results
WHERE season = 2026
ORDER BY holdet_rider_name, result_date DESC, race_name, result_label;

CREATE OR REPLACE VIEW v_giro_mapped_grand_tour_results_2025 AS
SELECT *
FROM v_giro_mapped_rider_results
WHERE season = 2025
  AND grand_tour_slug IN ('giro-d-italia', 'tour-de-france', 'vuelta-a-espana')
  AND result_scope IN (
      'GRAND_TOUR_STAGE',
      'GRAND_TOUR_GC',
      'GRAND_TOUR_POINTS',
      'GRAND_TOUR_KOM',
      'GRAND_TOUR_YOUTH'
  )
ORDER BY holdet_rider_name, grand_tour_slug, result_date DESC, result_scope, result_label;

CREATE OR REPLACE VIEW v_giro_rider_summary AS
WITH mapped AS (
    SELECT
        gp.holdet_person_id,
        hp.rider_name AS holdet_rider_name,
        gp.pcs_rider_id,
        r.rider_name AS pcs_rider_name
    FROM giro_person_pcs_map gp
    JOIN giro_holdet_person hp ON hp.holdet_person_id = gp.holdet_person_id
    JOIN rider r ON r.pcs_rider_id = gp.pcs_rider_id
    WHERE gp.status = 'APPROVED'
),
results AS (
    SELECT
        m.holdet_person_id,
        count(*) FILTER (WHERE pr.season = 2026) AS results_2026_rows,
        count(DISTINCT pr.result_date) FILTER (WHERE pr.season = 2026) AS race_days_2026,
        count(DISTINCT pr.race_name) FILTER (WHERE pr.season = 2026) AS distinct_races_2026,
        count(*) FILTER (
            WHERE pr.season = 2026
              AND pr.result_scope = 'SEASON_RACE'
        ) AS stage_rows_2026,
        count(*) FILTER (
            WHERE pr.season = 2026
              AND pr.result_scope <> 'SEASON_RACE'
        ) AS classification_rows_2026,
        count(*) FILTER (
            WHERE pr.season = 2026
              AND pr.rank_position = 1
        ) AS wins_2026,
        count(*) FILTER (
            WHERE pr.season = 2026
              AND pr.rank_position <= 3
        ) AS podiums_2026,
        count(*) FILTER (
            WHERE pr.season = 2026
              AND pr.rank_position <= 10
        ) AS top10s_2026,
        coalesce(sum(pr.pcs_points) FILTER (WHERE pr.season = 2026), 0) AS pcs_points_2026_total,
        coalesce(sum(pr.uci_points) FILTER (WHERE pr.season = 2026), 0) AS uci_points_2026_total,
        max(pr.result_date) FILTER (WHERE pr.season = 2026) AS last_result_date_2026,
        count(*) FILTER (
            WHERE pr.season = 2025
              AND pr.result_scope = 'GRAND_TOUR_STAGE'
        ) AS gt_stage_rows_2025,
        count(DISTINCT pr.result_date) FILTER (
            WHERE pr.season = 2025
              AND pr.result_scope = 'GRAND_TOUR_STAGE'
        ) AS gt_stage_days_2025,
        count(DISTINCT pr.grand_tour_slug) FILTER (
            WHERE pr.season = 2025
              AND pr.grand_tour_slug IN ('giro-d-italia', 'tour-de-france', 'vuelta-a-espana')
        ) AS gt_count_2025,
        count(*) FILTER (
            WHERE pr.season = 2025
              AND pr.result_scope = 'GRAND_TOUR_STAGE'
              AND pr.grand_tour_slug = 'giro-d-italia'
        ) AS giro_stage_rows_2025,
        count(*) FILTER (
            WHERE pr.season = 2025
              AND pr.result_scope = 'GRAND_TOUR_STAGE'
              AND pr.grand_tour_slug = 'tour-de-france'
        ) AS tour_stage_rows_2025,
        count(*) FILTER (
            WHERE pr.season = 2025
              AND pr.result_scope = 'GRAND_TOUR_STAGE'
              AND pr.grand_tour_slug = 'vuelta-a-espana'
        ) AS vuelta_stage_rows_2025,
        min(pr.rank_position) FILTER (
            WHERE pr.season = 2025
              AND pr.result_scope = 'GRAND_TOUR_STAGE'
        ) AS best_gt_stage_result_2025,
        min(pr.rank_position) FILTER (
            WHERE pr.season = 2025
              AND pr.result_scope = 'GRAND_TOUR_GC'
        ) AS best_gt_gc_result_2025,
        min(pr.rank_position) FILTER (
            WHERE pr.season = 2025
              AND pr.result_scope = 'GRAND_TOUR_POINTS'
        ) AS best_gt_points_result_2025,
        min(pr.rank_position) FILTER (
            WHERE pr.season = 2025
              AND pr.result_scope = 'GRAND_TOUR_KOM'
        ) AS best_gt_kom_result_2025,
        min(pr.rank_position) FILTER (
            WHERE pr.season = 2025
              AND pr.result_scope = 'GRAND_TOUR_YOUTH'
        ) AS best_gt_youth_result_2025,
        count(*) FILTER (
            WHERE pr.season = 2025
              AND pr.result_scope = 'GRAND_TOUR_STAGE'
              AND pr.rank_position = 1
        ) AS gt_stage_wins_2025,
        count(*) FILTER (
            WHERE pr.season = 2025
              AND pr.result_scope = 'GRAND_TOUR_STAGE'
              AND pr.rank_position <= 5
        ) AS gt_stage_top5s_2025,
        count(*) FILTER (
            WHERE pr.season = 2025
              AND pr.result_scope = 'GRAND_TOUR_STAGE'
              AND pr.rank_position <= 10
        ) AS gt_stage_top10s_2025
    FROM mapped m
    LEFT JOIN giro_pcs_rider_result pr ON pr.pcs_rider_id = m.pcs_rider_id
    GROUP BY m.holdet_person_id
)
SELECT
    m.holdet_person_id,
    m.holdet_rider_name,
    m.pcs_rider_id,
    m.pcs_rider_name,
    coalesce(r.results_2026_rows, 0) AS results_2026_rows,
    coalesce(r.race_days_2026, 0) AS race_days_2026,
    coalesce(r.distinct_races_2026, 0) AS distinct_races_2026,
    coalesce(r.stage_rows_2026, 0) AS stage_rows_2026,
    coalesce(r.classification_rows_2026, 0) AS classification_rows_2026,
    coalesce(r.wins_2026, 0) AS wins_2026,
    coalesce(r.podiums_2026, 0) AS podiums_2026,
    coalesce(r.top10s_2026, 0) AS top10s_2026,
    coalesce(r.pcs_points_2026_total, 0) AS pcs_points_2026_total,
    coalesce(r.uci_points_2026_total, 0) AS uci_points_2026_total,
    r.last_result_date_2026,
    coalesce(r.gt_stage_rows_2025, 0) AS gt_stage_rows_2025,
    coalesce(r.gt_stage_days_2025, 0) AS gt_stage_days_2025,
    coalesce(r.gt_count_2025, 0) AS gt_count_2025,
    coalesce(r.giro_stage_rows_2025, 0) AS giro_stage_rows_2025,
    coalesce(r.tour_stage_rows_2025, 0) AS tour_stage_rows_2025,
    coalesce(r.vuelta_stage_rows_2025, 0) AS vuelta_stage_rows_2025,
    r.best_gt_stage_result_2025,
    r.best_gt_gc_result_2025,
    r.best_gt_points_result_2025,
    r.best_gt_kom_result_2025,
    r.best_gt_youth_result_2025,
    coalesce(r.gt_stage_wins_2025, 0) AS gt_stage_wins_2025,
    coalesce(r.gt_stage_top5s_2025, 0) AS gt_stage_top5s_2025,
    coalesce(r.gt_stage_top10s_2025, 0) AS gt_stage_top10s_2025
FROM mapped m
LEFT JOIN results r ON r.holdet_person_id = m.holdet_person_id
ORDER BY m.holdet_rider_name;

CREATE OR REPLACE VIEW v_giro_rider_browser AS
WITH manager_pool AS (
    SELECT
        holdet_person_id,
        holdet_player_id AS manager_holdet_player_id,
        holdet_team_name,
        position_title AS manager_category,
        is_out AS manager_is_out
    FROM v_giro_latest_player_pool
    WHERE cartridge_slug = 'giro-d-italia-manager-2026'
),
trading_pool AS (
    SELECT
        holdet_person_id,
        holdet_player_id AS trading_holdet_player_id,
        holdet_team_name,
        start_price AS trading_start_price,
        price AS trading_price,
        points AS trading_points,
        popularity AS trading_popularity,
        is_out AS trading_is_out
    FROM v_giro_latest_player_pool
    WHERE cartridge_slug = 'giro-d-italia-2026'
)
SELECT
    s.holdet_person_id,
    s.holdet_rider_name,
    s.pcs_rider_id,
    s.pcs_rider_name,
    coalesce(mp.holdet_team_name, tp.holdet_team_name) AS holdet_team_name,
    mp.manager_holdet_player_id,
    mp.manager_category,
    coalesce(mp.manager_is_out, FALSE) AS manager_is_out,
    tp.trading_holdet_player_id,
    tp.trading_start_price,
    tp.trading_price,
    tp.trading_points,
    tp.trading_popularity,
    coalesce(tp.trading_is_out, FALSE) AS trading_is_out,
    s.results_2026_rows,
    s.race_days_2026,
    s.distinct_races_2026,
    s.stage_rows_2026,
    s.classification_rows_2026,
    s.wins_2026,
    s.podiums_2026,
    s.top10s_2026,
    s.pcs_points_2026_total,
    s.uci_points_2026_total,
    s.last_result_date_2026,
    s.gt_stage_rows_2025,
    s.gt_stage_days_2025,
    s.gt_count_2025,
    s.giro_stage_rows_2025,
    s.tour_stage_rows_2025,
    s.vuelta_stage_rows_2025,
    s.best_gt_stage_result_2025,
    s.best_gt_gc_result_2025,
    s.best_gt_points_result_2025,
    s.best_gt_kom_result_2025,
    s.best_gt_youth_result_2025,
    s.gt_stage_wins_2025,
    s.gt_stage_top5s_2025,
    s.gt_stage_top10s_2025
FROM v_giro_rider_summary s
LEFT JOIN manager_pool mp ON mp.holdet_person_id = s.holdet_person_id
LEFT JOIN trading_pool tp ON tp.holdet_person_id = s.holdet_person_id
ORDER BY holdet_team_name, manager_category, holdet_rider_name;

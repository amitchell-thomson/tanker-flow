CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;

-- Raw AIS fixes: append-only, never updated
CREATE TABLE ais_fixes (
    server_ts       TIMESTAMPTZ      DEFAULT now(),
    fix_ts          TIMESTAMPTZ      NOT NULL,
    mmsi            BIGINT           NOT NULL,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    nav_status      SMALLINT,
    sog             REAL,
    source          TEXT             NOT NULL  -- 'aisstream-mmsi-{1,2,3}' | 'vesselfinder'
);

SELECT create_hypertable('ais_fixes', 'fix_ts');
SELECT set_chunk_time_interval('ais_fixes', INTERVAL '1 day');
CREATE UNIQUE INDEX ON ais_fixes (fix_ts, mmsi);
-- Latest-fix-per-vessel lookups (viz /api/vessels LATERAL, density track order).
-- Without it those queries seq-scan + disk-sort the whole hypertable.
CREATE INDEX IF NOT EXISTS ais_fixes_mmsi_fix_ts_idx ON ais_fixes (mmsi, fix_ts DESC);


CREATE TABLE vessel_state(
    server_ts       TIMESTAMPTZ     DEFAULT now(),
    state_ts        TIMESTAMPTZ     NOT NULL,
    mmsi            BIGINT          NOT NULL,
    draught         REAL,
    dest            TEXT,
    eta             JSONB,
    source          TEXT            NOT NULL
);

SELECT create_hypertable('vessel_state', 'state_ts');
SELECT set_chunk_time_interval('vessel_state', INTERVAL '1 day');
CREATE UNIQUE INDEX ON vessel_state (state_ts, mmsi);
-- Latest-draught-per-vessel lookups (viz /api/vessels LATERAL, laden.py).
CREATE INDEX IF NOT EXISTS vessel_state_mmsi_state_ts_idx ON vessel_state (mmsi, state_ts DESC);

-- Vessel registry: populated passively + enriched from VesselFinder
CREATE TABLE vessel_registry (
    mmsi                BIGINT           PRIMARY KEY,
    imo                 BIGINT,
    vessel_name         TEXT,
    call_sign           TEXT,
    vessel_type         SMALLINT,        -- AIS numeric type code (80-89 = tanker)
    flag                TEXT,
    -- VesselFinder enrichment
    vf_vessel_type      TEXT,            -- e.g. 'LNG Tanker', 'FSRU'
    year_built          SMALLINT,
    builder             TEXT,
    owner               TEXT,
    manager             TEXT,
    length_m            REAL,
    beam_m              REAL,
    gross_tonnage       INTEGER,
    net_tonnage         INTEGER,
    dwt                 INTEGER,
    design_draught      REAL,
    teu                 INTEGER,
    crude_capacity      INTEGER,
    gas_capacity_m3     INTEGER,
    -- Derived classification
    is_lng_carrier      BOOLEAN,
    is_fsru             BOOLEAN,
    excluded            BOOLEAN          NOT NULL DEFAULT FALSE,
    exclusion_reason    TEXT,
    -- Enrichment tracking
    enriched_at         TIMESTAMPTZ,
    vf_enrichment_status TEXT,           -- 'ok'|'not_found'|'error'|'pending'
    updated_at          TIMESTAMPTZ      DEFAULT now()
);

-- LNG terminal metadata: one row per terminal, referenced by terminal_zones
CREATE TABLE terminals (
    terminal_id     SERIAL PRIMARY KEY,
    terminal_name   VARCHAR(100) NOT NULL UNIQUE,
    country         CHAR(2)      NOT NULL,
    flow_direction  VARCHAR(10)  NOT NULL CHECK (flow_direction IN ('export','import')),
    in_signal_scope BOOLEAN      NOT NULL DEFAULT TRUE,
    is_fsru         BOOLEAN      NOT NULL DEFAULT FALSE,
    zone            TEXT         CHECK (zone IN ('usgulf','usatlantic','nweurope','baltic','iberian','wmed','emed')),
    fsru_host_mmsi  BIGINT,      -- For FSRU terminals: the MMSI of the resident FSRU vessel
    unlocode        TEXT,        -- UN/LOCODE (e.g. NLRTM) used by dest parser to resolve vessel_state.dest → terminal_id
    notes           TEXT
);

-- LNG terminal zones: polygons imported from QGIS, used for port event detection.
-- zone_type values:
--   'berth'     — vessel is alongside the terminal, cargo ops possible
--   'anchorage' — designated anchor area where vessels queue before berthing
--   'approach'  — macro envelope containing anchorage + channel + berth;
--                 covers the transit between anchorage and berth so the visit
--                 envelope stays open during channel transit
CREATE TABLE terminal_zones (
    id              SERIAL PRIMARY KEY,
    terminal_id     INTEGER      NOT NULL REFERENCES terminals(terminal_id),
    zone_type       VARCHAR(20)  NOT NULL CHECK (zone_type IN ('berth','anchorage','approach')),
    sub_zone        SMALLINT     NOT NULL DEFAULT 0,
    is_provisional  BOOLEAN      NOT NULL DEFAULT TRUE,
    source          VARCHAR(30),
    notes           TEXT,
    geom            geometry(MultiPolygon, 4326) NOT NULL,

    UNIQUE (terminal_id, zone_type, sub_zone)
);

CREATE INDEX idx_terminal_zones_geom     ON terminal_zones USING GIST (geom);
CREATE INDEX idx_terminal_zones_terminal ON terminal_zones (terminal_id);

-- Port events: derived from ais_fixes, recomputable.
-- cold_start = TRUE on the synthetic zone_entry + moored/anchored emitted when
-- a vessel's first observed fix is already inside a polygon (no preceding
-- transit observed).
CREATE TABLE port_events (
    id              BIGSERIAL        PRIMARY KEY,
    mmsi            BIGINT           NOT NULL,
    event_type      TEXT             NOT NULL,
    zone            TEXT             NOT NULL,
    terminal_id     INTEGER          REFERENCES terminals(terminal_id),
    event_time      TIMESTAMPTZ      NOT NULL,
    lat             REAL,
    lon             REAL,
    laden_flag      BOOLEAN,
    laden_source    TEXT             CHECK (laden_source IN ('draught', 'flow_direction')),
    -- Ingestion regime, generated from event_time vs the 2026-05-30 09:27 UTC
    -- cutover (mirrors config.REGIME_CUTOVER): 'bbox' = old throttled bbox
    -- subscription, 'mmsi_filter' = server-side MMSI filtering. STORED so it
    -- can never drift. See docs/review-2026-05-31-pre-signal-audit.md §0.
    regime          TEXT             GENERATED ALWAYS AS (
                        CASE WHEN event_time < TIMESTAMPTZ '2026-05-30 09:27:00+00'
                             THEN 'bbox' ELSE 'mmsi_filter' END) STORED,
    cold_start      BOOLEAN          NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ      DEFAULT now(),
    CONSTRAINT valid_event_type CHECK (
        event_type IN (
            'zone_entry','anchorage_entry','anchored','anchorage_exit',
            'moored','departed','zone_exit'
        )
    ),
    CONSTRAINT valid_zone CHECK (zone IN ('usgulf','usatlantic','nweurope','baltic','iberian','wmed','emed'))
);

CREATE INDEX ON port_events (mmsi, event_time DESC);
CREATE INDEX ON port_events (terminal_id, event_time DESC);
CREATE INDEX ON port_events (zone, event_type, event_time DESC);


-- Priority watchlist: derived nightly+hourly by pipeline/scoring.py. One row per
-- LNG/FSRU vessel in vessel_registry. The ingester reads top-N from this table
-- to pick the 150 MMSIs to subscribe to (100 persistent + 50 scan rotation).
CREATE TABLE priority_watchlist (
    mmsi                    BIGINT       PRIMARY KEY REFERENCES vessel_registry(mmsi),
    tier                    SMALLINT     NOT NULL,                -- 1-5; see pipeline/scoring.py
    score                   REAL         NOT NULL,                -- finer ordering within tier
    score_reason            TEXT,                                 -- e.g. 'in-zone:sabine', 'dest:NLRTM eta:3d'
    last_fix_ts             TIMESTAMPTZ,                          -- max(ais_fixes.fix_ts) for this mmsi
    last_zone_fix_ts        TIMESTAMPTZ,                          -- last fix inside any terminal_zones or config.ZONES rect
    parsed_dest_terminal_id INT          REFERENCES terminals(terminal_id),
    parsed_eta              TIMESTAMPTZ,
    in_slot                 BOOLEAN      NOT NULL DEFAULT FALSE,  -- set TRUE by aisstream.py after picking the 150
    slot_kind               TEXT,                                 -- 'persistent' | 'scan' | 'pinned' | NULL
    is_pinned               BOOLEAN      NOT NULL DEFAULT FALSE,  -- recent open laden leg (set by scoring.py); forced into a persistent slot so we re-acquire the vessel on its European approach (M1)
    last_scan_window_at     TIMESTAMPTZ,                          -- bumped each time a vessel is picked for a scan window; used to rotate the scan queue
    computed_at             TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX ix_priority_watchlist_tier_last_fix ON priority_watchlist (tier, last_fix_ts DESC);
CREATE INDEX ix_priority_watchlist_slot_kind_last_fix ON priority_watchlist (slot_kind, last_fix_ts);
CREATE INDEX ix_priority_watchlist_tier_scan_window ON priority_watchlist (tier, last_scan_window_at ASC NULLS FIRST);


-- Tier-promotion log: append-only record of priority_watchlist tier improvements.
-- Lets the TUI show recent promotions across restarts (not just since it started).
-- Written by pipeline/scoring.py (via='scoring', periodic re-rank) and
-- ingestion/aisstream.py (via='inline', instant promotion on a live in-zone fix).
-- vessel_name + zone are denormalised at write so the panel needs no extra join.
CREATE TABLE tier_promotions (
    id            BIGSERIAL    PRIMARY KEY,
    promoted_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    mmsi          BIGINT       NOT NULL,
    vessel_name   TEXT,
    old_tier      SMALLINT,
    new_tier      SMALLINT     NOT NULL,
    via           TEXT         NOT NULL CHECK (via IN ('scoring', 'inline')),
    reason        TEXT,
    zone          TEXT
);
CREATE INDEX ix_tier_promotions_promoted_at ON tier_promotions (promoted_at DESC);


-- Ingestion lifecycle events: append-only.
-- event_type values: 'connect','subscribed','planned_reconnect','disconnect','error','final_flush'
CREATE TABLE ingestion_events (
    event_ts        TIMESTAMPTZ      NOT NULL DEFAULT now(),
    source          TEXT             NOT NULL,
    event_type      TEXT             NOT NULL,
    detail          JSONB
);
SELECT create_hypertable('ingestion_events', 'event_ts');
SELECT set_chunk_time_interval('ingestion_events', INTERVAL '7 days');
CREATE INDEX ON ingestion_events (source, event_ts DESC);
CREATE INDEX ON ingestion_events (source, event_type, event_ts DESC);

-- Per-minute ingestion stats: one row per (source, bucket).
-- Written by the in-process MinuteAggregator when a minute boundary is crossed.
CREATE TABLE ingestion_stats_minute (
    bucket                      TIMESTAMPTZ NOT NULL,
    source                      TEXT        NOT NULL,
    fix_count                   INTEGER     NOT NULL,
    distinct_mmsi               INTEGER     NOT NULL,
    mean_lag_s                  REAL,
    p95_lag_s                   REAL,
    max_raw_q                   INTEGER,
    seconds_since_last_message  INTEGER,
    current_connection_age_s    INTEGER,
    PRIMARY KEY (source, bucket)
);
SELECT create_hypertable('ingestion_stats_minute', 'bucket');
SELECT set_chunk_time_interval('ingestion_stats_minute', INTERVAL '7 days');

-- Per-minute per-zone fix counts: one row per (source, bucket, zone).
-- Drives the TUI's per-zone breakdown without re-scanning ais_fixes.
CREATE TABLE ingestion_zone_minute (
    bucket          TIMESTAMPTZ NOT NULL,
    source          TEXT        NOT NULL,
    zone            TEXT        NOT NULL,
    fix_count       INTEGER     NOT NULL,
    PRIMARY KEY (source, bucket, zone)
);
SELECT create_hypertable('ingestion_zone_minute', 'bucket');
SELECT set_chunk_time_interval('ingestion_zone_minute', INTERVAL '7 days');


-- Continuous aggregates for ingestion monitoring
CREATE MATERIALIZED VIEW fixes_per_minute
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', fix_ts) AS bucket, COUNT(*) AS cnt
FROM ais_fixes
GROUP BY bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('fixes_per_minute',
    start_offset      => INTERVAL '2 days',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');

CREATE MATERIALIZED VIEW fixes_per_hour
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', fix_ts) AS bucket, COUNT(*) AS cnt
FROM ais_fixes
GROUP BY bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('fixes_per_hour',
    start_offset      => INTERVAL '365 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

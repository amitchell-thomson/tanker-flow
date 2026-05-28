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
    source          TEXT             NOT NULL  -- 'aisstream' | 'vesselfinder'
);

SELECT create_hypertable('ais_fixes', 'fix_ts');
SELECT set_chunk_time_interval('ais_fixes', INTERVAL '1 day');
CREATE UNIQUE INDEX ON ais_fixes (fix_ts, mmsi);


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
    cold_start      BOOLEAN          NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ      DEFAULT now(),
    CONSTRAINT valid_event_type CHECK (
        event_type IN ('zone_entry','anchored','moored','departed','zone_exit')
    ),
    CONSTRAINT valid_zone CHECK (zone IN ('usgulf','usatlantic','nweurope','baltic','iberian','wmed','emed'))
);

CREATE INDEX ON port_events (mmsi, event_time DESC);
CREATE INDEX ON port_events (terminal_id, event_time DESC);
CREATE INDEX ON port_events (zone, event_type, event_time DESC);


-- Ingestion health: one row per source, upserted on each heartbeat
CREATE TABLE ingestion_heartbeat (
    source          TEXT             PRIMARY KEY,
    status          TEXT             NOT NULL,
    last_heartbeat  TIMESTAMPTZ      NOT NULL
);


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

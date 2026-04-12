CREATE EXTENSION IF NOT EXISTS timescaledb;

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

SELECT create_hypertable('ais_fixes', 'server_ts');
SELECT set_chunk_time_interval('ais_fixes', INTERVAL '1 day');
CREATE INDEX ON ais_fixes (mmsi, server_ts DESC);


CREATE TABLE vessel_state(
    server_ts       TIMESTAMPTZ     DEFAULT now(),
    state_ts        TIMESTAMPTZ     NOT NULL,
    mmsi            BIGINT          NOT NULL,
    draught         REAL,
    dest            TEXT,
    eta             JSONB,
    source          TEXT            NOT NULL,
);

SELECT create_hypertable('vessel_state', 'server_ts');
SELECT set_chunk_time_interval('vessel_state', INTERVAL '1 day');
CREATE INDEX ON vessel_state (mmsi, server_ts DESC);

-- Vessel registry: populated passively + enriched from VesselFinder
CREATE TABLE vessel_registry (
    mmsi            BIGINT           PRIMARY KEY,
    imo             BIGINT,
    vessel_name     TEXT,
    call_sign       TEXT,
    vessel_type     SMALLINT,
    dwt             INTEGER,
    design_draught  REAL,
    flag            TEXT,
    enriched_at     TIMESTAMPTZ,     -- NULL means not yet enriched
    updated_at      TIMESTAMPTZ      DEFAULT now()
);

-- Port events: derived from ais_fixes, recomputable
CREATE TABLE port_events (
    id              BIGSERIAL        PRIMARY KEY,
    mmsi            BIGINT           NOT NULL,
    event_type      TEXT             NOT NULL,
    zone            TEXT             NOT NULL,
    event_time      TIMESTAMPTZ      NOT NULL,
    laden_flag      BOOLEAN,
    created_at      TIMESTAMPTZ      DEFAULT now(),
    CONSTRAINT valid_event_type CHECK (
        event_type IN ('zone_entry','anchored','moored','departed','zone_exit')
    ),
    CONSTRAINT valid_zone CHECK (zone IN ('usgulf','nweurope'))
);

CREATE INDEX ON port_events (mmsi, event_time DESC);
CREATE INDEX ON port_events (zone, event_type, event_time DESC);

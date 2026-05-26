-- =============================================================================
-- tanker-flow: terminals seed
-- 39 rows: 33 Tier-1 (in_signal_scope = TRUE) + 6 Tier-2 (in_signal_scope = FALSE)
--
-- FSRU flag: TRUE for every floating storage & regasification unit.
--   These rows still appear in the signal pipeline so that they can be
--   identified and excluded (Layer 2); is_fsru is the filter column.
--
-- Tier-2 terminals (in_signal_scope = FALSE):
--   Non-US exporters. Exist so that vessels loading there can be detected
--   and excluded from the US-departure signal — never to contribute to it.
--
-- Run once, before any QGIS digitizing. terminal_zones.terminal_id is NOT
-- NULL FK, so this table must be populated first.
-- Safe to re-run: uses upsert, existing rows are updated in place.
-- =============================================================================

INSERT INTO terminals (terminal_name, country, flow_direction, in_signal_scope, is_fsru, notes)
VALUES

-- ---------------------------------------------------------------------------
-- TIER 1 — US EXPORT (signal origin)
-- ---------------------------------------------------------------------------
('Sabine Pass',         'US', 'export', TRUE,  FALSE, 'Multiple berths — use sub_zone'),
('Plaquemines',         'US', 'export', TRUE,  FALSE, '3 berths — use sub_zone'),
('Calcasieu Pass',      'US', 'export', TRUE,  FALSE, NULL),
('Corpus Christi',      'US', 'export', TRUE,  FALSE, 'Stage 3 online 2024-25'),
('Cameron',             'US', 'export', TRUE,  FALSE, 'Hackberry, Calcasieu Channel'),
('Freeport',            'US', 'export', TRUE,  FALSE, 'Quintana Island, TX'),
('Golden Pass',         'US', 'export', TRUE,  FALSE, 'Under construction — first trains expected 2026'),
('Cove Point',          'US', 'export', TRUE,  FALSE, 'Maryland — US East Coast'),
('Elba Island',         'US', 'export', TRUE,  FALSE, 'Georgia — US East Coast, small'),

-- ---------------------------------------------------------------------------
-- TIER 1 — EU IMPORT, NW EUROPE
-- ---------------------------------------------------------------------------
('Gate (Rotterdam)',         'NL', 'import', TRUE,  FALSE, 'Maasvlakte — main NL hub'),
('Eemshaven FSRU',          'NL', 'import', TRUE,  TRUE,  'Two FSRUs; post-Ukraine build'),
('Zeebrugge',               'BE', 'import', TRUE,  FALSE, 'Reload hub; expanded 2025'),
('Dunkerque',               'FR', 'import', TRUE,  FALSE, 'France''s largest; reload capability'),
('Isle of Grain',           'GB', 'import', TRUE,  FALSE, 'Kent — key SE England hub'),
('South Hook',              'GB', 'import', TRUE,  FALSE, 'Milford Haven — UK''s largest; dedicated Qatari LNG'),
('Wilhelmshaven 1 FSRU',    'DE', 'import', TRUE,  TRUE,  'Hoegh Esperanza'),
('Wilhelmshaven 2 FSRU',    'DE', 'import', TRUE,  TRUE,  'Excelsior; online Aug 2025'),
('Brunsbuttel FSRU',        'DE', 'import', TRUE,  TRUE,  'Neptune FSRU'),
('Lubmin II FSRU',          'DE', 'import', TRUE,  TRUE,  'Deutsche Courage'),

-- ---------------------------------------------------------------------------
-- TIER 1 — EU IMPORT, IBERIA
-- ---------------------------------------------------------------------------
('Sines',               'PT', 'import', TRUE,  FALSE, 'Iberian hub; reload capability'),
('Barcelona',           'ES', 'import', TRUE,  FALSE, 'Largest in Spain; oldest in Europe'),
('Huelva',              'ES', 'import', TRUE,  FALSE, 'Reload capability'),
('Cartagena',           'ES', 'import', TRUE,  FALSE, 'Murcia'),
('Sagunto',             'ES', 'import', TRUE,  FALSE, 'Valencia'),
('Bilbao',              'ES', 'import', TRUE,  FALSE, NULL),

-- ---------------------------------------------------------------------------
-- TIER 1 — EU IMPORT, ITALY & ADRIATIC
-- ---------------------------------------------------------------------------
('Adriatic LNG',        'IT', 'import', TRUE,  FALSE, 'Offshore GBS — anchorage polygon disjoint from structure'),
('Piombino FSRU',       'IT', 'import', TRUE,  TRUE,  'Golar Tundra; Tyrrhenian coast'),
('Ravenna FSRU',        'IT', 'import', TRUE,  TRUE,  'BW Singapore; first cargo Jun 2025'),

-- ---------------------------------------------------------------------------
-- TIER 1 — EU IMPORT, BALTIC / CEE & GREECE
-- ---------------------------------------------------------------------------
('Swinoujscie',         'PL', 'import', TRUE,  FALSE, 'Poland''s main terminal'),
('Klaipeda FSRU',       'LT', 'import', TRUE,  TRUE,  'Independence FSRU — Baltic security anchor'),
('Krk (LNG Croatia)',   'HR', 'import', TRUE,  TRUE,  'FSRU; expanded to 6.1 bcm 2025 — CEE hub'),
('Revithoussa',         'GR', 'import', TRUE,  FALSE, 'Greece''s main onshore terminal; near Athens'),
('Alexandroupolis FSRU','GR', 'import', TRUE,  TRUE,  'East Med / Balkans hub; operational Oct 2024'),

-- ---------------------------------------------------------------------------
-- TIER 2 — NON-US EXPORTERS (exclusion only, in_signal_scope = FALSE)
-- ---------------------------------------------------------------------------
('Nigeria LNG (Bonny)',      'NG', 'export', FALSE, FALSE, 'Exclusion only — coarse polygon'),
('Arzew / Bethioua',        'DZ', 'export', FALSE, FALSE, 'Exclusion only — coarse polygon'),
('Skikda',                  'DZ', 'export', FALSE, FALSE, 'Exclusion only — coarse polygon'),
('Idku',                    'EG', 'export', FALSE, FALSE, 'Exclusion only — coarse polygon'),
('Damietta',                'EG', 'export', FALSE, FALSE, 'Exclusion only — coarse polygon'),
('Atlantic LNG (Pt Fortin)', 'TT', 'export', FALSE, FALSE, 'Exclusion only — coarse polygon')

ON CONFLICT (terminal_name) DO UPDATE SET
    country         = EXCLUDED.country,
    flow_direction  = EXCLUDED.flow_direction,
    in_signal_scope = EXCLUDED.in_signal_scope,
    is_fsru         = EXCLUDED.is_fsru,
    notes           = EXCLUDED.notes;

-- ---------------------------------------------------------------------------
-- Verification
-- ---------------------------------------------------------------------------
SELECT
    flow_direction,
    in_signal_scope,
    is_fsru,
    COUNT(*) AS n
FROM terminals
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- Expected:
--  export | false | false |  6   <- Tier-2 exclusion terminals
--  export | true  | false |  9   <- US export Tier-1
--  import | true  | false | 14   <- EU onshore import
--  import | true  | true  | 10   <- EU FSRU import

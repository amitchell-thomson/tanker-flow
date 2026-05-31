-- Seed UN/LOCODE values for terminals. Used by pipeline/dest_parser.py to
-- resolve vessel_state.dest → terminal_id. Codes follow the UN/LOCODE
-- standard (country + 3-letter location code). Most LNG receiving/exporting
-- terminals reuse the parent port's LOCODE rather than having their own
-- terminal-specific code, which is fine for our purposes (one terminal per
-- port in our scope, except where noted in comments).
--
-- Unknown / unverified codes left as NULL — the dest_parser's freeform-name
-- normalizer will pick those up from "EEMSHAVEN" / "ROTTERDAM" / etc. style
-- mentions.

-- US Gulf
UPDATE terminals SET unlocode = 'USSAB' WHERE terminal_name = 'Sabine Pass';
UPDATE terminals SET unlocode = 'USPLQ' WHERE terminal_name = 'Plaquemines';  -- vessels broadcast USPLQ (not the older USPMS)
UPDATE terminals SET unlocode = 'USCLU' WHERE terminal_name = 'Calcasieu Pass';
UPDATE terminals SET unlocode = 'USCRP' WHERE terminal_name = 'Corpus Christi';
UPDATE terminals SET unlocode = 'USCAU' WHERE terminal_name = 'Cameron';
UPDATE terminals SET unlocode = 'USFPO' WHERE terminal_name = 'Freeport';
UPDATE terminals SET unlocode = 'USPSX' WHERE terminal_name = 'Golden Pass';   -- Sabine area; Port Arthur cluster

-- US Atlantic
UPDATE terminals SET unlocode = 'USCVL' WHERE terminal_name = 'Cove Point';
UPDATE terminals SET unlocode = 'USEII' WHERE terminal_name = 'Elba Island';

-- NW Europe
UPDATE terminals SET unlocode = 'NLRTM' WHERE terminal_name = 'Gate (Rotterdam)';
UPDATE terminals SET unlocode = 'NLEEM' WHERE terminal_name = 'Eemshaven FSRU';
UPDATE terminals SET unlocode = 'BEZEE' WHERE terminal_name = 'Zeebrugge';
UPDATE terminals SET unlocode = 'FRDKK' WHERE terminal_name = 'Dunkerque';
UPDATE terminals SET unlocode = 'GBMIL' WHERE terminal_name = 'South Hook';     -- Milford Haven
UPDATE terminals SET unlocode = 'GBIOG' WHERE terminal_name = 'Isle of Grain';
UPDATE terminals SET unlocode = 'DEBRB' WHERE terminal_name = 'Brunsbuttel FSRU';
UPDATE terminals SET unlocode = 'DEWVN' WHERE terminal_name = 'Wilhelmshaven 1 FSRU';
UPDATE terminals SET unlocode = 'DEWVN' WHERE terminal_name = 'Wilhelmshaven 2 FSRU';
UPDATE terminals SET unlocode = 'DELUB' WHERE terminal_name = 'Lubmin II FSRU';  -- Lubmin

-- Baltic
UPDATE terminals SET unlocode = 'PLSWI' WHERE terminal_name = 'Swinoujscie';
UPDATE terminals SET unlocode = 'DEMUK' WHERE terminal_name = 'Mukran (Deutsche Ostsee)';
UPDATE terminals SET unlocode = 'LTKLJ' WHERE terminal_name = 'Klaipeda FSRU';

-- Iberian
UPDATE terminals SET unlocode = 'PTSIE' WHERE terminal_name = 'Sines';
UPDATE terminals SET unlocode = 'ESBIO' WHERE terminal_name = 'Bilbao';
UPDATE terminals SET unlocode = 'ESHUV' WHERE terminal_name = 'Huelva';

-- W Med
UPDATE terminals SET unlocode = 'ESBCN' WHERE terminal_name = 'Barcelona';
UPDATE terminals SET unlocode = 'ESCAR' WHERE terminal_name = 'Cartagena';
UPDATE terminals SET unlocode = 'ESSAG' WHERE terminal_name = 'Sagunto';
UPDATE terminals SET unlocode = 'ITRVS' WHERE terminal_name = 'Adriatic LNG';    -- Porto Viro / Rovigo
UPDATE terminals SET unlocode = 'ITPIO' WHERE terminal_name = 'Piombino FSRU';
UPDATE terminals SET unlocode = 'ITRAN' WHERE terminal_name = 'Ravenna FSRU';
UPDATE terminals SET unlocode = 'HRKRK' WHERE terminal_name = 'Krk (LNG Croatia)';

-- E Med
UPDATE terminals SET unlocode = 'GRRVT' WHERE terminal_name = 'Revithoussa';
UPDATE terminals SET unlocode = 'GRAXD' WHERE terminal_name = 'Alexandroupolis FSRU';

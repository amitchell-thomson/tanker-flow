## Overview

**tanker-flow** derives a leading Henry Hub/TTF spread price signal from live LNG carrier positions. It ingests AIS vessel position data, processes it into port events at major LNG terminals, and aggregates laden ton-miles in transit as a market signal.

Three monitored zones:
- **`usgulf`** — US Gulf Coast LNG export terminals (Sabine Pass, Freeport, Cameron, Corpus Christi)
- **`usatlantic`** — US East Coast LNG export terminals (Cove Point MD, Elba Island GA)
- **`nweurope`** — NW Europe LNG import terminals (Gate/Rotterdam, Zeebrugge, Dunkirk, South Hook)

## Structure Plan

```
tanker-flow/
├── ingestion/
│   ├── aisstream.py          # WebSocket subscriber, writes to DB
│   └── vesselfinder.py       # Weekly reconciliation + DWT enrichment
├── pipeline/
│   ├── port_events.py        # State machine: ais_fixes → port_events
│   └── signal.py             # laden_ton_miles_in_transit aggregation
├── data/
│   └── eia.py                # EIA API pull (US natural gas storage)
├── analysis/
│   ├── notebooks/            # Exploratory work only, not production logic
│   └── model.py              # Spread prediction model
├── db/
│   └── schema.sql            # Single source of truth for schema
├── tests/
├── config.py                 # Env vars, bounding boxes, thresholds
├── pyproject.toml
└── README.md
```

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
│   └── eia.py                # EIA API pull (Cushing stocks)
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

# config.py
from datetime import datetime, timezone

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    db_password: str
    db_user: str
    db_name: str
    db_host: str = "localhost"
    db_port: int = 5432
    aisstream_api_key: str
    vf_api_key: str = ""

    @property
    def database_url(self) -> str:
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    model_config = {"env_file": ".env"}


settings = Settings()  # type: ignore


# Geographic zones: (name, lat_min, lat_max, lon_min, lon_max).
# Source of truth for: AISstream subscription bboxes, in-process fix classification
# at ingest time, and the TUI's per-zone breakdown. Padded ~0.5° around terminal extents.
ZONES: list[tuple[str, float, float, float, float]] = [
    # US Gulf: Corpus Christi, Freeport, Calcasieu Pass, Golden Pass, Sabine Pass, Plaquemines
    ("usgulf", 27.0, 30.5, -98.0, -88.5),
    # US Atlantic: Elba Island (GA), Cove Point (MD)
    ("usatlantic", 31.5, 39.0, -82.0, -75.5),
    # Iberian Atlantic: Sines (PT), Huelva, Bilbao (ES)
    ("iberian", 36.5, 44.0, -10.0, -2.5),
    # NW Europe: South Hook, Isle of Grain, Dunkirk, Zeebrugge, Gate/Rotterdam, Eemshaven, Wilhelmshaven, Brunsbuttel
    ("nweurope", 50.5, 54.5, -6.0, 10.0),
    # Baltic: Mukran (DE), Swinoujscie (PL), Klaipeda FSRU (LT, ~55.74N 20.84E)
    ("baltic", 53.5, 56.2, 13.0, 21.5),
    # W Mediterranean: Cartagena, Sagunto, Barcelona (ES), Piombino (IT), Krk (HR)
    ("wmed", 36.0, 46.0, -2.0, 15.0),
    # E Mediterranean: Revithoussa, Alexandroupolis (GR)
    ("emed", 37.0, 41.5, 22.5, 26.5),
]


AIS_BOUNDING_BOXES = [
    [[lat_min, lon_min], [lat_max, lon_max]]
    for _, lat_min, lat_max, lon_min, lon_max in ZONES
]


# Ingestion regime cutover. The hard switch from the old "bbox + throttle"
# AISstream subscription (ais_fixes.source = 'aisstream'; subscribe to all
# vessels in the ZONES boxes, with AISstream randomly dropping vessels under
# load) to server-side MMSI filtering (source = 'aisstream-mmsi-{1,2,3}';
# ~150 tier-ranked MMSIs, reliable capture). The two regimes have OPPOSITE
# missingness biases, so every rate/count time series steps at this instant:
# segment on it and never train a model across it. This literal is mirrored in
# the generated `port_events.regime` column (db/init/schema.sql). See
# docs/review-2026-05-31-pre-signal-audit.md §0.
REGIME_CUTOVER = datetime(2026, 5, 30, 9, 27, 0, tzinfo=timezone.utc)


def regime_of(ts: datetime) -> str:
    """Return the ingestion regime ('bbox' | 'mmsi_filter') for a timestamp."""
    return "bbox" if ts < REGIME_CUTOVER else "mmsi_filter"

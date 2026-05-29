# config.py
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


# AISstream caps concurrent connections per API key at 3 (4th gets HTTP 429).
# More importantly, a single connection subscribed to all 7 bboxes triggers
# heavy server-side throttling, decaying to ~600 fixes/min within hours, while
# a 3-bbox subscription holds ~3300/min indefinitely. To cover all 7 zones we
# split across 2 concurrent connections: the high-volume "main" 3 zones (no
# decay) plus the low-volume "secondary" 4. See investigation in the README.
MAIN_ZONES = ["nweurope", "usgulf", "wmed"]
SECONDARY_ZONES = ["usatlantic", "iberian", "baltic", "emed"]


def bboxes_for_zones(zone_names: list[str]) -> list:
    by_name = {
        name: (lat_min, lat_max, lon_min, lon_max)
        for name, lat_min, lat_max, lon_min, lon_max in ZONES
    }
    return [
        [[lat_min, lon_min], [lat_max, lon_max]]
        for name in zone_names
        for lat_min, lat_max, lon_min, lon_max in [by_name[name]]
    ]

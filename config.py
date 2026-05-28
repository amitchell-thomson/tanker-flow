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

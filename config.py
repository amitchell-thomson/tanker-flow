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


# AISstream bounding boxes — [[sw_lat, sw_lon], [ne_lat, ne_lon]]
# Derived from terminal_zones extents with ~0.5° padding.
AIS_BOUNDING_BOXES = [
    # US Gulf: Corpus Christi, Freeport, Calcasieu Pass, Golden Pass, Sabine Pass, Plaquemines
    [[27.0, -98.0], [30.5, -88.5]],
    # US Atlantic: Elba Island (GA), Cove Point (MD)
    [[31.5, -82.0], [39.0, -75.5]],
    # Iberian Atlantic: Sines (PT), Huelva, Bilbao (ES)
    [[36.5, -10.0], [44.0, -2.5]],
    # NW Europe: South Hook, Isle of Grain, Dunkirk, Zeebrugge, Gate/Rotterdam, Eemshaven, Wilhelmshaven, Brunsbuttel
    [[50.5, -6.0], [54.5, 10.0]],
    # Baltic: Mukran (DE), Swinoujscie (PL), Klaipeda FSRU (LT, ~55.74N 20.84E)
    [[53.5, 13.0], [56.2, 21.5]],
    # W Mediterranean: Cartagena, Sagunto, Barcelona (ES), Piombino (IT), Krk (HR)
    [[36.0, -2.0], [46.0, 15.0]],
    # E Mediterranean: Revithoussa, Alexandroupolis (GR)
    [[37.0, 22.5], [41.5, 26.5]],
]

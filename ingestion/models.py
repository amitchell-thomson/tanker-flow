# ingestion/models.py

from datetime import datetime
from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Field, RootModel, field_validator, model_validator


class MetaData(BaseModel):
    MMSI: int
    ShipName: Optional[str] = None
    time_utc: datetime

    @field_validator("time_utc", mode="before")
    @classmethod
    def parse_timestamp(cls, v: str) -> datetime:
        v = v.replace(" UTC", "").strip()
        # Split into date-time and timezone parts on the space before +0000
        # e.g. "2026-04-17 11:19:46.4611 +0000"
        #   or "2026-04-17 11:19:46.192247737 +0000"
        dt_part, tz_part = v.rsplit(" ", 1)
        # dt_part may be "2026-04-17 11:19:46.4611" or "2026-04-17 11:19:46.192247737"
        if "." in dt_part:
            base, frac = dt_part.split(".")
            frac = frac[:6].ljust(6, "0")  # normalise to exactly 6 digits
            dt_part = f"{base}.{frac}"
        return datetime.strptime(f"{dt_part} {tz_part}", "%Y-%m-%d %H:%M:%S.%f %z")


class PositionReportMessage(BaseModel):
    NavigationalStatus: int
    Sog: float
    Latitude: float
    Longitude: float


class ShipStaticDataMessage(BaseModel):
    ImoNumber: Optional[int] = None
    CallSign: Optional[str] = None
    Type: Optional[int] = None
    MaximumStaticDraught: Optional[float] = None
    Destination: Optional[str] = None
    Eta: Optional[dict] = None

    @field_validator("MaximumStaticDraught")
    @classmethod
    def null_zero_draught(cls, v):
        return None if v == 0.0 else v


class PositionReport(BaseModel):
    MessageType: Literal["PositionReport"]
    MetaData: MetaData
    Message: PositionReportMessage

    @model_validator(mode="before")
    @classmethod
    def unwrap_message(cls, data):
        if "Message" in data and "PositionReport" in data["Message"]:
            data["Message"] = data["Message"]["PositionReport"]
        return data


class ShipStaticData(BaseModel):
    MessageType: Literal["ShipStaticData"]
    MetaData: MetaData
    Message: ShipStaticDataMessage

    @model_validator(mode="before")
    @classmethod
    def unwrap_message(cls, data):
        if "Message" in data and "ShipStaticData" in data["Message"]:
            data["Message"] = data["Message"]["ShipStaticData"]
        return data


class AISMessage(
    RootModel[
        Annotated[
            Union[PositionReport, ShipStaticData], Field(discriminator="MessageType")
        ]
    ]
):
    pass

# ingestion/models.py

from datetime import datetime
from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Field, RootModel, field_validator


class MetaData(BaseModel):
    MMSI: int
    ShipName: Optional[str] = None
    time_utc: str

    @field_validator("time_utc")
    @classmethod
    def parse_timestamp(cls, v: str) -> datetime:
        # "-04-12 19:10:08.192247737 +0000 UTC"
        v = v.replace(" UTC", "")
        parts = v.split(".")
        microseconds = parts[1][:6]
        v = f"{parts[0]}.{microseconds} +0000"
        return datetime.strptime(v, "%Y-%m-%d %H:%M:%S.%f %z")


class PositionReportMessage(BaseModel):
    NavigationalStatus: int
    Sog: float
    Latitude: float
    Longitude: float


class ShipStaticDataMessage(BaseModel):
    ImoNumber: Optional[int] = None
    Name: Optional[str] = None
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


class ShipStaticData(BaseModel):
    MessageType: Literal["ShipStaticData"]
    MetaData: MetaData
    Message: ShipStaticDataMessage


class AISMessage(
    RootModel[
        Annotated[
            Union[PositionReport, ShipStaticData], Field(discriminator="MessageType")
        ]
    ]
):
    pass

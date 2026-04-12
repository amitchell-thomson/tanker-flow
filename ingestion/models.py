# ingestion/models.py

from datetime import datetime
from re import S
from typing import Optional

from pydantic import BaseModel, field_validator


class MetaData(BaseModel):
    MMSI: int
    ShipName: Optional[str]
    time_utc: str

    @field_validator("time_utc")
    @classmethod
    def parse_timestamp(cls, v: str) -> datetime:
        # "2026-04-12 19:10:08.192247737 +0000 UTC"
        v = v.replace(" UTC", "")
        parts = v.split(".")
        microseconds = parts[1][:6]
        v = f"{parts[0]}.{microseconds} +0000"
        return datetime.strptime(v, "%Y-%m-%d %H:%M:%S.%f %z")


class PositionReportBody(BaseModel):
    NavigationalStatus: int
    Sog: float
    Latitude: float
    Longitude: float


class ShipStaticDataBody(BaseModel):
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


class MessageBody:
    PositionReport: Optional[PositionReportBody] = None
    ShipStaticData: Optional[ShipStaticDataBody] = None


class AISMessage:
    MessageType: str
    MetaData: MetaData
    Message: MessageBody

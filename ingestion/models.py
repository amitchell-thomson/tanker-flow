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
    Cog: Optional[float] = None

    @field_validator("Cog")
    @classmethod
    def null_unavailable_cog(cls, v):
        # AIS encodes "course not available" as 360 (raw 3600 ÷ 10). Valid COG is
        # 0–359.9°, so treat 360 as NULL rather than a real bearing.
        return None if v is None or v == 360.0 else v


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


class AISBaseMessage(BaseModel):
    @model_validator(mode="before")
    @classmethod
    def unwrap_message(cls, data):
        msg_type = data.get("MessageType")
        if "Message" in data and msg_type and msg_type in data["Message"]:
            data["Message"] = data["Message"][msg_type]
        return data


class PositionReport(AISBaseMessage):
    MessageType: Literal["PositionReport"]
    MetaData: MetaData
    Message: PositionReportMessage


class ShipStaticData(AISBaseMessage):
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


class VesselFinderMasterdata(BaseModel):
    IMO: int
    NAME: Optional[str] = None
    FLAG: Optional[str] = None
    TYPE: Optional[str] = None
    BUILT: Optional[int] = None
    BUILDER: Optional[str] = None
    OWNER: Optional[str] = None
    MANAGER: Optional[str] = None
    LENGTH: Optional[float] = None
    BEAM: Optional[float] = None
    MAXDRAUGHT: Optional[float] = None
    GT: Optional[int] = None
    NT: Optional[int] = None
    DWT: Optional[int] = None
    TEU: Optional[int] = None
    CRUDE: Optional[int] = None
    GAS: Optional[int] = None


class VesselFinderResponse(BaseModel):
    MASTERDATA: VesselFinderMasterdata


class VesselFinderAIS(BaseModel):
    """One live AIS position from the VesselFinder `/vessels` endpoint.

    Fields mirror the API response verbatim (uppercase). Sentinel-nulling follows
    the same conventions as the AISstream models above: COURSE 360 = "not
    available", HEADING 511 = "not available", DRAUGHT 0 = "unreported".
    TIMESTAMP and ETA are kept as raw strings and parsed downstream in
    ingestion.vf_rescue (parse_vf_timestamp / vf_eta_to_ais_dict). Extra fields
    in the response (NAME, CALLSIGN, TYPE, ZONE, ECA, A/B/C/D, …) are ignored.
    """

    MMSI: int
    IMO: Optional[int] = None
    TIMESTAMP: str
    LATITUDE: float
    LONGITUDE: float
    COURSE: Optional[float] = None
    SPEED: Optional[float] = None
    HEADING: Optional[float] = None
    NAVSTAT: Optional[int] = None
    DESTINATION: Optional[str] = None
    LOCODE: Optional[str] = None
    ETA: Optional[str] = None
    DRAUGHT: Optional[float] = None
    SRC: Optional[str] = None

    @field_validator("COURSE")
    @classmethod
    def null_unavailable_course(cls, v):
        # AIS "course not available" = 360. Valid COG is 0–359.9°.
        return None if v is None or v == 360.0 else v

    @field_validator("HEADING")
    @classmethod
    def null_unavailable_heading(cls, v):
        # AIS "heading not available" = 511.
        return None if v is None or v == 511.0 else v

    @field_validator("DRAUGHT")
    @classmethod
    def null_zero_draught(cls, v):
        return None if v == 0.0 else v


class VesselFinderLiveResponse(BaseModel):
    AIS: VesselFinderAIS

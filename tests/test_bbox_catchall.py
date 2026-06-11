"""Unit tests for the Stage-3c terminal-bbox catch-all.

The catch-all swaps a worker's scan connection for a geofence subscription over
the terminal boxes, injecting only in-scope LNG carriers (FSRUs excluded). The
two things worth pinning: the subscribe payload is a geofence with NO MMSI filter
(else it's not a catch-all), and parse_message's allow-list drops everything not
on it — while staying a strict no-op on the MMSI-filtered connections.
"""

from __future__ import annotations

import json

from config import Settings
from ingestion import aisstream as a
from ingestion.metrics import MinuteAggregator


def _position(mmsi: int, lat: float = 29.0, lon: float = -94.0) -> str:
    """A minimal valid AISstream PositionReport for `mmsi` (US-Gulf coords)."""
    return json.dumps(
        {
            "MessageType": "PositionReport",
            "MetaData": {
                "MMSI": mmsi,
                "time_utc": "2026-06-11 14:00:00.000000 +0000 UTC",
            },
            "Message": {
                "PositionReport": {
                    "NavigationalStatus": 0,
                    "Sog": 1.0,
                    "Latitude": lat,
                    "Longitude": lon,
                    "Cog": 100.0,
                }
            },
        }
    )


# --- subscribe payloads -------------------------------------------------------
def test_bbox_payload_is_a_geofence_with_no_mmsi_filter():
    boxes = [[[29.0, -95.0], [30.0, -94.0]]]
    p = a.build_bbox_subscribe_payload("KEY", boxes)
    assert p["APIKey"] == "KEY"
    assert p["BoundingBoxes"] == boxes
    # The whole point of a catch-all: hear EVERY vessel in the box.
    assert "FiltersShipMMSI" not in p
    # PositionReport only — static data arrives via the MMSI slot post-promotion.
    assert p["FilterMessageTypes"] == ["PositionReport"]


def test_mmsi_payload_still_constrains_by_mmsi():
    # Regression guard: the MMSI-filtered path is unchanged by the refactor.
    p = a.build_subscribe_payload("KEY", [111, 222])
    assert p["FiltersShipMMSI"] == ["111", "222"]
    assert "ShipStaticData" in p["FilterMessageTypes"]


# --- allow-list gate in parse_message ----------------------------------------
def test_bbox_allowlist_drops_unlisted_and_keeps_listed():
    st = a.IngestionState(source_name="aisstream-bbox", allow_mmsis={111})
    agg = MinuteAggregator(source="aisstream-bbox")

    a.parse_message(_position(999), st, agg)  # not an in-scope LNG carrier
    assert st.fix_buf == []  # dropped before insert

    a.parse_message(_position(111), st, agg)  # on the allow-list
    assert len(st.fix_buf) == 1
    assert st.fix_buf[0][1] == 111  # (fix_ts, mmsi, ...)


def test_mmsi_connection_has_no_allowlist_gate():
    # allow_mmsis is None on MMSI-filtered conns; the server-side filter already
    # constrains, so parse_message must keep every fix it receives.
    st = a.IngestionState(source_name="aisstream-mmsi-1")
    agg = MinuteAggregator(source="aisstream-mmsi-1")
    a.parse_message(_position(999), st, agg)
    assert len(st.fix_buf) == 1


# --- config flag --------------------------------------------------------------
def test_bbox_catchall_defaults_off():
    # Default off ⇒ every connection stays MMSI-filtered, behaviour unchanged.
    assert Settings(worker_id=0).bbox_catchall is False

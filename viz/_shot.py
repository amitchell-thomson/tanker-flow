"""Throwaway visual-verification helper: screenshot the dashboard at desktop +
phone widths. Run: uv run python viz/_shot.py  (server must be on :8000).
Captures a mobile vessel-track shot via /?focus=<mmsi> to confirm tap-to-path."""

import json
import sys
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent / "_shots"
OUT.mkdir(exist_ok=True)
BASE = "http://localhost:8000"


def a_vessel_mmsi() -> int | None:
    try:
        with urllib.request.urlopen(BASE + "/api/vessels", timeout=10) as r:
            data = json.load(r)
        return data[0]["mmsi"] if data else None
    except Exception:
        return None


def main() -> int:
    mmsi = a_vessel_mmsi()
    shots = [
        ("desktop-signals", "/signals", None),
        ("mobile-signals", "/signals", (390, 844)),
        ("mobile-map", "/", (390, 844)),
    ]
    if mmsi:
        shots.append(("mobile-map-track", f"/?focus={mmsi}", (390, 844)))

    with sync_playwright() as p:
        browser = p.chromium.launch()
        for name, path, vp in shots:
            ctx = browser.new_context(
                viewport={"width": vp[0], "height": vp[1]} if vp else {"width": 1440, "height": 900},
                device_scale_factor=2 if vp else 1,
                is_mobile=bool(vp),
            )
            page = ctx.new_page()
            page.goto(BASE + path, wait_until="commit", timeout=45000)
            page.wait_for_timeout(4500)
            page.screenshot(path=str(OUT / f"{name}.png"))
            print(f"  wrote {name}.png")
            ctx.close()
        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

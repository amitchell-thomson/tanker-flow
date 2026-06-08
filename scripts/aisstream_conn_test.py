#!/usr/bin/env python
"""AISstream per-IP connection-cap probe.

Re-establishes (and lets you re-test) the 2026-06-02 finding: AISstream caps
concurrent WebSocket connections at **3 per source IP** — the 4th is refused
(HTTP 429 at handshake, or an immediate server close) regardless of which API
key or account it uses. The only way past 3 is a second egress IP. See memory
`project_aisstream_ip_conn_cap`.

Two things to test:

  1. The per-IP ceiling, from the current host:
         uv run python scripts/aisstream_conn_test.py --ramp 5
     Expect conns 1-3 OK (streaming), conn 4+ REJECTED.

  2. That a second IP opens a *fresh* bucket (the scaling test). This is a
     deployment test — you need a genuinely different egress IP:
       - Easiest: run the same command from a second host / VPS / VPN, OR
       - On a box with multiple public IPs, bind the source address:
             uv run python scripts/aisstream_conn_test.py --ramp 5 --local-addr <secondary-public-ip>
     Hold 3 from IP-A in one terminal, then --ramp 1 from IP-B; if B connects
     and streams while A is saturated, multi-IP scaling is confirmed.

The `--key alt` flag uses AISSTREAM_API_KEY_ALT. Per the finding the key does
NOT change the cap (it's per-IP) — alt is useful only if you later want each
IP/worker on its own account for per-key throughput/quota isolation. Worth a
one-line confirmation: `--ramp 5 --key alt` from a saturated IP should still
REJECT at conn 4.

Subscribes to a full-globe bbox with no MMSI filter purely as a liveness probe
(floods messages, so "did it stream?" is unambiguous) — this is a connection
test, not the production filtered subscription.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx  # noqa: E402
import websockets  # noqa: E402

from config import settings  # noqa: E402

AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"
GLOBE_BBOX = [[[-90.0, -180.0], [90.0, 180.0]]]


@dataclass
class ConnResult:
    idx: int
    status: str = "PENDING"  # OK | REJECTED | ERROR
    msgs: int = 0
    detail: str = ""


async def echo_egress_ip() -> str:
    """Best-effort public egress IP (default route), for labelling the run.
    NOTE: with --local-addr the WebSocket source may differ from this."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            return (await client.get("https://api.ipify.org")).text.strip()
    except Exception as e:
        return f"<unknown: {e}>"


async def hold_one(
    idx: int,
    api_key: str,
    duration: float,
    local_addr: tuple[str, int] | None,
    results: list[ConnResult],
) -> None:
    """Open one connection, subscribe, count messages for `duration`s."""
    r = ConnResult(idx=idx)
    results[idx] = r
    connect_kwargs: dict = {"ping_timeout": None, "open_timeout": 10}
    if local_addr is not None:
        connect_kwargs["local_addr"] = local_addr
    try:
        async with websockets.connect(AISSTREAM_URL, **connect_kwargs) as ws:
            await ws.send(json.dumps({"APIKey": api_key, "BoundingBoxes": GLOBE_BBOX}))
            r.status = "OK"
            try:
                async with asyncio.timeout(duration):
                    async for _ in ws:
                        r.msgs += 1
            except TimeoutError:
                pass  # held the full duration — success
            # If the server closed us early (e.g. quota), reflect it.
            if r.msgs == 0:
                r.status = "REJECTED"
                r.detail = "connected but streamed 0 msgs (likely quota close)"
    except websockets.exceptions.InvalidStatus as e:
        r.status = "REJECTED"
        r.detail = f"handshake {e.response.status_code}"
    except Exception as e:
        r.status = "ERROR"
        r.detail = f"{type(e).__name__}: {e}"


async def run(args: argparse.Namespace) -> None:
    api_key = (
        settings.aisstream_api_key_alt
        if args.key == "alt"
        else settings.aisstream_api_key
    )
    if not api_key:
        sys.exit(f"No API key for --key {args.key} (check .env)")
    local_addr = (args.local_addr, 0) if args.local_addr else None

    egress = await echo_egress_ip()
    src = args.local_addr or "default route"
    print(
        f"egress IP (default route): {egress} | WS source: {src} | "
        f"key: {args.key} | ramping {args.ramp} conns, {args.duration}s each"
    )

    results: list[ConnResult] = [ConnResult(i) for i in range(args.ramp)]
    tasks = []
    for i in range(args.ramp):
        tasks.append(
            asyncio.create_task(
                hold_one(i, api_key, args.duration, local_addr, results)
            )
        )
        await asyncio.sleep(args.stagger)  # let each settle before the next
    await asyncio.gather(*tasks)

    print("\n conn | status   | msgs  | detail")
    print("------+----------+-------+----------------------------------------")
    for r in results:
        print(f"  {r.idx + 1:>3} | {r.status:<8} | {r.msgs:>5} | {r.detail}")
    ok = sum(1 for r in results if r.status == "OK")
    print(f"\n{ok}/{args.ramp} connections streamed. ", end="")
    if ok >= 3 and args.ramp > 3 and any(r.status == "REJECTED" for r in results):
        print("Per-IP ceiling confirmed at 3 (4th+ refused).")
    elif ok == args.ramp:
        print("All accepted — this IP's bucket had room for all of them.")
    else:
        print("Mixed result — read the detail column.")


def main() -> None:
    p = argparse.ArgumentParser(description="AISstream per-IP connection-cap probe")
    p.add_argument("--ramp", type=int, default=4, help="open up to N concurrent conns")
    p.add_argument("--key", choices=["primary", "alt"], default="primary")
    p.add_argument("--duration", type=float, default=12.0, help="seconds to hold each")
    p.add_argument("--stagger", type=float, default=1.5, help="seconds between opens")
    p.add_argument(
        "--local-addr",
        default=None,
        help="bind WS source to this local IP (multi-public-IP hosts only)",
    )
    asyncio.run(run(p.parse_args()))


if __name__ == "__main__":
    main()

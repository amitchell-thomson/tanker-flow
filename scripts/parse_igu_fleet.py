"""Parse Appendix 3 of the IGU World LNG Report 2025 into a CSV of the global
active LNG fleet (carriers + FSRUs + FSUs).

Output columns: imo, name, shipowner, shipbuilder, capacity_cm, cargo_type,
vessel_type, propulsion_type, delivery_year.

Usage:
    uv run --with pypdf python scripts/parse_igu_fleet.py \\
        --pdf path/to/igu-world-lng-report-2025.pdf \\
        --out db/seed/lng_fleet_igu_2025.csv

The PDF is published by the International Gas Union at:
    https://www.igu.org/igu-reports/2025-world-lng-report

Parsing strategy: pages 62-74 of the report contain Appendix 3. Within each
page the rows are anchored on a 7-digit IMO number. Between consecutive IMOs,
the remaining tokens encode: name, shipowner, shipbuilder, capacity, cargo
type, vessel type, propulsion type, delivery year (in that visual order).
"""

import argparse
import csv
import re
from pathlib import Path

import pypdf

# Appendix 3 ("active fleet") spans these pages in the 2025 edition. The page
# count is stable across the high-resolution and lo-res variants of the PDF.
APPENDIX_3_PAGES = range(62, 75)

VESSEL_TYPE_KEYWORDS = ["Q-Max", "Q-Flex", "FSRU", "FSU", "Floating Storage", "Conventional"]
CARGO_TYPE_KEYWORDS = ["Membrane", "Spherical", "SPB", "Prismatic"]
PROPULSION_KEYWORDS = [
    "X-DF", "ME-GI", "ME-GA", "DFDE", "TFDE", "SSD", "Steam",
]


def extract_appendix_text(pdf_path: Path) -> str:
    reader = pypdf.PdfReader(str(pdf_path))
    chunks = []
    for i in APPENDIX_3_PAGES:
        chunks.append(reader.pages[i].extract_text() or "")
    return "\n".join(chunks)


def first_match(chunk: str, keywords: list[str]) -> str:
    for kw in keywords:
        if kw in chunk:
            return kw
    return ""


def parse_row(chunk: str) -> dict:
    """chunk is the text between an IMO and the next IMO. Extract fields."""
    chunk_flat = re.sub(r"\s+", " ", chunk).strip()

    year_matches = re.findall(r"\b(19[89]\d|20[0-3]\d)\b", chunk_flat)
    year = year_matches[-1] if year_matches else ""

    # Capacity is a 5-6 digit number; usually appears before the year token.
    nums = [n for n in re.findall(r"\b(\d{5,6})\b", chunk_flat) if n != year]
    capacity = nums[0] if nums else ""

    cargo_type = first_match(chunk_flat, CARGO_TYPE_KEYWORDS)
    vessel_type = first_match(chunk_flat, VESSEL_TYPE_KEYWORDS)
    propulsion = first_match(chunk_flat, PROPULSION_KEYWORDS)

    # Reconstruct shipowner/shipbuilder/name: the chunk is "<name> <owner> <builder> <capacity> <cargo> <vtype> <prop> <year>"
    # Slice off the tail (capacity + cargo + vtype + prop + year), what remains is the lead = name + owner + builder.
    tail_tokens = [t for t in (capacity, cargo_type, vessel_type, propulsion, year) if t]
    lead = chunk_flat
    for tok in tail_tokens:
        idx = lead.find(tok)
        if idx >= 0:
            lead = lead[:idx]
    lead = lead.strip()

    return {
        "name_owner_builder_raw": lead,
        "capacity_cm": capacity,
        "cargo_type": cargo_type,
        "vessel_type": vessel_type,
        "propulsion_type": propulsion,
        "delivery_year": year,
    }


def parse_fleet(pdf_path: Path) -> list[dict]:
    text = extract_appendix_text(pdf_path)
    # Strip headers / pagination noise that would confuse field tokenization.
    text = re.sub(r"\d+\s+\d+\s+IGU World LNG report - 2025 Edition", "", text)
    text = re.sub(
        r"Appendix 3:.*?(?:\(continued\)|end-2024)", "", text, flags=re.DOTALL
    )

    imo_re = re.compile(r"\b(9\d{6})\b")
    matches = list(imo_re.finditer(text))

    rows = []
    for j, m in enumerate(matches):
        start = m.end()
        end = matches[j + 1].start() if j + 1 < len(matches) else len(text)
        chunk = text[start:end]
        parsed = parse_row(chunk)
        parsed["imo"] = m.group(1)
        rows.append(parsed)
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    args = ap.parse_args()

    rows = parse_fleet(args.pdf)

    fieldnames = [
        "imo",
        "name_owner_builder_raw",
        "capacity_cm",
        "cargo_type",
        "vessel_type",
        "propulsion_type",
        "delivery_year",
    ]
    with args.out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})

    print(f"Wrote {len(rows)} rows to {args.out}")


if __name__ == "__main__":
    main()

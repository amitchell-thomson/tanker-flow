"""Zip the compile-ready paper for Overleaf (New Project -> Upload Project).

Only what Overleaf needs: main.tex, numbers.tex, refs.bib, sections/, tables/
and the PDF figures. Results JSON, generators and PNGs stay out. Uses the
standard library because `zip` is not installed on the build host.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE.parent / "tanker-flow-paper.zip"


def main() -> None:
    files = [HERE / "main.tex", HERE / "numbers.tex", HERE / "refs.bib"]
    files += sorted((HERE / "sections").glob("*.tex"))
    files += sorted((HERE / "tables").glob("*.tex"))
    files += sorted((HERE / "figures").glob("*.pdf"))
    with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED) as z:
        for f in files:
            z.write(f, f.relative_to(HERE))
    print(f"wrote {OUT.name}: {len(files)} files, {OUT.stat().st_size:,} bytes")
    print("upload to Overleaf: New Project -> Upload Project -> this zip; compile with pdfLaTeX")


if __name__ == "__main__":
    main()

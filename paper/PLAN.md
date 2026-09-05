# Paper plan — audit of the record, then the Overleaf build

Written 2026-09-05 after re-running every replay and re-checking every number the
paper would carry. Part 1 is what the audit found (including four claims that had to
be corrected or withdrawn). Part 2 is the build plan for the first Overleaf version.

---

## Part 1 · Audit: the plan so far, against the results, the code and the numbers

### A. Number provenance

Every number below was **reproduced today from a `make` target or a live query**,
unless marked otherwise. Nothing in the paper may cite a number that is not in this
table or in a generated `tables/*.tex` fragment.

| number | value | how verified |
|---|---|---|
| AIS fixes | 54,494,686, 2016-01-01 → 2026-08-10 | live query |
| port events | 87,874 (noaa 44,363 · gfw 40,273 · mmsi_filter 1,974 · bbox 1,264) | live query |
| signal panel | 1,032,982 rows · 34 keys · dual basis | live query |
| in-scope fleet | 828 LNG carriers / FSRUs | live query |
| laden US export departures | 9,855 | live query |
| model panel | 20 features · 77,500 rows · 2,764 complete days from 2018-01 | `make model-panel --summary` |
| D-006 leak at as-of 2020-06-01 | 20,351 legs naive · 4,713 bounded · **13,130 (64 %) closed by a future arrival** · 2,101 phantom declarations | `paper/figures.py --only leakage` (recomputes both paths) |
| A1 (D-013) | W1 MAE 2.314 vs persist 2.217 / clim 1.814 · cov80 73.7 % | `make a1-replay` |
| A2 (D-018) | 3.961 vs 2.551 / 2.152 · cov80 75.7 % · `ballast_arrivals_1w` sign falsified | `make a2-replay` |
| A4 (D-023) | 2.104 vs 2.694 / **2.088** · α = 0.251 · ~7.0-week window · cov80 72.6 % | `make a4-replay` |
| A5 (D-021) | BOCPD 17 d / 12 % recall / 0.13 FA · **N2: 12 d / 38 % recall (6/16) / 0.19 FA** | `make a5-replay` |
| H1 (D-030) | truth 5.89 → 0.75 over W1→W4 · nulls 2.23 → 5.25 · NOT COMPARABLE | `make a1-h1` |
| Part B, full panel (D-029) | 395 wks · M2 −10.4 % (h=1) / −19.2 % (h=4) vs M0 · max \|t\| 1.72 · max pR² 0.009 | `make b0-replay` |
| **Part B, coverage-truncated (D-033) — primary** | 379 wks to 2025-12-29 · M2 **−9.3 %** (h=1) · max \|t\| **1.50** · four flow signals right-signed at h=4 | `make b0-coverage` |
| mechanisms, coverage (D-033) | H2 t −0.89, holdout β −2.51 (same sign) · H3 t +0.46 (wrong sign) · H4 tight −0.17 < loose −1.00 | `make mechanisms-coverage` |
| power floor | detectable partial R² ≈ t²/n = **1.75 %** at n = 327, \|t\| > 2.394 · observed ≤ 0.9 % | computed |
| physical scale | EU-bound at-sea stock mean 3.69M m³ = 2.21 bcm = **2.3 days** of EU demand · 1σ = 1.37 d · weekly spread σ = $11.22 | live query; **assumes EU demand 350 bcm/yr — needs a citation** |
| NOAA-only capture vs EIA | **46.3 % (2020)** · 93.3 % (2023) · 102.7 % (2024) · 105.1 % (2025) | live query |
| coverage break | `gas_in_transit_eu` 4.80M → 0.41M m³, Dec-25 → Jan-26 · NOAA ends 2025-12-31 · GFW 2026-02-21 · **24 % of holdout exposed** | live query |
| TTF roll check | slope 0.9990 · R² 0.9999 · mean rel diff +0.007 % over 106 months | `make check-ttf-roll` |
| observation lag of a `departed` event | NOAA: median 0 h, >24 h 0.3 % · live: median 0 h, p90 2.6 h, >24 h 1.5 % | live query, n = 1,500 / 343 |

### B. Claims corrected or withdrawn by the audit

1. **"US loadings track EIA to a few percent."** Wrong as stated. The shipped
   `make capture-rate` mixes regimes and reads **110–134 %** (NOAA + GFW double-count).
   NOAA-only: **46 % in 2020 rising to 93–105 % in 2023–25**. The paper cites the
   NOAA-only series and says why the tool's default is not it. SIGNALS.md:31 to fix.
2. **Seasonality "≈ 1.48×".** `make validate-signals` measures **1.35×** today.
   MODELS.md:58, MODELS.md:218 and `analysis/fwl.py:5` to fix.
3. **"Validation sweep green / gate cleared."** The gate is **red**: `vintage` FAILS
   with 910 rows. Diagnosis (today): **842/910 are an artefact of the check** — it
   compares the *last print of the day* with a full-day recompute, and 842 of those
   prints were taken mid-day before the day's fixes had arrived (the D-006 "midnight
   as-of" effect). The **residual 68 are d+1 prints** with +50–63 % differences on the
   at-sea stock signals at n ≈ 12 — consistent with late fixes back-dating `departed`
   events. All 68 sit in the live tail (post-2026-05-30), **outside the D-033 modelling
   sample**. Action: fix the check to compare post-day prints only; re-run; report both.
4. **"H2's sign flips on the holdout."** Withdrawn in D-033; the flip was the
   contaminated 24 %. Must not appear.
5. **A5 as a "deployable artefact".** Must carry **recall 38 %** next to 12 d / 0.19 FA.
   A detector that catches 6 of 16 labelled outages is real but limited.
6. **Pre-registration evidence.** `git log` shows **D-000→D-026 landed in one commit**
   (`4239d13`, 2026-09-05, weeks after the August work), and D-028+D-029 and
   D-031+D-032 were each batched with their result. **Git provides no independent
   ordering for any pre-registration in this project.** The paper states this in §4
   and §9, and notes the substance (signs, bars, holdout) is nonetheless recorded before
   each fit in the log text.
7. **Claim 1 (leakage) is demoted** from headline to methods, per the author's
   challenge: as "we had look-ahead bias and fixed it" it is a bug report. What survives
   is narrower — (a) the unit of observation (a voyage leg) is *defined retrospectively*,
   so status labels are hindsight-assigned (`open_floating` 95 → 9); (b) a **vintage**
   leak: 2,101 declarations from a feed that did not exist in 2020, which a date filter
   on observations cannot catch. No claim about the literature is made.
8. **"knowable = point-in-time."** Stated precisely: point-in-time on `event_time`.
   Event timestamps are back-dated to the transition fix; the measured observation lag
   is ~0 (table above), so this is immaterial on the decade panel, but the live vintage
   residual (item 3) shows the live tail can still move on a handful of days.

### C. Consistency to resolve before drafting

- **Primary sample = coverage-truncated** (D-033). Figures 1 and 3 currently use the
  full panel → regenerate on the coverage horizon; full-panel versions go to Appendix B.
- **Headline** = a pre-registered, power-bounded null. Contribution = discipline + bound.
- Cite: Muth (1960) for EWMA optimality under a local-level process; Newey–West (1987);
  Frisch–Waugh (1933) / Lovell (1963); Bonferroni; an EU gas-demand source for 350 bcm.

### D. Gaps — what the paper needs that does not exist yet

| gap | build |
|---|---|
| Part A convergence figure (+84 % → +27.6 % → +0.8 %) and H1 artefact figure | replays gain `--json` writing `paper/results/*.json`; figures read them |
| tables: inventory, pre-registration timeline, A-headline, ladder (h=1,4; coverage + full), FWL scan, mechanisms, capture-by-year | `paper/tables.py` → `paper/tables/*.tex` (booktabs fragments) |
| every number in prose | `paper/numbers.tex` of `\newcommand`s generated by `tables.py` — no number is typed by hand |
| figs 1 & 3 on the coverage horizon | `figures.py --end coverage` |
| vintage check fix | `validate_signals.py`: post-day prints only; keep the old count as a secondary line |
| doc corrections (B.1, B.2) | SIGNALS.md, MODELS.md, fwl.py |

---

## Part 2 · The Overleaf build

### Layout

```
paper/
  main.tex                 # article, 11pt; \input{sections/*}; \input{tables/*}
  numbers.tex              # GENERATED \newcommand{\leakShare}{64} …
  sections/
    00_abstract.tex 01_intro.tex 02_data.tex 03_panel.tex 04_prereg.tex
    05_part_a.tex 06_part_b.tex 07_power.tex 08_discussion.tex 09_limitations.tex
    appendix_a_signals.tex appendix_b_full_panel.tex appendix_c_repro_ai.tex
  figures/                 # GENERATED PDF (+PNG for slides)
  tables/                  # GENERATED .tex fragments
  results/                 # GENERATED JSON from the replays
  refs.bib
  figures.py  tables.py    # generators
```

Make targets: `paper-results` (run replays with `--json`), `paper-figures`,
`paper-tables`, `paper` (all three, then zip `paper/` for Overleaf upload).

### LaTeX conventions (arXiv-safe)

`article` + `geometry, amsmath, amssymb, booktabs, graphicx, hyperref, natbib,
microtype, caption`. pdfLaTeX, Computer Modern — no custom fonts, no shell-escape, no
`minted`. One float per figure, vector PDF. Tables `\input` from `tables/`. Prose
numbers via `numbers.tex` macros. Bibliography `natbib` + `plainnat`.

There is **no TeX toolchain on this machine**: I write and lint by inspection; the
author compiles on Overleaf and pastes back the log. Keep packages conservative so the
first compile is boring.

### Sections and word budgets (~7,000 words + appendix)

| § | title | words | carries |
|---|---|---|---|
| 0 | Abstract | 200 | the one-sentence claim + the bound |
| 1 | Introduction | 900 | question; answer; the three credibility pillars; contribution; roadmap |
| 2 | Data and pipeline | 900 | Table 1 inventory; Fig 1 (spread + stock, coverage horizon) |
| 3 | Panel construction | 1,000 | three pairings; dual basis; retrospective units + vintage (Fig 2); publication lags; band aggregation; coverage seam; vintage log result (both numbers) |
| 4 | Pre-registration protocol | 500 | Table: D-number · date · what was fixed · commit; the batching paragraph |
| 5 | Part A | 800 | Table A-headline; Fig convergence; H1 as the self-correction example |
| 6 | Part B | 1,200 | Table ladder; Fig 3 FWL; Table mechanisms; H2 corrected |
| 7 | Power and scope | 500 | Fig 4; the 1.75 % / 0.9 % sentence; physical scale |
| 8 | Discussion | 600 | efficiency; commercial equivalents; what would change the answer |
| 9 | Limitations | 500 | EU asymmetry; destination blindness; live-only composites; capture gradient; coverage seam; `regime='all'` residual; commit batching; vintage residual |
| A | Signal definitions | — | the 8 features + composites, from SIGNALS.md |
| B | Full-panel results | — | D-029 / D-032 tables and the full-panel Figs 1, 3 |
| C | Reproducibility + AI disclosure | — | make targets; the disclosure wording (author's) |

### Order of work

0. **Fixes that change numbers** — vintage check; seasonality + EIA doc corrections;
   A5 recall in docs; regenerate Figs 1/3 on the coverage horizon. Commit.
1. **Results → JSON → tables → numbers.tex.** `--json` on a1/a2/a4/a5/b0/mechanisms
   replays; `tables.py`. Commit.
2. **Skeleton compiles** on Overleaf with real tables/figures and placeholder prose.
3. **Draft §2, §3, §5, §6, §7** (methods/results).
4. **Propose §1, §8, §9** for the author to rewrite.
5. **Author review**: read D-006, D-028, D-031, D-033; challenge; revise.
6. **arXiv**: `q-fin.ST` primary, cross-list `econ.EM`; first-time submitters may need
   endorsement — check early; ancillary link to a tagged GitHub release.

### Division of labour

Author: title; the central claim's wording; every cut; the EU-demand citation; the AI
disclosure wording; first Overleaf compile; rewrite §1 and §8; be able to defend every
choice in §4–§7 as their own.
Me: everything generated; §2–§3, §5–§7 drafts; proposals for §1, §8, §9; revisions.

### For the meeting

The artefact worth having in hand is a **compiling skeleton with real tables and
figures**, even with rough prose — steps 0–2. Prose follows.

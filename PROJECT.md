# PROJECT.md — TSX Cross-Sectional Quant Research System

> Paste this file at the start of every Claude Code session to restore full context.

---

## Primary Goal — Learning to Code

**The main point of this project is to teach the owner how to code.** The quant system is
the vehicle, not the only objective. A pipeline that runs but isn't understood is a failed
outcome here, even if its numbers are right.

What that means in practice, for anyone (human or Claude) working on this repo:

- Explain the **Python mechanics**, not just the business logic — what a list comprehension
  is doing structurally, why `dict.get(k, {})` chaining avoids a crash, what a type hint
  means, when a variable belongs inside vs. outside a loop. Do this as part of delivering
  code, not only when asked.
- Prefer the plainer construct over the clever one when the owner will have to read and
  modify it unaided — and say why.
- When a design decision gets made, spell out the reasoning so the next such call can be
  made without help.
- **Lead finance/quant concepts with a concrete example before naming the technique.**
  "Did this stock go up when gold went up, and by how much on average" lands; "rolling
  regression coefficient" does not — until after the example.
- Skill calibration: Python is beginner-to-intermediate and needs syntax spelled out.
  Finance and quant reasoning is strong and fast — look-ahead bias, point-in-time
  correctness, factor exposure, cross-sectional vs. time-series bias all land without
  hand-holding.

**Corollary on running things:** the owner runs the pipeline scripts, not Claude. Claude
writes the code, explains it, states what will happen, and hands over a copy-pasteable
command. Read-only inspection (DB shape queries, diagnosing an API response) is fine and
expected. Anything that writes, deletes, or takes minutes goes to the owner — partly for
control over a shared Railway Postgres, partly because doing it is how the learning happens.

---

## North Star

**Rank TSX-listed companies cross-sectionally — which names will beat the median TSX stock
over the next quarter — with a deliberate tilt toward under-researched ones.**

This is not a general-purpose stock screener. It is built to catch a particular kind of
mispricing: Canadian names where a macro or thematic tailwind is building but the price
hasn't moved yet, and where thin analyst coverage lets that gap persist.

**On "small cap".** The original framing was small caps only. The data isn't that — the
modelling universe is the whole TSX main board, and 44% of its rows are large or mega caps
(RY, TD, SHOP, ENB, CNR all qualify). Rather than restrict the universe, size is handled as
a **feature**: `market_cap` and `avg_dollar_volume_90d` are inputs, so the model can learn a
size premium where one exists instead of having it imposed by selection. Measured on the
2022–2026 out-of-sample set, it does lean that way unprompted — the top-ranked decile sits
at market-cap percentile 0.38 against 0.55 for the bottom.

**The tension to keep in view.** Modelling is filtered to `tier = main` (≥$100k/day) so
signals are tradeable — and that filter removes precisely the thinly-covered names the edge
thesis is about. The stocks most likely to be mispriced are the ones you cannot trade in
size. That is a real constraint on the strategy, not a bug to fix.

The system is also a financial media business. Every signal it generates is potential
content. The audience (Twitter → newsletter → paid community) is built on the same research
the model produces.

---

## Investment Philosophy

Three questions, answered in order:

1. **Is a theme emerging?** (Sentiment — theme detection via news/filings)
2. **Is this company positioned to benefit?** (Fundamentals — do they have the assets,
   revenue, or exposure to actually capture the trend?)
3. **Has the market noticed yet?** (Technical — price/momentum, is it early or already
   priced in?)

**Why Canadian stocks specifically:** Lower analyst coverage means mispricings last longer,
most pronounced in smaller names — TSX small caps are structurally under-researched relative
to US equivalents. That's the edge, and it is pursued through **features, not universe
selection**: `market_cap`, `avg_dollar_volume_90d` and the liquidity tier are model inputs,
so the size/coverage effect is something the model can find and weight rather than something
imposed by a screen. See the note under *North Star* on what this costs.

**How this maps to the build today:** these three are currently **one model over one feature
set**, not three models. See *Deviations* below.

---

## Stock Universe

- **All TSX main-board stocks** — not limited to Composite/Small Cap index constituents
- **594 tickers** after purging preferred shares, ADRs/CDRs, funds, and foreign-currency listings
- **Excludes** TSXV (too speculative, too illiquid), ETFs, mutual funds, sub-$0.50 stocks
- **Delisted tickers are deliberately retained** — dropping them would introduce survivorship
  bias into backtests. ~138 of 594 have price data ending before the current date.
- Ticker format: `SYMBOL.TO` throughout (both FMP and yFinance accept it here)

**Size composition** of the modelling set (row-weighted, `main` tier, labelled rows). Median
market cap is **$1.56B**; the largest name is $409B. This is a whole-market universe, not a
small cap one:

| market cap (CAD) | rows | share |
|---|---|---|
| ≥ $10B — mega | 6,285 | 16.4% |
| $2–10B — large | 10,649 | 27.8% |
| $500M–2B — mid | 13,057 | 34.1% |
| $100–500M — small | 7,961 | 20.8% |
| < $100M — micro | 364 | 0.9% |

**Liquidity tiers.** Each snapshot is tagged `main` or `early_watch` at a $100k/day average
dollar volume threshold:

| tier | tickers | median traded/day |
|---|---|---|
| `main` | 581 | $3,499,195 |
| `early_watch` | 217 | $39,130 |

Modelling currently uses **`main` only**. In `early_watch` a $20k position is half a typical
day's volume, so a signal there isn't actionable. Critically, the two are **not pooled**:
`early_watch` carries ~2x the return volatility and a far fatter tail, and its *median*
return is actually lower than `main`'s — its higher mean comes from a few enormous winners,
the pattern least likely to survive out-of-sample. Model it separately if at all.

---

## Architecture — How It Works

```
FMP API ──> 1_fetch_universe.py ──> fmp_covered.txt
                                          │
            2_fmp_pipeline.py <────────────┘
                 │  profiles, 10yr prices, income/balance/cashflow,
                 │  key metrics, dividends, earnings surprises
                 v
           PostgreSQL (Railway)
                 ^
                 │  gold, Brent, CADUSD, ^VIX, US/CA yields, BoC rate
            3_macro_pipeline.py
                 │
                 v
            5_factor_compute.py ──> financial_scores
                 │                  (Piotroski F-Score, Altman Z' — quarterly TTM, PIT)
                 v
            6_feature_compute.py ─> feature_snapshots
                 │                  ~90 features, monthly, point-in-time correct
                 │                  + forward returns (Pass 2) + sector percentiles (Pass 3)
                 v
            7_eda.py ────────────> quant_clean.parquet + quant_clean_manifest.json
                 │                  exclusions, target, train/test split
                 v
            8_model.py ──────────> LightGBM ranker + decile report
```

Not yet wired: Discord alerts, Twitter/X posting, hosting of the model itself.

---

## Tech Stack

| Layer | Tool | Status |
|---|---|---|
| Database | PostgreSQL on **Railway** | Live, `QUANT_DB_URL` in `.env` |
| Financial data | **FMP API** (`/stable`) | Primary. Premium tier; some endpoints still gated |
| Macro | FMP + **Bank of Canada API** (free) | 9 series, 2015→present |
| ML | **LightGBM** | `8_model.py` |
| Analysis | pandas 3.x, numpy, seaborn, pyarrow | `pandas` pinned `>=3.0,<4.0` — see *Recurring bugs* |
| Discord bot | discord.py | Separate, already live (hockey/jobs) |
| Brokerage | IBKR (primary), Questrade (backup) | Not wired up |

**Language:** Python throughout. Simple and working beats elegant.

---

## The Pipeline — actual scripts

| Script | What it does | Runtime |
|---|---|---|
| `1_fetch_universe.py` | TSX list from FMP screener → `fmp_covered.txt` | ~10 min |
| `2_fmp_pipeline.py` | All raw FMP data → Postgres | ~3–4 hrs |
| `3_macro_pipeline.py` | Commodities, FX, rates → `macro_data` | ~10 min |
| `5_factor_compute.py` | Piotroski + Altman Z' → `financial_scores` | ~5 min |
| `6_feature_compute.py` | Monthly feature snapshots, 3 passes | ~35–40 min |
| `7_eda.py` | Clean, exclude, target, split → parquet + manifest | ~1 min |
| `8_model.py` | Train + evaluate LightGBM | minutes |

**There is no `4_`.** It was a Google Trends pipeline, fully removed (look-ahead bias from
Google's window-relative normalisation; flat-zero data for micro-caps).

Utilities: `backfill_currency.py`, `backfill_ticker_flags.py`, `backfill_balance_sheet_fields.py`,
`purge_tickers.py`, `export_csv.py`, `spot_check_random100.py`, `check_db.py`, `quant_db.py`
(schema + connection), `run_quant_setup.py` (orchestration).

Useful flags: `5_factor_compute.py --reset` (wipe scores, rebuild — removes orphans a plain
upsert can't reach); `6_feature_compute.py --returns-only / --percentiles-only /
--clean-stale-returns`; most scripts take `--test`.

---

## What's Built

- [x] RSS news scraper, Claude theme detection, Claude tweet generation (pre-existing)
- [x] FMP coverage validation
- [x] PostgreSQL schema (`quant_db.py`) with idempotent `ALTER TABLE` migrations
- [x] Full FMP data pipeline
- [x] Macro pipeline (FMP + Bank of Canada)
- [x] Piotroski F-Score — quarterly TTM, point-in-time
- [x] Altman Z'-Score — quarterly TTM, point-in-time
- [x] Feature snapshots — monthly, PIT-correct, ~90 feature columns
- [x] Per-company factor betas (gold, oil, CADUSD, rates, market) + R²
- [x] Self-built equal-weighted market index
- [x] EDA + cleaning + bias exclusions
- [x] LightGBM model with embargoed temporal split and decile evaluation
- [ ] Sentiment scoring layer — **blocked**, see *Known Gaps*
- [ ] Sector/thematic aggregation
- [ ] Discord alerts from quant signals
- [ ] Twitter/X auto-posting
- [ ] Hosting/deployment of the model

---

## Deviations From The Original Plan

Documented because the original build order is no longer the plan being followed.

**1. One model, not three models plus a meta-model.**
Original: separate technical, fundamental and sentiment models, combined by a LightGBM
meta-model. Actual: a single LightGBM classifier over one combined feature set.

Why: you can't skip to a meta-model — it needs working sub-models to combine, and none
existed. A single model gives a baseline to beat, matches the project's own "simple and
working first" rule, and the feature set already spans all three signal types. The
three-model split is now a **refactor target**, not a starting point.

**2. The sentiment layer is unbuilt, not merely unfinished.**
Original: formalise the existing Claude RSS→theme pipeline into scored output. Actual:
`news_articles` holds 227k rows but covers only **2026-03-03 → 2026-07-15**. That makes
every news-derived feature a proxy for *when* the snapshot was taken rather than what
happened to the company — `news_count_30d` is 0 on every pre-2026-03 row and averages 58
after. All news features are excluded. Unblocking this needs historical news coverage
spanning 2019→present, which FMP does not supply at the current tier.

**3. The universe grew and then shrank.**
Original: TSX Composite + Small Cap, ~450. Then: all TSX main-board, ~684. Now: **594**,
after purging preferred shares, ADRs/CDRs, funds and foreign-currency listings.

**4. The target was defined, and it isn't "goes up".**
Not in the original plan at all. See *Modelling Decisions*.

**5. `^GSPTSX` is paywalled, so the benchmark is self-built.**
`relative_strength_vs_tsx_3m` and `tsx_return_3m` can never populate. Replaced with an
equal-weighted index built from the universe's own price history — free, and arguably a
better benchmark, since the cap-weighted TSX Composite is dominated by a handful of banks
and railways that this system doesn't pick from.

**6. Google Trends was removed entirely**, not paused.

---

## Modelling Decisions

**Target: `outperforms` = beat the MEDIAN stock on the same snapshot date.**

Not `forward_return_3m > 0`. That version had no benchmark in it despite the name, and
measured on this data it ranged from **5.0% positive** for 2019-12 snapshots to **84.5%**
for 2025-04 — a 79-point spread driven entirely by *when* the snapshot was taken, not which
stock it was. A model trained on that learns market timing, which its features can't
actually do.

It also closes a leak: `gold_change_3m`, `oil_change_3m`, `cadusd_change_3m` and
`yield_curve_spread` are identical for every ticker on a date, so they act as a date
fingerprint. With a date-driven label a tree can memorise "this macro regime → everything
rose". Benchmarking *within* the date removes that, because a feature constant across a date
cannot separate winners from losers inside it.

Median by **date only**, deliberately not date+sector — sector-neutralising would strip out
the thematic tailwind this project exists to catch.

**Factor betas pair with the macro columns; neither works alone.**
A beta varies across tickers, so it can rank within a date. But high gold beta is good when
gold rises and bad when it falls — so the macro column is the *regime switch*. A tree splits
on `gold_change_3m` first, then on `gold_beta` within that branch. Supply both as separate
columns; don't pre-multiply them into a `beta × move` term, which bakes in a linear
assumption the tree discovers better itself.

Validation that they're real: gold miners (ABX/AEM/K) show gold_beta 1.39–1.79 with
**gold_r2 0.49–0.64**; banks and grocers sit at 0.00–0.03. The R² columns exist precisely to
separate a true exposure from a noisy fit.

**Split by date, with a 3-month embargo.**
Snapshots are monthly but labels look 3 months forward, so consecutive rows share ~2/3 of
their return window. Train through 2024-06-30 and test from 2024-07-31 and that last
training label is measured through 2024-09-26 — which fully contains the first test row's
window. One label horizon is discarded at the seam (~1,300 rows). The same reasoning makes
ordinary k-fold CV invalid here; CV needs the same purging.

**The honest sample size is ~30, not 38,913.**
90 monthly snapshots contain only ~30 non-overlapping 3-month periods. The cross-section is
wide (579 tickers), the history is short. Trust cross-sectional findings; be very skeptical
of anything resembling regime or timing skill.

**Evaluate on decile returns, not accuracy.** A 54%-accurate model can lose money and a
51%-accurate one can make plenty. `8_model.py` ranks within each date, then reports what
each decile actually returned and **what share of months the top decile beat the bottom** —
a spread carried by two good quarters is noise.

---

## Modelling Results So Far

Five configurations run against the same 2019–2026 history. Summarised because the *pattern*
matters more than any single number.

| run | objective | horizon | test AUC | decile spread | what it learned |
|---|---|---|---|---|---|
| 1 | binary | 3m | 0.4822 | −5.70% | memorised periods via macro columns |
| 2 | binary, ranked features | 3m | 0.4929 | −4.27% | value / low-vol / contrarian |
| 3 | binary, ranked | 1m | 0.5062 | −1.47% | nothing (stopped at 3 trees) |
| 4 | lambdarank, raw-return grades | 3m | 0.4974 | +6.79% | **bought volatility** |
| 5 | lambdarank, vol-adjusted grades | 3m | 0.5122 | +2.94% | mild, unproven edge |

**The through-line: the model finds whichever risk tilt the metric rewards.** A binary target
produced a low-volatility tilt (high hit rate, flat mean return). A raw-return ranking target
produced the opposite — its top decile ran 3.4x the volatility of the bottom for a *worse*
return per unit of risk, which looked like a +6.79% discovery and was a leveraged bet on a
market that rose in all four folds. Grading on return-per-unit-of-volatility closed that
escape route and the spread fell to +2.94%.

**Run 5 is not significant.** t ≈ 1.09 on non-overlapping windows (the naive t of 1.94 over
all 40 months is the overlap illusion). With a spread std near 10% and ~13 independent
observations, establishing a 3% quarterly edge would need ~44 windows — **about 11 years**.
Seven years cannot settle it.

Only decile 10 differentiates (8.51% vs ~5.6% flat across deciles 1–9). For a long-only
strategy that is the bucket that matters, but it does mean the ranking is not monotone.

**The backtest is close to spent.** Five rounds of looking at a test result and changing
something is itself a form of overfitting that no number in the output can reveal. Further
gains should come from *new information* — real historical news, insider trades, analyst
revisions, a second universe — or from forward-testing, not from more configurations.
The live prediction set (snapshots with features but no label yet) is the cheapest source of
genuinely uncontaminated evidence.

---

## Data Integrity — found and fixed

Keep this list. Nearly every item hid behind a successful-looking run.

**Currency.** ~38% of the universe (226/594) reports in USD while trading in CAD.
`tickers.reported_currency` tracks it; `compute_ttm()` converts every monetary field to CAD.
Before this, `dynamic_ev_ebitda` added a CAD market cap to USD net debt.

**`filed_at` was systematically wrong — a real look-ahead bug.** FMP's `filingDate` equals
the period-end for ~94% of TSX-only filers (physically impossible) and shows multi-month
lags for the rest; its enrichment only works for US SEC/EDGAR filers. Now always computed as
`fiscal_date + 45d` (quarterly) / `90d` (annual) from NI 51-102 deadlines — conservative,
erring toward assuming data was known *later*.
*Side effect:* `filing_lag_days` became a constant and is now a dead feature — it measures
our own convention, not the company's reporting speed.

**Piotroski was computed but invisible.** `financial_scores` held 23k scores while
`feature_snapshots` had no column for them. Wired through; now ~95% populated.

**Altman was in `FEATURE_COLS` at 1.1% populated.** The inputs didn't exist —
`balance_sheets` never mapped `totalCurrentAssets`, `totalCurrentLiabilities` or
`retainedEarnings` from FMP. Backfilled to 98.5%; Z' now ~97% populated.
Uses the **1983 Z' (book equity)** variant, not the original market-value Z, so every term is
same-statement/same-currency and needs no FX handling.
Z' is miscalibrated for this universe — median **1.18**, below Altman's 1.23 distress line,
because asset-heavy businesses (rails, pipelines, utilities, REITs) score structurally low.
Use `altman_z_pct`, the within-sector rank, not the raw level.

**1,950 fabricated zero labels.** The forward-return lookup had no lower bound, so for a
delisted ticker the base and future queries both returned the same final price → return
exactly 0.0. Worse, those are disproportionately **M&A takeouts that paid a premium**, so the
model was being taught the inverse of the truth. Fixed with a 10-day staleness bound; 2,320
stale 3m labels cleared.

**ACT.TO has corrupt prices** — `0.0001 → 69.81 → 2.6168 → 68.05` on consecutive days, giving
a +2,616,700% label. That one ticker moved the 3m return stddev from **0.33 to 131**. Excluded.
The other 19 tickers flagged for price anomalies change stddev only 0.3321 → 0.3277 and stay.

**`dropna()` was selection bias.** Requiring all ~50 features present kept only
completely-covered rows — systematically the larger, better-covered companies, biasing the
universe away from exactly the under-researched names this project targets. Removed; it cost
28% of rows. LightGBM handles NaN natively.

**Verified clean:** `close` **is** split-adjusted (confirmed on SHOP.TO's 10:1); sector
percentiles correctly `PARTITION BY snapshot_date, sector`; feature coverage is a flat
91–95% across all years.

---

## Known Gaps

**FMP tier limits (confirmed, not code bugs).** `HGUSD` (copper), `CLUSD` (WTI) and `^GSPTSX`
return HTTP 402 on every endpoint and URL variant tried. So `copper_change_3m`,
`relative_strength_vs_tsx_3m` and `tsx_return_3m` are structurally null. Worth checking the
FMP dashboard for a commodities add-on.

**Empty source tables.** `analyst_ratings`, `insider_trades`, `price_targets`,
`upgrades_downgrades`, `institutional_ownership`, `earnings_call_transcripts` have 0 rows and
nothing populates them — so ~13 features are 100% NULL and excluded.
`analyst_estimates.num_analysts_eps` is 0/10,690 populated.

**`analyst_estimates` is a snapshot, not a time series** — `retrieved_at` spans only
2026-06-01 → 2026-07-16 while `fiscal_date` spans 2007 → 2032. Those features are harmless
*only because they're empty*. Populating them naively would hand every pre-2026 snapshot
estimates that didn't exist yet.

**Returns are price returns, not total returns** — no dividend adjustment. Systematically
penalises utilities, telecom, REITs and banks by roughly 1–1.75% per 3-month window.
A defensible choice, but a deliberate one.

**Dual-class duplication** — 16 companies across 32 tickers (ACO-X/ACO-Y, BBD-A/BBD-B,
CTC-A/CTC...) with identical fundamentals, 4.6% of rows. A temporal split largely neutralises
the leak; what remains is mild double-weighting. Only a real leak under random/k-fold splits.

**ASTL.TO fails feature compute** — pandas 3.0 returns `datetime64` rather than `object` from
`.dt.date` when a column is entirely NULL, breaking a date comparison. Costs 93 rows on one
ticker. Pre-existing, low priority.

**1,029 sub-penny/zero closes** across 7 tickers — corrupt by construction given the $0.50
universe floor. Queued for the next rebuild.

---

## Recurring Bug Classes — check these first

1. **`RealDictCursor` rows where tuples are expected.** Hit 3+ times. **Never pass
   `quant_db.get_conn()` to `pandas.read_sql`** — use a plain `psycopg2.connect()`. This
   corrupted every `export_csv.py` output for weeks, undetected because only row *counts*
   were checked, never contents.
2. **Missing `init_schema()` in a DB-writing script.** Hit three times. `CREATE TABLE IF NOT
   EXISTS` never alters a live table, so a new column computes fine in memory then fails at
   INSERT. New columns go in the `new_cols` `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` list.
3. **Silent truncation from a missing date range.** FMP caps commodity/forex history at ~5yrs
   without explicit `from`/`to`, and `treasury-rates` at ~90 days per call. No error — just
   less data.
4. **No dedup before a batch upsert.** `ON CONFLICT DO UPDATE` fails if one batch contains the
   same key twice.
5. **A filter not checking a boolean the API already provides** (`isActivelyTrading`, `isAdr`).
   Use `isAdr`, never `country`, to find depositary receipts — `country` is *domicile*, and
   ~74 legitimate TSX names read 'US' (all of Brookfield, Restaurant Brands, Newmont).
   `isFund` is unreliable alone — it flagged InterRent REIT while missing identical peers.
6. **An upsert can't reach orphans.** Rows from an earlier run with different logic survive
   and still get read. `financial_scores` accumulated 5,666 of them, some dated 2001.
7. **Unpinned major versions.** pandas 3.0's `.dt.date` change arrived silently. `pandas` is
   now pinned.
8. **Verify against actual values, not row counts or "no error."** Nearly every bug above hid
   behind a successful-looking run.

---

## Coding Conventions

- **Simple and working first.** No premature abstraction.
- **One script per concern.** Numbered in pipeline order.
- **All secrets in `.env`.** Never hardcode keys; use `python-dotenv`.
- **Comments explain *why*, not *what*** — especially the non-obvious trade-off that made a
  line look odd. Most comments in `quant/` exist because something was subtly wrong before.
- **Point-in-time correctness is non-negotiable.** Any feature must use only data whose
  `filed_at` ≤ `snapshot_date`.
- **A missing feature is recoverable; a silently wrong one isn't.** When inputs are
  questionable, return `None` rather than a plausible-looking number.
- Ticker format `SYMBOL.TO`; one shared `quant_db.py` for connections and schema.

**Environment note:** the repo lives under OneDrive, so `.env` and all files sync across
machines — but installed packages do not. On a new machine expect
`python -m pip install -r requirements.txt` even though the code is already there. Use
`python -m pip`, not bare `pip`, so the install lands in the interpreter actually in use.

---

## Business Model Context

The research system and the media business are the same thing. Signals → content → audience
→ revenue.

Funnel: Free Twitter → Free Newsletter → Paid Substack → Premium Discord community.

Brand persona: anonymous quant, data-driven, Canadian equities focus, independent (not
reflexively contrarian). Voice should read as a sharp under-the-radar analyst, not a hype
account.

The *content* can still skew toward overlooked names even though the *model* covers the
whole board — the under-researched end is where the interesting stories are, and nobody
needs another take on Royal Bank. Just don't let the brand's small cap framing drift back
into an assumption about what the model is actually ranking.

---

*Last updated: October 2026*

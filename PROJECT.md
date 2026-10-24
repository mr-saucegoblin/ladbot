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
- **~408 names actually scored** at a live snapshot, from ~625 tickers carrying history
- Purged: preferred shares, funds, foreign-currency listings, and **122 depositary receipts**
  (see *Data Integrity*) — the reviewed list is `quant/cdr_exclusions.txt`
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
| `8_model.py` | Train + evaluate LightGBM (walk-forward) | minutes |
| `9_score.py` | Score a live snapshot → `predictions` table | ~2 min |
| `10_factor_baseline.py` | IC and decile spreads per feature, no model | ~2 min |
| `11_selection_stability.py` | Was the feature cut honest? train-only vs full-sample | ~3 min |

**There is no `4_`.** It was a Google Trends pipeline, fully removed (look-ahead bias from
Google's window-relative normalisation; flat-zero data for micro-caps).

Utilities: `backfill_currency.py`, `backfill_ticker_flags.py`, `backfill_balance_sheet_fields.py`,
`purge_tickers.py`, `export_csv.py`, `spot_check_random100.py`, `check_db.py`, `quant_db.py`
(schema + connection), `run_quant_setup.py` (orchestration).

Useful flags: `5_factor_compute.py --reset` (wipe scores, rebuild — removes orphans a plain
upsert can't reach); `6_feature_compute.py --returns-only / --percentiles-only /
--clean-stale-returns / --as-of / --new-only`; most scripts take `--test`.

**`--new-only` is what makes a weekly run viable.** By default `6_feature_compute.py`
recomputes every month-end since 2019 — ~59,000 (ticker, date) pairs to produce ~629 new
rows, or 1.1% useful work. A finished snapshot cannot change: every feature in it is a
trailing window ending on its own date. `--new-only` skips pairs already stored and always
recomputes the `--as-of` row, since that is the live one whose price features move daily.

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

**Why LightGBM.**

*Tabular and small.* ~40k rows, 16 features. Gradient-boosted trees are the strongest
tool class at this size; a neural net needs far more data and would lose.

*Native missing-value handling.* Features run 3–18% null — `dynamic_ev_ebitda` is 17.7%,
`gross_margin_trend` 10.3%. Most models force imputation, and imputing a valuation ratio
means inventing a number for a company that does not have one. LightGBM learns a direction
to send NaN at each split, so "no EV/EBITDA for this company" stays a fact instead of
becoming a fabricated median.

*Interactions for free.* The two most interesting findings in this data are both
interactions: high ROIC hurting rather than helping, and dilution mattering only past about
the 85th percentile. A linear model cannot represent either without someone knowing to write
the term.

*`lambdarank` fits the actual problem.* The task is "order these ~400 names within this
month", not "predict each name's return". LightGBM supports learning-to-rank with query
groups directly; most libraries do not.

It is also fast on CPU, which matters because `9_score.py` retrains on every weekly run.

**The tree count is an output, not a setting.**

`model_tag` ends in `_itN` — `it170`, `it131`, `it231` have all appeared. Nobody picked
those. `rounds` caps the search at 800, `early_stopping(100)` halts once validation has not
improved for 100 consecutive rounds, and the booster rewinds to its best iteration. N is
wherever that landed on that week's data, which is why it is in the tag: it distinguishes
two runs over the same snapshot. The Discord blurb reads N out of the tag at post time, so
it is never stated twice and cannot disagree with the model.

**The model is deliberately crippled, and that is the point.**

40,451 rows is not 40,451 observations. It is 90 monthly snapshots of *overlapping* 3-month
returns — roughly 30 independent periods, 13 of them in the test window. `num_leaves` 31,
`min_data_in_leaf` 200, `feature_fraction` 0.7, `bagging_fraction` 0.8 and `lambda_l2` 5.0
are all constraints on how much it can memorise, chosen against the effective sample rather
than the row count. A larger model fits this history better and predicts worse.


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
| 6 | run 5, clean universe, 16 features | 3m | 0.5201 | **+3.46%** | momentum + small + cheap |

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

### Run 6 — the current model (October 2026)

Run 5's successor, after three changes: 122 depositary receipts removed from the universe,
the feature set cut from 85 to 16, and near-duplicate features dropped.

| metric | value |
|---|---|
| top-minus-bottom decile spread | **+3.46%** per 3 months |
| t-stat, non-overlapping windows | **2.28** |
| market-neutral spread (beta +0.01) | +3.48%, t = 2.25 |
| long-only vs universe | +2.30%, t = 2.31 (market-neutral +2.59%, t = 2.65) |
| months top half beat bottom | 30 of 40 |
| pooled out-of-sample AUC | 0.5201 |

**It is not a disguised market bet.** Regressing the monthly spread on the universe return
gives a beta of +0.01 — the edge survives market-neutralisation essentially unchanged. Run 4
failed exactly this test.

**But it is fragile.** Drop the three best months and it falls to +2.75%, t = 1.72 — under
the bar. Forty overlapping months are ~13 independent periods.

**And the selection is contaminated.** The 63 weak features were cut using t-stats measured
over all 90 months, including the walk-forward's own test windows, so the model was handed
the columns that happened to work in the period it was then scored on. Choosing features is
a fitted decision even though no model was fitted when the t-stats were computed.
`11_selection_stability.py` exists to measure how much of the gain survives honest selection;
it has not been run. **Quote the edge as "roughly 2–3.5% per quarter", never a t-stat.**

**What the model actually is:** momentum, small size, and cheapness — where low operating
margin acts as a value proxy rather than a quality signal. Seven of the current top ten are
carried by "small cap", five by "low operating margin". That is a real, interpretable tilt,
and it is the opposite of quality investing.

**The backtest is close to spent.** Five rounds of looking at a test result and changing
something is itself a form of overfitting that no number in the output can reveal. Further
gains should come from *new information* — real historical news, insider trades, analyst
revisions, a second universe — or from forward-testing, not from more configurations.
The live prediction set (snapshots with features but no label yet) is the cheapest source of
genuinely uncontaminated evidence.

---

## Data Integrity — found and fixed

### Depositary receipts — 22% of the universe was not Canadian

The worst one, found October 2026. The TSX screener lists ~122 CIBC-style Canadian
Depositary Receipts: CAD-hedged wrappers around Apple, Nvidia, Nestlé, LVMH, Berkshire.
FMP serves the **parent's** profile and fundamentals under the `.TO` symbol, so Apple
appeared as a Toronto-listed Technology company with 166,000 employees in Cupertino.

Three detection methods fail:

- **`isAdr` is `False` for every one of them.** A backfill across 751 tickers found zero
  ADRs in a universe that was 22% CDRs. The `adr-and-funds` purge preset therefore finds
  only funds.
- **`country = 'US'` over-captures.** Brookfield Wealth Solutions, Coeur Mining and
  Ivanhoe Electric are US-domiciled and genuinely TSX-listed.
- **The ISIN is `CA…`** for all of them — the receipt really is a Canadian security.

What works: dollar-volume ÷ market cap. A CDR reports its parent's market cap but trades
thin in Toronto. CDRs measured 8–352 ppm; real TSX names 980–6,457 ppm. **That screen is a
shortlist generator only** — it also flags small foreign-domiciled TSX listings, so the
result was reviewed by hand and lives in `quant/cdr_exclusions.txt` (122 excluded, 19 kept).

**The damage was never confined to those rows.** `fill_sector_percentiles()` ranks within
sector, so Nvidia in Technology shifted the percentile of every Canadian tech name, and five
modelled features are sector-relative. Excluding them at `7_eda.py` does *not* undo it —
`6_feature_compute.py` has to re-run. Removing them took the decile spread from +4.42% to
+3.46%: the megacaps were easy wins, and a lower number on a real universe is worth more.

`6_feature_compute.py` now hard-aborts via `assert_no_depositary_receipts()` when any
reviewed CDR is still stored.


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

1. **A flag that doesn't flag.** `tickers.is_adr` is `False` for every depositary receipt,
   and `income_statements.filed_at` is `fiscal_date + 45` for every row rather than an
   observed date. Both *look* like data and are actually constants. Before building logic
   on a provider's boolean or date, check its distribution: a column with one value is a
   column telling you nothing. This cost six weeks of redundant API calls and put Apple in
   a Canadian stock-picking model.
2. **`RealDictCursor` rows where tuples are expected.** Hit 3+ times. **Never pass
   `quant_db.get_conn()` to `pandas.read_sql`** — use a plain `psycopg2.connect()`. This
   corrupted every `export_csv.py` output for weeks, undetected because only row *counts*
   were checked, never contents.
3. **Missing `init_schema()` in a DB-writing script.** Hit three times. `CREATE TABLE IF NOT
   EXISTS` never alters a live table, so a new column computes fine in memory then fails at
   INSERT. New columns go in the `new_cols` `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` list.
4. **Silent truncation from a missing date range.** FMP caps commodity/forex history at ~5yrs
   without explicit `from`/`to`, and `treasury-rates` at ~90 days per call. No error — just
   less data.
5. **No dedup before a batch upsert.** `ON CONFLICT DO UPDATE` fails if one batch contains the
   same key twice.
6. **A filter not checking a boolean the API already provides** — `isActivelyTrading` and
   `isEtf` are worth honouring. **`isAdr` is not one of them:** an earlier version of this
   list said to use it for depositary receipts, and that advice was wrong — FMP returns
   `False` for all 122 of them. `country` fails too (it is *domicile*; ~74 legitimate TSX
   names read 'US'). Use the reviewed `quant/cdr_exclusions.txt`. `isFund` is unreliable
   alone — it flagged InterRent REIT while missing identical peers.
7. **An upsert can't reach orphans.** Rows from an earlier run with different logic survive
   and still get read. `financial_scores` accumulated 5,666 of them, some dated 2001.
8. **Unpinned major versions.** pandas 3.0's `.dt.date` change arrived silently. `pandas` is
   now pinned.
9. **Verify against actual values, not row counts or "no error."** Nearly every bug above hid
   behind a successful-looking run.

---

## Serving — Discord and Railway

The research system and the thing people see are two halves joined by one table.

**`predictions`.** `9_score.py --save` writes the ranked list there with a `drivers` JSONB
column holding each pick's SHAP breakdown. The bot reads that table and computes nothing.
Whatever writes it — a laptop, Railway, a GitHub Action — the serving side cannot tell.
That decoupling is why the Discord work never had to wait on the pipeline being hosted.

**Schedule** (all times ET, in `discord_bot.py`):

| when | task | what |
|---|---|---|
| Friday 17:00 | `weekly_pipeline` | six pipeline steps, an hour after the close |
| Sunday 10:00 | `sunday_picks` | posts the top 10 to `SCAN_CHANNEL_ID` |

Friday 17:00 means the snapshot is *dated* Friday **and** *priced* from Friday's close. A
weekend run produces the same date reaching back to the same close with two extra days for
something to break. The caveat is that FMP must have published the close by 17:00.

**`pipeline_runner.py`** runs each step as a subprocess, not an import. Partly because
`2_fmp_pipeline.py` is not a legal identifier, mainly for isolation: these jobs allocate
heavily and call `sys.exit` on bad input, so a separate process returns its memory for
certain and a `SystemExit` in step three cannot take the bot down. It **stops at the first
failure** — running `9_score.py` after `6_feature_compute.py` failed would publish a ranking
built on last week's features while dating it today.

Progress appears as one self-editing Discord message in `OPS_CHANNEL_ID`, marking each step
pending / running / done with its duration.

**Failure degrades to silence.** `sunday_picks` refuses data older than `MAX_PICKS_AGE_DAYS`
(14), so a failed refresh means no post rather than last week's picks presented as this
week's. No post is obviously nothing; a stale post looks exactly like a good one.

**Output.** `table_image.py` renders the picks as a PNG in `chart_generator.py`'s synthwave
palette — portrait, because Discord scales to message width and a landscape strip becomes
unreadable on a phone. Driver labels name a *direction* (`small cap 97`, not `size 3`) so a
row reads without decoding. The ANSI text table in `quant_picks.py` is the fallback, and is
not a lesser option: it is the version whose numbers can be selected and read by a screen
reader.

`!picks` takes no argument. `PICKS_COUNT` is the single knob, read by both the command and
the Sunday post.

**The text above the table is split by how often it changes.** The first version was a
five-sentence paragraph describing the model, printed every week — identical every week,
and therefore read about once. Worse than useless: it trains the eye to skip the space
immediately above the table, which is the one place something that mattered would appear.
So the standing description moved to **`!model`**, where it is opt-in, can afford four
sections, and explains the mechanism (including the tree count, which belongs in a
sentence and not in a one-liner). What remains above the table is one derived identity
line plus `turnover_note()`.

`model_detail()` returns a payload of real embed **fields** rather than one string.
Discord spaces fields and weights their names itself, so the sections read as sections;
`**Header.**` glued to the front of a sentence is an essay wearing labels, and that is
what got the first draft flagged as AI-written. The prose was the other half of that —
an em dash per clause, every point framed as "not X but Y", a closing aphorism — and the
fix there was plain sentences and contractions. Sections returning does not license the
voice returning with them. `_field()` truncates at 1024 characters because an over-long
field value makes Discord reject the **whole message**, and the factor list is derived
from whatever `9_score.py` stored.

**Weekly turnover is usually zero, and that is the point.** Oct 16 → Oct 23 held all ten
names and only shuffled the middle ranks. That follows directly from the factor set: 12-month
momentum, TTM margins and quarterly fundamentals carry almost no new information across seven
days, so a weekly re-score re-derives nearly the same ordering. It is a property of a
3-month-horizon model rather than a bug — and it is exactly why `Unchanged since Oct 16` is
worth one line. For a holder, "nothing changed, don't trade" is the useful answer, and the
week the line says otherwise is the week they need to read it.

A consequence worth noting: running the pipeline weekly buys little over running it monthly.
It stays weekly because it is cheap (50 min, unattended) and because a weekly run surfaces a
broken step within days instead of within a month.

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

*Last updated: 23 October 2026*

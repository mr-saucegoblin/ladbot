"""
7_eda.py — PACE: pAce (Analyze)

Step 7: Data exploration, cleaning, and visualizations.
Loads from feature_snapshots (PostgreSQL) and saves a cleaned dataset
to quant_clean.parquet for use by 8_model.py.

Usage:
    python 7_eda.py
"""

# ══════════════════════════════════════════════════════════════════════════════
# PACE: Plan (context recap)
# ══════════════════════════════════════════════════════════════════════════════
#
# Business problem: Predict which TSX stocks will have a positive 3-month
# forward return (outperforms = 1) vs not (outperforms = 0).
#
# This script covers:
#   - Loading feature_snapshots from PostgreSQL
#   - df.info(), df.describe(), missing values, duplicates, outliers
#   - Creating the target variable "outperforms"
#   - Selecting well-covered features (< 30% null)
#   - Seaborn visualizations (sector, boxplots, scatterplot, heatmap)
#   - Saving clean dataset to quant_clean.parquet


# ══════════════════════════════════════════════════════════════════════════════
# Step 1. Imports
# ══════════════════════════════════════════════════════════════════════════════

import datetime
import json
import sys
import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns

pd.set_option("display.max_columns", None)

# --keep-weak builds the UNFILTERED candidate set so 10_factor_baseline.py can
# re-measure every feature on training data only. The normal run picks features
# using all 90 months, so the walk-forward's own test windows helped decide which
# features it was given — a selection-bias loop this flag exists to measure.
# Output goes to its own parquet so the modelling one is never clobbered.
KEEP_WEAK    = "--keep-weak" in sys.argv
OUT_PARQUET  = "quant_clean_all.parquet" if KEEP_WEAK else "quant_clean.parquet"
OUT_MANIFEST = ("quant_clean_all_manifest.json" if KEEP_WEAK
                else "quant_clean_manifest.json")

from dotenv import load_dotenv
from quant_db import get_conn

load_dotenv()


# ══════════════════════════════════════════════════════════════════════════════
# Prediction horizon
# ══════════════════════════════════════════════════════════════════════════════
#
# Which forward return the model is asked to rank. Snapshots are monthly, so the
# horizon decides how much the labels of neighbouring rows overlap — and overlap
# is what destroys effective sample size:
#
#     horizon   window    independent periods
#     1m         29d       92     <- consecutive rows barely overlap
#     3m         88d       31
#     6m        176d       15
#     12m       352d        7
#
# Every row count is ~38k regardless, but 92 independent periods versus 31 is a
# 3x difference in how much the evaluation can actually tell you. With roughly 30
# periods you cannot separate a real edge from luck; with 90 you have a chance.
#
# The trade-off runs the other way on signal: fundamentals only move once a
# quarter, so at a 1-month horizon most fundamental features have little new to
# say and returns are driven more by flows and news. Momentum and reversal
# features are the opposite — short-term reversal is strongest at ~1 month and has
# largely decayed by 3.
#
# Set to one of: forward_return_1m / 3m / 6m / 12m.
TARGET_HORIZON = "forward_return_3m"

# Calendar days each horizon spans, used for the SQL cutoff and the train/test
# embargo. Mirrors FORWARD_RETURN_OFFSETS in 6_feature_compute.py (trading days
# x 1.4), so the two scripts cannot drift apart.
HORIZON_DAYS = {
    "forward_return_1m":   29,
    "forward_return_3m":   88,
    "forward_return_6m":  176,
    "forward_return_12m": 352,
}[TARGET_HORIZON]


# ══════════════════════════════════════════════════════════════════════════════
# Step 2. Data Exploration (Initial EDA and data cleaning)
# ══════════════════════════════════════════════════════════════════════════════

# ── 2.1  Load the dataset ─────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("  Step 2.1 — Loading data from feature_snapshots")
print("=" * 70)

conn = get_conn()
with conn.cursor() as cur:
    # Rows where the chosen forward return has already been realised. The date
    # cutoff is the horizon plus a few days of slack, so it tracks TARGET_HORIZON
    # instead of being hardcoded to one window.
    cur.execute("""
        SELECT *
        FROM feature_snapshots
        WHERE {target} IS NOT NULL
          AND snapshot_date <= CURRENT_DATE - INTERVAL '{days} days'
        ORDER BY snapshot_date ASC, ticker ASC
    """.format(target=TARGET_HORIZON, days=HORIZON_DAYS + 3))
    rows = cur.fetchall()
conn.close()

df0 = pd.DataFrame([dict(r) for r in rows])

# psycopg2 returns NUMERIC columns as Python Decimal — convert to float
str_cols = {"ticker", "sector", "tier", "snapshot_date"}
for col in df0.columns:
    if col not in str_cols:
        df0[col] = pd.to_numeric(df0[col], errors="coerce")

df0["snapshot_date"] = pd.to_datetime(df0["snapshot_date"]).dt.date

print(f"\nRows loaded:       {len(df0):,}")
print(f"Unique tickers:    {df0['ticker'].nunique():,}")
print(f"Date range:        {df0['snapshot_date'].min()} → {df0['snapshot_date'].max()}")
print(f"Columns:           {len(df0.columns)}")


# ── 2.1b  Exclusions from the data audit ──────────────────────────────────────
#
# Applied here, before any statistic is computed, so every number printed below
# describes the set the model will actually see.

# ACT.TO's price series is corrupt — it contains sub-penny closes (0.0001) next to
# $69 closes, in a universe screened to a $0.50 minimum. Its labels run to
# +2,616,700%, and that single ticker moves the 3-month return stddev across all
# 46,660 labelled rows from 0.33 to 131.35. Excluding the other 19 tickers flagged
# for price anomalies changes stddev only from 0.3321 to 0.3277, so they stay.
CORRUPT_PRICE_TICKERS = {"ACT.TO"}

# Which liquidity tier to model. 'main' is >= $100k/day average dollar volume
# (median $3.5M/day); 'early_watch' is below it (median $39k/day — a $20k position
# is half a typical day's volume, so a signal there is not actionable).
#
# Pooling the two is the specific thing to avoid: early_watch carries ~2x the
# return volatility (sd 0.56 vs 0.26) and a far fatter tail, so its outliers
# dominate the training loss. Its *median* return is actually lower than main's
# (1.72% vs 2.05%), meaning its higher mean comes from a few enormous winners —
# the pattern least likely to survive out-of-sample. Model early_watch separately
# if you want it, don't mix it in.
TIERS_TO_MODEL = {"main"}

before = len(df0)
df0 = df0[~df0["ticker"].isin(CORRUPT_PRICE_TICKERS)]
after_corrupt = len(df0)
df0 = df0[df0["tier"].isin(TIERS_TO_MODEL)]

print("\n  Audit exclusions:")
print(f"    corrupt price data ({', '.join(sorted(CORRUPT_PRICE_TICKERS))}): "
      f"-{before - after_corrupt:,} rows")
print(f"    tier not in {sorted(TIERS_TO_MODEL)}: -{after_corrupt - len(df0):,} rows")
print(f"    remaining: {len(df0):,} rows, {df0['ticker'].nunique():,} tickers")


# ── 2.2  Basic dataset information ────────────────────────────────────────────

print("\n" + "─" * 60)
print("  Step 2.2 — df0.head()")
print("─" * 60)
print(df0[["ticker", "snapshot_date", "sector", "tier",
           "return_3m", "roa", "ev_to_ebitda", TARGET_HORIZON]].head())

print("\n" + "─" * 60)
print("  df0.info()")
print("─" * 60)
df0.info()


# ── 2.3  Descriptive statistics ───────────────────────────────────────────────

print("\n" + "─" * 60)
print("  Step 2.3 — df0.describe() (key columns)")
print("─" * 60)
key_cols = [
    "return_1m", "return_3m", "return_12m_1m", "rsi_14",
    "roa", "gross_margin", "ev_to_ebitda", "pe_ratio",
    "net_debt_to_ebitda", TARGET_HORIZON,
]
key_cols = [c for c in key_cols if c in df0.columns]
print(df0[key_cols].describe())


# ── 2.4  Missing values ───────────────────────────────────────────────────────

print("\n" + "─" * 60)
print("  Step 2.4 — Missing values")
print("─" * 60)
null_pct = df0.isnull().mean().sort_values(ascending=False)
print("  Columns with any missing values:")
print(null_pct[null_pct > 0].to_string())


# ── 2.5  Create target variable ───────────────────────────────────────────────

# outperforms = 1 if the stock beat the MEDIAN stock on its own snapshot date.
#
# Not `forward_return_3m > 0`. That version had no benchmark in it at all despite
# the name — it asked "did this stock go up", which in a rising market is mostly a
# question about the calendar. Measured on this data it ran from 5.0% positive for
# 2019-12 snapshots (looking forward into the COVID crash) to 84.5% for 2025-04,
# a 79.5-point spread driven entirely by *when* the snapshot was taken.
#
# That matters more than a skewed class balance. gold_change_3m, oil_change_3m,
# cadusd_change_3m and yield_curve_spread hold the same value for every ticker on
# a given date, so they act as a date fingerprint; paired with a date-driven label
# a tree can learn "this macro regime => everything rose" and memorise periods
# instead of ranking companies. Benchmarking within the date closes that channel
# automatically, because a feature that is constant across a date cannot separate
# winners from losers inside it.
#
# Median is taken by date ONLY, deliberately not by date+sector. Sector-relative
# would neutralise sector bets, and backing a sector early is the thematic edge
# this project is built to find — see PROJECT.md. The model should be free to
# favour a sector.
median_by_date = df0.groupby("snapshot_date")[TARGET_HORIZON].transform("median")
df0["outperforms"] = (df0[TARGET_HORIZON] > median_by_date).astype(int)

print("\n" + "─" * 60)
print("  Step 2.5 — Target variable: outperforms")
print("─" * 60)
print(df0["outperforms"].value_counts())
print()
print(df0["outperforms"].value_counts(normalize=True).round(3))


# ── 2.5b  Graded target for a ranking objective ───────────────────────────────
#
# A second label: which decile of that month the stock's forward return landed in,
# 0 (worst) to 9 (best).
#
# `outperforms` is binary, so its loss function treats a stock that beat the median
# by 0.1% exactly like one that beat it by 40%. The walk-forward run showed the cost
# of that precisely: hit rate climbed monotonically across deciles (55.7% -> 59.5%)
# while mean return stayed flat. The model was ranking probability-of-winning
# correctly and was completely indifferent to size — because that is what it was
# asked to do, and size is what we then evaluated it on.
#
# A graded label lets LightGBM's lambdarank objective punish misplacing a big winner
# far more than a marginal one. Its default label_gain is 2^i - 1, so grade 9 is
# worth 511 and grade 5 worth 31 — errors at the top of the list dominate, which is
# where you would actually be buying.
N_RANK_GRADES = 10


def _grade(s: pd.Series) -> pd.Series:
    # rank(method="first") breaks ties by position so qcut always finds N distinct
    # edges. Guard the thin-date case rather than letting one short month abort.
    if s.notna().sum() < N_RANK_GRADES:
        return pd.Series(np.nan, index=s.index)
    return pd.qcut(s.rank(method="first"), N_RANK_GRADES, labels=False)


# Grade on return PER UNIT OF RISK, not raw return.
#
# The first lambdarank run showed why. Told that landing a stock in the top grade
# was worth 511 against a middling 31, the model found the cheapest way to satisfy
# that: buy variance. Its top decile was the most volatile, smallest, least liquid,
# lowest-Piotroski, most distressed names available — 3.4x the volatility of the
# bottom decile for 2.9x the return, i.e. a WORSE return per unit of risk (0.28 vs
# 0.32). The +6.79% spread was a leveraged bet on a market that rose in all four
# folds, and would have inverted just as violently in a down quarter.
#
# Dividing by trailing volatility removes that escape route: a high-variance name
# now has to beat its own risk, so buying beta stops paying. total_vol is a trailing
# 104-week figure known at snapshot time, so using it as a denominator introduces no
# look-ahead. It is taken raw here, before the cross-sectional ranking further down.
#
# Set to False to grade on raw return instead and reproduce the earlier behaviour.
RISK_ADJUST_TARGET = True

if RISK_ADJUST_TARGET and "total_vol" in df0.columns:
    vol = df0["total_vol"].where(df0["total_vol"] > 0)      # 0 or negative -> NaN
    grade_basis = df0[TARGET_HORIZON] / vol
    basis_name = f"{TARGET_HORIZON} / total_vol"
else:
    grade_basis = df0[TARGET_HORIZON]
    basis_name = TARGET_HORIZON

df0["_grade_basis"] = grade_basis
df0["target_rank"] = df0.groupby("snapshot_date")["_grade_basis"].transform(_grade)
df0 = df0.drop(columns="_grade_basis")

print(f"\n  graded target 'target_rank': {int(df0['target_rank'].notna().sum()):,} rows "
      f"in {N_RANK_GRADES} grades (0 = worst decile of the month, 9 = best)")
print(f"  graded on: {basis_name}")


# ── 2.6  Select features (drop sparse columns) ───────────────────────────────

# Columns we never use as features
IDENTIFIER_COLS = {
    "id", "ticker", "snapshot_date",
    "forward_return_1m", "forward_return_3m",
    "forward_return_6m", "forward_return_12m",
    # Graded label from 2.5b — a target, never an input.
    "target_rank",
}

# Features excluded on bias grounds, not sparsity — the <30% null filter below
# will NOT catch these, which is exactly why they need naming.
#
# The news columns leak the calendar. news_articles only covers 2026-03-03 to
# 2026-07-15, so news_count_30d is 0 on all 45,206 non-null rows before
# 2026-03 and averages 57.97 after it. That makes it a near-perfect indicator of
# "is this row recent", which a tree will split on to date the row — information
# it could never have at prediction time. It is 96.8% non-null, so it sails
# through a sparsity filter. Restore these only once there is real historical
# news coverage spanning the whole backfill window.
LEAKY_FEATURES = {
    "news_count_30d",
    "news_count_90d",
    "news_velocity_accel",
}

# Features that are 100% NULL because nothing populates their source table.
# The sparsity filter would drop these anyway; listing them documents why they're
# gone so they aren't mistaken for features that merely underperformed.
#   - analyst block: upgrades_downgrades / price_targets are empty tables, and
#     analyst_estimates.num_analysts_eps is 0/10,690 populated
#   - institutional_ownership, earnings_call_transcripts: empty tables
#   - copper / TSX-index columns: HGUSD and ^GSPTSX are HTTP 402 on the current
#     FMP plan, so they are structurally unobtainable, not merely missing
EMPTY_FEATURES = {
    "num_analysts", "upgrades_90d", "downgrades_90d",
    "eps_revision_3m_pct", "revenue_revision_3m_pct", "price_target_change_3m_pct",
    "institutional_ownership_pct", "institutional_ownership_change",
    "earnings_call_sentiment", "news_sentiment_avg_30d",
    "copper_change_3m", "relative_strength_vs_tsx_3m", "tsx_return_3m",
}

# Features with zero variance after the row filters above — they carry no
# information and a sparsity filter can't see the problem, since they're 100% populated.
#   - tier: constant by construction once TIERS_TO_MODEL selects a single tier
#   - filing_lag_days: a tautology. filed_at is no longer read from FMP (its
#     filingDate was unusable for TSX-only filers) but computed as fiscal_date + 45d
#     for quarterly / 90d for annual, so this column can only ever return the
#     constant we assumed. It measures our own convention, not the company's
#     reporting speed. It would become a real feature again only if a true filing
#     date were sourced, e.g. from SEDAR+.
CONSTANT_FEATURES = {
    "tier",
    "filing_lag_days",
}

# FMP key_metrics ratios that embed a PRICE but only refresh when a filing lands.
# Measured on this data they take ~1.97 distinct values per ticker-year — they are
# step functions, frozen for months at a time (ABX.TO sat at pe_ratio 74.44 for nine
# consecutive months of 2023 while its price moved), so within a quarter they carry
# no information about price at all.
#
# Two problems, not one. Besides being stale, the ratio-form ones are
# non-monotonic when the denominator goes negative: a company losing $1 has a P/E
# of -500 and one losing $100m has a P/E of -2, so the ordering is meaningless.
# 30% of pe_ratio, 33% of price_to_fcf and 18% of ev_to_ebitda are negative.
#
# Every one is replaced by a dynamic_* column computed as snapshot-date price over
# CAD-normalised TTM fundamentals, which refresh ~11.9 times a year.
#
# NOT included here: roa, roe, roic, current_ratio, interest_coverage and
# net_debt_to_ebitda. Those hold no price term, so updating only at a filing is
# correct behaviour rather than staleness. They are still only ~1.97/year where the
# ttm_* twins manage 4.95, which is worth revisiting once feature importance is in —
# but the annual figures are audited and the quarterlies aren't, so they stay for now.
STALE_PRICED_FEATURES = {
    "pe_ratio",         # -> dynamic_pe
    "price_to_sales",   # -> dynamic_ps
    "price_to_book",    # -> dynamic_pb
    "price_to_fcf",     # -> dynamic_pfcf
    "ev_to_ebitda",     # -> dynamic_ev_ebitda
    "fcf_yield",        # -> dynamic_fcf_yield (populates on the next feature rebuild)
    "earnings_yield",   # -> dynamic_earnings_yield (ditto)
}

# Features with no standalone predictive power, measured by 10_factor_baseline.py:
# |t| < 1.0 on the information coefficient across all 90 months, using
# non-overlapping windows. Everything kept is at |t| >= 1.0.
#
# Why cut at all: the model carried 85 features against roughly 13 independent
# 3-month periods. Adding 17 of them took the walk-forward spread from +2.94% to
# -0.50% — not a significant move on its own, but the direction is what you would
# expect from piling noise columns onto a sample that cannot support them. The
# baseline says the best single factor reaches t ~= 2.0 and most sit near zero, so
# a leaner set loses very little and overfits less.
#
# TWO DELIBERATE EXCEPTIONS, kept despite failing the bar:
#   sector      — categorical, so the IC test never ranked it. The model needs it:
#                 93% of the backtest edge came from selection *within* sectors.
#   market_cap  — univariate t of -0.32, but a conditioning variable rather than a
#                 standalone signal, and PROJECT.md's thesis requires size to be a
#                 feature rather than a universe screen. An IC test cannot detect
#                 "value works better in small caps"; only an interaction can.
#
# CAVEAT: this selection used all 90 months, including the walk-forward's test
# windows, so the next model result is mildly optimistic. The clean version selects
# features inside each fold on training data only. Worth knowing before treating any
# improvement as real.
#
# Refresh this list whenever the features or the data change — re-run
# 10_factor_baseline.py and re-cut.
WEAK_FEATURES = {
    "altman_z_pct", "altman_z_score", "avg_dollar_volume_90d", "cadusd_beta",
    "cadusd_r2", "current_ratio", "debt_to_assets", "dynamic_earnings_yield",
    "dynamic_fcf_yield", "dynamic_pb", "dynamic_pb_sect", "dynamic_pe",
    "earnings_acceleration", "earnings_growth_yoy", "ev_to_ebitda_pct",
    "fcf_conversion", "fcf_margin", "gold_beta", "gold_r2", "high_52w_proximity",
    "idio_vol", "interest_coverage", "latest_eps_surprise_pct", "market_beta",
    "market_r2", "net_debt_to_ebitda", "oil_r2",
    "operating_cf_to_assets", "pe_ratio_pct", "piotroski_score", "rates_beta",
    "rates_r2", "return_1m", "return_3m", "return_3m_sect", "return_3m_vol_adj",
    "revenue_acceleration", "revenue_growth_yoy", "roa", "roe", "roic", "rsi_14",
    "shares_change_3yr", "total_vol", "total_vol_sect", "ttm_current_ratio",
    "ttm_debt_to_assets", "ttm_debt_to_assets_sect", "ttm_earnings_growth",
    "ttm_earnings_growth_sect", "ttm_fcf_margin", "ttm_net_debt_to_ebitda",
    "ttm_net_debt_to_ebitda_sect", "ttm_operating_cf_to_assets",
    "ttm_operating_margin", "ttm_operating_margin_sect", "ttm_revenue_growth",
    "ttm_revenue_growth_sect", "ttm_roa", "ttm_roa_sect", "ttm_roe", "ttm_roic",
    "volume_ratio_90d",
}

# Near-duplicates of a better version of the same measurement (|corr| >= 0.70).
#
# Keeping both halves of a pair buys nothing and actively causes confusion. When two
# features carry the same information a tree picks one and the other collects the
# residue, and SHAP then splits the credit arbitrarily between them. That is exactly
# what produced "penalised for being cheap" on a cheap stock: dynamic_ps was carrying
# the value signal coherently (corr between value and its own SHAP: -0.81) while
# ps_ratio_pct — the same measurement, ranked within sector — was left holding noise
# (-0.03, the only incoherent feature in the whole model).
#
# Which half to keep is decided on univariate merit from 10_factor_baseline.py, NOT
# on which one the model happened to latch onto. Coherence is an artifact of the
# tree's arbitrary choice between twins: drop one and the survivor absorbs the
# signal regardless.
#
#   return_12m_1m, return_12m_1m_sect  -> return_12m_1m_vol_adj  (t 2.00, corr .95/.83)
#   shares_change_1yr                  -> shares_change_1yr_sect (t -2.58, corr .91)
#   ttm_gross_margin                   -> gross_margin           (t -1.44, corr .91)
#   dynamic_ps                         -> ps_ratio_pct           (t -1.65, corr .87)
#   dynamic_pfcf                       -> dynamic_fcf_yield_sect (t 1.96, corr .77)
#
# The last pair are reciprocals of each other — price/FCF against FCF/price — which
# is why their t-stats carry opposite signs while describing one bet.
REDUNDANT_FEATURES = {
    "return_12m_1m",
    "return_12m_1m_sect",
    "shares_change_1yr",
    "ttm_gross_margin",
    "dynamic_ps",
    "dynamic_pfcf",
}

EXCLUDED_FEATURES = (
    LEAKY_FEATURES | EMPTY_FEATURES | CONSTANT_FEATURES | STALE_PRICED_FEATURES
    | WEAK_FEATURES | REDUNDANT_FEATURES
)

# Under --keep-weak only the exclusions about CORRECTNESS survive. Leaky, empty,
# constant and stale-priced columns stay banned however the script is run — dropping
# those is not a judgement call. WEAK and REDUNDANT are the two performance
# judgements, and they are the ones that used test-window data to decide.
if KEEP_WEAK:
    EXCLUDED_FEATURES = (
        LEAKY_FEATURES | EMPTY_FEATURES | CONSTANT_FEATURES | STALE_PRICED_FEATURES
    )
    print()
    print(f"  --keep-weak: {len(WEAK_FEATURES | REDUNDANT_FEATURES)} "
          f"performance-selected exclusions reinstated as candidates")

# Columns kept even if they breach the sparsity threshold below.
#
# dynamic_pe is 70% populated and dynamic_pfcf 67%, because both return NULL when
# TTM earnings or free cash flow are non-positive. That missingness is not a data
# gap — it IS the signal "this company is currently lossmaking", and LightGBM learns
# a default direction for NaN rather than discarding the row. Dropping them on a
# null count would throw away the only live valuation measures in favour of the
# frozen ones above.
ALWAYS_KEEP = {
    "dynamic_pe", "dynamic_ps", "dynamic_pb", "dynamic_pfcf", "dynamic_ev_ebitda",
    "dynamic_earnings_yield", "dynamic_fcf_yield",
}

candidate_cols = [
    c for c in df0.columns
    if c not in IDENTIFIER_COLS
    and c not in EXCLUDED_FEATURES
    and c != "outperforms"
]

print("\n  Feature exclusions (bias, not sparsity):")
for c in sorted(LEAKY_FEATURES):
    if c in df0.columns:
        print(f"    leaks the calendar: {c}")
print(f"    known-empty source tables: {len([c for c in EMPTY_FEATURES if c in df0.columns])} columns")
for c in sorted(CONSTANT_FEATURES):
    if c in df0.columns:
        print(f"    zero variance: {c}")

null_rates_by_col = df0[candidate_cols].isnull().mean()
# A column survives if it clears the sparsity bar OR is whitelisted above — but a
# whitelisted column that is 100% empty is still dropped, so a dynamic_* feature
# that hasn't been computed yet doesn't enter the model as a column of pure NaN.
usable_cols = [
    c for c in candidate_cols
    if (null_rates_by_col[c] < 0.30)
    or (c in ALWAYS_KEEP and null_rates_by_col[c] < 1.0)
]

print("\n" + "─" * 60)
print("  Step 2.6 — Feature selection (<30% null)")
print("─" * 60)
print(f"\nCandidate features:    {len(candidate_cols)}")
print(f"Features kept:         {len(usable_cols)}")
print(f"Features dropped:      {len(candidate_cols) - len(usable_cols)}")
print(f"\nDropped (>30% null):")
dropped = null_rates_by_col[
    (null_rates_by_col >= 0.30) & (~null_rates_by_col.index.isin(usable_cols))
].sort_values(ascending=False)
print(dropped.to_string() if not dropped.empty else "  (none)")

rescued = [c for c in usable_cols if null_rates_by_col[c] >= 0.30]
if rescued:
    print(f"\nKept despite >30% null (whitelisted — missingness is itself signal):")
    for c in rescued:
        print(f"  {c:<26}{1 - null_rates_by_col[c]:>6.1%} populated")

print(f"\nKept features:")
print(usable_cols)

# Working copy.
#
# ticker, snapshot_date and forward_return_3m are carried through even though none
# of them is ever a feature.
#
# snapshot_date: 8_model.py splits train/test *by time*. With financial panel data
# a random split puts June 2024 rows in train and March 2024 rows in test, so the
# model is scored on a period it already learned from and the backtest reads far
# better than reality.
#
# forward_return_3m: the target is a 0/1, which only supports classification
# metrics. The actual return is what shows whether the top-ranked names made money
# — a model can be 54% accurate and still unprofitable, or 51% accurate and very
# profitable, depending on the size of the moves it catches.
#
# ticker: to confirm no name straddles the split, and to group errors by company.
KEEP_FOR_MODELLING = ["ticker", "snapshot_date", TARGET_HORIZON]

df1 = df0[KEEP_FOR_MODELLING + usable_cols + ["outperforms", "target_rank"]].copy()


# Deliberately NOT df1.dropna().
#
# Two reasons. First, bias: requiring every one of ~50 features to be present
# keeps only the rows with complete coverage, and those are systematically the
# larger, better-covered companies — the missingness is not random, so dropping it
# silently reshapes the universe toward big caps and away from the under-researched
# names this project is built to find. Second, it's unnecessary: LightGBM learns a
# default split direction for NaN natively, so a missing value is information the
# model can use rather than a row it must discard.
complete_case_rows = len(df1.dropna())
print(f"\nRows kept (NaNs preserved):         {len(df1):,}")
print(f"  for reference, dropna() would leave: {complete_case_rows:,} "
      f"({complete_case_rows/len(df1):.1%}) — not used, see comment")


# ── 2.7  Check for duplicates ────────────────────────────────────────────────

print("\n" + "─" * 60)
print("  Step 2.7 — Duplicates")
print("─" * 60)
n_dups = df1.duplicated().sum()
print(f"Duplicate rows: {n_dups}")
if n_dups > 0:
    df1 = df1.drop_duplicates(keep="first")
    print(f"Rows after deduplication: {len(df1):,}")
else:
    print("No duplicates found.")


# ── 2.8  Check for outliers ───────────────────────────────────────────────────

print("\n" + "─" * 60)
print("  Step 2.8 — Outlier check (return_3m)")
print("─" * 60)

# Plotted from df0, not df1. df1 holds only the selected FEATURES, and feature
# selection now drops return_3m (|t| = 0.19 on the factor baseline), which made this
# section raise KeyError. The EDA should describe the data regardless of which
# columns the model ends up using, and df0 carries every column with raw values.
plt.figure(figsize=(6, 3))
plt.title("Boxplot to detect outliers for return_3m", fontsize=12)
sns.boxplot(x=df0["return_3m"])
plt.tight_layout()
plt.savefig("plot_01_outliers_return3m.png", dpi=100)
plt.show()
print("Saved: plot_01_outliers_return3m.png")

p25 = df0["return_3m"].quantile(0.25)
p75 = df0["return_3m"].quantile(0.75)
iqr = p75 - p25
upper_lim = p75 + 1.5 * iqr
lower_lim = p25 - 1.5 * iqr

print(f"\nreturn_3m IQR bounds:  [{lower_lim:.1%}, {upper_lim:.1%}]")
outliers = df0[(df0["return_3m"] > upper_lim) | (df0["return_3m"] < lower_lim)]
print(f"Outlier rows:  {len(outliers):,} ({len(outliers) / len(df0):.1%})")

# Save IQR bounds to a file so 3_model.py can use them for the LR step
pd.DataFrame({"lower_lim": [lower_lim], "upper_lim": [upper_lim]}).to_csv(
    "quant_iqr_bounds.csv", index=False
)
print("Saved: quant_iqr_bounds.csv")


# ══════════════════════════════════════════════════════════════════════════════
# pAce: Analyze Stage — Visualizations
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("  pAce: ANALYZE — Data Visualizations")
print("=" * 70)


# ── 3.1  Sector breakdown by outcome ─────────────────────────────────────────

if "sector" in df1.columns:
    plt.figure(figsize=(14, 5))
    sns.histplot(
        data=df1, x="sector", hue="outperforms",
        hue_order=[0, 1], multiple="dodge", shrink=0.6,
    )
    plt.xticks(rotation=45, ha="right")
    plt.title("Counts of outperformed (1) / underperformed (0) by sector", fontsize=13)
    plt.tight_layout()
    plt.savefig("plot_02_sector_breakdown.png", dpi=100)
    plt.show()
    print("Saved: plot_02_sector_breakdown.png")


# ── 3.2  Boxplots: return_3m and roa by outcome ───────────────────────────────

# Same layout as Salifort's "satisfaction_level + hours worked" side-by-side

# df0, not df1 — these describe the data, and feature selection now drops both
# return_3m and roa, which would silently skip the plot entirely.
plot_feats = [f for f in ["return_3m", "roa"] if f in df0.columns]
if len(plot_feats) == 2:
    fig, axes = plt.subplots(1, 2, figsize=(18, 7))

    for ax, feat in zip(axes, plot_feats):
        sns.boxplot(
            data=df0, x=feat, y="outperforms",
            hue="outperforms", orient="h", ax=ax, legend=False,
        )
        ax.set_title(f"{feat} by outcome", fontsize=13)
        ax.set_xlabel(feat)
        ax.set_ylabel("outperforms")

    plt.tight_layout()
    plt.savefig("plot_03_boxplots_features.png", dpi=100)
    plt.show()
    print("Saved: plot_03_boxplots_features.png")


# ── 3.3  Scatterplot: momentum vs valuation ──────────────────────────────────

# Analogous to Salifort's hours vs satisfaction scatterplot with axvline
# Same reasoning as above — read from df0 so the plot survives feature selection,
# and check both axes exist before attempting it.
if {"ev_to_ebitda", "return_3m", "outperforms"}.issubset(df0.columns):
    plot_df = df0[
        (df0["ev_to_ebitda"] > 0) & (df0["ev_to_ebitda"] < 40)
    ].copy()

    plt.figure(figsize=(16, 9))
    sns.scatterplot(
        data=plot_df,
        x="return_3m", y="ev_to_ebitda",
        hue="outperforms", alpha=0.4,
        palette={0: "#d62728", 1: "#1f77b4"},
    )
    plt.axvline(x=0, color="#333333", ls="--", lw=1.2, label="0% trailing return")
    plt.title("3-month trailing return vs EV/EBITDA valuation", fontsize=13)
    plt.legend(title="outperforms")
    plt.tight_layout()
    plt.savefig("plot_04_scatter_momentum_valuation.png", dpi=100)
    plt.show()
    print("Saved: plot_04_scatter_momentum_valuation.png")


# ── 3.4  Correlation heatmap ─────────────────────────────────────────────────

# Limit to 14 numeric features to keep the heatmap readable
heatmap_candidates = [
    c for c in usable_cols
    if c in df1.columns
    and pd.api.types.is_numeric_dtype(df1[c])
    and c not in {"avg_dollar_volume_90d", "market_cap", "tier"}
][:14]

plt.figure(figsize=(16, 11))
hm = sns.heatmap(
    df1[heatmap_candidates + ["outperforms"]].corr(),
    vmin=-1, vmax=1, annot=True, fmt=".2f",
    cmap=sns.color_palette("vlag", as_cmap=True),
    annot_kws={"size": 8},
)
hm.set_title("Correlation Heatmap", fontdict={"fontsize": 14}, pad=12)
plt.tight_layout()
plt.savefig("plot_05_correlation_heatmap.png", dpi=100)
plt.show()
print("Saved: plot_05_correlation_heatmap.png")


# ── 3.5  Group stats (like Salifort's satisfaction_level groupby) ─────────────

print("\n" + "─" * 60)
print("  pAce — Group stats by outcome")
print("─" * 60)

for feat in ["return_3m", "roa", "ev_to_ebitda", "gross_margin"]:
    if feat not in df0.columns:
        continue
    # String names, not [np.mean, np.median]: passing raw numpy functions to .agg()
    # no longer resolves to the pandas aggregation, so median came back all-NaN.
    grp = df0.groupby("outperforms")[feat].agg(["mean", "median"]).round(4)
    print(f"\n{feat}:")
    print(grp)


# ══════════════════════════════════════════════════════════════════════════════
# Save clean dataset for 3_model.py
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("  Saving clean dataset")
print("=" * 70)

# The feature list has to travel in its own file, not in df1.attrs.
#
# DataFrame.attrs is in-memory metadata only — to_parquet does not persist it, so
# the previous version of this script dropped the selection on the floor and
# 8_model.py had no way to tell a feature column from a carried identifier. A
# sidecar JSON is explicit and readable without pandas.
# ── Cross-sectional ranking ───────────────────────────────────────────────────
#
# Replace each numeric feature with its percentile rank among the other stocks on
# the SAME snapshot date, so a value says "20th cheapest of 430 names this month"
# rather than "P/B of 1.5".
#
# Why it matters here: a P/B of 1.5 in March 2020, when the market had just
# crashed and everything was cheap, means something completely different from a
# P/B of 1.5 in December 2021 when everything was expensive. LightGBM splits on a
# GLOBAL threshold — it sees 1.5 both times and treats them the same. It cannot
# rank within a date by itself, however many trees you give it.
#
# That mismatch is what the first model run exposed. The target already asked a
# date-relative question ("beat the median stock that month") while the features
# were absolute levels, so the tree reached for yield_curve_spread, oil_change_3m
# and friends — not to rank companies, but to work out WHICH PERIOD it was in so
# it could calibrate those levels. Training longer made it better at that and
# worse on unseen periods: val AUC 0.555 -> 0.575 while test AUC 0.491 -> 0.482.
#
# Ranking also disposes of those macro columns without a judgement call. A column
# holding one value for every ticker on a date gets the identical rank for all of
# them, so its cross-sectional variance is zero and it is dropped below — no
# hand-picked exclusion list needed.
#
# Set to False to reproduce the absolute-level baseline for comparison.
CROSS_SECTIONAL_RANK = True

if CROSS_SECTIONAL_RANK:
    print("\n" + "─" * 60)
    print("  Cross-sectional ranking (within snapshot_date)")
    print("─" * 60)

    numeric_feats = [
        c for c in usable_cols
        if c in df1.columns and pd.api.types.is_numeric_dtype(df1[c])
    ]
    non_numeric = [c for c in usable_cols if c not in numeric_feats]

    # Find the flat columns BEFORE ranking, by counting distinct raw values per
    # date. Testing after the fact does not work: ranking a group whose values are
    # all identical gives every row the average rank (n+1)/2, scaled by pct=True to
    # (n+1)/(2n) — so a month of 430 names yields 0.50116 and one of 440 yields
    # 0.50114. The column then has a tiny nonzero spread and survives a std check,
    # and what survives encodes how many stocks were in the universe that month,
    # which is another date proxy. Counting distinct raw values is unambiguous.
    nuniq_per_date = df1.groupby("snapshot_date")[numeric_feats].nunique(dropna=True)
    flat = [c for c in numeric_feats if nuniq_per_date[c].max() <= 1]

    numeric_feats = [c for c in numeric_feats if c not in flat]

    # rank(pct=True) maps each value to its position within the group, scaled to
    # 0-1. NaN stays NaN, which is what we want — a missing value should not be
    # given a middling rank as if it were known to be average.
    for c in numeric_feats:
        df1[c] = df1.groupby("snapshot_date")[c].rank(pct=True)

    if flat:
        print(f"  dropped — no variation within a date ({len(flat)}):")
        for c in flat:
            print(f"      {c}")
        df1 = df1.drop(columns=flat)
        usable_cols = [c for c in usable_cols if c not in flat]

    # numeric_feats already has the flat columns removed, so no second subtraction.
    print(f"  ranked {len(numeric_feats)} numeric features")
    print(f"  left as-is (non-numeric): {non_numeric}")
    print(f"  features remaining: {len(usable_cols)}")


# ── Train / test boundary ─────────────────────────────────────────────────────
#
# Split by date, with a gap. The gap is the part that's easy to miss: snapshots are
# monthly but each label measures a 3-month forward return, so consecutive rows
# share roughly two thirds of their return window. Train through 2024-06-30 and
# test from 2024-07-31 and that final training label is measured through
# 2024-09-26 — which completely contains the first test row's window. The model
# would already have been told what happened in the test period, via its own
# training labels.
#
# Discarding one full label horizon at the seam costs ~1,300 rows and removes the
# overlap entirely. The same reasoning rules out ordinary k-fold cross-validation
# on this data: folds would share return windows, so CV needs the same purging.
TRAIN_END    = datetime.date(2024, 6, 30)
EMBARGO_DAYS = HORIZON_DAYS             # exactly one label horizon
TEST_START   = TRAIN_END + datetime.timedelta(days=EMBARGO_DAYS)

_sd       = pd.to_datetime(df1["snapshot_date"]).dt.date
n_train   = int((_sd <= TRAIN_END).sum())
n_embargo = int(((_sd > TRAIN_END) & (_sd < TEST_START)).sum())
n_test    = int((_sd >= TEST_START).sum())

print("\n" + "─" * 60)
print("  Train / test split (temporal, embargoed)")
print("─" * 60)
print(f"  train    <= {TRAIN_END}                {n_train:>7,} rows")
print(f"  embargo     {TRAIN_END} .. {TEST_START}  {n_embargo:>7,} rows  (discarded)")
print(f"  test     >= {TEST_START}                {n_test:>7,} rows")

manifest = {
    "feature_cols":        usable_cols,
    "target":              "outperforms",
    "target_rank":         "target_rank",
    "rank_grades":         N_RANK_GRADES,
    "target_horizon":      TARGET_HORIZON,
    "identifier_cols":     KEEP_FOR_MODELLING,
    "train_end":           str(TRAIN_END),
    "test_start":          str(TEST_START),
    "embargo_days":        EMBARGO_DAYS,
    "split_rows":          {"train": n_train, "embargo_dropped": n_embargo, "test": n_test},
    "rows":                len(df1),
    "tiers_modelled":      sorted(TIERS_TO_MODEL),
    "excluded_tickers":    sorted(CORRUPT_PRICE_TICKERS),
    "excluded_features":   sorted(EXCLUDED_FEATURES),
    "snapshot_date_min":   str(df1["snapshot_date"].min()),
    "snapshot_date_max":   str(df1["snapshot_date"].max()),
    "nans_preserved":      True,
    "cross_sectional_rank": CROSS_SECTIONAL_RANK,
}
with open(OUT_MANIFEST, "w", encoding="utf-8") as fh:
    json.dump(manifest, fh, indent=2)

df1.to_parquet(OUT_PARQUET, index=False)
print(f"\nSaved: {OUT_PARQUET}  ({len(df1):,} rows, {len(df1.columns)} columns)")
print(f"Saved: {OUT_MANIFEST}  ({len(usable_cols)} features, target='outperforms')")
print("\nRun 8_model.py next — split train/test on snapshot_date, never randomly.")

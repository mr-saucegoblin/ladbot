"""
feature_compute.py — Compute monthly feature snapshots for all tickers.

Reads raw data tables, computes ~60 features, writes to feature_snapshots.
Snapshots are taken on the last calendar day of each month. All features are
point-in-time correct: only data with filed_at <= snapshot_date is used.

Three passes:
  1. Feature computation  — one row per ticker per month-end
  2. Forward return fill  — fills forward_return_* for completed periods
  3. Sector percentiles   — fills ev_to_ebitda_pct, pe_ratio_pct, ps_ratio_pct

Usage:
    python feature_compute.py                # full backfill + all passes
    python feature_compute.py --test         # first 5 tickers, no DB write
    python feature_compute.py --returns-only # skip feature compute, fill returns only
    python feature_compute.py --percentiles-only
    python feature_compute.py --clean-stale-returns  # NULL returns built on stale prices
"""

import argparse
import datetime
import math
import os

import numpy as np
import pandas as pd
import psycopg2.extras
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from tqdm import tqdm
from quant_db import get_conn, init_schema

load_dotenv()

# 2019 is the earliest start the data actually supports: FMP's quarterly history
# runs out around 2016-17, so only 71 tickers have the 8 quarters TTM needs by
# Jan 2018 — versus 474 by Jan 2019. Going earlier just adds rows that are null
# on every fundamental feature.
BACKFILL_START        = datetime.date(2019, 1, 31)
MAIN_TIER_MIN_VOL     = 100_000   # avg daily dollar volume threshold
FORWARD_RETURN_DAYS   = {
    "forward_return_1m":  21,
    "forward_return_3m":  63,
    "forward_return_6m":  126,
    "forward_return_12m": 252,
}

# How far before a target date a price may sit and still be accepted as that date's
# price. Without a bound, "the last close on or before date X" silently returns a
# price from years earlier once a ticker stops trading — and because the base and
# future lookups then return the *same* final close, the return computes as exactly
# 0.0. That produced 1,950 fabricated zero labels, concentrated in delisted names,
# many of them M&A takeouts that actually paid holders a premium. 10 days clears a
# long weekend plus a holiday or a short trading halt, and is far tighter than any
# real delisting gap.
MAX_PRICE_STALENESS_DAYS = 10

# Calendar-day offset used to approximate each trading-day horizon (63 trading days
# ~= 88 calendar days). Derived here so the fill and the cleanup agree by construction.
FORWARD_RETURN_OFFSETS = {col: int(td * 1.4) for col, td in FORWARD_RETURN_DAYS.items()}

# ── Factor betas ──────────────────────────────────────────────────────────────
#
# gold_change_3m and friends hold one value per snapshot date, shared by all 590
# tickers, so they have zero variance *within* a date and cannot on their own say
# which stock beats which. A beta fixes that: it measures how much this particular
# company's stock moves when the factor moves, so it varies across tickers.
#
# Both halves are needed, as separate columns. Beta alone is ambiguous — high gold
# beta is good when gold rises and bad when it falls — so the macro column acts as
# the regime switch: a tree splits on gold_change_3m first (varies across dates),
# then on gold_beta inside that branch. Supplying a pre-multiplied beta x move term
# instead would bake in a linear assumption the tree discovers better by itself.
#
# Weekly rather than daily returns: gold trades nearly around the clock while the
# TSX runs 09:30-16:00 ET, and that non-synchronous window biases daily commodity
# betas toward zero. Weekly sampling largely removes the effect.
BETA_WINDOW_WEEKS = 104   # 2 years of weekly returns
BETA_MIN_WEEKS    = 52    # need at least 1 year before a beta is meaningful

# series -> (feature prefix, how to difference it).
# Yields are levels in percent, so their "return" is a change in the yield itself;
# prices and FX are proportional, so they use percentage change.
FACTOR_SPECS = {
    "GCUSD":  ("gold",   "pct"),
    "BZUSD":  ("oil",    "pct"),
    "CADUSD": ("cadusd", "pct"),
    "CA10Y":  ("rates",  "diff"),
}

# The market factor is built from our own universe rather than an index, because
# ^GSPTSX returns HTTP 402 on the current FMP plan (which is also why
# relative_strength_vs_tsx_3m and tsx_return_3m are 100% NULL). An equal-weighted
# index of the 594 tickers we already hold is free, and is arguably the better
# benchmark here anyway: the TSX Composite is dominated by bank and energy
# mega-caps, which is not the universe this system actually picks from.
MARKET_FACTOR_NAME = "market"


# ── Date helpers ───────────────────────────────────────────────────────────────

def last_day_of_month(year: int, month: int) -> datetime.date:
    if month == 12:
        return datetime.date(year, 12, 31)
    return datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)


def generate_snapshot_dates(start: datetime.date, end: datetime.date,
                            as_of: datetime.date | None = None) -> list[datetime.date]:
    """
    Month-end dates from start to end, plus an optional off-cycle date.

    `as_of` exists for scoring between month-ends — you want this week's ranking on
    a Thursday, not to wait for the 31st. The resulting row is a normal snapshot:
    every feature is a trailing window ending on that date, so it is point-in-time
    correct like any other.

    Two caveats worth knowing. It breaks the regular monthly spacing the overlap
    and embargo arithmetic assumes, which is immaterial for one extra row among 90
    but would matter if you added them weekly. And because PIT gating treats a
    filing as available 45 days after period end, a mid-October snapshot sees the
    same fundamentals as September's — what actually differs is everything
    price-derived: momentum, RSI, volatility, betas, market cap and the dynamic
    valuation ratios.
    """
    dates = []
    year, month = start.year, start.month
    while True:
        d = last_day_of_month(year, month)
        if d > end:
            break
        if d >= start:
            dates.append(d)
        month += 1
        if month > 12:
            month = 1
            year += 1
    if as_of is not None and as_of not in dates:
        dates.append(as_of)
        dates.sort()
    return dates


# ── Math helpers ───────────────────────────────────────────────────────────────

def safe_div(num, denom):
    if num is None or denom is None:
        return None
    try:
        d = float(denom)
        return float(num) / d if d != 0 else None
    except (TypeError, ValueError):
        return None


# Bounds beyond which a change in share count is a corporate action rather than
# capital allocation.
#
# FMP's prices are retroactively split-adjusted (verified on SHOP.TO's 10:1) but its
# reported share counts are NOT, so every split shows up here as a phantom buyback or
# phantom issuance. IPO.TO went 90,119,356 -> 15,019,892 shares between 2024-09 and
# 2024-12 — an exact 6:1 reverse split, recorded as an 83% "buyback". It then placed
# in the top 10 of a live pick list, with shares_change_1yr as its largest positive
# contributor.
#
# A genuine buyback of more than half the float in a year does not happen, so the
# floor catches reverse splits with near-certainty. The ceiling is the harder call: a
# doubling can be a real financing (a strong negative signal worth keeping) or a 2:1
# forward split (noise), and nothing in the data separates them. Both become NaN,
# which loses some real dilution signal — accepted because a silently wrong value is
# worse than a missing one, and LightGBM treats NaN as information rather than a hole.
SHARE_CHANGE_FLOOR      = -0.50   # 1yr: a >50% share reduction is a reverse split
SHARE_CHANGE_FLOOR_3YR  = -0.70   # 3yr: allow a larger genuine cumulative buyback
SHARE_CHANGE_CEILING    =  1.00   # a doubling is ambiguous — split or financing


def plausible_share_change(val, floor: float = SHARE_CHANGE_FLOOR,
                           ceiling: float = SHARE_CHANGE_CEILING):
    """Return val, or None when it is outside the range a real buyback/issue can reach."""
    if val is None:
        return None
    return None if (val < floor or val > ceiling) else val


def pct_change(new_val, old_val):
    if new_val is None or old_val is None:
        return None
    try:
        o = float(old_val)
        return (float(new_val) - o) / abs(o) if o != 0 else None
    except (TypeError, ValueError):
        return None


def compute_rsi(closes: pd.Series, period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    delta = closes.diff()
    gain  = delta.clip(lower=0).ewm(alpha=1 / period, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(alpha=1 / period, adjust=False).mean()
    rs    = gain / loss.replace(0, np.nan)
    rsi   = 100 - (100 / (1 + rs))
    val   = rsi.iloc[-1]
    return float(val) if not math.isnan(val) else None


def nearest_before(df: pd.DataFrame, date_col: str, cutoff: datetime.date) -> pd.Series | None:
    """Return the most recent row where date_col <= cutoff, or None."""
    if df.empty:
        return None
    col = df[date_col]
    # Normalize datetime64 columns to date objects for consistent comparison
    if pd.api.types.is_datetime64_any_dtype(col):
        mask = col <= pd.Timestamp(cutoff)
    else:
        mask = col <= cutoff
    if not mask.any():
        return None
    return df[mask].iloc[-1]


def safe_int(val) -> int | None:
    """Convert to int, returning None if val is None or NaN."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if math.isnan(f) else int(f)
    except (TypeError, ValueError):
        return None


def spans_about_a_year(window: pd.DataFrame) -> bool:
    """
    True if these 4 quarters actually cover ~12 months. Guards against a missing
    quarter: .tail(4) would silently reach back a 5th quarter's worth of calendar
    time, so the TTM sum would cover 9 months of activity while being treated as
    a full year — understating revenue/earnings by ~25% with no error raised.
    """
    if len(window) < 4:
        return False
    span = (window["fiscal_date"].iloc[-1] - window["fiscal_date"].iloc[0]).days
    return 240 <= span <= 430


def compute_ttm(fund_quarterly: pd.DataFrame, snapshot_date: datetime.date,
                fx_to_cad: float = 1.0) -> dict | None:
    """
    Sum the 4 most recent quarters filed on or before snapshot_date.
    Returns dict of TTM aggregates, or None if fewer than 4 quarters available.
    Also returns prior-year TTM (quarters 5-8) for YoY growth.

    fx_to_cad multiplies every monetary field so the result is always in CAD,
    regardless of the company's reporting currency. ~38% of this universe reports
    in USD while trading in CAD, and mixing the two silently corrupts every
    valuation ratio and makes market caps incomparable. Share counts and margins
    are left alone — they aren't currency-denominated.
    """
    if fund_quarterly.empty:
        return None

    avail = fund_quarterly[fund_quarterly["filed_at"] <= snapshot_date].tail(8)
    if len(avail) < 4:
        return None

    curr4 = avail.tail(4)
    if not spans_about_a_year(curr4):
        return None

    prev4 = avail.head(4) if len(avail) >= 8 else None
    if prev4 is not None and not spans_about_a_year(prev4):
        prev4 = None

    def ttm_sum(col):
        vals = curr4[col].dropna()
        return float(vals.sum()) * fx_to_cad if len(vals) == 4 else None

    def prev_sum(col):
        if prev4 is None:
            return None
        vals = prev4[col].dropna()
        return float(vals.sum()) * fx_to_cad if len(vals) == 4 else None

    def latest(col):
        """Most recent value of a balance-sheet item, converted to CAD."""
        if col not in curr4.columns:
            return None
        vals = curr4[col].dropna()
        return float(vals.iloc[-1]) * fx_to_cad if len(vals) else None

    ttm = {
        "revenue":          ttm_sum("revenue"),
        "gross_profit":     ttm_sum("gross_profit"),
        "net_income":       ttm_sum("net_income"),
        "ebitda":           ttm_sum("ebitda"),
        "operating_income": ttm_sum("operating_income"),
        "operating_cf":     ttm_sum("operating_cash_flow"),
        "fcf":              ttm_sum("free_cash_flow"),
        # Share count is not currency-denominated — never FX-converted.
        "shares":           float(curr4["shares_outstanding"].dropna().iloc[-1]) if curr4["shares_outstanding"].dropna().any() else None,
        "total_assets":     latest("total_assets"),
        "total_debt":       latest("total_debt"),
        "cash":             latest("cash_and_equivalents"),
        "total_equity":     latest("total_equity"),
        # Balance-sheet levels, taken from the latest quarter rather than summed.
        # These back the quarterly-refreshing twins of FMP's current_ratio and
        # net_debt_to_ebitda, which are otherwise stuck at ~2 updates a year.
        "current_assets":      latest("current_assets"),
        "current_liabilities": latest("current_liabilities"),
        # Prior year TTM for growth comparisons
        "prev_revenue":     prev_sum("revenue"),
        "prev_net_income":  prev_sum("net_income"),
        # Per-quarter gross margins for trend
        "q_gross_margins":  [safe_div(r["gross_profit"], r["revenue"]) for _, r in curr4.iterrows()],
    }
    return ttm


def price_on_or_before(prices: pd.DataFrame, cutoff: datetime.date) -> float | None:
    """Return the closing price on or before cutoff."""
    row = nearest_before(prices, "date", cutoff)
    return float(row["close"]) if row is not None else None


def price_n_days_before(prices: pd.DataFrame, cutoff: datetime.date, n: int) -> float | None:
    """Return close approximately n calendar days before cutoff."""
    target = cutoff - datetime.timedelta(days=n)
    row = nearest_before(prices, "date", target)
    return float(row["close"]) if row is not None else None


# ── Data fetchers (per ticker, load everything into memory once) ───────────────

def fetch_prices(cur, ticker: str) -> pd.DataFrame:
    cur.execute("""
        SELECT date, open, high, low, close, volume
        FROM price_history
        WHERE ticker = %s
        ORDER BY date ASC
    """, (ticker,))
    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([dict(r) for r in rows])
    # Ensure date is Python date objects (not datetime64) for consistent comparison
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(object)
    return df


def fetch_fundamentals(cur, ticker: str, period: str) -> pd.DataFrame:
    """Join income / balance / cashflow / key_metrics for one period type."""
    cur.execute("""
        SELECT
            i.fiscal_date, i.filed_at,
            i.revenue, i.gross_profit, i.operating_income, i.net_income,
            i.ebitda, i.shares_outstanding,
            b.total_assets, b.total_debt, b.total_equity, b.cash_and_equivalents, b.net_debt,
            b.current_assets, b.current_liabilities,
            c.operating_cash_flow, c.free_cash_flow,
            km.roa, km.roe, km.roic,
            km.ev_to_ebitda, km.pe_ratio, km.price_to_sales,
            km.price_to_book, km.price_to_fcf, km.fcf_yield, km.earnings_yield,
            km.net_debt_to_ebitda, km.current_ratio, km.interest_coverage,
            km.market_cap, km.enterprise_value
        FROM income_statements i
        LEFT JOIN balance_sheets b
            ON i.ticker = b.ticker AND i.period = b.period AND i.fiscal_date = b.fiscal_date
        LEFT JOIN cash_flows c
            ON i.ticker = c.ticker AND i.period = c.period AND i.fiscal_date = c.fiscal_date
        LEFT JOIN key_metrics km
            ON i.ticker = km.ticker AND i.period = km.period AND i.fiscal_date = km.fiscal_date
        WHERE i.ticker = %s AND i.period = %s
        ORDER BY i.fiscal_date ASC
    """, (ticker, period))
    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([dict(r) for r in rows])
    df["fiscal_date"] = pd.to_datetime(df["fiscal_date"]).dt.date
    df["filed_at"]    = pd.to_datetime(df["filed_at"]).dt.date
    return df


def fetch_financial_scores(cur, ticker: str) -> pd.DataFrame:
    cur.execute("""
        SELECT fiscal_date, filed_at, piotroski_score, altman_z_score
        FROM financial_scores
        WHERE ticker = %s
        ORDER BY fiscal_date ASC
    """, (ticker,))
    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([dict(r) for r in rows])
    df["fiscal_date"] = pd.to_datetime(df["fiscal_date"]).dt.date
    df["filed_at"]    = pd.to_datetime(df["filed_at"]).dt.date
    # filed_at is NULL for rows inserted by fmp_pipeline (FMP snapshot has no filing date).
    # Fall back to fiscal_date + 90 days so the lookup works.
    mask = df["filed_at"].isna()
    df.loc[mask, "filed_at"] = df.loc[mask, "fiscal_date"].apply(
        lambda d: d + datetime.timedelta(days=90)
    )
    return df


def fetch_analyst_estimates(cur, ticker: str) -> pd.DataFrame:
    cur.execute("""
        SELECT fiscal_date, period, retrieved_at,
               estimated_eps_avg, estimated_revenue_avg, num_analysts_eps
        FROM analyst_estimates
        WHERE ticker = %s
        ORDER BY fiscal_date ASC, retrieved_at ASC
    """, (ticker,))
    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([dict(r) for r in rows])
    df["fiscal_date"] = pd.to_datetime(df["fiscal_date"]).dt.date
    return df


def fetch_earnings_surprises(cur, ticker: str) -> pd.DataFrame:
    cur.execute("""
        SELECT fiscal_date, actual_eps, estimated_eps, surprise_eps_pct
        FROM earnings_surprises
        WHERE ticker = %s
        ORDER BY fiscal_date ASC
    """, (ticker,))
    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([dict(r) for r in rows])
    df["fiscal_date"] = pd.to_datetime(df["fiscal_date"]).dt.date
    return df


def fetch_upgrades_downgrades(cur, ticker: str) -> pd.DataFrame:
    cur.execute("""
        SELECT published_date, action, to_price_target
        FROM upgrades_downgrades
        WHERE ticker = %s
        ORDER BY published_date ASC
    """, (ticker,))
    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([dict(r) for r in rows])
    df["published_date"] = pd.to_datetime(df["published_date"]).dt.date
    return df


def fetch_price_targets(cur, ticker: str) -> pd.DataFrame:
    cur.execute("""
        SELECT published_date, price_target
        FROM price_targets
        WHERE ticker = %s
        ORDER BY published_date ASC
    """, (ticker,))
    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([dict(r) for r in rows])
    df["published_date"] = pd.to_datetime(df["published_date"]).dt.date
    return df


def fetch_news(cur, ticker: str) -> pd.DataFrame:
    cur.execute("""
        SELECT published_date::date AS published_date, sentiment_score
        FROM news_articles
        WHERE ticker = %s
        ORDER BY published_date ASC
    """, (ticker,))
    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([dict(r) for r in rows])
    df["published_date"] = pd.to_datetime(df["published_date"]).dt.date
    return df


def fetch_transcripts(cur, ticker: str) -> pd.DataFrame:
    cur.execute("""
        SELECT transcript_date, claude_sentiment_score
        FROM earnings_call_transcripts
        WHERE ticker = %s AND transcript_date IS NOT NULL
        ORDER BY transcript_date ASC
    """, (ticker,))
    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([dict(r) for r in rows])
    df["transcript_date"] = pd.to_datetime(df["transcript_date"]).dt.date
    return df


def fetch_institutional(cur, ticker: str) -> pd.DataFrame:
    cur.execute("""
        SELECT filing_date, pct_outstanding, change_in_shares
        FROM institutional_ownership
        WHERE ticker = %s
        ORDER BY filing_date ASC
    """, (ticker,))
    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([dict(r) for r in rows])
    df["filing_date"] = pd.to_datetime(df["filing_date"]).dt.date
    return df


def fetch_all_macro(cur) -> dict[str, pd.DataFrame]:
    """Load all macro series into memory as {series: DataFrame}."""
    cur.execute("SELECT DISTINCT series FROM macro_data")
    series_list = [r["series"] for r in cur.fetchall()]
    result = {}
    for series in series_list:
        cur.execute("""
            SELECT date, value FROM macro_data
            WHERE series = %s ORDER BY date ASC
        """, (series,))
        rows = cur.fetchall()
        if rows:
            df = pd.DataFrame([dict(r) for r in rows])
            df["date"] = pd.to_datetime(df["date"]).dt.date
            result[series] = df
    return result


def fetch_market_returns(cur) -> pd.DataFrame:
    """
    Build an equal-weighted daily return series for the whole universe — our stand-in
    for a market index, since ^GSPTSX is paywalled.

    Equal- rather than cap-weighted on purpose: a cap-weighted Canadian index is
    roughly a bet on five banks and two railways, so beta against it mostly measures
    "do you look like a mega-cap". Equal weighting makes the benchmark the average
    stock, which is the thing this universe is actually picked from.

    Only tickers that were trading on a given date contribute to that date, so a
    company delisted in 2022 is in the index up to 2022 and absent afterwards —
    no survivorship distortion inside the series itself.

    The |return| < 0.5 filter keeps corrupt prices out of the benchmark. ACT.TO
    alone swings 25x between consecutive days; without the guard a handful of bad
    tickers would move the index every stock is then measured against.
    """
    cur.execute("""
        WITH r AS (
            SELECT date,
                   close / LAG(close) OVER (PARTITION BY ticker ORDER BY date) - 1 AS ret
            FROM price_history
            WHERE close > 0
        )
        SELECT date, AVG(ret) AS mkt_ret, COUNT(*) AS n
        FROM r
        WHERE ret IS NOT NULL AND ret > -0.5 AND ret < 0.5
        GROUP BY date
        HAVING COUNT(*) >= 20
        ORDER BY date ASC
    """)
    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([dict(r) for r in rows])
    df["date"] = pd.to_datetime(df["date"])
    return df[["date", "mkt_ret"]].set_index("date")["mkt_ret"].astype(float).to_frame()


def _weekly_returns(dates, values, how: str) -> pd.Series:
    """
    Resample a daily series to weekly (Friday-ended) and difference it.

    how='pct'  -> percentage change, for prices and FX
    how='diff' -> absolute change, for yields already quoted in percent
    """
    s = pd.Series(
        pd.to_numeric(pd.Series(values), errors="coerce").values,
        index=pd.to_datetime(pd.Series(dates)),
    ).sort_index()
    weekly = s.resample("W-FRI").last()
    return weekly.pct_change() if how == "pct" else weekly.diff()


def compute_factor_betas(prices: pd.DataFrame, macro: dict,
                         market: pd.DataFrame) -> pd.DataFrame:
    """
    Rolling factor betas for one ticker, one row per week.

    Returned frame is indexed by week-ending date with columns
    <factor>_beta / <factor>_r2 per factor, plus total_vol and idio_vol. The caller
    looks up the most recent row on or before each snapshot date, which keeps the
    whole thing point-in-time by construction: a trailing window ending at or before
    the snapshot can only contain data that already existed.

    beta = cov(stock, factor) / var(factor), computed with pandas' rolling pairwise
    covariance rather than a regression per window — one vectorised pass per factor
    instead of ~93 separate fits per ticker.
    """
    if prices.empty or len(prices) < 60:
        return pd.DataFrame()

    stock = _weekly_returns(prices["date"], prices["close"], "pct")
    cols = {}

    factors = {}
    for series, (name, how) in FACTOR_SPECS.items():
        df = macro.get(series)
        if df is not None and not df.empty:
            factors[name] = _weekly_returns(df["date"], df["value"], how)
    if market is not None and not market.empty:
        factors[MARKET_FACTOR_NAME] = (
            market["mkt_ret"].resample("W-FRI").apply(lambda x: (1 + x).prod() - 1)
        )

    for name, fac in factors.items():
        # sort=True explicitly: the stock and factor indexes rarely match exactly
        # (holidays differ between the TSX and the commodity markets), so concat
        # takes their union. That union must stay in chronological order or every
        # rolling window below is computed over shuffled time. Current pandas sorts
        # by default but warns that it will stop, so this pins the behaviour we need.
        joined = pd.concat(
            [stock.rename("s"), fac.rename("f")], axis=1, sort=True
        ).dropna()
        if len(joined) < BETA_MIN_WEEKS:
            continue
        roll_s = joined["s"].rolling(BETA_WINDOW_WEEKS, min_periods=BETA_MIN_WEEKS)
        cov = roll_s.cov(joined["f"])
        var = joined["f"].rolling(BETA_WINDOW_WEEKS, min_periods=BETA_MIN_WEEKS).var()
        corr = roll_s.corr(joined["f"])
        # var == 0 would mean a factor that never moved over the window; guard so it
        # yields NaN rather than inf.
        cols[f"{name}_beta"] = (cov / var.replace(0, np.nan))
        cols[f"{name}_r2"]   = corr ** 2

    if not cols:
        return pd.DataFrame()

    out = pd.DataFrame(cols)

    # Annualised total volatility of the stock itself. Low-volatility names have
    # historically outperformed on a risk-adjusted basis, and it costs one more
    # rolling pass over a series already built.
    out["total_vol"] = (
        stock.rolling(BETA_WINDOW_WEEKS, min_periods=BETA_MIN_WEEKS).std() * np.sqrt(52)
    )

    # Idiosyncratic volatility — the part of the stock's movement the market does
    # not explain, which is where company-specific information shows up. Taken from
    # the closed form sd_residual = sd_stock * sqrt(1 - r2) rather than by building
    # residuals, which avoids a second regression pass.
    mkt_r2 = out.get(f"{MARKET_FACTOR_NAME}_r2")
    if mkt_r2 is not None:
        out["idio_vol"] = out["total_vol"] * np.sqrt((1 - mkt_r2).clip(lower=0))

    # Macro series land a few weeks behind price data (macro_data currently ends
    # 2026-09-10 against prices to 2026-09-30), so the most recent weeks would carry
    # a market beta but no commodity betas — blanking them on exactly the snapshots
    # used for live predictions. A 104-week rolling beta moves negligibly over two
    # months, so carrying the last known value forward is safe. ffill propagates
    # past values forward only, so it cannot introduce look-ahead.
    out = out.ffill(limit=8)

    out = out.dropna(how="all")
    if out.empty:
        return pd.DataFrame()
    out = out.reset_index().rename(columns={"index": "date"})
    out["date"] = pd.to_datetime(out["date"]).dt.date.astype(object)
    return out


# ── Feature computation ────────────────────────────────────────────────────────

def macro_change_3m(macro: dict, series: str, cutoff: datetime.date) -> float | None:
    df = macro.get(series)
    if df is None or df.empty:
        return None
    curr = nearest_before(df, "date", cutoff)
    prior_date = cutoff - datetime.timedelta(days=91)
    prior = nearest_before(df, "date", prior_date)
    if curr is None or prior is None:
        return None
    return pct_change(curr["value"], prior["value"])


def macro_value(macro: dict, series: str, cutoff: datetime.date) -> float | None:
    df = macro.get(series)
    if df is None or df.empty:
        return None
    row = nearest_before(df, "date", cutoff)
    return float(row["value"]) if row is not None else None


def fx_multiplier_to_cad(reported_currency: str | None, macro: dict,
                         snapshot_date: datetime.date) -> float | None:
    """
    Multiplier converting a company's reporting currency into CAD, using the rate
    as of snapshot_date (PIT-correct — that rate was knowable then).

    CADUSD is quoted as CAD->USD (1 CAD ~ 0.70 USD), so USD->CAD is its reciprocal.
    Returns None if the rate is unavailable or the currency isn't one we can
    convert, so the caller can skip rather than emit a silently wrong number.
    """
    if reported_currency == "CAD":
        return 1.0
    if reported_currency == "USD":
        rate = macro_value(macro, "CADUSD", snapshot_date)
        if rate and rate > 0:
            return 1.0 / rate
        return None
    return None


def compute_snapshot(
    snapshot_date: datetime.date,
    ticker: str,
    sector: str,
    reported_currency: str | None,
    prices: pd.DataFrame,
    fund_annual: pd.DataFrame,
    fund_quarterly: pd.DataFrame,
    scores: pd.DataFrame,
    estimates: pd.DataFrame,
    surprises: pd.DataFrame,
    upgrades: pd.DataFrame,
    price_targets: pd.DataFrame,
    news: pd.DataFrame,
    transcripts: pd.DataFrame,
    institutional: pd.DataFrame,
    macro: dict,
    betas: pd.DataFrame,
) -> dict | None:
    """
    Compute all features for one ticker on one snapshot_date.
    Returns a dict of column: value, or None if insufficient data.
    """
    f = {"ticker": ticker, "snapshot_date": snapshot_date, "sector": sector}

    # ── Price features ─────────────────────────────────────────────────────────

    if prices.empty:
        return None

    prices_to_date = prices[prices["date"] <= snapshot_date]
    if len(prices_to_date) < 30:
        return None

    price_now = float(prices_to_date["close"].iloc[-1])

    # Momentum
    p1m  = price_n_days_before(prices_to_date, snapshot_date, 21)
    p3m  = price_n_days_before(prices_to_date, snapshot_date, 91)
    p12m = price_n_days_before(prices_to_date, snapshot_date, 365)

    f["return_1m"]      = pct_change(price_now, p1m)
    f["return_3m"]      = pct_change(price_now, p3m)
    f["return_12m_1m"]  = pct_change(p1m, p12m)  # 12-1 month momentum: t-12m to t-1m

    # 52-week high proximity
    high_52w = prices_to_date[prices_to_date["date"] >= snapshot_date - datetime.timedelta(days=365)]["high"].max()
    f["high_52w_proximity"] = safe_div(price_now, high_52w)

    # RSI
    last_60 = prices_to_date["close"].tail(60)
    f["rsi_14"] = compute_rsi(last_60)

    # Volume ratio
    vol_series = prices_to_date["volume"].tail(90)
    recent_vol = float(prices_to_date["volume"].iloc[-1]) if not prices_to_date.empty else None
    avg_vol_90 = float(vol_series.mean()) if len(vol_series) >= 20 else None
    f["volume_ratio_90d"] = safe_div(recent_vol, avg_vol_90)

    # Avg dollar volume (liquidity — used for tier)
    close_vol = (prices_to_date["close"] * prices_to_date["volume"]).tail(90)
    avg_dv_90 = float(close_vol.mean()) if len(close_vol) >= 20 else None
    f["avg_dollar_volume_90d"] = avg_dv_90
    f["tier"] = "main" if (avg_dv_90 and avg_dv_90 >= MAIN_TIER_MIN_VOL) else "early_watch"

    # ── Fundamental features ───────────────────────────────────────────────────

    curr = nearest_before(fund_annual, "filed_at", snapshot_date) if not fund_annual.empty else None

    if curr is not None:
        # Get prior year row for growth calcs
        prior_mask = (fund_annual["filed_at"] <= snapshot_date) & \
                     (fund_annual["fiscal_date"] < curr["fiscal_date"])
        prior_rows = fund_annual[prior_mask]
        prev = prior_rows.iloc[-1] if not prior_rows.empty else None

        # Profitability
        f["roa"]                    = curr.get("roa")
        f["roe"]                    = curr.get("roe")
        f["roic"]                   = curr.get("roic")
        f["gross_margin"]           = safe_div(curr.get("gross_profit"), curr.get("revenue"))
        f["operating_margin"]       = safe_div(curr.get("operating_income"), curr.get("revenue"))
        f["fcf_margin"]             = safe_div(curr.get("free_cash_flow"), curr.get("revenue"))
        f["operating_cf_to_assets"] = safe_div(curr.get("operating_cash_flow"), curr.get("total_assets"))

        # Growth
        f["revenue_growth_yoy"]  = pct_change(curr.get("revenue"),     prev.get("revenue") if prev is not None else None)
        f["earnings_growth_yoy"] = pct_change(curr.get("net_income"),  prev.get("net_income") if prev is not None else None)

        # Leverage / Safety
        f["debt_to_assets"]     = safe_div(curr.get("total_debt"), curr.get("total_assets"))
        f["net_debt_to_ebitda"] = curr.get("net_debt_to_ebitda")
        f["interest_coverage"]  = curr.get("interest_coverage")
        f["current_ratio"]      = curr.get("current_ratio")

        # Valuation
        f["ev_to_ebitda"]   = curr.get("ev_to_ebitda")
        f["pe_ratio"]       = curr.get("pe_ratio")
        f["price_to_sales"] = curr.get("price_to_sales")
        f["price_to_book"]  = curr.get("price_to_book")
        f["price_to_fcf"]   = curr.get("price_to_fcf")
        f["fcf_yield"]      = curr.get("fcf_yield")
        f["earnings_yield"] = curr.get("earnings_yield")

        # market_cap is deliberately NOT taken from the annual row here — FMP's
        # value is in the reporting currency (USD for ~38% of this universe) and
        # is priced as of the fiscal year-end, so it's both incomparable across
        # tickers and up to 15 months stale. It's computed in CAD from the
        # snapshot-date price in the TTM block below instead.

        # Filing lag
        try:
            lag = (curr["filed_at"] - curr["fiscal_date"]).days
            f["filing_lag_days"] = lag if lag > 0 else None
        except Exception:
            f["filing_lag_days"] = None

        # Capital allocation — share count trend
        curr_shares = curr.get("shares_outstanding")
        if prev is not None:
            f["shares_change_1yr"] = plausible_share_change(
                pct_change(curr_shares, prev.get("shares_outstanding")))
        yr3_mask = (fund_annual["filed_at"] <= snapshot_date) & \
                   (fund_annual["fiscal_date"] <= curr["fiscal_date"] - datetime.timedelta(days=900))
        yr3_rows = fund_annual[yr3_mask]
        if not yr3_rows.empty:
            f["shares_change_3yr"] = plausible_share_change(
                pct_change(curr_shares, yr3_rows.iloc[-1].get("shares_outstanding")),
                floor=SHARE_CHANGE_FLOOR_3YR)

    # ── TTM + dynamic valuation + growth quality ──────────────────────────────

    # All monetary TTM fields come back in CAD. If the rate is unavailable (or the
    # currency isn't convertible), fx is None and we skip the TTM block entirely —
    # a missing feature is recoverable, a silently wrong one is not.
    fx = fx_multiplier_to_cad(reported_currency, macro, snapshot_date)
    ttm = compute_ttm(fund_quarterly, snapshot_date, fx) if fx is not None else None
    if ttm:
        # TTM margins
        f["ttm_gross_margin"] = safe_div(ttm["gross_profit"], ttm["revenue"])
        f["ttm_fcf_margin"]   = safe_div(ttm["fcf"], ttm["revenue"])

        # TTM YoY growth (more current than annual filing growth)
        f["ttm_revenue_growth"]  = pct_change(ttm["revenue"],    ttm["prev_revenue"])
        f["ttm_earnings_growth"] = pct_change(ttm["net_income"], ttm["prev_net_income"])

        # FCF conversion quality (how much of net income becomes free cash flow)
        f["fcf_conversion"] = safe_div(ttm["fcf"], ttm["net_income"])

        # TTM profitability / leverage — same ratios as the annual block above,
        # but refreshed quarterly instead of once a year. Currency-invariant
        # (both sides CAD-converted), so these would be correct either way; the
        # point is recency, matching the quarterly Piotroski cadence.
        f["ttm_roa"]                    = safe_div(ttm["net_income"], ttm["total_assets"])
        f["ttm_roe"]                    = safe_div(ttm["net_income"], ttm["total_equity"])
        f["ttm_operating_margin"]       = safe_div(ttm["operating_income"], ttm["revenue"])
        f["ttm_operating_cf_to_assets"] = safe_div(ttm["operating_cf"], ttm["total_assets"])
        f["ttm_debt_to_assets"]         = safe_div(ttm["total_debt"], ttm["total_assets"])

        # Quarterly-refreshing twins of two FMP key_metrics ratios that had none.
        # FMP's versions come off the ANNUAL statement, so they update ~1.97 times
        # per ticker-year; these update ~5, same as the other ttm_* columns.
        #
        # current_ratio is a ratio of two balances at one instant, so unlike the
        # profitability twins there is nothing to sum — the latest quarter IS the
        # TTM value. Its inputs only became available with the Altman backfill.
        f["ttm_current_ratio"] = safe_div(
            ttm["current_assets"], ttm["current_liabilities"]
        )

        # Net debt = gross debt less cash. Both sides are CAD here: latest() and
        # ttm_sum() each apply fx_to_cad, so this does not repeat the mistake that
        # once divided a CAD numerator by a USD denominator.
        if ttm["total_debt"] is not None and ttm["cash"] is not None:
            f["ttm_net_debt_to_ebitda"] = safe_div(
                ttm["total_debt"] - ttm["cash"], ttm["ebitda"]
            )

        # ROIC on invested capital = debt + equity. Approximate (ignores leases
        # and excess cash) but consistent across tickers, which is what matters
        # for a cross-sectional feature.
        if ttm["total_debt"] is not None and ttm["total_equity"] is not None:
            invested_capital = ttm["total_debt"] + ttm["total_equity"]
            f["ttm_roic"] = safe_div(ttm["operating_income"], invested_capital)

        # Gross margin trend — slope across last 4 quarters (positive = expanding)
        gm_series = [x for x in ttm["q_gross_margins"] if x is not None]
        if len(gm_series) >= 3:
            x = list(range(len(gm_series)))
            n = len(x)
            slope = (n * sum(i*v for i,v in zip(x,gm_series)) - sum(x)*sum(gm_series)) / \
                    max(n * sum(i**2 for i in x) - sum(x)**2, 1e-10)
            f["gross_margin_trend"] = slope

        # Dynamic valuation using current price + TTM fundamentals
        if price_now and ttm["shares"] and ttm["shares"] > 0:
            ttm_eps = safe_div(ttm["net_income"], ttm["shares"])
            ttm_rev_per_share = safe_div(ttm["revenue"], ttm["shares"])
            ttm_fcf_per_share = safe_div(ttm["fcf"], ttm["shares"])
            book_per_share = safe_div(ttm["total_equity"], ttm["shares"])

            f["dynamic_pe"]   = safe_div(price_now, ttm_eps)          if ttm_eps and ttm_eps > 0 else None
            f["dynamic_ps"]   = safe_div(price_now, ttm_rev_per_share) if ttm_rev_per_share and ttm_rev_per_share > 0 else None
            f["dynamic_pb"]   = safe_div(price_now, book_per_share)    if book_per_share and book_per_share > 0 else None
            f["dynamic_pfcf"] = safe_div(price_now, ttm_fcf_per_share) if ttm_fcf_per_share and ttm_fcf_per_share > 0 else None

            # Yield form of the two ratios above — earnings/price rather than
            # price/earnings. Deliberately NOT gated on a positive numerator, because
            # inverting the ratio fixes what makes a negative P/E useless: with
            # earnings on top the measure stays monotonic through zero. A company
            # losing a little has a small negative yield and one losing a lot has a
            # large negative yield, which is a real ordering a tree can split on,
            # whereas their P/Es would be -500 and -2 respectively.
            #
            # So these cover the ~30% of rows that dynamic_pe and dynamic_pfcf have
            # to leave NULL, and they replace FMP's fcf_yield / earnings_yield, which
            # were correct in form but priced at the fiscal period end.
            f["dynamic_earnings_yield"] = safe_div(ttm_eps, price_now)
            f["dynamic_fcf_yield"]      = safe_div(ttm_fcf_per_share, price_now)

            # CAD market cap from the snapshot-date price — current and comparable
            # across tickers, unlike FMP's reporting-currency annual figure.
            market_cap = price_now * ttm["shares"]
            f["market_cap"] = market_cap

            # Dynamic EV/EBITDA = (market cap + net debt) / TTM EBITDA.
            # net debt = total debt - cash (most recent quarterly balance sheet).
            # All three terms are CAD now; before the FX fix this added a CAD
            # market cap to USD net debt and divided by USD EBITDA.
            if ttm["total_debt"] is not None:
                cash = ttm.get("cash") or 0
                net_debt = ttm["total_debt"] - cash
                ev = market_cap + net_debt
                f["dynamic_ev_ebitda"] = safe_div(ev, ttm["ebitda"]) if ttm["ebitda"] and ttm["ebitda"] > 0 else None

    # Quarterly growth (QoQ) + acceleration
    if not fund_quarterly.empty:
        avail_q = fund_quarterly[fund_quarterly["filed_at"] <= snapshot_date]
        if len(avail_q) >= 2:
            q_curr = avail_q.iloc[-1]
            q_prev = avail_q.iloc[-2]
            f["revenue_growth_qoq"] = pct_change(q_curr.get("revenue"), q_prev.get("revenue"))

            # Acceleration: compare latest QoQ growth vs prior QoQ growth
            if len(avail_q) >= 3:
                q_prev2 = avail_q.iloc[-3]
                prior_qoq_rev = pct_change(q_prev.get("revenue"), q_prev2.get("revenue"))
                curr_qoq_rev  = f.get("revenue_growth_qoq")
                if curr_qoq_rev is not None and prior_qoq_rev is not None:
                    f["revenue_acceleration"] = curr_qoq_rev - prior_qoq_rev

                prior_qoq_earn = pct_change(q_prev.get("net_income"), q_prev2.get("net_income"))
                curr_qoq_earn  = pct_change(q_curr.get("net_income"), q_prev.get("net_income"))
                if curr_qoq_earn is not None and prior_qoq_earn is not None:
                    f["earnings_acceleration"] = curr_qoq_earn - prior_qoq_earn

    # Piotroski F-Score and Altman Z'-Score — both come off the same
    # financial_scores row (the most recent one filed on or before the snapshot),
    # so one lookup serves both.
    if not scores.empty:
        z_row = nearest_before(scores, "filed_at", snapshot_date)
        if z_row is not None:
            f["piotroski_score"] = safe_int(z_row.get("piotroski_score"))
            f["altman_z_score"]  = z_row.get("altman_z_score")

    # ── Analyst features ───────────────────────────────────────────────────────

    if not estimates.empty:
        # Number of analysts covering this stock
        recent_est = estimates[estimates["fiscal_date"] >= snapshot_date]
        if not recent_est.empty:
            f["num_analysts"] = safe_int(recent_est["num_analysts_eps"].max())

        # EPS revision momentum — compare most recent estimate vs 90 days ago
        now_eps = estimates[estimates["fiscal_date"] >= snapshot_date]
        ago_date = snapshot_date - datetime.timedelta(days=91)

        if not now_eps.empty:
            latest_eps = now_eps["estimated_eps_avg"].iloc[0]
            prior_eps_rows = estimates[
                (estimates["fiscal_date"] >= snapshot_date) &
                (pd.to_datetime(estimates.get("retrieved_at", pd.NaT)).dt.date <= ago_date
                 if "retrieved_at" in estimates.columns else pd.Series(True, index=estimates.index))
            ]
            if not prior_eps_rows.empty:
                f["eps_revision_3m_pct"]     = pct_change(latest_eps, prior_eps_rows["estimated_eps_avg"].iloc[0])
                f["revenue_revision_3m_pct"] = pct_change(
                    now_eps["estimated_revenue_avg"].iloc[0],
                    prior_eps_rows["estimated_revenue_avg"].iloc[0]
                )

    # Upgrades / downgrades in last 90 days
    if not upgrades.empty:
        window_start = snapshot_date - datetime.timedelta(days=90)
        recent_ud = upgrades[
            (upgrades["published_date"] >= window_start) &
            (upgrades["published_date"] <= snapshot_date)
        ]
        f["upgrades_90d"]   = int((recent_ud["action"] == "upgrade").sum())
        f["downgrades_90d"] = int((recent_ud["action"] == "downgrade").sum())

    # Price target revision momentum
    if not price_targets.empty:
        pt_now  = nearest_before(price_targets, "published_date", snapshot_date)
        pt_3m   = nearest_before(price_targets, "published_date", snapshot_date - datetime.timedelta(days=91))
        if pt_now is not None and pt_3m is not None:
            f["price_target_change_3m_pct"] = pct_change(
                pt_now.get("price_target"), pt_3m.get("price_target")
            )

    # ── Earnings quality ───────────────────────────────────────────────────────

    if not surprises.empty:
        past = surprises[surprises["fiscal_date"] <= snapshot_date].tail(8)
        if not past.empty:
            # Latest surprise
            f["latest_eps_surprise_pct"] = float(past.iloc[-1]["surprise_eps_pct"] or 0)
            # Consecutive beats
            beats = 0
            for _, row in past.iloc[::-1].iterrows():
                surp = row.get("surprise_eps_pct") or 0
                if float(surp) > 0:
                    beats += 1
                else:
                    break
            f["consecutive_eps_beats"] = beats

    # Earnings call sentiment
    if not transcripts.empty:
        t_row = nearest_before(transcripts, "transcript_date", snapshot_date)
        if t_row is not None:
            f["earnings_call_sentiment"] = t_row.get("claude_sentiment_score")

    # ── Macro features ─────────────────────────────────────────────────────────

    f["gold_change_3m"]   = macro_change_3m(macro, "GCUSD",   snapshot_date)
    f["oil_change_3m"]    = macro_change_3m(macro, "BZUSD",   snapshot_date)  # Brent proxy for WTI
    f["copper_change_3m"] = macro_change_3m(macro, "HGUSD",   snapshot_date)  # NULL — not on current FMP plan
    f["cadusd_change_3m"] = macro_change_3m(macro, "CADUSD",  snapshot_date)
    f["tsx_return_3m"]    = macro_change_3m(macro, "^GSPTSX", snapshot_date)  # NULL — not on current FMP plan

    # ── Factor betas ───────────────────────────────────────────────────────────
    #
    # The per-company half of the macro story. The *_change_3m values above are
    # identical for every ticker on this date; these say how much THIS company
    # historically moves when that factor moves, so together they let a tree learn
    # "gold rose AND this name is gold-sensitive". Each *_r2 reports how much of the
    # stock's variance the factor actually explains, which distinguishes a real
    # exposure (gold miners run 0.49-0.64) from a spurious one (banks, 0.00-0.03).
    if not betas.empty:
        b_row = nearest_before(betas, "date", snapshot_date)
        if b_row is not None:
            for col in betas.columns:
                if col == "date":
                    continue
                val = b_row.get(col)
                f[col] = None if val is None or pd.isna(val) else float(val)

    # Momentum scaled by the stock's own volatility.
    #
    # A 40% gain means something different for a utility than for a junior miner:
    # for the utility it is many standard deviations of normal movement, for the
    # miner it is a quiet quarter. Dividing by trailing vol asks "how large was
    # this move relative to how much this stock usually moves", which is both the
    # standard construction and more robust than the raw return.
    #
    # Placed after the beta block because total_vol is computed there. Both
    # ingredients already existed — this is arithmetic on them, not new data.
    tv = f.get("total_vol")
    if tv:
        for src, dst in [("return_12m_1m", "return_12m_1m_vol_adj"),
                         ("return_3m",     "return_3m_vol_adj")]:
            f[dst] = safe_div(f.get(src), tv)

    ca10y = macro_value(macro, "CA10Y", snapshot_date)
    ca2y  = macro_value(macro, "CA2Y",  snapshot_date)
    f["yield_curve_spread"] = (ca10y - ca2y) if (ca10y and ca2y) else None

    # Relative strength vs TSX
    tsx_3m = f.get("tsx_return_3m")
    ret_3m = f.get("return_3m")
    f["relative_strength_vs_tsx_3m"] = (
        (ret_3m - tsx_3m) if (ret_3m is not None and tsx_3m is not None) else None
    )

    # ── News / sentiment features ──────────────────────────────────────────────

    if not news.empty:
        d30 = snapshot_date - datetime.timedelta(days=30)
        d90 = snapshot_date - datetime.timedelta(days=90)
        news_30 = news[(news["published_date"] >= d30) & (news["published_date"] <= snapshot_date)]
        news_90 = news[(news["published_date"] >= d90) & (news["published_date"] <= snapshot_date)]

        f["news_count_30d"] = len(news_30)
        f["news_count_90d"] = len(news_90)

        # Acceleration: 30d annualized pace vs 90d annualized pace
        pace_30 = len(news_30) * (365 / 30) if len(news_30) > 0 else 0
        pace_90 = len(news_90) * (365 / 90) if len(news_90) > 0 else 0
        f["news_velocity_accel"] = safe_div(pace_30, pace_90) if pace_90 > 0 else None

        scores_30 = news_30["sentiment_score"].dropna()
        f["news_sentiment_avg_30d"] = float(scores_30.mean()) if len(scores_30) > 0 else None

    # ── Institutional ownership ────────────────────────────────────────────────

    if not institutional.empty:
        inst_row = nearest_before(institutional, "filing_date", snapshot_date)
        if inst_row is not None:
            f["institutional_ownership_pct"]    = inst_row.get("pct_outstanding")
            f["institutional_ownership_change"] = inst_row.get("change_in_shares")

    # ── Forward returns (computed if future prices available) ──────────────────

    today = datetime.date.today()
    for col, trading_days in FORWARD_RETURN_DAYS.items():
        future_date = snapshot_date + datetime.timedelta(days=int(trading_days * 1.4))
        # Only compute if the full forward window has passed — otherwise we'd be
        # using the latest available price as a proxy for the future price.
        if future_date > today:
            continue
        future_price = price_on_or_before(prices, future_date)
        if future_price and future_price > 0 and price_now > 0:
            f[col] = (future_price - price_now) / price_now
        # else leave NULL — will be filled later as history accumulates

    return f


# ── DB upsert ──────────────────────────────────────────────────────────────────

FEATURE_COLS = [
    "ticker", "snapshot_date", "sector", "market_cap", "avg_dollar_volume_90d", "tier",
    "roa", "roe", "roic", "gross_margin", "operating_margin", "fcf_margin", "operating_cf_to_assets",
    "revenue_growth_yoy", "revenue_growth_qoq", "earnings_growth_yoy",
    "debt_to_assets", "net_debt_to_ebitda", "interest_coverage", "current_ratio",
    "piotroski_score", "altman_z_score",
    "gold_beta", "gold_r2", "oil_beta", "oil_r2", "cadusd_beta", "cadusd_r2",
    "rates_beta", "rates_r2", "market_beta", "market_r2",
    "total_vol", "idio_vol",
    "ev_to_ebitda", "pe_ratio", "price_to_sales", "price_to_book", "price_to_fcf", "fcf_yield", "earnings_yield",
    "shares_change_1yr", "shares_change_3yr",
    "num_analysts", "upgrades_90d", "downgrades_90d",
    "eps_revision_3m_pct", "revenue_revision_3m_pct", "price_target_change_3m_pct",
    "consecutive_eps_beats", "latest_eps_surprise_pct", "earnings_call_sentiment", "filing_lag_days",
    "return_12m_1m", "return_3m", "return_1m", "high_52w_proximity", "rsi_14",
    "volume_ratio_90d", "relative_strength_vs_tsx_3m",
    "gold_change_3m", "oil_change_3m", "copper_change_3m", "cadusd_change_3m",
    "yield_curve_spread", "tsx_return_3m",
    "news_count_30d", "news_count_90d", "news_velocity_accel", "news_sentiment_avg_30d",
    "institutional_ownership_pct", "institutional_ownership_change",
    "dynamic_pe", "dynamic_ps", "dynamic_pb", "dynamic_ev_ebitda", "dynamic_pfcf",
    "dynamic_earnings_yield", "dynamic_fcf_yield",
    "ttm_revenue_growth", "ttm_earnings_growth", "ttm_gross_margin", "ttm_fcf_margin",
    "revenue_acceleration", "earnings_acceleration", "gross_margin_trend", "fcf_conversion",
    "ttm_roa", "ttm_roe", "ttm_roic", "ttm_operating_margin",
    "ttm_operating_cf_to_assets", "ttm_debt_to_assets",
    "ttm_current_ratio", "ttm_net_debt_to_ebitda",
    # NOTE: the sector-percentile destinations (*_sect, *_pct) are deliberately
    # NOT listed here. Pass 3 writes them with its own UPDATE; putting them in
    # FEATURE_COLS would make Pass 1's upsert overwrite them with NULL, which
    # only survives today because Pass 3 happens to run afterwards. 7_eda.py
    # reads the table with SELECT *, so they still reach the model.
    "return_12m_1m_vol_adj",
    "return_3m_vol_adj",
    "forward_return_1m", "forward_return_3m", "forward_return_6m", "forward_return_12m",
]

_FORWARD_COLS = {"forward_return_1m", "forward_return_3m", "forward_return_6m", "forward_return_12m"}
UPDATE_COLS = [c for c in FEATURE_COLS if c not in ("ticker", "snapshot_date") and c not in _FORWARD_COLS]


def upsert_snapshots(cur, rows: list[dict]):
    if not rows:
        return

    col_str    = ", ".join(FEATURE_COLS)
    update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in UPDATE_COLS)

    data = [tuple(r.get(c) for c in FEATURE_COLS) for r in rows]

    execute_values(cur, f"""
        INSERT INTO feature_snapshots ({col_str})
        VALUES %s
        ON CONFLICT (ticker, snapshot_date) DO UPDATE SET
            {update_str}
    """, data)


# ── Pass 2: fill forward returns ───────────────────────────────────────────────

def fill_forward_returns(conn):
    """
    For each snapshot row with NULL forward returns, look up future prices and fill.
    Only runs on rows where the forward window has passed.
    """
    print("\n--- Pass 2: Forward returns ---")
    today = datetime.date.today()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT ticker, snapshot_date FROM feature_snapshots
            WHERE forward_return_3m IS NULL
              AND snapshot_date <= %s
            ORDER BY ticker, snapshot_date
        """, (today - datetime.timedelta(days=63),))
        to_fill = cur.fetchall()

    print(f"  {len(to_fill):,} rows to fill")
    updated = 0

    bar = tqdm(to_fill, desc="  Forward returns", unit="row",
               bar_format="  {l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]")

    with conn.cursor() as cur:
        for row in bar:
            ticker        = row["ticker"]
            snapshot_date = row["snapshot_date"]

            # Both lookups are bounded below by MAX_PRICE_STALENESS_DAYS so a dead
            # ticker yields no price rather than its final close — see that constant.
            cur.execute("""
                SELECT close FROM price_history
                WHERE ticker = %s AND date <= %s AND date >= %s
                ORDER BY date DESC LIMIT 1
            """, (ticker, snapshot_date,
                  snapshot_date - datetime.timedelta(days=MAX_PRICE_STALENESS_DAYS)))
            base = cur.fetchone()
            if not base:
                continue
            price_base = float(base["close"])

            updates = {}
            for col, offset in FORWARD_RETURN_OFFSETS.items():
                future = snapshot_date + datetime.timedelta(days=offset)
                if future > today:
                    continue
                cur.execute("""
                    SELECT close FROM price_history
                    WHERE ticker = %s AND date <= %s AND date >= %s
                    ORDER BY date DESC LIMIT 1
                """, (ticker, future,
                      future - datetime.timedelta(days=MAX_PRICE_STALENESS_DAYS)))
                fp = cur.fetchone()
                if fp and price_base > 0:
                    updates[col] = (float(fp["close"]) - price_base) / price_base

            if updates:
                set_clause = ", ".join(f"{c} = %s" for c in updates)
                cur.execute(f"""
                    UPDATE feature_snapshots SET {set_clause}
                    WHERE ticker = %s AND snapshot_date = %s
                """, (*updates.values(), ticker, snapshot_date))
                updated += 1

    conn.commit()
    bar.close()
    print(f"  Updated {updated:,} rows")


# ── Cleanup: drop forward returns that were filled from stale prices ───────────

def clean_stale_forward_returns(conn):
    """
    NULL out forward returns that the unbounded lookup filled from a stale price.

    No refill is needed afterwards: a row that fails this test has no price near
    its target date, so there is genuinely no return to compute — NULL is the
    correct final value, and NULL is what the bounded fill would now produce. That
    makes this a single set-based UPDATE per horizon rather than a row-by-row
    refill of all ~49k labelled rows, which would take hours.

    Rows that pass the test keep the values they already have: the bounded and
    unbounded lookups return the same price whenever a price exists in the window.
    """
    print("\n--- Cleanup: stale forward returns ---")
    with conn.cursor() as cur:
        for col, offset in FORWARD_RETURN_OFFSETS.items():
            cur.execute(f"""
                SELECT COUNT(*) AS n, COUNT(*) FILTER (WHERE {col} = 0) AS zeros
                FROM feature_snapshots fs
                WHERE {col} IS NOT NULL
                  AND (
                    NOT EXISTS (
                        SELECT 1 FROM price_history p
                        WHERE p.ticker = fs.ticker
                          AND p.date <= fs.snapshot_date
                          AND p.date >= fs.snapshot_date - %s
                    )
                    OR NOT EXISTS (
                        SELECT 1 FROM price_history p
                        WHERE p.ticker = fs.ticker
                          AND p.date <= fs.snapshot_date + %s
                          AND p.date >= fs.snapshot_date + %s - %s
                    )
                  )
            """, (MAX_PRICE_STALENESS_DAYS, offset, offset, MAX_PRICE_STALENESS_DAYS))
            r = cur.fetchone()
            n, zeros = r["n"], r["zeros"]

            cur.execute(f"""
                UPDATE feature_snapshots fs
                SET {col} = NULL
                WHERE {col} IS NOT NULL
                  AND (
                    NOT EXISTS (
                        SELECT 1 FROM price_history p
                        WHERE p.ticker = fs.ticker
                          AND p.date <= fs.snapshot_date
                          AND p.date >= fs.snapshot_date - %s
                    )
                    OR NOT EXISTS (
                        SELECT 1 FROM price_history p
                        WHERE p.ticker = fs.ticker
                          AND p.date <= fs.snapshot_date + %s
                          AND p.date >= fs.snapshot_date + %s - %s
                    )
                  )
            """, (MAX_PRICE_STALENESS_DAYS, offset, offset, MAX_PRICE_STALENESS_DAYS))
            print(f"  {col:<20} cleared {cur.rowcount:>6,}  "
                  f"(of which exact zeros: {zeros:,})")
        conn.commit()
    print("  Done. Cleared rows stay NULL — no price exists near their target date.")


# ── Pass 3: sector percentiles ─────────────────────────────────────────────────

# (source column, destination column) for the within-sector percentile pass.
#
# Why this list grew: the walk-forward's Brinson attribution showed 93% of the
# top decile's excess return came from stock SELECTION inside sectors (t=1.64) and
# only 7% from sector allocation (t=0.46). The edge is "best among its peers" —
# yet only four features were measured that way, leaving the other sixty-odd to
# rank a bank's ROE directly against a gold miner's.
#
# altman_z_pct is the proof of concept: raw Altman puts REITs at the 0.33
# percentile purely because asset-heavy businesses score low on working capital
# and asset turnover, while the sector-relative version puts them at 0.49.
#
# Direction differs by metric and the model works it out per column — for
# valuation a LOW percentile is cheap, for quality and momentum a HIGH percentile
# is strong. Naming: the original four kept their `_pct` suffix (renaming would
# break the saved manifest and parquet); new ones use `_sect`.
SECTOR_PERCENTILES = [
    # — valuation (low = cheap relative to peers)
    ("dynamic_ev_ebitda",          "ev_to_ebitda_pct"),
    ("dynamic_pe",                 "pe_ratio_pct"),
    ("dynamic_ps",                 "ps_ratio_pct"),
    ("dynamic_pb",                 "dynamic_pb_sect"),
    ("dynamic_fcf_yield",          "dynamic_fcf_yield_sect"),
    # — quality / safety (high = stronger than peers)
    ("altman_z_score",             "altman_z_pct"),
    ("piotroski_score",            "piotroski_sect"),
    ("ttm_roa",                    "ttm_roa_sect"),
    ("ttm_roic",                   "ttm_roic_sect"),
    ("ttm_operating_margin",       "ttm_operating_margin_sect"),
    ("ttm_operating_cf_to_assets", "ttm_op_cf_to_assets_sect"),
    # — leverage. Most sector-dependent of all: REITs and utilities carry debt by
    #   design, so an absolute debt ratio mostly identifies the industry.
    ("ttm_debt_to_assets",         "ttm_debt_to_assets_sect"),
    ("ttm_net_debt_to_ebitda",     "ttm_net_debt_to_ebitda_sect"),
    # — growth
    ("ttm_revenue_growth",         "ttm_revenue_growth_sect"),
    ("ttm_earnings_growth",        "ttm_earnings_growth_sect"),
    # — momentum. return_12m_1m was the single largest SHAP contributor on five of
    #   the first live pick list's ten names.
    ("return_12m_1m",              "return_12m_1m_sect"),
    ("return_3m",                  "return_3m_sect"),
    # — capital allocation. shares_change_1yr drove 8 of those 10 picks.
    ("shares_change_1yr",          "shares_change_1yr_sect"),
    # — risk. Sector baselines differ enormously; a miner's "normal" vol is a
    #   utility's crisis.
    ("total_vol",                  "total_vol_sect"),
]


def fill_sector_percentiles(conn):
    """
    Compute sector-relative percentile ranks for EV/EBITDA, P/E, P/S.
    Uses PostgreSQL PERCENT_RANK() window function.

    Ranks the dynamic_* ratios, not FMP's copied ones. The copied ratios are
    priced at the annual fiscal year-end (so 3-15 months stale by the time the
    row is usable) and are computed in the reporting currency, which makes them
    incomparable across tickers — exactly the two things a cross-sectional
    percentile must not have. The dynamic ones use the snapshot-date price over
    CAD-normalised TTM fundamentals.
    """
    print("\n--- Pass 3: sector percentiles ---")
    with conn.cursor() as cur:
        for metric, col in SECTOR_PERCENTILES:
            cur.execute(f"""
                UPDATE feature_snapshots fs
                SET {col} = sub.pct_rank
                FROM (
                    SELECT ticker, snapshot_date,
                        PERCENT_RANK() OVER (
                            PARTITION BY snapshot_date, sector
                            ORDER BY {metric} ASC
                        ) AS pct_rank
                    FROM feature_snapshots
                    WHERE {metric} IS NOT NULL AND sector IS NOT NULL
                ) sub
                WHERE fs.ticker        = sub.ticker
                  AND fs.snapshot_date = sub.snapshot_date
            """)
        conn.commit()
    print("  Sector percentiles updated.")


# ── Main ───────────────────────────────────────────────────────────────────────

def assert_universe_classified(conn, allow_unclassified: bool = False):
    """
    Refuse to build features while any ticker has an unpopulated is_adr flag.

    WHY THIS IS A HARD STOP AND NOT A WARNING
    -----------------------------------------
    1_fetch_universe.py adds whatever the exchange lists, which includes Canadian
    Depositary Receipts — CAD-hedged wrappers around Apple, Microsoft, NVIDIA and
    friends. They trade with .TO tickers and look Canadian to every query in this
    file. backfill_ticker_flags.py sets is_adr from FMP's profile endpoint and
    purge_tickers.py removes them, but a NEW ticker arrives with is_adr = NULL, and
    NULL is not TRUE, so every downstream filter waves it through.

    The damage is not limited to the wrappers' own rows. fill_sector_percentiles()
    ranks each stock against its sector, so one megacap in Technology moves the
    percentile of every Canadian tech name in the table. Five modelled features are
    sector-relative. A silent warning scrolls past in a run this long; by the time
    anyone reads it the percentiles are already written.

    Caught in Oct 2026 after 13 CDRs reached tier='main' and were scored by the
    model. --allow-unclassified exists for deliberate exceptions, not convenience.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ticker, name FROM tickers
            WHERE is_adr IS NULL ORDER BY ticker
        """)
        rows = cur.fetchall()
    if not rows:
        return

    names = [f"{r['ticker']} ({r['name']})" if r["name"] else r["ticker"]
             for r in rows]
    print()
    print("  " + "!" * 56)
    print(f"  {len(rows)} ticker(s) have is_adr = NULL and are unclassified.")
    print("  Depositary receipts hiding in here would corrupt every sector")
    print("  percentile, not just their own rows.")
    print()
    for n in names[:15]:
        print(f"    {n}")
    if len(names) > 15:
        print(f"    ... and {len(names) - 15} more")
    print()
    print("  Fix before computing features:")
    print("      python backfill_ticker_flags.py")
    print("      python purge_tickers.py --preset adr-and-funds --dry-run")
    print("      python purge_tickers.py --preset adr-and-funds")
    print("  " + "!" * 56)

    if not allow_unclassified:
        raise SystemExit(
            "  ABORTED - classify the universe first, or pass "
            "--allow-unclassified to override.")
    print("  --allow-unclassified set: continuing with an unclassified universe.")


def assert_no_depositary_receipts(conn, allow_unclassified: bool = False):
    """
    Refuse to build features while any reviewed depositary receipt is stored.

    This is the check that actually bites. The is_adr guard above cannot find
    CDRs, because FMP reports isAdr = False for every one of them: a backfill
    across 751 tickers found zero ADRs in a universe that was 22% CDRs. Apple,
    Nvidia, Nestle and 119 others were being ranked as Toronto-listed companies,
    and because fill_sector_percentiles() ranks within sector, they moved the
    percentile of every Canadian name sharing their sector.

    cdr_exclusions.txt is the reviewed list; cdr_list.py explains why it is a
    file rather than a database flag or a live API lookup.
    """
    from cdr_list import load_cdr_exclusions
    excluded = sorted(load_cdr_exclusions())
    if not excluded:
        print("  NOTE: cdr_exclusions.txt missing or empty - CDRs unchecked.")
        return

    with conn.cursor() as cur:
        cur.execute(
            "SELECT ticker, name FROM tickers WHERE ticker = ANY(%s) "
            "ORDER BY ticker", (excluded,))
        rows = cur.fetchall()
    if not rows:
        print(f"  universe check: clean ({len(excluded)} CDRs on file, "
              "none stored)")
        return

    print()
    print("  " + "!" * 56)
    print(f"  {len(rows)} depositary receipt(s) are still in the database.")
    print("  These are not Canadian companies. Leaving them in corrupts every")
    print("  sector percentile, not only their own rows.")
    print()
    for r in rows[:15]:
        print(f"    {r['ticker']:<12} {(r['name'] or '')[:44]}")
    if len(rows) > 15:
        print(f"    ... and {len(rows) - 15} more")
    print()
    print("  Remove them first:")
    print("      python purge_tickers.py --cdr-list --dry-run")
    print("      python purge_tickers.py --cdr-list")
    print("  " + "!" * 56)

    if not allow_unclassified:
        raise SystemExit(
            "  ABORTED - purge the depositary receipts first, or pass "
            "--allow-unclassified to override.")
    print("  --allow-unclassified set: continuing with CDRs in the universe.")


def run(test_mode: bool = False, returns_only: bool = False, percentiles_only: bool = False,
        clean_stale_returns: bool = False, as_of: datetime.date | None = None,
        allow_unclassified: bool = False, new_only: bool = False):
    print("=" * 60)
    print("  FEATURE COMPUTE")
    print("=" * 60)

    # Applies pending column migrations. Without this, adding a feature column to
    # quant_db.py computes fine in memory and then fails on every single ticker at
    # INSERT time, because CREATE TABLE IF NOT EXISTS never alters a live table.
    init_schema()
    conn = get_conn()

    # Before any work: is the universe actually what we think it is? This runs
    # ahead of the early-return branches too, because --percentiles-only is the
    # single most dangerous mode to run on an unclassified universe.
    assert_universe_classified(conn, allow_unclassified)
    assert_no_depositary_receipts(conn, allow_unclassified)

    try:
        if clean_stale_returns:
            clean_stale_forward_returns(conn)
            return

        if returns_only:
            fill_forward_returns(conn)
            return

        if percentiles_only:
            fill_sector_percentiles(conn)
            return

        with conn.cursor() as cur:
            cur.execute("SELECT ticker, sector, reported_currency FROM tickers ORDER BY ticker")
            all_tickers = [(r["ticker"], r["sector"] or "Unknown", r["reported_currency"])
                           for r in cur.fetchall()]

            tqdm.write("Pre-loading macro data...")
            macro = fetch_all_macro(cur)
            tqdm.write(f"  {len(macro)} macro series loaded")

            # One equal-weighted index for the whole universe, built once and shared
            # by every ticker's market beta. It is the stand-in for ^GSPTSX, which
            # is paywalled on the current FMP plan.
            tqdm.write("Building equal-weighted market index...")
            market_returns = fetch_market_returns(cur)
            tqdm.write(f"  {len(market_returns):,} trading days in the index\n")

        if test_mode:
            all_tickers = all_tickers[:5]
            tqdm.write(f"TEST MODE — {len(all_tickers)} tickers\n")

        snapshot_dates = generate_snapshot_dates(BACKFILL_START, datetime.date.today(),
                                                 as_of=as_of)
        tqdm.write(f"Window: {snapshot_dates[0]} to {snapshot_dates[-1]}  ({len(snapshot_dates)} monthly snapshots)")

        # --new-only: skip (ticker, date) pairs already stored.
        #
        # By default every month-end since 2019 is recomputed on every run:
        # ~93 snapshots x ~570 tickers of work to land one new row each. A
        # finished snapshot cannot change - every feature in it is a trailing
        # window ending on its own date, so once that window's inputs exist the
        # answer is fixed. The one thing that does keep changing is forward
        # returns, and fill_forward_returns() handles those separately.
        #
        # The as-of date is recomputed even when present, because that is the
        # live row and its price-derived features move every day.
        existing = {}
        if new_only:
            with conn.cursor() as cur:
                cur.execute("SELECT ticker, snapshot_date FROM feature_snapshots")
                for r in cur.fetchall():
                    existing.setdefault(r["ticker"], set()).add(r["snapshot_date"])
            tqdm.write(f"--new-only: {sum(len(v) for v in existing.values()):,} "
                       f"existing snapshots will be skipped")
        tqdm.write("")

        total_rows  = 0
        total_nulls = 0

        ticker_bar = tqdm(
            all_tickers, desc="Tickers", unit="ticker",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}"
        )

        for ticker, sector, reported_currency in ticker_bar:
            ticker_bar.set_postfix(ticker=ticker, snapshots="–", non_null="–", refresh=True)

            try:
                with conn.cursor() as cur:
                    prices         = fetch_prices(cur, ticker)
                    fund_annual    = fetch_fundamentals(cur, ticker, "annual")
                    fund_quarterly = fetch_fundamentals(cur, ticker, "quarterly")
                    scores         = fetch_financial_scores(cur, ticker)
                    estimates      = fetch_analyst_estimates(cur, ticker)
                    surprises      = fetch_earnings_surprises(cur, ticker)
                    upgrades       = fetch_upgrades_downgrades(cur, ticker)
                    pt             = fetch_price_targets(cur, ticker)
                    news           = fetch_news(cur, ticker)
                    transcripts    = fetch_transcripts(cur, ticker)
                    institutional  = fetch_institutional(cur, ticker)

                # Computed once per ticker, not once per snapshot: the rolling
                # windows cover every snapshot date in a single vectorised pass.
                betas = compute_factor_betas(prices, macro, market_returns)

                wanted = snapshot_dates
                if new_only:
                    have = existing.get(ticker, set())
                    wanted = [d for d in snapshot_dates
                              if d not in have or d == as_of]
                    if not wanted:
                        ticker_bar.set_postfix(ticker=ticker, snapshots=0,
                                               refresh=True)
                        continue

                rows = []
                for snap_date in wanted:
                    result = compute_snapshot(
                        snap_date, ticker, sector, reported_currency,
                        prices, fund_annual, fund_quarterly, scores,
                        estimates, surprises, upgrades, pt,
                        news, transcripts, institutional, macro, betas
                    )
                    if result:
                        rows.append(result)

                # Count non-null features in latest snapshot for quality reporting
                non_null_pct = 0
                if rows:
                    latest = rows[-1]
                    feat_vals = [v for k, v in latest.items()
                                 if k in FEATURE_COLS and k not in ("ticker","snapshot_date","sector","tier")]
                    non_null_pct = int(100 * sum(1 for v in feat_vals if v is not None) / max(len(feat_vals), 1))

                ticker_bar.set_postfix(
                    ticker=ticker,
                    snapshots=len(rows),
                    non_null=f"{non_null_pct}%",
                    refresh=True
                )

                if test_mode:
                    if rows:
                        r = rows[-1]
                        def fmt(v, dp=2):
                            return f"{v:,.{dp}f}" if isinstance(v, (int, float)) else "N/A"

                        mcap = r.get("market_cap")
                        tqdm.write(
                            f"  {ticker:<12} {len(rows):>3} snaps | "
                            f"{reported_currency or '?':<4} | "
                            f"mcap={fmt(mcap / 1e6) + 'M CAD' if mcap else 'N/A':>14} | "
                            f"dyn_pe={fmt(r.get('dynamic_pe')):>9} | "
                            f"dyn_ev/ebitda={fmt(r.get('dynamic_ev_ebitda')):>9} | "
                            f"ret_3m={fmt(r.get('return_3m'), 3):>7}"
                        )
                    continue

                with conn.cursor() as cur:
                    upsert_snapshots(cur, rows)
                conn.commit()

                total_rows += len(rows)

            except Exception as e:
                conn.rollback()
                tqdm.write(f"  x ERROR {ticker}: {e}")

        ticker_bar.close()

        if not test_mode:
            print(f"\n  Total snapshots upserted: {total_rows:,}")
            fill_forward_returns(conn)
            fill_sector_percentiles(conn)

        print(f"\n{'='*60}")
        print(f"  DONE")
        print(f"{'='*60}")

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test",             action="store_true")
    parser.add_argument("--returns-only",     action="store_true")
    parser.add_argument("--percentiles-only", action="store_true")
    parser.add_argument("--as-of", default=None,
                        help="Also compute an off-cycle snapshot for this date, "
                             "e.g. 2026-10-16, for scoring between month-ends")
    parser.add_argument("--clean-stale-returns", action="store_true",
                        help="NULL forward returns that were filled from a stale "
                             "price (no close within MAX_PRICE_STALENESS_DAYS of "
                             "the target date). Set-based, runs in seconds.")
    parser.add_argument("--new-only", action="store_true",
                        help="Only compute snapshot dates not already stored "
                             "(plus --as-of, which is always refreshed). Turns "
                             "a full rebuild into a weekly top-up.")
    parser.add_argument("--allow-unclassified", action="store_true",
                        help="Proceed even when tickers have is_adr = NULL. "
                             "Unclassified depositary receipts corrupt every "
                             "sector percentile, so this is for deliberate "
                             "exceptions only.")
    args = parser.parse_args()
    run(
        test_mode=args.test,
        returns_only=args.returns_only,
        percentiles_only=args.percentiles_only,
        clean_stale_returns=args.clean_stale_returns,
        as_of=datetime.date.fromisoformat(args.as_of) if args.as_of else None,
        allow_unclassified=args.allow_unclassified,
        new_only=args.new_only,
    )

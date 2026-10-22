"""
factor_compute.py — Compute historical factors from raw financial statement data in Postgres.

Current scope (Phase 0):
    - Historical Piotroski F-Score (quarterly TTM, PIT-correct)
    - Historical Altman Z'-Score  (quarterly TTM, PIT-correct)

Piotroski F-Score: 9-point quality scoring system based on financial statements.
Computed from trailing-twelve-month windows rolled up from discrete quarterly
filings, compared year-over-year against the TTM window 4 quarters earlier (which
cancels seasonality). One row per ticker per quarter in financial_scores.

Why TTM rather than Piotroski's classic annual definition: an annual score is
only knowable ~90 days after fiscal year-end and then stays the most recent
score until the *next* annual lands — so the model would see a quality signal
3 to 15 months stale. Quarterly TTM refreshes it 4x more often, which matters
for a system trying to catch deterioration before the market prices it in.

Altman Z'-Score: distress/bankruptcy predictor. Adds two things nothing else in the
feature set captures — cumulative profitability (retained earnings / assets, which
separates a company that has earned its equity from one that has issued it) and
short-term liquidity structure (working capital / assets).

The *original* 1968 Z-Score uses market value of equity in X4. This uses Altman's
1983 Z' revision, which substitutes book equity, for a specific reason: statements
here are reported in the filer's own currency — USD for ~38% of this universe —
while share prices are CAD. A market-value X4 would divide a CAD market cap by USD
total liabilities, which is precisely the bug that corrupted dynamic_ev_ebitda before
the currency fix. Every Z' term is a ratio of two figures from the same statement in
the same currency, so the score is currency-invariant and needs no FX handling at
all. The market-value dimension isn't lost to the model either — it already receives
market_cap and dynamic_pb as separate features and can combine them itself.

The FMP /financial-scores endpoint only returns a single current snapshot — no date,
no history. Computing it here from raw statements gives us a proper time series,
which is required for point-in-time correct backtesting.

Requires current_assets / current_liabilities / retained_earnings on balance_sheets
(Altman X1 and X2). Those columns were added later than the original FMP pull — run
backfill_balance_sheet_fields.py first or Z' comes out NULL on every row.

Usage:
    python 5_factor_compute.py            # full run, all tickers (upsert only)
    python 5_factor_compute.py --test     # first 10 tickers only
    python 5_factor_compute.py --reset    # wipe the table first, then recompute
"""

import argparse
import os
from datetime import timedelta

import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()


# ── DB connection ──────────────────────────────────────────────────────────────

def get_conn():
    url = os.getenv("QUANT_DB_URL")
    if not url:
        raise RuntimeError("QUANT_DB_URL not set in .env")
    return psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)


# ── Helpers ────────────────────────────────────────────────────────────────────

def safe_div(num, denom):
    """Return num / denom, or None if either is None/zero."""
    if num is None or denom is None or denom == 0:
        return None
    return float(num) / float(denom)


def find_prior_year(periods: list[dict], current_date) -> dict | None:
    """
    Given a sorted list of period dicts (each with a 'fiscal_date' key),
    find the one closest to (current_date - 365 days).

    Tolerance is ±45 days: on quarterly data consecutive periods sit ~91 days
    apart, so a wider window could silently match Q-3 or Q-5 instead of Q-4.
    """
    target = current_date - timedelta(days=365)
    best = None
    best_delta = timedelta(days=46)

    for p in periods:
        delta = abs(p["fiscal_date"] - target)
        if delta < best_delta:
            best_delta = delta
            best = p

    return best


# ── TTM windowing ──────────────────────────────────────────────────────────────

# Summed across the trailing 4 quarters (they accumulate over a period).
# Every one of these must be present in all 4 quarters or the window is dropped —
# Piotroski needs all of them, and a partial sum understates by a quarter.
TTM_FLOW_FIELDS = ["revenue", "gross_profit", "net_income", "operating_cash_flow"]

# Also flows, but only Altman needs them, so they must NOT gate window creation:
# adding operating_income to the list above would discard windows that Piotroski
# could still score perfectly well. Summed when complete, None otherwise.
TTM_OPTIONAL_FLOW_FIELDS = ["operating_income"]

# Taken from the most recent quarter as-is (they're balances at a point in time,
# so summing them would be meaningless).
TTM_POINT_IN_TIME_FIELDS = [
    "total_assets", "total_debt", "shares_outstanding", "current_ratio",
    # Altman Z' inputs.
    "total_liabilities", "total_equity",
    "current_assets", "current_liabilities", "retained_earnings",
]


def build_ttm_windows(quarters: list[dict]) -> list[dict]:
    """
    Roll discrete quarterly rows into trailing-twelve-month windows.

    Each window is labelled by the fiscal_date of its most recent quarter.
    Verified that FMP returns discrete (not year-to-date cumulative) quarters
    for this universe — CNR's four 2025 quarters sum exactly to its annual
    revenue, net income and operating cash flow — so summing is correct.

    Skips any window that doesn't have 4 genuinely consecutive quarters, or
    where a flow field is missing, since a gap would silently produce a TTM
    that understates by a quarter.
    """
    windows = []

    for i in range(3, len(quarters)):
        window = quarters[i - 3 : i + 1]
        latest = window[-1]

        # Guard: the 4 quarters must actually span ~1 year. A real gap (missing
        # quarter) shows up as a span well over 365 days.
        span_days = (latest["fiscal_date"] - window[0]["fiscal_date"]).days
        if not (240 <= span_days <= 430):
            continue

        # Guard: every flow field present in every quarter, or the sum is wrong.
        if any(q.get(f) is None for q in window for f in TTM_FLOW_FIELDS):
            continue

        # Guard: the window needs a real asset base. FMP ships ~155 quarterly rows
        # in this universe with total_assets of 0 or less (disproportionately Q1
        # filings) while still reporting billions in equity — corrupt, not a tiny
        # company. Dropping the window matters because safe_div returns None on a
        # zero denominator and compute_piotroski scores a None signal as 0, so an
        # asset-less window doesn't fail loudly, it quietly returns an F-score up to
        # 4 points too low. Every asset-denominated ratio in both scores (F1/F3/F4/F9
        # and Altman X1/X2/X3/X5) is unusable here, so there's nothing left to score.
        if latest.get("total_assets") is None or float(latest["total_assets"]) <= 0:
            continue

        ttm = {"fiscal_date": latest["fiscal_date"]}
        for f in TTM_FLOW_FIELDS:
            ttm[f] = sum(float(q[f]) for q in window)
        for f in TTM_OPTIONAL_FLOW_FIELDS:
            # all() over a generator is True for an empty window too, but a window
            # is always exactly 4 quarters here, so that case can't arise.
            if all(q.get(f) is not None for q in window):
                ttm[f] = sum(float(q[f]) for q in window)
            else:
                ttm[f] = None
        for f in TTM_POINT_IN_TIME_FIELDS:
            ttm[f] = latest.get(f)

        windows.append(ttm)

    return windows


# ── Piotroski F-Score ──────────────────────────────────────────────────────────

def compute_piotroski(curr: dict, prev: dict) -> tuple[int, dict]:
    """
    Compute Piotroski F-Score (0-9) from a TTM window and the TTM window from
    4 quarters earlier. Period-agnostic — every signal is a ratio, a sign test,
    or a same-currency comparison, so this works on annual or TTM input alike
    (and is currency-invariant, which is why USD reporters score identically).

    curr / prev: dicts with keys:
        revenue, gross_profit, net_income, shares_outstanding  (income)
        total_assets, total_debt                               (balance)
        operating_cash_flow                                    (cashflow)
        current_ratio                                          (key_metrics, may be None)

    Returns (score, signal_dict) where signal_dict has keys f1..f9.
    """
    signals = {}

    # ── Profitability (4 signals) ──────────────────────────────────

    # F1: ROA > 0  (net income / total assets)
    roa_curr = safe_div(curr["net_income"], curr["total_assets"])
    signals["f1"] = 1 if (roa_curr is not None and roa_curr > 0) else 0

    # F2: Operating cash flow > 0
    signals["f2"] = 1 if (curr["operating_cash_flow"] is not None
                           and curr["operating_cash_flow"] > 0) else 0

    # F3: ROA improving year-over-year
    roa_prev = safe_div(prev["net_income"], prev["total_assets"])
    signals["f3"] = 1 if (roa_curr is not None
                           and roa_prev is not None
                           and roa_curr > roa_prev) else 0

    # F4: Accruals quality — operating CF / total assets > ROA
    #     (cash earnings exceed accrual earnings → higher quality)
    cf_roa = safe_div(curr["operating_cash_flow"], curr["total_assets"])
    signals["f4"] = 1 if (cf_roa is not None
                           and roa_curr is not None
                           and cf_roa > roa_curr) else 0

    # ── Leverage / Liquidity (3 signals) ──────────────────────────

    # F5: Leverage decreased  (total_debt / total_assets fell)
    lev_curr = safe_div(curr["total_debt"], curr["total_assets"])
    lev_prev = safe_div(prev["total_debt"], prev["total_assets"])
    if lev_curr is None or lev_prev is None:
        signals["f5"] = 0
    else:
        signals["f5"] = 1 if lev_curr < lev_prev else 0

    # F6: Current ratio improved  (from key_metrics; 0 if unavailable)
    cr_curr = curr.get("current_ratio")
    cr_prev = prev.get("current_ratio")
    if cr_curr is not None and cr_prev is not None:
        signals["f6"] = 1 if cr_curr > cr_prev else 0
    else:
        signals["f6"] = 0

    # F7: No new shares issued (dilution check)
    #     Allow 1% tolerance for rounding / minor option exercises
    sh_curr = curr["shares_outstanding"]
    sh_prev = prev["shares_outstanding"]
    if sh_curr is not None and sh_prev is not None and sh_prev > 0:
        signals["f7"] = 1 if sh_curr <= sh_prev * 1.01 else 0
    else:
        signals["f7"] = 0

    # ── Operating Efficiency (2 signals) ──────────────────────────

    # F8: Gross margin improved
    gm_curr = safe_div(curr["gross_profit"], curr["revenue"])
    gm_prev = safe_div(prev["gross_profit"], prev["revenue"])
    if gm_curr is not None and gm_prev is not None:
        signals["f8"] = 1 if gm_curr > gm_prev else 0
    else:
        signals["f8"] = 0

    # F9: Asset turnover improved  (revenue / total assets)
    at_curr = safe_div(curr["revenue"], curr["total_assets"])
    at_prev = safe_div(prev["revenue"], prev["total_assets"])
    if at_curr is not None and at_prev is not None:
        signals["f9"] = 1 if at_curr > at_prev else 0
    else:
        signals["f9"] = 0

    score = sum(signals.values())
    return score, signals


# ── Altman Z'-Score ────────────────────────────────────────────────────────────

# Altman (1983) Z' coefficients — the book-equity revision. See module docstring
# for why this variant rather than the original market-value Z.
Z_COEFFS = {
    "x1": 0.717,   # working capital / total assets      — liquidity
    "x2": 0.847,   # retained earnings / total assets    — cumulative profitability
    "x3": 3.107,   # EBIT / total assets                 — operating productivity
    "x4": 0.420,   # book equity / total liabilities      — solvency buffer
    "x5": 0.998,   # revenue / total assets              — asset turnover
}

# Z' interpretation bands (lower = more distressed):
#   > 2.90        safe
#   1.23 - 2.90   grey zone
#   < 1.23        distress
Z_DISTRESS_THRESHOLD = 1.23
Z_SAFE_THRESHOLD     = 2.90

# Backstop against corrupt inputs that pass the per-term guards. Real Z' values sit
# roughly in -10..+20 (the "safe" band starts at 2.90); even a debt-free junior with
# a huge equity/liabilities ratio lands in the tens. Anything past this is arithmetic
# on bad data, not a signal — e.g. BAM.TO's 2022-09-30 filing reports total_liabilities
# of $73.12 against $12.86B of assets, giving X4 = 175,847,921 and Z' = 73.9M, which
# overflowed the NUMERIC(10,4) column and aborted that ticker's entire upsert. Set
# generously so it only ever catches corruption, never a real company.
Z_MAX_PLAUSIBLE = 1000.0


def compute_altman(curr: dict) -> tuple[float | None, dict]:
    """
    Compute Altman Z'-Score from a single TTM window. Unlike Piotroski this needs
    no prior-year window — every term is a level or a same-period ratio, not a
    year-over-year comparison.

    Returns (score, terms) where terms holds the five x-ratios for inspection.
    Returns (None, terms) if any term can't be computed: a Z' missing a component
    isn't a smaller Z', it's a different and wrong number, so there's no partial
    credit here. safe_div already returns None on a None or zero denominator,
    which covers the total_assets == 0 and total_liabilities == 0 cases.

    curr: a TTM window dict with keys
        revenue, operating_income                              (income, TTM sums)
        total_assets, total_liabilities, total_equity,
        current_assets, current_liabilities, retained_earnings (balance, point-in-time)
    """
    assets = curr.get("total_assets")
    liabilities = curr.get("total_liabilities")

    # safe_div only rejects a denominator of exactly zero, so negatives have to be
    # screened here. Both are structurally impossible on a real balance sheet and
    # both occur in this data: 76 quarterly rows report assets <= 0 and 16 report
    # negative total_liabilities. A negative denominator is the dangerous case —
    # it doesn't overflow or error, it silently flips the sign of the ratio and
    # produces a plausible-looking score that is simply wrong.
    if assets is not None and float(assets) <= 0:
        assets = None
    if liabilities is not None and float(liabilities) <= 0:
        liabilities = None

    # Working capital is a derived level, not a stored field — it needs both
    # current-asset and current-liability sides, so it's None if either is absent.
    ca = curr.get("current_assets")
    cl = curr.get("current_liabilities")
    working_capital = (float(ca) - float(cl)) if (ca is not None and cl is not None) else None

    terms = {
        "x1": safe_div(working_capital, assets),
        "x2": safe_div(curr.get("retained_earnings"), assets),
        "x3": safe_div(curr.get("operating_income"), assets),
        "x4": safe_div(curr.get("total_equity"), liabilities),
        "x5": safe_div(curr.get("revenue"), assets),
    }

    if any(v is None for v in terms.values()):
        return None, terms

    score = sum(Z_COEFFS[k] * terms[k] for k in Z_COEFFS)

    # Final plausibility screen — see Z_MAX_PLAUSIBLE.
    if abs(score) > Z_MAX_PLAUSIBLE:
        return None, terms

    return round(score, 4), terms


# ── Data fetch ─────────────────────────────────────────────────────────────────

def fetch_quarterly_data(cur, ticker: str) -> list[dict]:
    """
    Fetch all quarterly periods for a ticker, joining income / balance / cashflow / key_metrics.
    Returns list of dicts sorted by fiscal_date ascending, ready for TTM rollup.
    """
    cur.execute("""
        SELECT
            i.fiscal_date,
            i.revenue,
            i.gross_profit,
            i.net_income,
            i.operating_income,
            i.shares_outstanding,
            b.total_assets,
            b.total_debt,
            b.total_liabilities,
            b.total_equity,
            b.current_assets,
            b.current_liabilities,
            b.retained_earnings,
            c.operating_cash_flow,
            km.current_ratio
        FROM income_statements i
        JOIN balance_sheets b
            ON  i.ticker      = b.ticker
            AND i.period      = b.period
            AND i.fiscal_date = b.fiscal_date
        JOIN cash_flows c
            ON  i.ticker      = c.ticker
            AND i.period      = c.period
            AND i.fiscal_date = c.fiscal_date
        LEFT JOIN key_metrics km
            ON  i.ticker      = km.ticker
            AND i.period      = km.period
            AND i.fiscal_date = km.fiscal_date
        WHERE i.ticker = %s
          AND i.period = 'quarterly'
        ORDER BY i.fiscal_date ASC
    """, (ticker,))
    return [dict(r) for r in cur.fetchall()]


# ── Main ───────────────────────────────────────────────────────────────────────

def run(test_mode: bool = False, reset: bool = False):
    print("=== Historical Piotroski F-Score + Altman Z' Computation ===\n")

    # Applies pending column migrations — CREATE TABLE IF NOT EXISTS never alters
    # a live table, so without this a new column fails at INSERT time instead.
    from quant_db import init_schema
    init_schema()

    conn = get_conn()
    cur = conn.cursor()

    # Load ticker list
    cur.execute("SELECT ticker FROM tickers ORDER BY ticker")
    tickers = [r["ticker"] for r in cur.fetchall()]

    if test_mode:
        tickers = tickers[:10]
        print(f"TEST MODE — first 10 tickers: {tickers}\n")
    else:
        print(f"Full run: {len(tickers)} tickers\n")

    # A plain upsert can only refresh rows whose (ticker, fiscal_date) the current
    # logic still produces. Rows left behind by earlier runs — a different
    # BACKFILL range, the pre-TTM *annual* Piotroski definition, FMP's
    # /financial-scores snapshot endpoint — survive untouched, and they are not
    # inert: 6_feature_compute picks the nearest row by filed_at <= snapshot_date,
    # so an orphan can win that lookup and feed a stale score of a different
    # definition into the feature set. Measured at 5,666 such rows before this
    # flag existed, some dated 2001.
    #
    # This script is the sole legitimate writer of piotroski_score and
    # altman_z_score, and beneish_m_score has never been populated, so a full
    # delete loses nothing that isn't rebuilt below in the same run. Opt-in
    # rather than automatic, because it is a destructive write.
    if reset and not test_mode:
        cur.execute("DELETE FROM financial_scores")
        print(f"  Deleted {cur.rowcount:,} existing financial_scores row(s) — "
              f"recompute below is the table's only source.\n")
        conn.commit()

    computed = 0
    skipped_no_data = 0
    skipped_no_prior = 0
    total_rows = 0
    altman_rows = 0
    failed = []

    bar = tqdm(tickers, desc="Tickers", unit="ticker",
               bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}")

    for i, ticker in enumerate(bar, 1):
        bar.set_postfix_str(ticker, refresh=False)
        try:
            quarters = fetch_quarterly_data(cur, ticker)
            periods = build_ttm_windows(quarters)

            # Need at least 5 TTM windows: one to score, plus the window 4
            # quarters earlier to compare it against.
            if len(periods) < 5:
                skipped_no_data += 1
                if test_mode:
                    tqdm.write(f"  [{i:3}] {ticker}: skip — {len(quarters)} quarters → "
                               f"only {len(periods)} TTM window(s)")
                continue

            rows_to_upsert = []
            # Counted per ticker, not straight into the global, because a failed
            # upsert rolls the whole ticker back — incrementing the global inside
            # the period loop reported rows that were never committed (which is how
            # the Altman fill rate came out at 100.2%).
            ticker_altman_rows = 0

            for j, curr in enumerate(periods):
                # Altman needs no prior-year window — it's all same-period levels and
                # ratios — so it's computed before the prior-year check and survives
                # periods Piotroski has to skip (notably each ticker's earliest
                # windows, which have nothing 4 quarters back to compare against).
                z_score, _z_terms = compute_altman(curr)

                prior = find_prior_year(periods[:j], curr["fiscal_date"])
                if prior is None:
                    skipped_no_prior += 1
                    if z_score is None:
                        # Nothing to write for this period at all.
                        continue
                    score, signals = None, {}
                else:
                    score, signals = compute_piotroski(curr, prior)

                # FMP's filingDate is unreliable for TSX-only Canadian filers (confirmed
                # live: mirrors fiscal_date for most records, implausible year-plus "lags"
                # for the rest) — see the matching fix in 2_fmp_pipeline.py. We use the
                # regulatory deadline as a conservative floor instead: NI 51-102 requires
                # interim (quarterly) statements within 45 days of period end. The TTM
                # window is only knowable once its most recent quarter is filed, so the
                # lag applies to the window's end date. PIT-safe — never assumes data is
                # available before it could plausibly exist.
                filed_at = curr["fiscal_date"] + timedelta(days=45)

                rows_to_upsert.append((
                    ticker,
                    curr["fiscal_date"],
                    filed_at,
                    score,
                    z_score,
                ))
                if z_score is not None:
                    ticker_altman_rows += 1

                if test_mode:
                    sig_str = " ".join(f"F{k[1]}={v}" for k, v in signals.items())
                    z_str = f"{z_score:>7.2f}" if z_score is not None else "      -"
                    tqdm.write(f"  [{i:3}] {ticker} {curr['fiscal_date']}  "
                               f"F={score if score is not None else '-'}  Z'={z_str}  {sig_str}")

            if rows_to_upsert:
                # Straight EXCLUDED assignment rather than COALESCE: this is a full
                # deterministic recompute from the same source statements, so a NULL
                # here means "not computable from the data", which is the value that
                # should win over whatever a previous run stored.
                execute_values(cur, """
                    INSERT INTO financial_scores
                        (ticker, fiscal_date, filed_at, piotroski_score, altman_z_score)
                    VALUES %s
                    ON CONFLICT (ticker, fiscal_date) DO UPDATE SET
                        piotroski_score = EXCLUDED.piotroski_score,
                        altman_z_score  = EXCLUDED.altman_z_score,
                        filed_at        = EXCLUDED.filed_at
                """, rows_to_upsert)
                conn.commit()
                computed += 1
                total_rows += len(rows_to_upsert)
                altman_rows += ticker_altman_rows

        except Exception as e:
            conn.rollback()
            tqdm.write(f"  ERROR {ticker}: {e}")
            failed.append(ticker)

    bar.close()
    conn.close()

    print(f"\n{'='*50}")
    print(f"Done.")
    print(f"  Tickers with scores computed:  {computed}")
    print(f"  Skipped (< 5 TTM windows):     {skipped_no_data}")
    print(f"  Periods w/o prior yr (F only): {skipped_no_prior}")
    print(f"  Total rows upserted:           {total_rows}")
    print(f"  Rows with an Altman Z':        {altman_rows}"
          + (f"  ({altman_rows/total_rows:.1%})" if total_rows else ""))
    if failed:
        print(f"  Errors: {', '.join(failed)}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Run on first 10 tickers only")
    parser.add_argument("--reset", action="store_true",
                        help="DELETE all financial_scores rows first, so this run is "
                             "the table's only source. Clears orphans from earlier "
                             "runs that a plain upsert can't reach and that "
                             "6_feature_compute would otherwise read as current.")
    args = parser.parse_args()
    run(test_mode=args.test, reset=args.reset)

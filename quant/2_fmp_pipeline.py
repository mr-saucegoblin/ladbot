"""
2_fmp_pipeline.py — Pull FMP data for all covered TSX tickers and store in PostgreSQL.

Run after 1_fetch_universe.py has generated fmp_covered.txt.

Steps per ticker:
  1.  Upsert company profile (sector, industry, name) into tickers table
  2.  Pull 10 years of daily price history to price_history
  3.  Pull annual + quarterly income statements to income_statements
  4.  Pull annual + quarterly balance sheets to balance_sheets
  5.  Pull annual + quarterly cash flows to cash_flows
  6.  Pull annual + quarterly key metrics to key_metrics
  7.  Pull dividend history to dividends
  8.  Pull analyst upgrades/downgrades to upgrades_downgrades
  9.  Pull earnings call transcripts to earnings_call_transcripts
  10. Pull institutional ownership to institutional_ownership
  11. Pull earnings surprises to earnings_surprises

Usage:
    # Test with 5 tickers first to verify field mappings:
    python 2_fmp_pipeline.py --test

    # Full run:
    python 2_fmp_pipeline.py
"""

import argparse
import datetime
import os
import pathlib
import time
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from tqdm import tqdm
from quant_db import get_conn, init_schema

load_dotenv()

FMP_KEY = os.getenv("FMP_API_KEY")
FMP_BASE = "https://financialmodelingprep.com/stable"
DELAY = 0.10        # seconds between requests — stays under 750/min Premium limit
COVERED_FILE = pathlib.Path(__file__).parent / "fmp_covered.txt"
PRICE_HISTORY_START = "2015-01-01"

_timing = {"api_calls": 0, "api_secs": 0.0}


# ── Ticker list ────────────────────────────────────────────────────────────────

def load_covered_tickers(path: str = COVERED_FILE) -> list[str]:
    """
    Read fmp_covered.txt, minus anything on the reviewed depositary-receipt list.

    The filter is here, at READ time, and not only in 1_fetch_universe.py where
    the file is written. On 2026-10-23 that distinction cost a pipeline run: the
    CDRs had been purged from the database, 1_fetch_universe.py had been taught to
    exclude them, but it had not been re-run - so fmp_covered.txt still listed all
    122, this script upserted every one of them straight back into `tickers`, and
    6_feature_compute.py aborted on finding them.

    A list of tickers to process is a thing that goes stale. Filtering where it is
    consumed means a stale file cannot reintroduce what a purge removed, and no
    one has to remember to regenerate it first.
    """
    with open(path) as f:
        tickers = [line.strip() for line in f if line.strip()]

    from cdr_list import load_cdr_exclusions
    excluded = load_cdr_exclusions()
    if excluded:
        keep = [t for t in tickers if t not in excluded]
        if len(keep) != len(tickers):
            print(f"  cdr_exclusions.txt: skipping {len(tickers) - len(keep)} "
                  f"depositary receipts still listed in {pathlib.Path(path).name}")
        return keep
    print("  WARNING: cdr_exclusions.txt missing - depositary receipts NOT filtered")
    return tickers


# ── FMP HTTP helpers ───────────────────────────────────────────────────────────

def _timed_get(url: str, params: dict):
    """HTTP GET with one 429 retry. Accumulates real request time into _timing."""
    _timing["api_calls"] += 1
    t0 = time.perf_counter()
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 429:
            tqdm.write(f"    .. Rate limited — waiting 60s...")
            _timing["api_secs"] += time.perf_counter() - t0
            time.sleep(60)
            t0 = time.perf_counter()
            r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        return r
    finally:
        _timing["api_secs"] += time.perf_counter() - t0


def fmp_get(endpoint: str, symbol: str, params: dict = None) -> list | dict | None:
    """Make one FMP stable API call using ?symbol=TICKER. Returns parsed JSON or None."""
    url = f"{FMP_BASE}/{endpoint}"
    p = {"symbol": symbol, "apikey": FMP_KEY}
    if params:
        p.update(params)
    try:
        return _timed_get(url, p).json()
    except Exception as e:
        tqdm.write(f"    x FMP error [{endpoint}]: {e}")
        return None


def fmp_get_tickers(endpoint: str, ticker: str, params: dict = None) -> list | dict | None:
    """Variant using ?tickers=TICKER instead of ?symbol=TICKER. Required for stock-news."""
    url = f"{FMP_BASE}/{endpoint}"
    p = {"tickers": ticker, "apikey": FMP_KEY}
    if params:
        p.update(params)
    try:
        return _timed_get(url, p).json()
    except Exception as e:
        tqdm.write(f"    x FMP error [{endpoint}]: {e}")
        return None


# ── Profile ────────────────────────────────────────────────────────────────────

def upsert_ticker_profile(cur, ticker: str) -> bool | None:
    """
    Fetch /stable/profile?symbol=TICKER, upsert into tickers table.

    Returns is_adr: True (depositary receipt), False (operating company), or None
    (profile unavailable, so classification is unknown).

    The profile response carries isAdr and isFund and this function used to discard
    them, which is how ~160 tickers ended up with is_adr = NULL. Those nulls defeat
    purge_tickers.py's adr-and-funds preset — NULL is not TRUE — so CAD-hedged
    wrappers around Apple, NVIDIA and Broadcom reached tier='main' and were scored
    by the model as if they were Canadian companies.

    Writing the flags here costs nothing: the call is already being made on every
    ticker, every run. A ticker is now classified the moment it first appears,
    which is strictly better than backfilling it later and hoping someone remembers.
    """
    data = fmp_get("profile", ticker)
    time.sleep(DELAY)

    if not data or not isinstance(data, list) or len(data) == 0:
        # Insert a minimal row so foreign key constraints work for other tables.
        # is_adr stays NULL deliberately: unknown must not masquerade as "not an
        # ADR", and 6_feature_compute.py hard-aborts on any NULL it finds.
        cur.execute("""
            INSERT INTO tickers (ticker)
            VALUES (%s)
            ON CONFLICT (ticker) DO NOTHING
        """, (ticker,))
        return None

    p = data[0]
    is_adr = bool(p.get("isAdr"))
    cur.execute("""
        INSERT INTO tickers (ticker, name, sector, industry, is_adr, is_fund, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (ticker) DO UPDATE SET
            name       = EXCLUDED.name,
            sector     = EXCLUDED.sector,
            industry   = EXCLUDED.industry,
            is_adr     = EXCLUDED.is_adr,
            is_fund    = EXCLUDED.is_fund,
            updated_at = NOW()
    """, (
        ticker,
        p.get("companyName"),
        p.get("sector"),
        p.get("industry"),
        is_adr,
        bool(p.get("isFund")),
    ))
    return is_adr


# ── Price history ──────────────────────────────────────────────────────────────

def load_price_history(cur, ticker: str, from_date: datetime.date = None) -> int:
    """
    Fetch daily OHLCV from /stable/historical-price-eod/full.
    from_date: when set (incremental mode), only pulls records on/after that date.
    Without an explicit from date, FMP silently caps this endpoint at ~5.5 years
    of history — so a full (non-incremental) pull always passes PRICE_HISTORY_START
    to get the real full range.
    Bulk-inserts into price_history. Skips duplicates (ON CONFLICT DO NOTHING).
    """
    params = {"from": (from_date.isoformat() if from_date else PRICE_HISTORY_START)}
    data = fmp_get("historical-price-eod/full", ticker, params=params)
    time.sleep(DELAY)

    # Stable API returns a plain list (v3 used {"historical": [...]})
    if not data or not isinstance(data, list):
        print(f"    No price history for {ticker}")
        return 0

    rows = []
    for item in data:
        rows.append((
            ticker,
            item.get("date"),
            item.get("open"),
            item.get("high"),
            item.get("low"),
            item.get("close"),
            None,   # stable API does not provide adjClose
            item.get("volume"),
        ))

    if not rows:
        return 0

    execute_values(cur, """
        INSERT INTO price_history (ticker, date, open, high, low, close, adj_close, volume)
        VALUES %s
        ON CONFLICT (ticker, date) DO NOTHING
    """, rows)

    return len(rows)


# ── Fundamentals ───────────────────────────────────────────────────────────────

# Maps FMP JSON keys to db column names for each statement type.
# Note: FMP spells it "fillingDate" (their typo) — must match exactly.
FIELD_MAPS = {
    "income-statement": {
        "revenue":               "revenue",
        "grossProfit":           "gross_profit",
        "operatingIncome":       "operating_income",
        "netIncome":             "net_income",
        "ebitda":                "ebitda",
        "eps":                   "eps",
        "weightedAverageShsOut": "shares_outstanding",
    },
    "balance-sheet-statement": {
        "totalAssets":             "total_assets",
        "totalLiabilities":        "total_liabilities",
        "totalStockholdersEquity": "total_equity",
        "cashAndCashEquivalents":  "cash_and_equivalents",
        "totalDebt":               "total_debt",
        "netDebt":                 "net_debt",
        # Altman Z' inputs — see compute_altman() in 5_factor_compute.py.
        "totalCurrentAssets":      "current_assets",
        "totalCurrentLiabilities": "current_liabilities",
        "retainedEarnings":        "retained_earnings",
    },
    "cash-flow-statement": {
        "operatingCashFlow":  "operating_cash_flow",
        "capitalExpenditure": "capex",
        "freeCashFlow":       "free_cash_flow",
        "dividendsPaid":      "dividends_paid",
    },
}

TABLE_NAMES = {
    "income-statement":        "income_statements",
    "balance-sheet-statement": "balance_sheets",
    "cash-flow-statement":     "cash_flows",
}


def load_fundamentals(cur, ticker: str, statement: str, period: str) -> int:
    """
    Pull one statement type (income/balance/cashflow) for one period (annual/quarterly).
    Upserts rows into the appropriate table.

    statement: 'income-statement' | 'balance-sheet-statement' | 'cash-flow-statement'
    period:    'annual' | 'quarter'   (FMP uses 'quarter', not 'quarterly')
    """
    params = {"limit": 20} if period == "annual" else {"period": "quarter", "limit": 40}
    data = fmp_get(statement, ticker, params=params)
    time.sleep(DELAY)

    if not data or not isinstance(data, list):
        return 0

    field_map = FIELD_MAPS[statement]
    table = TABLE_NAMES[statement]
    db_period = "quarterly" if period == "quarter" else "annual"

    columns = list(field_map.values())
    col_str = ", ".join(columns)
    update_pairs = ", ".join(f"{col} = EXCLUDED.{col}" for col in columns)

    # Statements are reported in the company's own currency, often USD even for
    # CAD-traded TSX names — keep tickers.reported_currency current from the
    # response we're already parsing. Only on one statement/period combination
    # so this fires once per ticker per run, not six times.
    if statement == "income-statement" and period == "annual":
        currency = data[0].get("reportedCurrency")
        if currency:
            cur.execute(
                "UPDATE tickers SET reported_currency = %s WHERE ticker = %s",
                (currency, ticker),
            )

    # FMP's filingDate is unreliable for TSX-only Canadian filers — confirmed
    # live against the raw API: it equals fiscal_date (the period-end) for the
    # large majority of records, and shows implausible multi-month-to-year
    # "lags" for the rest (e.g. 549 days) — neither is a real filing date.
    # Use the Canadian securities regulatory deadline as a conservative,
    # always-correct floor instead: NI 51-102 requires interim (quarterly)
    # statements within 45 days, annual within 90 days. This can understate
    # true availability by days/weeks but is never look-ahead-unsafe.
    filing_lag_days = 45 if db_period == "quarterly" else 90

    rows = []
    for item in data:
        fiscal_date_str = item.get("date")

        if not fiscal_date_str:
            continue

        fiscal_date = datetime.date.fromisoformat(fiscal_date_str)
        filed_at = fiscal_date + datetime.timedelta(days=filing_lag_days)

        values = [item.get(fmp_key) for fmp_key in field_map.keys()]
        rows.append((ticker, db_period, fiscal_date, filed_at, *values))

    if not rows:
        return 0

    execute_values(cur, f"""
        INSERT INTO {table} (ticker, period, fiscal_date, filed_at, {col_str})
        VALUES %s
        ON CONFLICT (ticker, period, fiscal_date) DO UPDATE SET
            filed_at = EXCLUDED.filed_at,
            {update_pairs}
    """, rows)

    return len(rows)


# ── Key Metrics ────────────────────────────────────────────────────────────────

def load_key_metrics(cur, ticker: str, period: str) -> int:
    """
    Pull historical key metrics from /stable/key-metrics for one period.
    Covers valuation multiples, yield, profitability and leverage ratios.

    period: 'annual' | 'quarter'
    """
    params = {"limit": 20} if period == "annual" else {"period": "quarter", "limit": 40}
    data = fmp_get("key-metrics", ticker, params=params)
    time.sleep(DELAY)

    if not data or not isinstance(data, list):
        return 0

    db_period = "quarterly" if period == "quarter" else "annual"

    rows = []
    for item in data:
        fiscal_date = item.get("date")
        if not fiscal_date:
            continue

        # FMP stable API uses full names; check aliases from older API versions too.
        ev_to_ebitda = (
            item.get("evToEBITDA")
            or item.get("evToEbitda")
            or item.get("enterpriseValueOverEBITDA")
        )

        rows.append((
            ticker,
            db_period,
            fiscal_date,
            item.get("marketCap"),
            item.get("enterpriseValue"),
            ev_to_ebitda,
            item.get("evToOperatingCashFlow"),
            item.get("evToFreeCashFlow"),
            item.get("earningsYield"),
            item.get("freeCashFlowYield"),
            item.get("returnOnEquity"),
            item.get("returnOnAssets"),
            item.get("returnOnInvestedCapital"),
            item.get("netDebtToEBITDA"),
            item.get("currentRatio"),
        ))

    if not rows:
        return 0

    # Only insert/update the key-metrics fields here.
    # Ratio fields (pe_ratio, price_to_sales, etc.) come from load_ratios()
    # and must not be overwritten with NULL here.
    execute_values(cur, """
        INSERT INTO key_metrics (
            ticker, period, fiscal_date,
            market_cap, enterprise_value,
            ev_to_ebitda, ev_to_operating_cf, ev_to_fcf,
            earnings_yield, fcf_yield,
            roe, roa, roic,
            net_debt_to_ebitda, current_ratio
        )
        VALUES %s
        ON CONFLICT (ticker, period, fiscal_date) DO UPDATE SET
            market_cap         = EXCLUDED.market_cap,
            enterprise_value   = EXCLUDED.enterprise_value,
            ev_to_ebitda       = EXCLUDED.ev_to_ebitda,
            ev_to_operating_cf = EXCLUDED.ev_to_operating_cf,
            ev_to_fcf          = EXCLUDED.ev_to_fcf,
            earnings_yield     = EXCLUDED.earnings_yield,
            fcf_yield          = EXCLUDED.fcf_yield,
            roe                = EXCLUDED.roe,
            roa                = EXCLUDED.roa,
            roic               = EXCLUDED.roic,
            net_debt_to_ebitda = EXCLUDED.net_debt_to_ebitda,
            current_ratio      = EXCLUDED.current_ratio
    """, rows)

    return len(rows)


# ── Ratios ─────────────────────────────────────────────────────────────────────

def load_ratios(cur, ticker: str, period: str) -> int:
    """
    Pull historical valuation and quality ratios from /stable/ratios.
    Upserts only the ratio columns in key_metrics — does not touch EV/yield/ROE fields
    populated by load_key_metrics(), preserving those values.

    period: 'annual' | 'quarter'
    """
    params = {"limit": 20} if period == "annual" else {"period": "quarter", "limit": 40}
    data = fmp_get("ratios", ticker, params=params)
    time.sleep(DELAY)

    if not data or not isinstance(data, list):
        return 0

    db_period = "quarterly" if period == "quarter" else "annual"

    rows = []
    for item in data:
        fiscal_date = item.get("date")
        if not fiscal_date:
            continue

        rows.append((
            ticker,
            db_period,
            fiscal_date,
            item.get("priceToEarningsRatio"),
            item.get("priceToSalesRatio"),
            item.get("priceToBookRatio"),
            item.get("priceToFreeCashFlowRatio"),
            item.get("debtToEquityRatio"),
            item.get("interestCoverageRatio"),
            item.get("dividendPayoutRatio"),
            item.get("dividendYield"),
        ))

    if not rows:
        return 0

    # Insert the row if it doesn't exist, then update only ratio columns.
    # market_cap, ev, roe, etc. will be NULL on insert here but preserved
    # if load_key_metrics() already ran (ON CONFLICT touches only ratio cols).
    execute_values(cur, """
        INSERT INTO key_metrics (
            ticker, period, fiscal_date,
            pe_ratio, price_to_sales, price_to_book, price_to_fcf,
            debt_to_equity, interest_coverage, payout_ratio, dividend_yield
        )
        VALUES %s
        ON CONFLICT (ticker, period, fiscal_date) DO UPDATE SET
            pe_ratio          = EXCLUDED.pe_ratio,
            price_to_sales    = EXCLUDED.price_to_sales,
            price_to_book     = EXCLUDED.price_to_book,
            price_to_fcf      = EXCLUDED.price_to_fcf,
            debt_to_equity    = EXCLUDED.debt_to_equity,
            interest_coverage = EXCLUDED.interest_coverage,
            payout_ratio      = EXCLUDED.payout_ratio,
            dividend_yield    = EXCLUDED.dividend_yield
    """, rows)

    return len(rows)




# ── Earnings Surprises ────────────────────────────────────────────────────────

def load_earnings_surprises(cur, ticker: str) -> int:
    """
    Pull historical earnings actuals vs estimates from /stable/earnings.
    'date' is the earnings announcement date — used as fiscal_date for PIT lookups.
    Both EPS and revenue surprise are stored.
    """
    data = fmp_get("earnings", ticker, params={"limit": 40})
    time.sleep(DELAY)

    if not data or not isinstance(data, list):
        return 0

    rows = []
    for item in data:
        report_date  = item.get("date")
        eps_actual   = item.get("epsActual")
        eps_est      = item.get("epsEstimated")

        if not report_date or eps_actual is None:
            continue

        surprise_eps     = (eps_actual - eps_est) if eps_est is not None else None
        surprise_eps_pct = (surprise_eps / abs(eps_est)) if (surprise_eps is not None and eps_est and eps_est != 0) else None

        rows.append((
            ticker,
            report_date,
            eps_est,
            eps_actual,
            surprise_eps,
            surprise_eps_pct,
        ))

    if not rows:
        return 0

    execute_values(cur, """
        INSERT INTO earnings_surprises (
            ticker, fiscal_date,
            estimated_eps, actual_eps, surprise_eps, surprise_eps_pct
        )
        VALUES %s
        ON CONFLICT (ticker, fiscal_date) DO UPDATE SET
            estimated_eps    = EXCLUDED.estimated_eps,
            actual_eps       = EXCLUDED.actual_eps,
            surprise_eps     = EXCLUDED.surprise_eps,
            surprise_eps_pct = EXCLUDED.surprise_eps_pct
    """, rows)

    return len(rows)


# ── Dividends ──────────────────────────────────────────────────────────────────

def load_dividends(cur, ticker: str) -> int:
    """
    Pull full dividend history from /stable/dividends.
    One row per ex-dividend date.
    """
    data = fmp_get("dividends", ticker)
    time.sleep(DELAY)

    if not data or not isinstance(data, list):
        return 0

    # FMP occasionally returns two entries for the same ex-dividend date (data
    # revisions) — dedupe here since ON CONFLICT DO UPDATE can't touch the same
    # row twice within a single batch. Last one wins (most recently listed by FMP).
    rows_by_date = {}
    for item in data:
        ex_div_date = item.get("date")
        if not ex_div_date:
            continue

        rows_by_date[ex_div_date] = (
            ticker,
            ex_div_date,
            item.get("paymentDate") or None,
            item.get("recordDate") or None,
            item.get("declarationDate") or None,
            item.get("dividend"),
            item.get("adjDividend"),
        )

    rows = list(rows_by_date.values())
    if not rows:
        return 0

    execute_values(cur, """
        INSERT INTO dividends (
            ticker, ex_dividend_date, payment_date, record_date,
            declared_date, dividend, adjusted_dividend
        )
        VALUES %s
        ON CONFLICT (ticker, ex_dividend_date) DO UPDATE SET
            payment_date      = EXCLUDED.payment_date,
            record_date       = EXCLUDED.record_date,
            declared_date     = EXCLUDED.declared_date,
            dividend          = EXCLUDED.dividend,
            adjusted_dividend = EXCLUDED.adjusted_dividend
    """, rows)

    return len(rows)








# ── Incremental freshness check ───────────────────────────────────────────────

def get_ticker_freshness(cur, ticker: str) -> dict:
    """
    Single DB query per ticker returning the most recent date for each endpoint.
    Used in incremental mode to decide which API calls to skip.

    Every value here must be the date the data BECAME AVAILABLE, not the date the
    period it describes ended. Mixing the two silently breaks the skip logic: a
    quarter ending June 30 is not published until mid-August, so comparing "today
    minus June 30" against an 80-day threshold calls the data stale six weeks before
    its replacement exists, and re-downloads ten endpoints that cannot have changed.

      last_price     date       — the bar's own date, already an availability date.
      last_quarterly filed_at   — NOT fiscal_date. See above. COALESCE covers rows
                                  predating the column; +45 days is the lag FMP
                                  assumes (every row in the table is exactly 45,
                                  so filed_at is modelled, not observed).
      last_dividend  ex_div     — the event date, already an availability date.
      last_surprise  fiscal_date— misleading column name: FMP's earnings-surprise
                                  endpoint puts the REPORT date here, not the period
                                  end (values like 2026-08-13 are not quarter ends).
                                  Already an availability date; leave it alone.
    """
    cur.execute("""
        SELECT
            (SELECT MAX(date)             FROM price_history     WHERE ticker = %s)                          AS last_price,
            (SELECT MAX(COALESCE(filed_at, fiscal_date + 45))
                                          FROM income_statements WHERE ticker = %s AND period = 'quarterly') AS last_quarterly,
            (SELECT MAX(ex_dividend_date) FROM dividends         WHERE ticker = %s)                          AS last_dividend,
            (SELECT MAX(fiscal_date)      FROM earnings_surprises WHERE ticker = %s)                         AS last_surprise
    """, (ticker,) * 4)
    row = cur.fetchone()
    return dict(row) if row else {}


# ── Main ───────────────────────────────────────────────────────────────────────

# Endpoint steps shown in the sub-progress bar
STEPS = [
    "profile", "price", "income/A", "income/Q", "balance/A", "balance/Q",
    "cashflow/A", "cashflow/Q", "metrics/A", "metrics/Q", "ratios/A", "ratios/Q",
    "dividends", "earnings",
]


def run(test_mode: bool = False, incremental: bool = False):
    print("=" * 60)
    print("  FMP DATA PIPELINE")
    if incremental:
        print("  MODE: incremental (skips endpoints with recent data)")
    print("=" * 60)

    if not FMP_KEY:
        print("ERROR: FMP_API_KEY not set in .env")
        return

    try:
        tickers = load_covered_tickers()
    except FileNotFoundError:
        print(f"ERROR: {COVERED_FILE} not found. Run 1_fetch_universe.py first.")
        return

    if test_mode:
        tickers = tickers[:5]
        tqdm.write(f"TEST MODE — {len(tickers)} tickers: {tickers}\n")
    elif os.getenv("RETRY_TICKERS"):
        tickers = os.getenv("RETRY_TICKERS").split(",")
        tqdm.write(f"RETRY MODE — {len(tickers)} tickers\n")
    else:
        tqdm.write(f"Full run: {len(tickers)} tickers, {len(STEPS)} endpoints each\n")

    _timing["api_calls"] = 0
    _timing["api_secs"] = 0.0
    t_start = time.perf_counter()

    init_schema()
    conn = get_conn()

    today = datetime.date.today()

    def _is_fresh(d, days: int) -> bool:
        return d is not None and (today - d).days < days

    totals = {k: 0 for k in [
        "price", "key_metrics", "ratios", "dividends", "earnings_surprises",
    ]}
    skipped = {"price": 0, "fundamentals": 0, "adr": 0}
    failed = []

    ticker_bar = tqdm(tickers, desc="Tickers", unit="ticker",
                      bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]")

    try:
        for ticker in ticker_bar:
            ticker_bar.set_postfix_str(ticker, refresh=True)

            step_bar = tqdm(STEPS, desc="  Endpoints", leave=False, unit="ep",
                            bar_format="  {l_bar}{bar}| {n_fmt}/{total_fmt} {postfix}")

            try:
                with conn.cursor() as cur:

                    freshness = get_ticker_freshness(cur, ticker) if incremental else {}

                    # -- Profile (always pulled — fast, keeps name/sector current) --
                    step_bar.set_postfix_str("profile")
                    is_adr = upsert_ticker_profile(cur, ticker)

                    # A depositary receipt is a CAD-hedged wrapper around a foreign
                    # stock, not a TSX operating company. Its fundamentals belong to
                    # the US filer and its price is that stock times FX, so pulling
                    # the remaining 13 endpoints buys nothing and puts a non-Canadian
                    # megacap into a Canadian stock-picking universe. The flag is now
                    # committed, so purge_tickers.py can clear any history already
                    # collected under this ticker.
                    if is_adr:
                        conn.commit()
                        skipped["adr"] += 1
                        step_bar.update(len(STEPS) - 1)
                        tqdm.write(f"  {ticker:<12} CDR/ADR - skipped")
                        continue
                    step_bar.update(1)

                    # -- Price history --
                    step_bar.set_postfix_str("price history")
                    if incremental and _is_fresh(freshness.get("last_price"), 3):
                        skipped["price"] += 1
                        price_label = " SKIP"
                    else:
                        from_date = freshness.get("last_price") if incremental else None
                        n = load_price_history(cur, ticker, from_date=from_date)
                        totals["price"] += n
                        price_label = f"{n:>5}"
                    tqdm.write(f"  {ticker:<12} price={price_label}  ", end="")
                    step_bar.update(1)

                    # -- Fundamentals: income / balance / cashflow / metrics / ratios --
                    # Skip all 10 calls together if the newest quarterly filing LANDED
                    # less than 80 days ago. Filings arrive ~91 days apart, so anything
                    # inside 80 days means the next one is not out yet and all ten
                    # endpoints would return what the DB already holds.
                    skip_fundamentals = incremental and _is_fresh(freshness.get("last_quarterly"), 80)
                    if skip_fundamentals:
                        skipped["fundamentals"] += 1
                        step_bar.update(10)
                    else:
                        for statement, label in [
                            ("income-statement",       "income"),
                            ("balance-sheet-statement","balance"),
                            ("cash-flow-statement",    "cashflow"),
                        ]:
                            for period in ["annual", "quarter"]:
                                step_bar.set_postfix_str(f"{label}/{period}")
                                load_fundamentals(cur, ticker, statement, period)
                                step_bar.update(1)

                        for period in ["annual", "quarter"]:
                            step_bar.set_postfix_str(f"metrics/{period}")
                            n = load_key_metrics(cur, ticker, period)
                            totals["key_metrics"] += n
                            step_bar.update(1)

                        for period in ["annual", "quarter"]:
                            step_bar.set_postfix_str(f"ratios/{period}")
                            n = load_ratios(cur, ticker, period)
                            totals["ratios"] += n
                            step_bar.update(1)

                    # -- Dividends --
                    step_bar.set_postfix_str("dividends")
                    if not (incremental and _is_fresh(freshness.get("last_dividend"), 30)):
                        n = load_dividends(cur, ticker)
                        totals["dividends"] += n
                    step_bar.update(1)

                    # -- Earnings Surprises --
                    step_bar.set_postfix_str("earnings surprises")
                    if incremental and _is_fresh(freshness.get("last_surprise"), 80):
                        tqdm.write(f"earnings=SKP")
                    else:
                        n = load_earnings_surprises(cur, ticker)
                        totals["earnings_surprises"] += n
                        tqdm.write(f"earnings={n:>3}")
                    step_bar.update(1)

                conn.commit()

            except Exception as e:
                if conn.closed:
                    tqdm.write(f"  x ERROR {ticker}: {e}  (connection dropped — reconnecting)")
                    conn = get_conn()
                else:
                    conn.rollback()
                    tqdm.write(f"  x ERROR {ticker}: {e}")
                failed.append(ticker)

            finally:
                step_bar.close()

    finally:
        conn.close()
        ticker_bar.close()

    elapsed = time.perf_counter() - t_start
    rate_sleep = _timing["api_calls"] * DELAY
    db_secs = max(0.0, elapsed - _timing["api_secs"] - rate_sleep)

    print(f"\n{'='*60}")
    print(f"  COMPLETE")
    print(f"{'='*60}")
    print(f"  Tickers processed:    {len(tickers) - len(failed):>6} / {len(tickers)}")
    print(f"  Price rows:           {totals['price']:>8,}")
    print(f"  Key metrics rows:     {totals['key_metrics']:>8,}")
    print(f"  Ratio rows:           {totals['ratios']:>8,}")
    print(f"  Dividend rows:        {totals['dividends']:>8,}")
    print(f"  Earnings surprise rows:{totals['earnings_surprises']:>7,}")
    if skipped["adr"]:
        print()
        print(f"  Skipped as CDR/ADR:   {skipped['adr']:>6} tickers")
        print(f"    Flags are now set — clear their stored history with:")
        print(f"    python purge_tickers.py --preset adr-and-funds --dry-run")
    if incremental:
        print(f"\n  Incremental skips:")
        print(f"    Price   (< 3 days old):   {skipped['price']:>5} tickers")
        print(f"    Fundmtl (< 80 days old):  {skipped['fundamentals']:>5} tickers")
    print(f"\n  Timing  ({elapsed / 60:.1f} min total):")
    print(f"    FMP responses  ({_timing['api_calls']:,} calls):  {_timing['api_secs']:>5.0f}s  ({_timing['api_secs'] / 60:.1f} min)")
    print(f"    Rate-limit sleep ({_timing['api_calls']:,}×{DELAY}s): {rate_sleep:>5.0f}s  ({rate_sleep / 60:.1f} min)")
    print(f"    DB writes + other:            {db_secs:>5.0f}s  ({db_secs / 60:.1f} min)")
    if failed:
        print(f"\n  Failed ({len(failed)}): {', '.join(failed)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test",        action="store_true", help="Run on first 5 tickers only")
    parser.add_argument("--incremental", action="store_true", help="Skip endpoints with data < freshness threshold")
    args = parser.parse_args()
    run(test_mode=args.test, incremental=args.incremental)

"""
macro_pipeline.py — Pull macro time series into the macro_data table.

Series pulled:
  From FMP (historical-price-eod/full endpoint):
    GCUSD   — Gold spot price (USD)
    BZUSD   — Brent Crude (WTI proxy — CLUSD needs a higher FMP plan)
    CADUSD  — CAD/USD exchange rate
    ^VIX    — CBOE Volatility Index
    (CLUSD / HGUSD / ^GSPTSX disabled — 402 on current FMP plan, see FMP_PRICE_SERIES)

  From FMP treasury-rates endpoint (capped at ~90 days/call, paginated back to 2015):
    US2Y    — 2-year Treasury yield
    US10Y   — 10-year Treasury yield

  From Bank of Canada API (free, no key required):
    BOCRATE — Bank of Canada overnight rate
    CA2Y    — GoC 2-year benchmark yield
    CA10Y   — GoC 10-year benchmark yield

Usage:
    python macro_pipeline.py          # full run
    python macro_pipeline.py --test   # print first 5 rows per series, no DB write
"""

import argparse
import datetime
import os
import time
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from tqdm import tqdm
from quant_db import get_conn, init_schema

load_dotenv()

FMP_KEY  = os.getenv("FMP_API_KEY")
FMP_BASE = "https://financialmodelingprep.com/stable"
DELAY    = 0.25

# Series to pull via FMP historical price endpoint
FMP_PRICE_SERIES = [
    "GCUSD",    # Gold
    "BZUSD",    # Brent Crude (WTI proxy — CLUSD requires higher FMP plan)
    "CADUSD",   # CAD/USD
    "^VIX",     # Volatility index
    # "CLUSD"   -- 402 on current plan
    # "HGUSD"   -- 402 on current plan (copper)
    # "^GSPTSX" -- 402 on current plan (TSX composite)
]


# ── FMP helpers ────────────────────────────────────────────────────────────────

def fmp_price_history(symbol: str, start_year: int = 2015) -> list[dict]:
    """
    Pull full available daily price history for a symbol from FMP.

    Without an explicit from/to range, this endpoint silently defaults to
    ~5 years of history for commodity/forex/index symbols (unlike individual
    stocks, which return their full history with no params) — so we always
    pass an explicit start date to get the real full range.
    """
    url = f"{FMP_BASE}/historical-price-eod/full"
    params = {
        "symbol": symbol,
        "apikey": FMP_KEY,
        "from": f"{start_year}-01-01",
        "to": datetime.date.today().isoformat(),
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 429:
            print(f"    Rate limited on {symbol} — waiting 60s...")
            time.sleep(60)
            r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"    FMP error [{symbol}]: {e}")
        return []


def fmp_treasury_window(start: datetime.date, end: datetime.date) -> list[dict]:
    """
    Pull US Treasury yields for one date window from FMP /stable/treasury-rates.
    Each row has one field per maturity (month1..month6, year1..year30).
    FMP caps this endpoint at ~90 days per call when the window reaches
    today, so full history requires calling this repeatedly — see
    fmp_treasury_full().
    """
    url = f"{FMP_BASE}/treasury-rates"
    params = {"apikey": FMP_KEY, "from": start.isoformat(), "to": end.isoformat()}
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 429:
            time.sleep(60)
            r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"    FMP error [treasury-rates {start}..{end}]: {e}")
        return []


def fmp_treasury_full(start_year: int = 2015) -> list[dict]:
    """
    Paginate fmp_treasury_window in ~80-day windows to cover full history
    from start_year to today. Returns one row per date with all maturities.
    """
    window = datetime.timedelta(days=80)
    today = datetime.date.today()
    cur = datetime.date(start_year, 1, 1)

    all_rows = []
    while cur <= today:
        end = min(cur + window, today)
        all_rows.extend(fmp_treasury_window(cur, end))
        time.sleep(DELAY)
        cur = end + datetime.timedelta(days=1)

    return all_rows


# ── Bank of Canada API ─────────────────────────────────────────────────────────

def boc_series(series_id: str) -> list[dict]:
    """
    Pull any Bank of Canada Valet series by ID.
    Returns list of {date, value} dicts.
    """
    url = f"https://www.bankofcanada.ca/valet/observations/{series_id}/json"
    try:
        r = requests.get(url, params={"start_date": "2015-01-01"}, timeout=15)
        r.raise_for_status()
        obs = r.json().get("observations", [])
        rows = []
        for o in obs:
            date = o.get("d")
            val  = o.get(series_id, {}).get("v")
            if date and val:
                rows.append({"date": date, "value": float(val)})
        return rows
    except Exception as e:
        tqdm.write(f"    BoC API error [{series_id}]: {e}")
        return []


# ── DB upsert ──────────────────────────────────────────────────────────────────

def upsert_series(cur, series: str, rows: list[dict], source: str):
    """Upsert a list of {date, value} dicts into macro_data."""
    if not rows:
        return 0

    data = [(series, r["date"], r["value"], source) for r in rows if r.get("date") and r.get("value") is not None]
    if not data:
        return 0

    execute_values(cur, """
        INSERT INTO macro_data (series, date, value, source)
        VALUES %s
        ON CONFLICT (series, date) DO UPDATE SET
            value  = EXCLUDED.value,
            source = EXCLUDED.source
    """, data)

    return len(data)


# BoC series: name stored in macro_data -> BoC Valet series ID
BOC_SERIES = {
    "BOCRATE": "V39079",              # BoC overnight target rate
    "CA2Y":    "BD.CDN.2YR.DQ.YLD",  # GoC 2-year benchmark yield
    "CA10Y":   "BD.CDN.10YR.DQ.YLD", # GoC 10-year benchmark yield
}

# US Treasury series: name stored in macro_data -> field in FMP's treasury-rates response
TREASURY_SERIES = {
    "US2Y":  "year2",
    "US10Y": "year10",
}


# ── Main ───────────────────────────────────────────────────────────────────────

def run(test_mode: bool = False):
    print("=" * 60)
    print("  MACRO DATA PIPELINE")
    print("=" * 60)

    all_labels = FMP_PRICE_SERIES + list(TREASURY_SERIES.keys()) + list(BOC_SERIES.keys())
    init_schema()
    conn = get_conn()
    total = 0

    try:
        with conn.cursor() as cur:
            bar = tqdm(all_labels, desc="Series", unit="series",
                       bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")

            # FMP price series (commodities, forex, indices)
            for symbol in FMP_PRICE_SERIES:
                bar.set_postfix_str(symbol)
                raw = fmp_price_history(symbol)
                time.sleep(DELAY)
                rows = [{"date": r["date"], "value": r["close"]} for r in raw if r.get("date") and r.get("close") is not None]

                if test_mode:
                    tqdm.write(f"  {symbol}: {len(rows)} rows -- sample: {rows[:2]}")
                else:
                    n = upsert_series(cur, symbol, rows, "fmp")
                    total += n
                    tqdm.write(f"  {symbol:<12} {n:>5,} rows")
                bar.update(1)

            # US Treasury yields (one paginated pull covers both US2Y and US10Y)
            bar.set_postfix_str("treasury")
            treasury_rows = fmp_treasury_full()
            for series_name, field in TREASURY_SERIES.items():
                rows = [{"date": r["date"], "value": r[field]}
                        for r in treasury_rows if r.get("date") and r.get(field) is not None]

                if test_mode:
                    tqdm.write(f"  {series_name}: {len(rows)} rows -- sample: {rows[:2]}")
                else:
                    n = upsert_series(cur, series_name, rows, "fmp")
                    total += n
                    tqdm.write(f"  {series_name:<12} {n:>5,} rows")
                bar.update(1)

            # Bank of Canada series (overnight rate + GoC 2yr/10yr yields)
            for series_name, series_id in BOC_SERIES.items():
                bar.set_postfix_str(series_name)
                rows = boc_series(series_id)

                if test_mode:
                    tqdm.write(f"  {series_name}: {len(rows)} rows -- sample: {rows[-2:]}")
                else:
                    n = upsert_series(cur, series_name, rows, "boc")
                    total += n
                    tqdm.write(f"  {series_name:<12} {n:>5,} rows")
                bar.update(1)

            bar.close()

        if not test_mode:
            conn.commit()
            print(f"\n  Total rows upserted: {total:,}")

        print(f"{'='*60}")

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Print sample rows, skip DB write")
    args = parser.parse_args()
    run(test_mode=args.test)

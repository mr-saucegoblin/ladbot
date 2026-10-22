"""
backfill_balance_sheet_fields.py — Fill current_assets, current_liabilities and
retained_earnings on existing balance_sheets rows.

Why this exists: 2_fmp_pipeline.py's balance-sheet field map only ever pulled 6
of FMP's fields. The three above were never mapped, so the Altman Z' inputs X1
(working capital / total assets) and X2 (retained earnings / total assets) had
no source data in the DB at all — which is why altman_z_score sat at ~1% filled,
populated only by residue from an older FMP-snapshot write.

FMP does return all three (verified live on CNR.TO: totalCurrentAssets,
totalCurrentLiabilities, retainedEarnings). 2_fmp_pipeline.py now maps them, so
this script is a one-time catch-up for the ~31.5k rows already stored — far
cheaper than re-running the full 3-4 hour pipeline for three columns.

Only UPDATEs rows that already exist (matched on ticker + period + fiscal_date).
Deliberately does not INSERT: a fiscal date FMP returns now but that isn't already
stored would otherwise land as a row with three columns filled and the other six
NULL, which is worse than absent. Re-run 2_fmp_pipeline.py if you want new periods.

Usage:
    python backfill_balance_sheet_fields.py          # all tickers
    python backfill_balance_sheet_fields.py --test   # first 10 only, no DB write
"""

import argparse
import os
import time

import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from tqdm import tqdm

from quant_db import get_conn, init_schema

load_dotenv()

FMP_KEY = os.getenv("FMP_API_KEY")
FMP_BASE = "https://financialmodelingprep.com/stable"
DELAY = 0.10

# FMP key → db column, for the three fields this script backfills.
FIELDS = {
    "totalCurrentAssets":      "current_assets",
    "totalCurrentLiabilities": "current_liabilities",
    "retainedEarnings":        "retained_earnings",
}

# (FMP period param, db period value). Both are stored, so both need filling.
PERIODS = [("annual", "annual", 20), ("quarter", "quarterly", 40)]


def fetch_balance_sheets(ticker: str, period: str, limit: int) -> list[dict]:
    """
    Pull balance sheet rows for one ticker/period. Returns [] on any failure —
    a transient API error should skip a ticker, not abort the whole backfill.
    """
    params = {"symbol": ticker, "limit": limit, "apikey": FMP_KEY}
    if period == "quarter":
        params["period"] = "quarter"

    try:
        r = requests.get(f"{FMP_BASE}/balance-sheet-statement", params=params, timeout=20)
        if r.status_code == 429:
            tqdm.write("    Rate limited — waiting 60s...")
            time.sleep(60)
            r = requests.get(f"{FMP_BASE}/balance-sheet-statement", params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        tqdm.write(f"  x {ticker} ({period}): {e}")
        return []


def run(test_mode: bool = False):
    print("=" * 60)
    print("  BALANCE SHEET FIELD BACKFILL")
    print("  (current_assets, current_liabilities, retained_earnings)")
    print("=" * 60)

    if not FMP_KEY:
        print("ERROR: FMP_API_KEY not set in .env")
        return

    # Applies the pending ALTER TABLE migrations — CREATE TABLE IF NOT EXISTS is a
    # no-op on a live table, so without this every UPDATE below fails on an
    # undefined column.
    init_schema()
    conn = get_conn()

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT ticker FROM tickers ORDER BY ticker")
            tickers = [r["ticker"] for r in cur.fetchall()]

        if test_mode:
            tickers = tickers[:10]
            print(f"TEST MODE — {len(tickers)} tickers, no DB write\n")
        else:
            print(f"Full run: {len(tickers)} tickers\n")

        rows_updated = 0
        tickers_touched = 0
        no_data = []

        bar = tqdm(tickers, desc="Tickers", unit="ticker",
                   bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}")

        for ticker in bar:
            bar.set_postfix_str(ticker, refresh=False)

            # Keyed by (ticker, period, fiscal_date) so a duplicate fiscal date in
            # FMP's response can't put the same key in one batch twice — execute_values
            # with a self-join would otherwise update a row from an arbitrary duplicate.
            batch = {}

            for fmp_period, db_period, limit in PERIODS:
                data = fetch_balance_sheets(ticker, fmp_period, limit)
                time.sleep(DELAY)

                for row in data:
                    fiscal_date = row.get("date")
                    if not fiscal_date:
                        continue
                    batch[(ticker, db_period, fiscal_date)] = tuple(
                        row.get(fmp_key) for fmp_key in FIELDS
                    )

            if not batch:
                no_data.append(ticker)
                continue

            values = [key + vals for key, vals in batch.items()]

            if test_mode:
                filled = sum(1 for v in values if any(x is not None for x in v[3:]))
                tqdm.write(f"  {ticker:<12} {len(values):>3} periods, {filled:>3} with data")
                continue

            with conn.cursor() as cur:
                # Plain UPDATE via a VALUES join rather than an upsert, so only
                # already-stored periods are touched. The casts are required:
                # values coming through execute_values arrive untyped, and Postgres
                # won't compare an unknown to a date or numeric column on its own.
                execute_values(cur, """
                    UPDATE balance_sheets AS b
                    SET current_assets      = v.current_assets,
                        current_liabilities = v.current_liabilities,
                        retained_earnings   = v.retained_earnings
                    FROM (VALUES %s) AS v (
                        ticker, period, fiscal_date,
                        current_assets, current_liabilities, retained_earnings
                    )
                    WHERE b.ticker      = v.ticker
                      AND b.period      = v.period
                      AND b.fiscal_date = v.fiscal_date
                """, values, template="(%s, %s, %s::date, %s::numeric, %s::numeric, %s::numeric)")
                rows_updated += cur.rowcount
            conn.commit()
            tickers_touched += 1

        bar.close()

        print(f"\n{'='*60}")
        print(f"  Tickers processed:   {tickers_touched}")
        print(f"  Rows updated:        {rows_updated:,}")
        if no_data:
            print(f"  No FMP data ({len(no_data)}): {', '.join(no_data[:15])}"
                  + (" ..." if len(no_data) > 15 else ""))
        print(f"{'='*60}\n")

        if not test_mode:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) AS total,
                           COUNT(current_assets)      AS ca,
                           COUNT(current_liabilities) AS cl,
                           COUNT(retained_earnings)   AS re
                    FROM balance_sheets
                """)
                r = cur.fetchone()
                print(f"  balance_sheets coverage after backfill ({r['total']:,} rows):")
                print(f"    current_assets       {r['ca']:>7,}  ({r['ca']/r['total']:.1%})")
                print(f"    current_liabilities  {r['cl']:>7,}  ({r['cl']/r['total']:.1%})")
                print(f"    retained_earnings    {r['re']:>7,}  ({r['re']/r['total']:.1%})")
                print()

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="First 10 tickers, no DB write")
    args = parser.parse_args()
    run(test_mode=args.test)

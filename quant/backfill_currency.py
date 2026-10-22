"""
backfill_currency.py — Populate tickers.reported_currency from FMP.

Financial statements are reported in the company's own currency, which for ~38%
of this TSX universe is USD (or occasionally something else) even though the
shares trade in CAD. Mixing the two silently corrupts market_cap, any size
factor, and any ratio computed as price/fundamentals — e.g. FNV.TO's implied
price from market_cap/shares was off from its actual CAD close by exactly the
CADUSD rate, to five decimals, every quarter.

One lightweight API call per ticker (pulls a single statement row just to read
its reportedCurrency field) rather than re-pulling all fundamentals.

Usage:
    python backfill_currency.py          # all tickers
    python backfill_currency.py --test   # first 10 only, no DB write
"""

import argparse
import os
import time
from collections import Counter

import requests
from dotenv import load_dotenv
from tqdm import tqdm

from quant_db import get_conn, init_schema

load_dotenv()

FMP_KEY = os.getenv("FMP_API_KEY")
FMP_BASE = "https://financialmodelingprep.com/stable"
DELAY = 0.10


def fetch_currency(ticker: str) -> str | None:
    """Read reportedCurrency off a single statement row. None if unavailable."""
    try:
        r = requests.get(
            f"{FMP_BASE}/income-statement",
            params={"symbol": ticker, "period": "quarter", "limit": 1, "apikey": FMP_KEY},
            timeout=15,
        )
        if r.status_code == 429:
            tqdm.write("    Rate limited — waiting 60s...")
            time.sleep(60)
            r = requests.get(
                f"{FMP_BASE}/income-statement",
                params={"symbol": ticker, "period": "quarter", "limit": 1, "apikey": FMP_KEY},
                timeout=15,
            )
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and data:
            return data[0].get("reportedCurrency")
        return None
    except Exception as e:
        tqdm.write(f"  x {ticker}: {e}")
        return None


def run(test_mode: bool = False):
    print("=" * 60)
    print("  REPORTED CURRENCY BACKFILL")
    print("=" * 60)

    if not FMP_KEY:
        print("ERROR: FMP_API_KEY not set in .env")
        return

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

        counts = Counter()
        bar = tqdm(tickers, desc="Tickers", unit="ticker",
                   bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}")

        for ticker in bar:
            bar.set_postfix_str(ticker, refresh=False)
            currency = fetch_currency(ticker)
            counts[currency or "NO_DATA"] += 1
            time.sleep(DELAY)

            if test_mode:
                tqdm.write(f"  {ticker:<12} {currency}")
                continue

            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE tickers SET reported_currency = %s, updated_at = NOW() WHERE ticker = %s",
                    (currency, ticker),
                )
            conn.commit()

        bar.close()

        print(f"\n{'='*60}")
        print("  Currency breakdown:")
        for currency, n in counts.most_common():
            print(f"    {currency:<10} {n:>5}")
        print(f"{'='*60}")

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="First 10 tickers, no DB write")
    args = parser.parse_args()
    run(test_mode=args.test)

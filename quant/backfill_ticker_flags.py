"""
backfill_ticker_flags.py — Populate tickers.is_adr / tickers.is_fund from FMP.

The TSX universe contains two kinds of entity that aren't operating companies
and shouldn't be scored by a stock-picking model:

  * Canadian Depositary Receipts — CAD-hedged wrappers around foreign stocks
    (Broadcom, Eli Lilly, Cisco, Citigroup). They have no fundamentals under
    the .TO symbol because the company files under its US ticker.
  * Funds / investment vehicles (Purpose High Interest Savings Fund, Pimco
    Monthly Enhanced Income). No income statement — they aren't businesses.

The `country` field does NOT identify the first group: Brookfield, Restaurant
Brands and Newmont all read 'US' too, and dropping them would gut the universe.
The profile endpoint's `isAdr` flag separates them cleanly.

One profile call per ticker.

Usage:
    python backfill_ticker_flags.py          # all tickers
    python backfill_ticker_flags.py --test   # first 10 only, no DB write
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


def fetch_flags(ticker: str) -> tuple[bool | None, bool | None]:
    """Return (is_adr, is_fund) from the profile endpoint. (None, None) on failure."""
    try:
        r = requests.get(
            f"{FMP_BASE}/profile",
            params={"symbol": ticker, "apikey": FMP_KEY},
            timeout=15,
        )
        if r.status_code == 429:
            tqdm.write("    Rate limited — waiting 60s...")
            time.sleep(60)
            r = requests.get(
                f"{FMP_BASE}/profile",
                params={"symbol": ticker, "apikey": FMP_KEY},
                timeout=15,
            )
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and data:
            p = data[0]
            # isEtf folded into is_fund — both mean "not an operating company"
            is_fund = bool(p.get("isFund")) or bool(p.get("isEtf"))
            return bool(p.get("isAdr")), is_fund
        return None, None
    except Exception as e:
        tqdm.write(f"  x {ticker}: {e}")
        return None, None


def run(test_mode: bool = False):
    print("=" * 60)
    print("  TICKER FLAGS BACKFILL (is_adr / is_fund)")
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
            is_adr, is_fund = fetch_flags(ticker)
            time.sleep(DELAY)

            if is_adr:
                counts["ADR"] += 1
            elif is_fund:
                counts["FUND"] += 1
            elif is_adr is None:
                counts["NO_DATA"] += 1
            else:
                counts["operating company"] += 1

            if test_mode:
                tqdm.write(f"  {ticker:<12} is_adr={is_adr}  is_fund={is_fund}")
                continue

            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE tickers SET is_adr = %s, is_fund = %s, updated_at = NOW() WHERE ticker = %s",
                    (is_adr, is_fund, ticker),
                )
            conn.commit()

        bar.close()

        print(f"\n{'='*60}")
        print("  Breakdown:")
        for label, n in counts.most_common():
            print(f"    {label:<20} {n:>5}")
        print(f"{'='*60}")

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="First 10 tickers, no DB write")
    args = parser.parse_args()
    run(test_mode=args.test)

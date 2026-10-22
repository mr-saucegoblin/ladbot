"""
1_fetch_universe.py — Build the TSX stock universe directly from FMP.

Pulls all TSX-listed stocks from FMP's stock screener, filters out TSXV
and ETFs, and writes the ticker list to fmp_covered.txt.

Run this whenever the TSX ticker list needs refreshing (not needed every
data pull — monthly or when adding new coverage is enough).

Usage:
    python 1_fetch_universe.py
"""

import os
import pathlib
import re
import time
import requests
from tqdm import tqdm
from dotenv import load_dotenv

from cdr_list import load_cdr_exclusions

load_dotenv()

FMP_KEY = os.getenv("FMP_API_KEY")
FMP_BASE = "https://financialmodelingprep.com/stable"
OUTPUT_FILE = pathlib.Path(__file__).parent / "fmp_covered.txt"

# Minimum price filter — excludes penny stocks with no liquidity
MIN_PRICE = 0.50

# TSX preferred shares: TICKER-P*.TO (e.g. RY-PZ.TO, TD-PFK.TO, ENB-PN.TO).
# Distinct from dual-class common shares (TICKER-A/-B/-X/-Y) and trust/LP
# units (TICKER-U/-UN), which are NOT preferred and should stay in scope.
PREFERRED_SHARE_RE = re.compile(r"-P[A-Z]+\.TO$")


# Lookups that threw rather than answering. A failed check is NOT a "no" — see
# is_depositary_receipt() — so these are counted and reported loudly at the end.
_adr_check_failures: list[str] = []


def is_depositary_receipt(symbol: str) -> bool:
    """
    True if FMP flags this symbol as a depositary receipt.

    Fails OPEN (returns False) on error, because dropping a real company over a
    transient timeout is worse than letting one through. That trade-off is still
    right, but it is only safe if the failures are visible: on 2026-09-30 this
    check silently let all 14 CDRs into fmp_covered.txt, and the summary happily
    reported "Skipped (CDR/ADR): 0" as though the universe were clean. Apple,
    NVIDIA and Broadcom then reached tier='main' and were scored by the model.

    So every failure is recorded in _adr_check_failures and surfaced by run().
    Fail open, but never fail quiet. 2_fmp_pipeline.py now also writes is_adr from
    the profile it already fetches, so anything slipping past here is caught on the
    next data pull rather than at modelling time.
    """
    try:
        r = requests.get(
            f"{FMP_BASE}/profile",
            params={"symbol": symbol, "apikey": FMP_KEY},
            timeout=15,
        )
        if r.status_code == 429:
            time.sleep(60)
            r = requests.get(
                f"{FMP_BASE}/profile",
                params={"symbol": symbol, "apikey": FMP_KEY},
                timeout=15,
            )
        r.raise_for_status()
        data = r.json()
        time.sleep(0.10)
        if isinstance(data, list) and data:
            return bool(data[0].get("isAdr"))
        # An empty profile is not a clean "no" either — nothing was classified.
        _adr_check_failures.append(symbol)
        return False
    except Exception:
        _adr_check_failures.append(symbol)
        return False


def fetch_tsx_stocks() -> list[dict]:
    """
    Fetch all TSX-listed stocks from FMP stock screener.
    Returns list of dicts with symbol, name, price, exchange.
    """
    url = f"{FMP_BASE}/company-screener"
    params = {
        "exchange": "TSX",
        "apikey": FMP_KEY,
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 429:
            raise RuntimeError("FMP rate limit hit (429). Wait a minute and retry.")
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        raise RuntimeError(f"FMP request failed: {e}") from e

    if not data or not isinstance(data, list):
        raise RuntimeError(f"Unexpected response from FMP: {str(data)[:200]}")

    return data


def run():
    print("=== FMP Universe Fetcher ===\n")

    if not FMP_KEY:
        print("ERROR: FMP_API_KEY not set in .env")
        return

    print("Fetching TSX stock list from FMP...")
    try:
        stocks = fetch_tsx_stocks()
    except RuntimeError as e:
        print(f"ERROR: {e}")
        return
    print(f"  Raw results: {len(stocks)} stocks\n")

    # Filter: TSX only (not TSXV), price above minimum, no ETFs, actively trading
    filtered = []
    skipped_tsxv = 0
    skipped_price = 0
    skipped_etf = 0
    skipped_inactive = 0
    skipped_preferred = 0
    skipped_adr = 0

    for s in stocks:
        exchange = (s.get("exchangeShortName") or s.get("exchange") or "").upper()
        symbol = s.get("symbol", "")
        price = s.get("price") or 0

        # Skip non-.TO symbols — filters out LSE cross-listings
        if not symbol.endswith(".TO"):
            continue

        # Skip mutual funds — FMP uses 0P* codes for funds, not stocks
        if symbol.startswith("0P"):
            continue

        # Skip ETFs — not useful for stock picking.
        #
        # Deliberately NOT also filtering on isFund here: FMP flags InterRent REIT
        # as a fund while leaving Canadian Apartment Properties and Boardwalk — the
        # same business — unflagged, so using it would silently drop real operating
        # REITs. True investment vehicles that slip through (e.g. Pimco Monthly
        # Enhanced Income) get caught later by purge_tickers.py's adr-and-funds
        # preset, which can corroborate isFund against the absence of any income
        # statement. That check isn't possible here — no fundamentals exist yet.
        if s.get("isEtf"):
            skipped_etf += 1
            continue

        # Skip TSXV — too speculative, too illiquid
        if "TSXV" in exchange or "TSX-V" in exchange or "TSX.V" in exchange:
            skipped_tsxv += 1
            continue

        # Skip sub-penny stocks
        if price < MIN_PRICE:
            skipped_price += 1
            continue

        # Skip delisted/acquired/inactive names — FMP's screener includes these
        # unless explicitly filtered; confirmed live that NVEI.TO, UFS.TO, RFP.TO
        # etc. still show up here with isActivelyTrading=False.
        if s.get("isActivelyTrading") is False:
            skipped_inactive += 1
            continue

        # Skip preferred shares — fixed-income-like, not a growth/thematic pick
        if PREFERRED_SHARE_RE.search(symbol):
            skipped_preferred += 1
            continue

        filtered.append({
            "symbol": symbol,
            "name": s.get("companyName") or s.get("name") or "",
            "price": price,
            "exchange": exchange,
        })

    # ── Depositary receipt check ───────────────────────────────────────────────
    # CDRs (Broadcom, Eli Lilly, Cisco...) are CAD-hedged wrappers around foreign
    # stocks, not TSX operating companies. The screener doesn't expose isAdr, so
    # this needs one profile call per surviving ticker — a few minutes, and the
    # universe is only refreshed occasionally.
    #
    # NOTE: do NOT filter on `country` instead. It's a domicile field — Brookfield,
    # Restaurant Brands, Newmont and ~70 other legitimate TSX names also read 'US'.
    # The reviewed list runs FIRST and is authoritative. FMP's isAdr flag is
    # False for every CIBC CDR, so the profile check below cannot find them; it
    # stays for true ADRs and as a second opinion. On 2026-09-30 that check
    # reported zero CDRs skipped while letting 122 into a 567-ticker universe.
    reviewed = load_cdr_exclusions()
    if reviewed:
        before = len(filtered)
        filtered = [x for x in filtered if x["symbol"] not in reviewed]
        skipped_reviewed = before - len(filtered)
        print()
        print(f"  Reviewed CDR list: {skipped_reviewed} excluded "
              f"({len(reviewed)} on file)")
    else:
        skipped_reviewed = 0
        print()
        print("  WARNING: cdr_exclusions.txt not found - depositary receipts "
              "will NOT be filtered.")

    print(f"\nChecking {len(filtered)} tickers for depositary receipts...")
    non_adr = []
    for s in tqdm(filtered, desc="Profiles", unit="ticker",
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]"):
        if is_depositary_receipt(s["symbol"]):
            skipped_adr += 1
        else:
            non_adr.append(s)
    filtered = non_adr

    # Sort alphabetically
    filtered.sort(key=lambda x: x["symbol"])

    # Write output
    with open(OUTPUT_FILE, "w") as f:
        f.write("\n".join(s["symbol"] for s in filtered))

    # Print summary
    print(f"Results:")
    print(f"  Total TSX stocks:    {len(filtered)}")
    print(f"  Skipped (TSXV):      {skipped_tsxv}")
    print(f"  Skipped (<${MIN_PRICE}): {skipped_price}")
    print(f"  Skipped (ETF):       {skipped_etf}")
    print(f"  Skipped (inactive):  {skipped_inactive}")
    print(f"  Skipped (preferred): {skipped_preferred}")
    print(f"  Skipped (reviewed CDR list): {skipped_reviewed}")
    print(f"  Skipped (FMP isAdr): {skipped_adr}")

    # A silent fail-open is how CDRs got in last time. Say so, in the summary,
    # where it cannot be missed.
    if _adr_check_failures:
        n = len(_adr_check_failures)
        print()
        print("  " + "!" * 56)
        print(f"  {n} ADR check(s) could not be completed (rate limit/timeout).")
        print("  Those tickers were KEPT, so this universe may contain CDRs.")
        print(f"    {', '.join(_adr_check_failures[:12])}"
              + (f" ... +{n - 12} more" if n > 12 else ""))
        print("  Re-run this script, or rely on 2_fmp_pipeline.py setting is_adr")
        print("  from the profile it fetches, then:")
        print("    python purge_tickers.py --preset adr-and-funds --dry-run")
        print("  " + "!" * 56)
    print(f"\nWritten to: {OUTPUT_FILE}")
    print(f"\nSample (first 10):")
    for s in filtered[:10]:
        print(f"  {s['symbol']:15} {s['name'][:40]:40} price={s['price']}")


if __name__ == "__main__":
    run()

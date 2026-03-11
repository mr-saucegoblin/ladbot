"""
validate_fmp.py — Check FMP API coverage for TSX universe.

Outputs:
  - fmp_coverage_report.txt  (human-readable summary)
  - fmp_covered.txt          (tickers FMP returns data for)
  - fmp_missing.txt          (tickers FMP has no data for)

Run once before building anything that depends on FMP data.
"""

import os
import time
import requests
import openpyxl
from dotenv import load_dotenv

load_dotenv()

FMP_KEY = os.getenv("FMP_API_KEY")
UNIVERSE_DIR = os.path.join(os.path.dirname(__file__), "Stock Universe")
XLSX_FILES = {
    "SPTSX":   "SPTSX as of Mar 07 20261.xlsx",
    "SPTSXS as of Mar 07 20261.xlsx": "SPTSXS as of Mar 07 20261.xlsx",
}

DELAY = 0.25  # seconds between FMP calls — stay well under rate limits


# ── Ticker loading (matches load_universe.py logic) ────────────────────────────

def to_yfinance_ticker(bloomberg: str) -> str:
    ticker = bloomberg.replace(" CT Equity", "").strip()
    ticker = ticker.replace("/", "-")
    if ticker.endswith("-U"):
        ticker = ticker + "N"
    return f"{ticker}.TO"


def load_tickers() -> list[tuple[str, str]]:
    """Returns list of (yfinance_ticker, company_name) tuples, deduplicated."""
    seen = {}
    files = [
        "SPTSX as of Mar 07 20261.xlsx",
        "SPTSXS as of Mar 07 20261.xlsx",
    ]
    for filename in files:
        path = os.path.join(UNIVERSE_DIR, filename)
        wb = openpyxl.load_workbook(path)
        ws = wb.active
        for row in ws.iter_rows(min_row=2, values_only=True):
            bloomberg_ticker, name = row[0], row[1]
            if not bloomberg_ticker or not name:
                continue
            ticker = to_yfinance_ticker(bloomberg_ticker)
            if ticker not in seen:
                seen[ticker] = name
    return list(seen.items())


# ── FMP check ─────────────────────────────────────────────────────────────────

def check_fmp(ticker: str) -> dict:
    """
    Query FMP quote endpoint for a single ticker.
    Returns dict with keys: ticker, has_data, price, volume, error
    """
    url = f"https://financialmodelingprep.com/api/v3/quote/{ticker}"
    try:
        r = requests.get(url, params={"apikey": FMP_KEY}, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data and isinstance(data, list) and len(data) > 0:
            q = data[0]
            return {
                "ticker": ticker,
                "has_data": True,
                "price": q.get("price"),
                "volume": q.get("volume"),
                "error": None,
            }
        else:
            return {"ticker": ticker, "has_data": False, "price": None, "volume": None, "error": "empty response"}
    except Exception as e:
        return {"ticker": ticker, "has_data": False, "price": None, "volume": None, "error": str(e)}


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    print("=== FMP Coverage Validation ===\n")

    tickers = load_tickers()
    print(f"Universe: {len(tickers)} unique tickers\n")

    covered = []
    missing = []
    errors = []

    for i, (ticker, name) in enumerate(tickers[:10], 1):
        result = check_fmp(ticker)
        result["name"] = name

        if result["has_data"]:
            covered.append(result)
            status = f"OK    price={result['price']}"
        else:
            if result["error"] and result["error"] != "empty response":
                errors.append(result)
                status = f"ERROR {result['error']}"
            else:
                missing.append(result)
                status = "MISS"

        print(f"  [{i:3}/{len(tickers)}] {ticker:15} {status}")
        time.sleep(DELAY)

    # ── Summary ───────────────────────────────────────────────────────────────
    total = len(tickers)
    n_covered = len(covered)
    n_missing = len(missing)
    n_errors = len(errors)
    coverage_pct = n_covered / total * 100

    summary_lines = [
        "=== FMP Coverage Report ===",
        f"",
        f"Universe:  {total} tickers",
        f"Covered:   {n_covered} ({coverage_pct:.1f}%)",
        f"Missing:   {n_missing}",
        f"Errors:    {n_errors}",
        f"",
        f"--- Covered tickers ---",
    ]
    for r in covered:
        summary_lines.append(f"  {r['ticker']:15} {r['name'][:40]:40} price={r['price']}")

    summary_lines += ["", "--- Missing tickers (no FMP data) ---"]
    for r in missing:
        summary_lines.append(f"  {r['ticker']:15} {r['name'][:40]}")

    if errors:
        summary_lines += ["", "--- Errors ---"]
        for r in errors:
            summary_lines.append(f"  {r['ticker']:15} {r['error']}")

    report = "\n".join(summary_lines)

    with open("fmp_coverage_report.txt", "w") as f:
        f.write(report)

    with open("fmp_covered.txt", "w") as f:
        f.write("\n".join(r["ticker"] for r in covered))

    with open("fmp_missing.txt", "w") as f:
        f.write("\n".join(r["ticker"] for r in missing))

    print(f"\n{'='*40}")
    print(f"Covered: {n_covered}/{total} ({coverage_pct:.1f}%)")
    print(f"Missing: {n_missing}")
    print(f"Errors:  {n_errors}")
    print(f"\nFiles written:")
    print(f"  fmp_coverage_report.txt")
    print(f"  fmp_covered.txt")
    print(f"  fmp_missing.txt")


if __name__ == "__main__":
    run()

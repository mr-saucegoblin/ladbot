"""
One-time setup script. Run this before the weekly bot.

Steps:
  1. Read both Bloomberg xlsx files from Stock Universe/
  2. Deduplicate and convert tickers to yfinance format
  3. Enrich each company with yfinance sector/industry/summary
  4. Batch-send to Claude for thematic tagging
  5. Store everything in SQLite

Re-run any time you want to refresh the universe (e.g. new index constituents).
"""

import json
import os
import time
import anthropic
import openpyxl
import yfinance as yf
from datetime import datetime, timezone
from dotenv import load_dotenv
from database import init_db, get_conn

load_dotenv()

UNIVERSE_DIR = os.path.join(os.path.dirname(__file__), "Stock Universe")
XLSX_FILES = {
    "SPTSX":   "SPTSX as of Mar 07 20261.xlsx",
    "SPTSXSM": "SPTSXS as of Mar 07 20261.xlsx",
}

TAGGING_BATCH_SIZE = 40    # companies per Claude call
YFINANCE_DELAY = 0.3       # seconds between yfinance calls to avoid rate limits
MODEL = "claude-sonnet-4-6"


# ── Ticker conversion ──────────────────────────────────────────────────────────

def to_yfinance_ticker(bloomberg: str) -> str:
    """Convert Bloomberg ticker to yfinance format."""
    ticker = bloomberg.replace(" CT Equity", "").strip()
    ticker = ticker.replace("/", "-")
    if ticker.endswith("-U"):
        ticker = ticker + "N"   # AP-U → AP-UN (unit trusts/REITs)
    return f"{ticker}.TO"


# ── Bloomberg xlsx reader ──────────────────────────────────────────────────────

def load_bloomberg_files() -> dict[str, dict]:
    """
    Read both xlsx files, deduplicate by ticker.
    Returns dict keyed by yfinance ticker.
    """
    companies = {}

    for source, filename in XLSX_FILES.items():
        path = os.path.join(UNIVERSE_DIR, filename)
        wb = openpyxl.load_workbook(path)
        ws = wb.active

        for row in ws.iter_rows(min_row=2, values_only=True):
            bloomberg_ticker, name, _, _, price = row
            if not bloomberg_ticker or not name:
                continue

            ticker = to_yfinance_ticker(bloomberg_ticker)

            if ticker not in companies:
                companies[ticker] = {
                    "ticker": ticker,
                    "bloomberg_ticker": bloomberg_ticker,
                    "name": name,
                    "price": price if isinstance(price, (int, float)) else None,
                    "source": source,
                }

        print(f"  Loaded {source}: {sum(1 for c in companies.values() if c['source'] == source)} companies")

    print(f"  Total unique companies: {len(companies)}")
    return companies


# ── yfinance enrichment ────────────────────────────────────────────────────────

def enrich_with_yfinance(companies: dict[str, dict]) -> dict[str, dict]:
    """
    Fetch sector, industry, and business summary for each ticker from yfinance.
    Skips tickers already in the DB (reuses cached data). Only fetches new ones.
    """
    # Load cached data from DB
    try:
        with get_conn() as conn:
            rows = conn.execute(
                "SELECT ticker, sector, industry, summary FROM companies"
            ).fetchall()
        cached = {r["ticker"]: dict(r) for r in rows}
    except Exception:
        cached = {}

    to_fetch = [t for t in companies if t not in cached]
    reused = len(companies) - len(to_fetch)
    if reused:
        print(f"  Reusing cached yfinance data for {reused} existing companies")
        for ticker, data in companies.items():
            if ticker in cached:
                data["sector"] = cached[ticker]["sector"] or ""
                data["industry"] = cached[ticker]["industry"] or ""
                data["summary"] = cached[ticker]["summary"] or ""

    if not to_fetch:
        print("  No new tickers to fetch from yfinance")
        return companies

    print(f"  Fetching yfinance data for {len(to_fetch)} new tickers...")
    for i, ticker in enumerate(to_fetch, 1):
        data = companies[ticker]
        try:
            info = yf.Ticker(ticker).info
            data["sector"] = info.get("sector", "")
            data["industry"] = info.get("industry", "")
            summary = info.get("longBusinessSummary", "")
            data["summary"] = summary[:400] if summary else ""
        except Exception as e:
            data["sector"] = ""
            data["industry"] = ""
            data["summary"] = ""
            print(f"  Warning: yfinance failed for {ticker} — {e}")

        if i % 50 == 0:
            print(f"  yfinance enrichment: {i}/{len(to_fetch)}")
        time.sleep(YFINANCE_DELAY)

    return companies


# ── Claude tagging ─────────────────────────────────────────────────────────────

def _build_tagging_prompt(batch: list[dict]) -> str:
    companies_block = "\n".join(
        f"{i+1}. {c['name']} | Sector: {c['sector']} | Industry: {c['industry']} | "
        f"Summary: {c['summary'][:200] if c['summary'] else 'N/A'}"
        for i, c in enumerate(batch)
    )

    return f"""You are a Canadian equity market analyst building a thematic investment database.

For each company below, assign 1–4 concise thematic investment tags based on the company's PRIMARY business only.

Rules:
- Tag only what the company primarily does — its core revenue driver.
- Do NOT tag byproduct metals. If a gold miner produces silver as a byproduct, tag it "gold mining" only.
- Do NOT tag conglomerate subsidiaries. Tag the dominant business line.
- A pure-play uranium miner should get "uranium". A diversified miner that happens to have uranium assets should get "diversified mining".

Tags should be specific and investable (e.g. "gold mining", "uranium", "oil sands", "lithium",
"copper", "natural gas", "renewable energy", "AI infrastructure", "semiconductors", "banking",
"insurance", "cannabis", "REITs", "agriculture", "fertilizer", "defence", "rare earths",
"silver", "zinc", "coal", "potash", "telecom", "pipelines", "diversified mining").

Use existing tags where they fit. Invent new ones only if truly needed.

## Companies
{companies_block}

Respond ONLY with valid JSON. No markdown, no explanation outside the JSON.

{{
  "companies": [
    {{
      "name": "<exact company name from input>",
      "tags": ["tag1", "tag2"]
    }}
  ]
}}"""


def tag_companies_with_claude(companies: dict[str, dict]) -> dict[str, dict]:
    """
    Send companies to Claude in batches for thematic tagging.
    Adds a 'tags' field (comma-separated string) to each company.
    """
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    company_list = list(companies.values())
    total = len(company_list)
    tagged = 0

    for i in range(0, total, TAGGING_BATCH_SIZE):
        batch = company_list[i : i + TAGGING_BATCH_SIZE]
        batch_num = i // TAGGING_BATCH_SIZE + 1
        print(f"  Tagging batch {batch_num} ({len(batch)} companies)...")

        # Retry up to 3 times with backoff on empty/failed responses
        success = False
        for attempt in range(3):
            if attempt > 0:
                wait = 10 * attempt
                print(f"    Retry {attempt}/2 (waiting {wait}s)...")
                time.sleep(wait)

            message = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                messages=[{"role": "user", "content": _build_tagging_prompt(batch)}],
            )

            raw = message.content[0].text.strip()
            if not raw:
                continue  # empty response — retry

            # Strip markdown code fences if Claude wrapped the JSON
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[-1]  # drop first line (```json or ```)
                raw = raw.rsplit("```", 1)[0].strip()

            try:
                data = json.loads(raw)
                results = data.get("companies", [])
                name_to_tags = {r["name"]: r.get("tags", []) for r in results}
                for company in batch:
                    tags = name_to_tags.get(company["name"], [])
                    company["tags"] = ", ".join(tags)
                    tagged += 1
                success = True
                break
            except Exception as e:
                print(f"    Parse error: {e} | Response preview: {raw[:120]!r}")
                continue

        if not success:
            print(f"  Warning: batch {batch_num} failed after 3 attempts — tags left empty")
            for company in batch:
                company["tags"] = ""

        time.sleep(3)  # pause between batches to avoid rate limits

    print(f"  Tagged {tagged}/{total} companies")
    return companies


# ── SQLite storage ─────────────────────────────────────────────────────────────

def save_to_db(companies: dict[str, dict]):
    """Upsert all companies into SQLite."""
    now = datetime.now(timezone.utc).isoformat()
    rows = [
        (
            c["ticker"], c["bloomberg_ticker"], c["name"],
            c.get("sector", ""), c.get("industry", ""), c.get("summary", ""),
            c.get("price"), c["source"], c.get("tags", ""), now,
        )
        for c in companies.values()
    ]

    with get_conn() as conn:
        conn.executemany("""
            INSERT INTO companies
                (ticker, bloomberg_ticker, name, sector, industry, summary, price, source, tags, tagged_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                name=excluded.name, sector=excluded.sector, industry=excluded.industry,
                summary=excluded.summary, price=excluded.price, tags=excluded.tags,
                tagged_at=excluded.tagged_at
        """, rows)

    print(f"  Saved {len(rows)} companies to database")


# ── Main ───────────────────────────────────────────────────────────────────────

def run():
    print("=== Ladbot TSX — Universe Loader ===\n")

    print("Step 1: Initialising database...")
    init_db()

    print("\nStep 2: Reading Bloomberg files...")
    companies = load_bloomberg_files()

    print("\nStep 3: Enriching with yfinance (this takes a few minutes)...")
    companies = enrich_with_yfinance(companies)

    print("\nStep 4: Tagging companies with Claude...")
    companies = tag_companies_with_claude(companies)

    print("\nStep 5: Saving to database...")
    save_to_db(companies)

    print("\nDone. Universe is ready.")
    print("Sample tags from first 5 companies:")
    for c in list(companies.values())[:5]:
        print(f"  {c['ticker']:15} {c['name'][:35]:35} → {c['tags']}")


if __name__ == "__main__":
    run()

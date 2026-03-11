"""
Runtime theme-to-company mapper.
Given a theme label (e.g. "Critical Minerals & Battery Metals"),
asks Claude to match it against companies in the DB by their tags,
then validates with a live yfinance price fetch.
"""

import json
import os
import anthropic
import yfinance as yf
from dotenv import load_dotenv
from database import get_conn

load_dotenv()

MODEL = "claude-sonnet-4-6"
TOP_COMPANIES = 3  # how many stock picks to return per theme


def _fetch_all_companies() -> list[dict]:
    """Pull all tagged companies from SQLite."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT ticker, name, sector, industry, tags FROM companies WHERE tags != '' ORDER BY name"
        ).fetchall()
    return [dict(r) for r in rows]


def _build_mapping_prompt(theme: str, theme_rationale: str, companies: list[dict]) -> str:
    companies_block = "\n".join(
        f"{i+1}. {c['ticker']} | {c['name']} | {c['industry']} | tags: {c['tags']}"
        for i, c in enumerate(companies)
    )
    return f"""You are a Canadian equity market analyst selecting TSX stock picks for a thematic investing report.

## This Week's Top Theme
{theme}

## Why This Theme Is Trending
{theme_rationale}

## TSX Company Universe (ticker | name | industry | thematic tags)
{companies_block}

## Instructions
- Select the {TOP_COMPANIES} TSX companies most directly exposed to this week's theme.
- Base your selection on the thematic tags and industry — not just name recognition.
- Prefer pure-play exposure over diversified conglomerates.
- For each pick, write one sentence explaining why it fits the theme this week.

Respond ONLY with valid JSON. No markdown, no explanation outside the JSON.

{{
  "picks": [
    {{
      "ticker": "<ticker e.g. CCO.TO>",
      "name": "<company name>",
      "reason": "<one sentence>"
    }}
  ]
}}"""


def _fetch_price_data(ticker: str) -> dict | None:
    """Fetch current price and 1-week return from yfinance. Returns None if unavailable."""
    try:
        hist = yf.Ticker(ticker).history(period="5d")
        if hist.empty or len(hist) < 2:
            return None
        price = round(hist["Close"].iloc[-1], 2)
        week_return = round((hist["Close"].iloc[-1] / hist["Close"].iloc[0] - 1) * 100, 1)
        return {"price": price, "week_return": week_return}
    except Exception:
        return None


def map_theme_to_companies(theme: str, rationale: str) -> list[dict]:
    """
    Given a theme label and rationale, returns top TSX company picks
    with live prices. Each dict has: ticker, name, reason, price.
    """
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    all_companies = _fetch_all_companies()

    if not all_companies:
        raise RuntimeError("Company database is empty. Run load_universe.py first.")

    print(f"Matching theme '{theme}' against {len(all_companies)} companies...")

    message = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        messages=[{"role": "user", "content": _build_mapping_prompt(theme, rationale, all_companies)}],
    )

    try:
        data = json.loads(message.content[0].text.strip())
    except Exception:
        import re
        match = re.search(r"\{.*\}", message.content[0].text, re.DOTALL)
        data = json.loads(match.group()) if match else {"picks": []}

    picks = data.get("picks", [])

    # Enrich with live price data
    print("Fetching price data...")
    for pick in picks:
        data = _fetch_price_data(pick["ticker"])
        if data:
            pick["price"] = data["price"]
            pick["week_return"] = data["week_return"]
        else:
            pick["price"] = None
            pick["week_return"] = None

    # Filter out any picks where price fetch failed (likely bad ticker)
    valid = [p for p in picks if p["price"] is not None]
    if len(valid) < len(picks):
        dropped = [p["ticker"] for p in picks if p["price"] is None]
        print(f"  Warning: dropped {dropped} (no live price found)")

    return valid

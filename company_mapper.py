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
TOP_COMPANIES = 1  # single best pick per theme


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
- Select the SINGLE best TSX company that BENEFITS from this week's theme — rising revenues, expanding margins, or direct tailwinds.
- Do NOT pick companies that are hurt by the theme (e.g. rate-sensitive stocks for a high-rates theme, importers for a tariff theme).
- Base your selection on the thematic tags and industry — not just name recognition.
- Prefer pure-play exposure over diversified conglomerates.
- Write one sentence explaining why this company stands to gain from this theme right now.

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


def _market_cap_label(market_cap: int | None) -> str | None:
    if not market_cap:
        return None
    if market_cap >= 10_000_000_000:
        return "Large Cap"
    if market_cap >= 2_000_000_000:
        return "Mid Cap"
    return "Small Cap"


def _fetch_price_data(ticker: str) -> dict | None:
    """Fetch price, week return, and supplementary data from yfinance. Returns None if price unavailable."""
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="5d")
        if hist.empty or len(hist) < 2:
            return None
        price = round(hist["Close"].iloc[-1], 2)
        week_return = round((hist["Close"].iloc[-1] / hist["Close"].iloc[0] - 1) * 100, 1)

        # Extra fields from info — all optional, fail gracefully
        sector = None
        try:
            info = t.info
            week_high = info.get("fiftyTwoWeekHigh")
            week_low = info.get("fiftyTwoWeekLow")
            vol = info.get("volume")
            avg_vol = info.get("averageVolume")
            volume_ratio = round(vol / avg_vol, 1) if vol and avg_vol else None
            market_cap_label = _market_cap_label(info.get("marketCap"))
            sector = info.get("sector")
        except Exception:
            week_high = week_low = volume_ratio = market_cap_label = None

        return {
            "price": price,
            "week_return": week_return,
            "week_high": week_high,
            "week_low": week_low,
            "volume_ratio": volume_ratio,
            "market_cap_label": market_cap_label,
            "sector": sector,
        }
    except Exception:
        return None


def map_theme_to_companies(theme: str, rationale: str, exclude_sectors: set[str] | None = None) -> list[dict]:
    """
    Given a theme label and rationale, returns top TSX company picks
    with live prices. Each dict has: ticker, name, reason, price.
    exclude_sectors: sector strings already used by prior picks — filtered out before Claude sees the list.
    """
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    all_companies = _fetch_all_companies()

    if not all_companies:
        raise RuntimeError("Company database is empty. Run load_universe.py first.")

    if exclude_sectors:
        all_companies = [c for c in all_companies if c.get("sector") not in exclude_sectors]

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
            pick.update(data)
        else:
            pick["price"] = None
            pick["week_return"] = None
            pick["week_high"] = None
            pick["week_low"] = None
            pick["volume_ratio"] = None
            pick["market_cap_label"] = None

    # Filter out any picks where price fetch failed (likely bad ticker)
    valid = [p for p in picks if p["price"] is not None]
    if len(valid) < len(picks):
        dropped = [p["ticker"] for p in picks if p["price"] is None]
        print(f"  Warning: dropped {dropped} (no live price found)")

    return valid

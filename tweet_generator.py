"""
Generate a Discord post thread.
Hook + one post per theme.
"""

import json
import os
import anthropic
from datetime import date
from dotenv import load_dotenv

load_dotenv()

MODEL = "claude-sonnet-4-6"


def _build_thread_prompt(theme_picks: list[dict], run_date: str, headline_count: int = 0) -> str:
    total_posts = len(theme_picks) + 1  # hook + themes

    # Build theme names list for the hook
    theme_labels = " | ".join(entry["theme"]["label"] for entry in theme_picks)

    themes_block = ""
    for i, entry in enumerate(theme_picks, 1):
        theme = entry["theme"]
        picks = entry["picks"]
        pick = picks[0] if picks else None
        if not pick:
            continue

        price_str = f"${pick['price']}" if pick.get("price") else "N/A"
        ret_str = f"{'+' if (pick.get('week_return') or 0) >= 0 else ''}{pick['week_return']}%" if pick.get("week_return") is not None else "N/A"
        high_str = f"${pick['week_high']}" if pick.get("week_high") else "N/A"
        low_str = f"${pick['week_low']}" if pick.get("week_low") else "N/A"
        vol_str = f"{pick['volume_ratio']}x avg" if pick.get("volume_ratio") else "N/A"
        cap_str = pick.get("market_cap_label") or "N/A"

        themes_block += (
            f"\nTheme {i}: {theme['label']}\n"
            f"Rationale: {theme['rationale']}\n"
            f"Pick: {pick['ticker']} | {pick['name']} | {cap_str}\n"
            f"  Price: {price_str} | Week return: {ret_str}\n"
            f"  52-wk range: {low_str} – {high_str} | Volume: {vol_str}\n"
        )

    return f"""You are writing a weekly TSX thematic watchlist for a Discord server.
Week of: {run_date}

## Data
{themes_block}

## Instructions
Write exactly {total_posts} posts.

Post 1 — Hook:
2-3 lines max. First line: "TSX Thematic Watchlist — Week of {run_date}". Second line: mention {headline_count} articles scanned, then list the themes by name exactly as given: {theme_labels}. End with a down-arrow emoji ↓.

Posts 2 to {total_posts} — One per theme, one stock pick each:
Each post must stand completely alone. Use Discord markdown (** for bold, no hashtags).

Format each theme post exactly like this:

**__[emoji] [Theme name]__**

[2-3 sentences explaining WHY this theme is moving right now. Be specific: macro catalyst, policy driver, commodity price, geopolitical event. Not generic.]

**[TICKER]** — [Short name] · [market_cap_label]
[2-3 sentences: what the company does, why it's the single best play for this theme this week vs all alternatives.]

💰 [price] · ↳ [week_return]% last week
📊 52-wk: [week_low] – [week_high] · Vol: [volume_ratio]x avg

## Rules
- Tone: confident, analytical, like a buy-side analyst writing a quick note. No fluff, no hedging.
- Use the exact tickers, prices, returns, and ranges from the data above.
- Short company names (e.g. "Barrick" not "Barrick Mining Corp").
- Format week return with + for positive (e.g. +4.2%) and nothing extra for negative (e.g. -1.8%).
- If any data field is N/A, omit that line rather than showing N/A.
- No Twitter/X references anywhere.

Respond ONLY with valid JSON, no markdown:
{{
  "posts": ["post 1 text", "post 2 text", ...]
}}"""


def generate_thread(theme_picks: list[dict], headline_count: int = 0) -> list[str]:
    """
    Generate a Discord post thread from theme picks.
    Returns a list of post strings ready to publish.
    """
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    run_date = date.today().strftime("%b %d, %Y")

    message = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": _build_thread_prompt(theme_picks, run_date, headline_count)}],
    )

    raw = message.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1]
        raw = raw.rsplit("```", 1)[0].strip()

    data = json.loads(raw)
    return data.get("posts", [])

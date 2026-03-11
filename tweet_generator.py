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


def _build_thread_prompt(theme_picks: list[dict], run_date: str) -> str:
    total_posts = len(theme_picks) + 1  # hook + themes

    themes_block = ""
    for i, entry in enumerate(theme_picks, 1):
        theme = entry["theme"]
        picks_lines = "\n".join(
            f"  - {p['ticker']} | {p['name']} | "
            f"{'+' if p.get('week_return', 0) >= 0 else ''}{p['week_return']}% last week"
            for p in entry["picks"] if p.get("week_return") is not None
        )
        themes_block += (
            f"\nTheme {i}: {theme['label']} (score {theme['score']}/10)\n"
            f"Rationale: {theme['rationale']}\n"
            f"Picks:\n{picks_lines}\n"
        )

    return f"""You are writing a weekly TSX thematic watchlist for a Discord server.
Week of: {run_date}

## Data
{themes_block}

## Instructions
Write exactly {total_posts} posts.

Post 1 — Hook (max 240 chars):
Short, punchy. State it's this week's TSX thematic watchlist. Tease {len(theme_picks)} themes. End with a down-arrow emoji.

Posts 2 to {total_posts} — One per theme:
Each post must stand completely alone. Use Discord markdown (** for bold, no hashtags).

Format each theme post exactly like this:

━━━━━━━━━━━━━━━━━━━━━━
**__[emoji] [Theme name]__**

[2-3 sentences explaining WHY this theme is moving right now. Be specific: macro catalyst, policy driver, commodity price, geopolitical event. Not generic.]

▸ **[TICKER]** — [Short name]
[2-3 sentences: what the company does, why it benefits from this theme this week, what makes it the right play vs peers.]
↳ [week_return]% last week

▸ **[TICKER]** — [Short name]
[2-3 sentences]
↳ [week_return]% last week

▸ **[TICKER]** — [Short name]
[2-3 sentences]
↳ [week_return]% last week

## Rules
- Tone: confident, analytical, like a buy-side analyst writing a quick note. No fluff, no hedging.
- Use the exact tickers and weekly returns from the data above.
- Short company names (e.g. "Barrick" not "Barrick Mining Corp").
- Format week return with + for positive (e.g. +4.2%) and nothing extra for negative (e.g. -1.8%).
- No Twitter/X references anywhere.

Respond ONLY with valid JSON, no markdown:
{{
  "posts": ["post 1 text", "post 2 text", ...]
}}"""


def generate_thread(theme_picks: list[dict]) -> list[str]:
    """
    Generate a Discord post thread from theme picks.
    Returns a list of post strings ready to publish.
    """
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    run_date = date.today().strftime("%b %d, %Y")

    message = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": _build_thread_prompt(theme_picks, run_date)}],
    )

    raw = message.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1]
        raw = raw.rsplit("```", 1)[0].strip()

    data = json.loads(raw)
    return data.get("posts", [])

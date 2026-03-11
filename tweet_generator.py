"""
Generate a post thread for X (Premium long-form format).
Hook + one long post per theme + CTA.
"""

import json
import os
import anthropic
from datetime import date
from dotenv import load_dotenv

load_dotenv()

MODEL = "claude-sonnet-4-6"


def _build_thread_prompt(theme_picks: list[dict], run_date: str) -> str:
    total_posts = len(theme_picks) + 2  # hook + themes + cta

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

    return f"""You are writing a weekly investing post thread for a Canadian markets account called @LadbotTSX.
Week of: {run_date}

## Data
{themes_block}

## Instructions
Write a thread of exactly {total_posts} posts.

Post 1 — Hook (max 240 chars):
Short, punchy. State it's last week's TSX thematic watchlist. Tease {len(theme_picks)} themes. End with a down-arrow.

Posts 2 to {len(theme_picks) + 1} — One per theme (long-form, 700–1000 chars each):
Each post must stand completely alone — a reader who only sees this one post gets the full picture.

Format:
[emoji] [Theme name]

[2-3 sentences explaining WHY this theme is moving right now. Be specific: macro catalyst, policy driver, commodity price, geopolitical event. Not generic.]

[For each pick, write 2-3 sentences: what the company does, exactly why it benefits from THIS theme THIS week, and what makes it the right play vs peers. Then show the return.]

• [TICKER] — [Short name]
  [2-3 sentence explanation of why this specific stock is the right play]
  [week_return]% last week

• [TICKER] — [Short name]
  [2-3 sentence explanation]
  [week_return]% last week

• [TICKER] — [Short name]
  [2-3 sentence explanation]
  [week_return]% last week

Post {total_posts} — CTA (max 240 chars):
Tell people to follow @LadbotTSX for weekly picks every Monday. Keep it short.

## Rules
- Tone: confident, analytical, like a buy-side analyst writing a quick note. No fluff, no hedging.
- Use the exact tickers and weekly returns from the data above.
- Short company names (e.g. "Barrick" not "Barrick Mining Corp").
- Format week return with + for positive (e.g. +4.2%) and nothing extra for negative (e.g. -1.8%).

Respond ONLY with valid JSON, no markdown:
{{
  "posts": ["post 1 text", "post 2 text", ...]
}}"""


def generate_thread(theme_picks: list[dict]) -> list[str]:
    """
    Generate a post thread from theme picks.
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

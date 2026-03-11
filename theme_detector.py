"""
Sends headlines to Claude and gets back a ranked list of trending investment themes.
Claude identifies themes freely — no predefined list, no ceiling on what can emerge.
Returns a list of dicts with label, score, and rationale.
"""

import json
import os
import re
import anthropic
from dotenv import load_dotenv

load_dotenv()

HEADLINE_CHUNK_SIZE = 80
MODEL = "claude-sonnet-4-6"
TOP_N_THEMES = 7   # max themes per chunk (before consolidation)
FINAL_N_THEMES = 7  # max themes after consolidation


def _parse_json(raw: str, context: str) -> dict | None:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            return json.loads(match.group())
        print(f"Warning: could not parse Claude response for {context}")
        return None


def _build_scan_prompt(headlines: list[str]) -> str:
    headlines_block = "\n".join(f"{i+1}. {h}" for i, h in enumerate(headlines))
    return f"""You are a Canadian equity market analyst. Below is a list of financial news headlines from the past week.

Identify up to {TOP_N_THEMES} distinct investment themes getting meaningful coverage, relevant to Canadian investors and the TSX.

## Headlines
{headlines_block}

## Instructions
- Derive themes organically from the headlines — no fixed categories.
- Each theme must be genuinely distinct. Do not create two themes that are sub-angles of the same story.
- Themes should be specific and actionable (e.g. "Nuclear Energy Revival" not just "Energy").
- Score each theme 1–10 based on volume and intensity of coverage.
- Only include themes with a score >= 3.
- Rank by score descending.
- Write a 1-sentence rationale grounded in specific headlines.

Respond ONLY with valid JSON. No markdown, no explanation outside the JSON.

{{
  "ranked_themes": [
    {{
      "label": "<concise theme name, 2-5 words>",
      "score": <integer 1-10>,
      "rationale": "<one sentence referencing specific headlines>"
    }}
  ]
}}"""


def _build_consolidation_prompt(raw_themes: list[dict]) -> str:
    themes_block = "\n".join(
        f"- {t['label']} (score: {t['score']}): {t['rationale']}"
        for t in raw_themes
    )
    return f"""You are a Canadian equity market analyst. Below is a raw list of investment themes identified from news headlines this week. Some themes overlap or are sub-angles of the same story.

## Raw Themes
{themes_block}

## Instructions
- Merge any themes that are clearly the same story or heavily overlapping (e.g. "Gold Price Surge" and "Gold and Precious Metals Rally" → "Gold & Precious Metals").
- Keep themes that are genuinely distinct even if related (e.g. "Critical Minerals" and "Uranium" can stay separate if both have strong independent coverage).
- Output the final top {FINAL_N_THEMES} themes after merging, re-scored and re-ranked.
- When merging, use the higher score and write a fresh rationale that covers the full merged story.

Respond ONLY with valid JSON. No markdown, no explanation outside the JSON.

{{
  "ranked_themes": [
    {{
      "label": "<concise theme name, 2-5 words>",
      "score": <number>,
      "rationale": "<one sentence>"
    }}
  ]
}}"""


def detect_themes(headlines: list[str]) -> list[dict]:
    """
    1. Scans headlines in chunks, collecting raw themes from each.
    2. Sends the raw theme list to Claude for consolidation and deduplication.
    Returns a clean, ranked list of distinct themes.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY not set in environment")

    client = anthropic.Anthropic(api_key=api_key)
    chunks = [headlines[i : i + HEADLINE_CHUNK_SIZE] for i in range(0, len(headlines), HEADLINE_CHUNK_SIZE)]

    raw_themes: list[dict] = []

    # --- Pass 1: scan each chunk ---
    for chunk_num, chunk in enumerate(chunks, 1):
        print(f"Scanning chunk {chunk_num}/{len(chunks)} ({len(chunk)} headlines)...")
        message = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            messages=[{"role": "user", "content": _build_scan_prompt(chunk)}],
        )
        data = _parse_json(message.content[0].text.strip(), f"chunk {chunk_num}")
        if data:
            raw_themes.extend(data.get("ranked_themes", []))

    if not raw_themes:
        return []

    # --- Pass 2: consolidate and deduplicate ---
    print(f"Consolidating {len(raw_themes)} raw themes...")
    message = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        messages=[{"role": "user", "content": _build_consolidation_prompt(raw_themes)}],
    )
    data = _parse_json(message.content[0].text.strip(), "consolidation")
    if not data:
        # Fall back to returning raw themes sorted by score if consolidation fails
        seen, results = set(), []
        for t in sorted(raw_themes, key=lambda x: x["score"], reverse=True):
            if t["label"].lower() not in seen:
                seen.add(t["label"].lower())
                results.append(t)
        return results[:FINAL_N_THEMES]

    return data.get("ranked_themes", [])

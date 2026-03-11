"""
Ladbot TSX — Phase 1 + 2: Theme scanner → TSX company picks.
"""

from news_scanner import fetch_headlines
from theme_detector import detect_themes
from company_mapper import map_theme_to_companies
from tweet_generator import generate_thread


def run():
    print("=== Ladbot TSX ===\n")

    print("Step 1: Fetching headlines from RSS feeds...")
    headlines = fetch_headlines()

    if not headlines:
        print("No headlines fetched. Check your internet connection.")
        return

    print(f"\nStep 2: Sending {len(headlines)} headlines to Claude for theme analysis...")
    themes = detect_themes(headlines)

    if not themes:
        print("No themes detected above threshold.")
        return

    print("\n=== Trending Investment Themes This Week ===\n")
    for rank, theme in enumerate(themes, 1):
        print(f"#{rank}  {theme['label']}  (score: {theme['score']}/10)")
        print(f"     {theme['rationale']}")
        print()

    top_themes = themes[:3]
    print(f"\nStep 3: Mapping top {len(top_themes)} themes to TSX companies...")

    theme_picks = []
    for theme in top_themes:
        picks = map_theme_to_companies(theme["label"], theme["rationale"])
        if picks:
            theme_picks.append({"theme": theme, "picks": picks})

    if not theme_picks:
        print("No valid company picks found.")
        return

    print("\n=== Top TSX Thematic Picks This Week ===\n")
    for entry in theme_picks:
        theme = entry["theme"]
        print(f"── {theme['label']}  (score: {theme['score']}/10) ──")
        for i, pick in enumerate(entry["picks"], 1):
            ret = pick.get("week_return")
            ret_str = f"{'+' if ret >= 0 else ''}{ret}% wk" if ret is not None else "N/A"
            print(f"  #{i}  {pick['ticker']}  {pick['name']}  ({ret_str})")
            print(f"       {pick['reason']}")
        print()

    print("Step 4: Generating tweet thread...")
    tweets = generate_thread(theme_picks)

    print("\n=== Post Thread Preview ===\n")
    for i, post in enumerate(tweets, 1):
        print(f"── Post {i}/{len(tweets)} ({len(post)} chars) ──")
        print(post)
        print()


if __name__ == "__main__":
    run()

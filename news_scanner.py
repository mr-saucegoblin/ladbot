"""
Fetches financial news headlines from Canadian and global RSS feeds.
Returns a deduplicated list of headline strings for theme analysis.
No API key required.
"""

import socket
from datetime import datetime, timezone, timedelta
import feedparser
import requests

socket.setdefaulttimeout(10)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Broad sources cover many topics — uncapped so they reflect the full market picture.
# Niche sources are topic-specific (gold, oil, defence, etc.) — capped so they can't
# single-handedly inflate a theme score just by volume.
NICHE_CAP = 15  # max headlines from any single niche/topic-specific source

BROAD_FEEDS = {
    # Canadian
    "Financial Post":          "https://financialpost.com/feed",
    "Yahoo Finance CA":        "https://ca.finance.yahoo.com/rss/topfinstories",
    "CBC Business":            "https://www.cbc.ca/cmlink/rss-business",
    "BNN Bloomberg":           "https://www.bnnbloomberg.ca/rss",
    "Globe and Mail Business": "https://www.theglobeandmail.com/business/?service=rss",
    # US & global macro
    "Reuters Business":        "https://feeds.reuters.com/reuters/businessNews",
    "Reuters Top News":        "https://feeds.reuters.com/reuters/topNews",
    "MarketWatch":             "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadline",
    "WSJ Markets":             "https://feeds.content.dowjones.io/public/rss/mw_marketpulse",
    "Yahoo Finance US":        "https://finance.yahoo.com/rss/topfinstories",
    "Seeking Alpha":           "https://seekingalpha.com/feed.xml",
    "Economist Finance":       "https://www.economist.com/finance-and-economics/rss.xml",
    "FT Markets":              "https://www.ft.com/rss/home/uk",
}

NICHE_FEEDS = {
    # Mining & metals
    "Stockhouse":              "https://stockhouse.com/rss/news",
    "Northern Miner":          "https://www.northernminer.com/feed/",
    "Junior Mining Network":   "https://www.juniorminingnetwork.com/feed",
    "Kitco News":              "https://www.kitco.com/rss/kitconews.rss",
    "Canadian Mining Journal": "https://www.canadianminingjournal.com/feed/",
    "Proactive Investors CA":  "https://www.proactiveinvestors.ca/rss/news_feed.rss",
    # Energy
    "OilPrice.com":            "https://oilprice.com/rss/main",
    "Rigzone":                 "https://www.rigzone.com/news/rss/rigzone_news.aspx",
    "Energy Now Canada":       "https://energynow.ca/feed/",
    # Defence
    "Breaking Defense":        "https://breakingdefense.com/feed/",
    "Defense News":            "https://www.defensenews.com/rss/",
    # Tech
    "IT World Canada":         "https://www.itworldcanada.com/feed",
    "TechCrunch":              "https://techcrunch.com/feed/",
    # Cannabis
    "MJBizDaily":              "https://mjbizdaily.com/feed/",
}

LOOKBACK_DAYS = 7


def _is_recent(entry) -> bool:
    published = entry.get("published_parsed")
    if not published:
        return True
    pub_dt = datetime(*published[:6], tzinfo=timezone.utc)
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    return pub_dt >= cutoff


def _fetch_feed(source: str, url: str, seen: set, cap: int | None = None) -> list[str]:
    """Fetch a single RSS feed and return new headlines not already in seen."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        feed = feedparser.parse(response.content)
        results = []
        for entry in feed.entries:
            if cap and len(results) >= cap:
                break
            if not _is_recent(entry):
                continue
            title = entry.get("title", "").strip()
            if title and title not in seen:
                seen.add(title)
                results.append(title)
        print(f"  {source}: {len(results)} headlines" + (f" (capped at {cap})" if cap and len(results) == cap else ""))
        return results
    except Exception as e:
        print(f"  Warning: {source} feed failed — {e}")
        return []


def fetch_headlines() -> list[str]:
    """
    Pulls headlines from all configured RSS feeds.
    Broad sources (Canadian + global macro) are uncapped.
    Niche sources (topic-specific) are capped at NICHE_CAP to prevent
    any single topic from dominating theme scores.
    Returns a deduplicated flat list of headline strings.
    """
    seen: set[str] = set()
    headlines: list[str] = []

    for source, url in BROAD_FEEDS.items():
        headlines.extend(_fetch_feed(source, url, seen, cap=None))

    for source, url in NICHE_FEEDS.items():
        headlines.extend(_fetch_feed(source, url, seen, cap=NICHE_CAP))

    print(f"\nFetched {len(headlines)} unique headlines total")
    return headlines

"""
Fetches financial news headlines from Canadian and global RSS feeds,
plus FMP General News API for broad macro/global coverage.
Returns a deduplicated list of headline strings for theme analysis.
"""

import os
import socket
from datetime import datetime, timezone, timedelta
import feedparser
import requests
from dotenv import load_dotenv

load_dotenv()

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


def _is_recent(entry, cutoff: datetime) -> bool:
    published = entry.get("published_parsed")
    if not published:
        return True
    pub_dt = datetime(*published[:6], tzinfo=timezone.utc)
    return pub_dt >= cutoff


def _fetch_feed(source: str, url: str, seen: set, cutoff: datetime, cap: int | None = None) -> list[str]:
    """Fetch a single RSS feed and return new headlines not already in seen."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        feed = feedparser.parse(response.content)
        results = []
        for entry in feed.entries:
            if cap and len(results) >= cap:
                break
            if not _is_recent(entry, cutoff):
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


def _fetch_fmp_general_news(seen: set, cutoff: datetime) -> list[str]:
    """Pull up to 250 general news headlines from FMP, filtered to cutoff."""
    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        print("  Warning: FMP_API_KEY not set — skipping FMP general news")
        return []
    try:
        response = requests.get(
            "https://financialmodelingprep.com/stable/news/general-latest",
            params={"limit": 250, "page": 0, "apikey": api_key},
            timeout=15,
        )
        response.raise_for_status()
        articles = response.json()
        results = []
        for article in articles:
            pub_str = article.get("publishedDate", "")
            if pub_str:
                try:
                    pub_dt = datetime.strptime(pub_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    if pub_dt < cutoff:
                        continue
                except ValueError:
                    pass
            title = (article.get("title") or "").strip()
            if title and title not in seen:
                seen.add(title)
                results.append(title)
        print(f"  FMP General News: {len(results)} headlines")
        return results
    except Exception as e:
        print(f"  Warning: FMP general news failed — {e}")
        return []


def fetch_headlines(lookback_hours: int = LOOKBACK_DAYS * 24) -> list[str]:
    """
    Pulls headlines from all configured RSS feeds plus FMP General News.
    Broad sources (Canadian + global macro) are uncapped.
    Niche sources (topic-specific) are capped at NICHE_CAP to prevent
    any single topic from dominating theme scores.
    Returns a deduplicated flat list of headline strings.
    lookback_hours controls how far back to look (default: 7 days for weekly scan).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    seen: set[str] = set()
    headlines: list[str] = []

    for source, url in BROAD_FEEDS.items():
        headlines.extend(_fetch_feed(source, url, seen, cutoff, cap=None))

    for source, url in NICHE_FEEDS.items():
        headlines.extend(_fetch_feed(source, url, seen, cutoff, cap=NICHE_CAP))

    headlines.extend(_fetch_fmp_general_news(seen, cutoff))

    print(f"\nFetched {len(headlines)} unique headlines total")
    return headlines

"""
Fetches financial news headlines from Canadian RSS feeds.
Returns a deduplicated list of headline strings for theme analysis.
No API key required.
"""

import socket
from datetime import datetime, timezone, timedelta
import feedparser
import requests

socket.setdefaulttimeout(10)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

RSS_FEEDS = {
    "Financial Post":        "https://financialpost.com/feed",
    "Yahoo Finance CA":      "https://ca.finance.yahoo.com/rss/topfinstories",
    "Stockhouse":            "https://stockhouse.com/rss/news",
    "CBC Business":          "https://www.cbc.ca/cmlink/rss-business",
    "BNN Bloomberg":         "https://www.bnnbloomberg.ca/rss",
    "Northern Miner":        "https://www.northernminer.com/feed/",
    "Junior Mining Network": "https://www.juniorminingnetwork.com/feed",
    "Kitco News":            "https://www.kitco.com/rss/kitconews.rss",
    "Canadian Mining Journal": "https://www.canadianminingjournal.com/feed/",
    "Proactive Investors CA": "https://www.proactiveinvestors.ca/rss/news_feed.rss",
    # Energy & commodities
    "OilPrice.com":          "https://oilprice.com/rss/main",
    "Rigzone":               "https://www.rigzone.com/news/rss/rigzone_news.aspx",
    "Energy Now Canada":     "https://energynow.ca/feed/",
    # Defence
    "Breaking Defense":      "https://breakingdefense.com/feed/",
    "Defense News":          "https://www.defensenews.com/rss/",
    # Tech
    "IT World Canada":       "https://www.itworldcanada.com/feed",
    "TechCrunch":            "https://techcrunch.com/feed/",
    # Cannabis
    "MJBizDaily":            "https://mjbizdaily.com/feed/",
    # Broader markets
    "Reuters Business":      "https://feeds.reuters.com/reuters/businessNews",
    "MarketWatch":           "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadline",
    "Globe and Mail Business": "https://www.theglobeandmail.com/business/?service=rss",
}

LOOKBACK_DAYS = 7


def _is_recent(entry) -> bool:
    published = entry.get("published_parsed")
    if not published:
        return True
    pub_dt = datetime(*published[:6], tzinfo=timezone.utc)
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    return pub_dt >= cutoff


def fetch_headlines() -> list[str]:
    """
    Pulls headlines from all configured RSS feeds using requests for reliability.
    Returns a deduplicated flat list of headline strings.
    """
    seen = set()
    headlines = []

    for source, url in RSS_FEEDS.items():
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            feed = feedparser.parse(response.content)
            count = 0
            for entry in feed.entries:
                if not _is_recent(entry):
                    continue
                title = entry.get("title", "").strip()
                if title and title not in seen:
                    seen.add(title)
                    headlines.append(title)
                    count += 1
            print(f"  {source}: {count} headlines")
        except Exception as e:
            print(f"  Warning: {source} feed failed — {e}")
            continue

    print(f"\nFetched {len(headlines)} unique headlines total")
    return headlines

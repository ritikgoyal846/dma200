# screener/services_news.py
from typing import List, Dict
from urllib.parse import quote_plus
import feedparser
import requests

# ➊ one reusable session with timeouts/retries
_session = requests.Session()
_session.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
})

def _company_query(symbol: str, name: str) -> str:
    base = symbol.replace(".NS", "")
    return f"{(name or base)} India"

def fetch_google_news(symbol: str, name: str, limit: int = 6, timeout: float = 6.0) -> List[Dict]:
    """
    Robust: fetch RSS with requests (timeout), then parse.
    Returns [] on any error.
    """
    try:
        q = _company_query(symbol, name)
        rss_url = f"https://news.google.com/rss/search?q={quote_plus(q)}&hl=en-IN&gl=IN&ceid=IN:en"
        r = _session.get(rss_url, timeout=timeout)
        r.raise_for_status()
        feed = feedparser.parse(r.content)

        items: List[Dict] = []
        for entry in feed.entries[:limit]:
            published = None
            try:
                # published_parsed may be missing
                if getattr(entry, "published_parsed", None):
                    t = entry.published_parsed
                    published = f"{t.tm_year:04d}-{t.tm_mon:02d}-{t.tm_mday:02d} {t.tm_hour:02d}:{t.tm_min:02d}"
            except Exception:
                published = None
            items.append({
                "title": entry.title,
                "link": entry.link,
                "published": published,
                "source": getattr(getattr(entry, "source", None), "title", None),
            })
        return items
    except Exception:
        return []

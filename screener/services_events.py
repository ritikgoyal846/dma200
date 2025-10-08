# screener/services_events.py
from typing import List, Dict, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime

BASE = "https://www.nseindia.com"
ANN_API = BASE + "/api/corporate-announcements"

def _session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=0.4, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-IN,en;q=0.9",
        "Accept": "application/json, text/plain, */*",
        "Connection": "keep-alive",
    })
    # warm-up to set cookies; NSE rejects requests without this
    s.get(BASE, timeout=6)
    return s

def fetch_nse_announcements(nse_symbol_no_ns: str, limit: int = 6, timeout: float = 8.0) -> List[Dict]:
    """Latest corporate announcements for a given NSE symbol (e.g., 'ICICIBANK')."""
    try:
        s = _session()
        r = s.get(ANN_API, params={"symbol": nse_symbol_no_ns.upper(), "index": "equities"}, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        rows = data.get("data") or data.get("rows") or data or []
        out = []
        for row in rows[:limit]:
            out.append({
                "date": row.get("sm_dt") or row.get("ANNOUNCEMENT_DATE") or row.get("ATTACHMENT-DATE"),
                "headline": row.get("HEADLINE") or row.get("sm_title") or row.get("subject"),
                "category": row.get("CATEGORY") or row.get("sm_cat"),
                "attachment": row.get("ATTACHMENT-LINK") or row.get("attachment"),
            })
        return out
    except Exception:
        return []

def _parse_dd_mmm_yyyy(d: Optional[str]) -> Optional[datetime]:
    if not d: return None
    for fmt in ("%d-%b-%Y", "%d-%b-%y", "%d/%m/%Y"):
        try:
            return datetime.strptime(d, fmt)
        except Exception:
            continue
    return None

def has_upcoming_event(ann_list: List[Dict], window_days: int = 7) -> Dict:
    """
    Look for 'sensitive' events within +/- window_days of today.
    Returns a summary with boolean and the nearest event info.
    """
    from datetime import datetime, timedelta
    sensitive = ("result", "earnings", "board", "dividend", "conference", "agm", "egm")
    today = datetime.today()
    closest = None

    for a in ann_list:
        text = (a.get("headline") or "").lower()
        if not any(k in text for k in sensitive):
            continue
        dt = _parse_dd_mmm_yyyy(a.get("date"))
        if not dt:
            continue
        if abs((dt - today).days) <= window_days:
            if closest is None or abs((dt - today).days) < abs((closest["date"] - today).days):
                closest = {"date": dt, "headline": a.get("headline"), "category": a.get("category"), "attachment": a.get("attachment")}

    return {
        "has_upcoming": closest is not None,
        "next_event": {
            "date": closest["date"].strftime("%Y-%m-%d") if closest else None,
            "headline": closest["headline"] if closest else None,
            "category": closest["category"] if closest else None,
            "attachment": closest["attachment"] if closest else None,
        }
    }

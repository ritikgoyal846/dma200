from django.http import JsonResponse
from django.shortcuts import render
from django.core.cache import cache
from .models import Ticker
from .services import scan_at_200dma
from .services_news import fetch_google_news
from concurrent.futures import ThreadPoolExecutor, as_completed
from .services_events import fetch_nse_announcements, has_upcoming_event


# Home page with table

def home(request):
    return render(request, "screener/index.html")

# API: /api/scan?tol=0.003

def api_scan(request):
    # ---- 1) Read query params ----
    # Tolerance (fraction, e.g., 0.003 => ±0.3%)
    try:
        tol = float(request.GET.get("tol", 0.003))
    except ValueError:
        tol = 0.003

    # Include extras via ?include=news
    include = (request.GET.get("include", "") or "").lower().split(",")
    include_news = "news" in include
    include_events = "events" in include 

    # Number of news items per matched symbol
    try:
        limit = int(request.GET.get("limit", 3))
    except ValueError:
        limit = 3

    try:
        event_window = int(request.GET.get("event_window", 15))  # NEW, default 15 days
    except ValueError:
        event_window = 15    

    # Cap how many matched symbols we enrich with news (protects latency)
    try:
        max_matches = int(request.GET.get("max", 20))
    except ValueError:
        max_matches = 20

    # ---- 2) Cache lookup ----
    cache_key = f"scan_200dma_tol_{tol:.4f}_news_{int(include_news)}_events_{int(include_events)}_lim_{limit}_ew_{event_window}"
    data = cache.get(cache_key)
    if data:
        return JsonResponse(data, safe=False)

    # ---- 3) Run the 200-DMA scan (fast) ----
    tickers = list(Ticker.objects.values("symbol", "name", "in_nifty50"))
    matches = scan_at_200dma(tickers, tol=tol)

    # build base rows; if enrichment requested, cap to max_matches
    rows = []
    take = matches[:max_matches] if (include_news or include_events) else matches
    for r in take:
        base = r.symbol.replace(".NS", "")
        rows.append({
            "symbol": r.symbol,
            "display": f"{base}" + (" (NIFTY50)" if r.in_nifty50 else ""),
            "name": r.name,
            "close": r.close,
            "sma200": r.sma200,
            "distance_pct": r.distance_pct,
            "in_nifty50": r.in_nifty50,
        })

    # --- enrichment tasks (news/events) run concurrently per row ---
    if (include_news or include_events) and rows:
        def task(row):
            sym = row["symbol"]
            base_no_ns = sym.replace(".NS", "")
            name = row.get("name") or base_no_ns
            out = {"symbol": sym}
            if include_news:
                out["news"] = fetch_google_news(sym, name, limit=limit, timeout=6.0)
            if include_events:
                anns = fetch_nse_announcements(base_no_ns, limit=limit, timeout=8.0)
                out["events"] = {
                    "announcements": anns,
                    "summary": has_upcoming_event(anns, window_days=event_window)
                }
            return out

        # small pool keeps things responsive
        enrich_map = {}
        with ThreadPoolExecutor(max_workers=4) as ex:
            futures = [ex.submit(task, row) for row in rows]
            for fut in as_completed(futures):
                try:
                    result = fut.result()
                    enrich_map[result["symbol"]] = result
                except Exception:
                    # fail-soft: just skip enrichment for that symbol
                    pass

        # attach enrichment back onto rows
        for row in rows:
            extra = enrich_map.get(row["symbol"], {})
            if include_news:
                row["news"] = extra.get("news", [])
            if include_events:
                row["events"] = extra.get("events", {"announcements": [], "summary": {"has_upcoming": False, "next_event": None}})

    data = {"count": len(rows), "tolerance": tol, "results": rows}
    cache.set(cache_key, data, timeout=60 * 30)
    return JsonResponse(data, safe=False)
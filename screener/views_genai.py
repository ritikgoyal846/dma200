# screener/views_genai.py
from concurrent.futures import ThreadPoolExecutor, as_completed
from django.http import JsonResponse
from django.core.cache import cache
from .models import Ticker
from .services import scan_at_200dma, fetch_history
from .services_ta import compute_signals
from .services_genai import ask_llm_for_strategy
from .services_events import fetch_nse_announcements, has_upcoming_event  # if you added events
from .services_genai import llm_health

def api_advise_llm(request):
    # read params
    try: tol = float(request.GET.get("tol", 0.003))
    except ValueError: tol = 0.003
    try: max_matches = int(request.GET.get("max", 8))
    except ValueError: max_matches = 8
    try: event_window = int(request.GET.get("event_window", 7))
    except ValueError: event_window = 7

    # optional risk config for the prompt
    try: risk_per_trade_pct = float(request.GET.get("risk_pct", 1.0))
    except ValueError: risk_per_trade_pct = 1.0
    capital = request.GET.get("capital")  # string -> keep as number if you want
    prefer_credit = (request.GET.get("prefer_credit","0") in ("1","true","True"))

    cache_key = f"advise_llm_tol_{tol:.4f}_max_{max_matches}_ew_{event_window}_rp_{risk_per_trade_pct}_pc_{prefer_credit}_{capital}"
    cached = cache.get(cache_key)
    if cached:
        return JsonResponse(cached, safe=False)

    # 1) base matches
    universe = list(Ticker.objects.values("symbol","name","in_nifty50"))
    matches = scan_at_200dma(universe, tol=tol)
    take = matches[:max_matches]

    # 2) gather context per symbol (history -> signals, events)
    rows = []
    for m in take:
        df = fetch_history(m.symbol)
        if df is None or df.empty: 
            continue
        sig = compute_signals(df)
        base = m.symbol.replace(".NS","")
        # optional event window
        anns = fetch_nse_announcements(base, limit=4) if 'fetch_nse_announcements' in globals() else []
        evsum = has_upcoming_event(anns, window_days=event_window) if anns else {"has_upcoming": False,"next_event": None}
        rows.append({
            "symbol": m.symbol, "display": f"{base}" + (" (NIFTY50)" if m.in_nifty50 else ""),
            "in_nifty50": m.in_nifty50,
            "close": m.close, "sma200": m.sma200, "distance_pct": m.distance_pct,
            "signals": sig,
            "events": {"announcements": anns, "summary": evsum}
        })

    # 3) call LLM per symbol (small pool)
    def task(row):
        ctx = {
            "close": row["close"],
            "sma200": row["sma200"],
            "distance_pct": row["distance_pct"],
            "signals": row["signals"],
            "in_nifty50": row["in_nifty50"],
            "event_window_hit": row["events"]["summary"]["has_upcoming"],
            "risk_per_trade_pct": risk_per_trade_pct,
            "capital": float(capital) if (capital and capital.replace('.','',1).isdigit()) else None,
            "prefer_credit": prefer_credit,
            "expiry_hint": "near-month"
        }
        plan = ask_llm_for_strategy(row["symbol"], row["symbol"].replace(".NS",""), ctx, timeout=25.0)
        return row, plan

    out = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = [ex.submit(task, r) for r in rows]
        for f in as_completed(futures):
            row, plan = f.result()
            out.append({
                "symbol": row["symbol"],
                "display": row["display"],
                "close": row["close"],
                "sma200": row["sma200"],
                "distance_pct": row["distance_pct"],
                "signals": row["signals"],
                "events": row["events"]["summary"],
                "advice": plan  # JSON with bias, strategies, entry/stop/targets
            })

    data = {"count": len(out), "tolerance": tol, "results": out}
    cache.set(cache_key, data, timeout=30*60)
    return JsonResponse(data, safe=False)



def api_llm_health(request):
    return JsonResponse(llm_health(), safe=False)
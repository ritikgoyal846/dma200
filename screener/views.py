from django.http import JsonResponse
from django.shortcuts import render
from django.core.cache import cache
from .models import Ticker
from .services import scan_at_200dma

# Home page with table

def home(request):
    return render(request, "screener/index.html")

# API: /api/scan?tol=0.003

def api_scan(request):
    try:
        tol = float(request.GET.get("tol", 0.003))  # ±0.3% default
    except ValueError:
        tol = 0.003

    cache_key = f"scan_200dma_tol_{tol:.4f}"
    data = cache.get(cache_key)
    if not data:
        tickers = list(Ticker.objects.values("symbol", "name", "in_nifty50"))
        results = scan_at_200dma(tickers, tol=tol)
        payload = [
            {
                "symbol": r.symbol,
                "display": f"{r.symbol.replace('.NS','')}" + (" (NIFTY50)" if r.in_nifty50 else ""),
                "name": r.name,
                "close": r.close,
                "sma200": r.sma200,
                "distance_pct": r.distance_pct,
                "in_nifty50": r.in_nifty50,
            }
            for r in results
        ]
        data = {"count": len(payload), "tolerance": tol, "results": payload}
        cache.set(cache_key, data, timeout=60 * 30)  # 30 min cache

    return JsonResponse(data, safe=False)
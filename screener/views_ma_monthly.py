# screener/views_ma_monthly.py
from __future__ import annotations
from typing import List
from django.http import JsonResponse, HttpRequest
from django.core.cache import cache

from .services_ma_monthly import scan_monthly_ma, get_universe

def api_ma_monthly(request: HttpRequest):
    """
    GET /api/ma_monthly
      ?ma=50,100           # choose one or both (defaults to 50,100)
      ?tol=0.003           # tolerance as fraction (0.3% default)
      ?limit=50            # cap per MA bucket
      ?universe=nifty100   # or 'tickers' if you use DB universe loader
      ?symbols=RELIANCE,ICICIBANK   # override universe
      ?refresh=1           # bypass cache for this call

    Returns:
      {
        "universe": "...",
        "ma_list": [50, 100],
        "tol": 0.003,
        "limit": 50,
        "count_by_ma": { "50": N, "100": M },
        "results": {
           "50": [ {symbol, display, close, sma50, distance_pct, in_nifty50}, ... ],
           "100": [ ... ]
        }
      }
    """
    # --- Params ---
    ma_param = (request.GET.get("ma") or "50,100").strip()
    ma_list: List[int] = []
    for tok in ma_param.split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            val = int(tok)
        except ValueError:
            continue
        if val > 0:
            ma_list.append(val)
    if not ma_list:
        ma_list = [50, 100]

    try:
        tol = float(request.GET.get("tol", 0.003))
    except ValueError:
        tol = 0.003

    try:
        limit = int(request.GET.get("limit", 50))
    except ValueError:
        limit = 50

    universe = (request.GET.get("universe") or "nifty100").lower()
    refresh = request.GET.get("refresh") in ("1", "true", "True", "yes")

    # Symbols override
    raw_syms = request.GET.get("symbols")
    if raw_syms:
        symbols: List[str] = []
        for t in raw_syms.split(","):
            t = (t or "").strip().upper()
            if not t:
                continue
            symbols.append(t if t.endswith(".NS") else f"{t}.NS")
    else:
        symbols = get_universe(universe=universe)

    # Cache key that encodes knobs
    key = f"ma_monthly_{universe}_ma_{'-'.join(map(str,sorted(ma_list)))}_tol_{tol:.4f}_lim_{limit}_nsyms_{len(symbols)}"
    if not refresh:
        cached = cache.get(key)
        if cached:
            return JsonResponse(cached, safe=False)

    # Compute
    buckets = scan_monthly_ma(symbols=symbols, ma_list=ma_list, tol=tol, limit=limit)

    payload = {
        "universe": universe,
        "ma_list": ma_list,
        "tol": tol,
        "limit": limit,
        "count_by_ma": {str(m): len(buckets.get(m, [])) for m in ma_list},
        "results": {str(m): buckets.get(m, []) for m in ma_list},
    }

    if not refresh:
        cache.set(key, payload, timeout=60 * 30)  # 30 minutes

    return JsonResponse(payload, safe=False)

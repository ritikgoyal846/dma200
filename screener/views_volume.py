# screener/views_volume.py
from __future__ import annotations

from typing import List
from django.http import JsonResponse, HttpRequest
from django.core.cache import cache

from .services_volume import (
    load_universe_symbols,
    top_volume_spikes,
)

def api_volume_spikes(request: HttpRequest):
    """
    GET /api/volume_spikes
      ?factor=3.0
      ?limit=5
      ?window=100
      ?universe=nifty100 | tickers
      ?symbols=RELIANCE,ICICIBANK  (override universe)
      ?use_last_nonzero=1
      ?debug=1                     (if no matches, also return 'debug_top' preview)
      ?refresh=1                   (bypass cache once)

    Response:
      {
        "universe": "...",
        "factor": 3.0,
        "limit": 5,
        "window": 100,
        "count": N,
        "results": [ ... ],
        "debug_top": [ ... ]   # only when debug=1 and results empty
      }
    """
    # --- Params ---
    try:
        factor = float(request.GET.get("factor", 3.0))
    except ValueError:
        factor = 3.0

    try:
        limit = int(request.GET.get("limit", 5))
    except ValueError:
        limit = 5

    try:
        window = int(request.GET.get("window", 100))
    except ValueError:
        window = 100

    universe = (request.GET.get("universe") or "nifty100").lower()
    refresh = request.GET.get("refresh") in ("1", "true", "True", "yes")
    use_last_nonzero = request.GET.get("use_last_nonzero") in ("1", "true", "True", "yes")
    debug = request.GET.get("debug") in ("1", "true", "True", "yes")

    # Optional: override universe with explicit symbols list
    raw_syms = request.GET.get("symbols")
    if raw_syms:
        symbols: List[str] = []
        for t in raw_syms.split(","):
            t = (t or "").strip().upper()
            if not t:
                continue
            symbols.append(t if t.endswith(".NS") else f"{t}.NS")
    else:
        symbols = load_universe_symbols(universe=universe)

    # Cache key must include knobs that change output
    cache_key = f"volspike_{universe}_win_{window}_factor_{factor:.3f}_limit_{limit}_count_{len(symbols)}_ulz_{int(use_last_nonzero)}"
    if not refresh:
        data = cache.get(cache_key)
        if data:
            return JsonResponse(data, safe=False)

    matches = top_volume_spikes(
        symbols=symbols,
        factor=factor,
        limit=limit,
        window=window,
        use_last_nonzero=use_last_nonzero,
    )

    payload = {
        "universe": universe,
        "factor": factor,
        "limit": limit,
        "window": window,
        "count": len(matches),
        "results": matches,
    }

    # Optional debug: if empty, preview top 10 by multiple even if below factor
    if debug and not matches:
        preview = top_volume_spikes(
            symbols=symbols,
            factor=0.0,        # gather candidates
            limit=10,
            window=window,
            use_last_nonzero=use_last_nonzero,
        )
        payload["debug_top"] = preview

    if not refresh:
        cache.set(cache_key, payload, timeout=60 * 10)  # 10 minutes

    return JsonResponse(payload, safe=False)

# screener/services_ma_monthly.py
from __future__ import annotations
from typing import List, Dict, Optional, Tuple
import os
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
from django.core.cache import cache

# Reuse your universe loader if present
try:
    from .services_volume import load_universe_symbols  # reads screener/data/nifty100.csv
except Exception:
    load_universe_symbols = None

# Optional: NIFTY50 tagging from DB
try:
    from .models import Ticker
    HAS_TICKER = True
except Exception:
    HAS_TICKER = False

MAX_WORKERS = 8

def _fetch_monthly(symbol: str) -> Optional[pd.DataFrame]:
    """
    Monthly bars (1mo) for ~20y to cover 100-month SMA comfortably.
    """
    try:
        df = yf.Ticker(symbol).history(period="20y", interval="1mo", auto_adjust=False, actions=False)
        if df is None or df.empty:
            return None
        # Standardize columns
        if "Close" not in df.columns:
            cols = {c.lower(): c for c in df.columns}
            if "close" in cols:
                df.rename(columns={cols["close"]: "Close"}, inplace=True)
            else:
                return None
        # Drop leading NaNs
        df = df.dropna(subset=["Close"])
        return df
    except Exception:
        return None

def _sma_distance_monthly(df: pd.DataFrame, n: int) -> Optional[Tuple[float, float, float]]:
    """
    Returns (last_close, sma_n, distance_pct) where distance_pct = 100 * (close - sma) / sma
    on monthly timeframe. Requires >= n bars.
    """
    close = df["Close"].astype(float)
    if len(close) < n:
        return None
    sma = close.rolling(n).mean()
    last_close = float(close.iloc[-1])
    last_sma = float(sma.iloc[-1])
    if last_sma == 0 or pd.isna(last_sma):
        return None
    dist_pct = 100.0 * (last_close - last_sma) / last_sma
    return last_close, last_sma, dist_pct

def scan_monthly_ma(
    symbols: List[str],
    ma_list: List[int],
    tol: float = 0.003,
    limit: int = 50
) -> Dict[int, List[Dict]]:
    """
    For each MA window in ma_list (e.g., [50,100]), return matches where
    abs(distance_pct) <= tol*100 (because distance_pct is in percent).
    Results are sorted by absolute distance ascending (closest first).
    """
    # Preload NIFTY50 tag map if possible
    in50_map: Dict[str, bool] = {}
    if HAS_TICKER:
        try:
            in50_map = dict(Ticker.objects.values_list("symbol", "in_nifty50"))
        except Exception:
            in50_map = {}

    out: Dict[int, List[Dict]] = {m: [] for m in ma_list}

    def task(sym: str) -> Optional[Dict]:
        df = _fetch_monthly(sym)
        if df is None:
            return None
        # compute for each MA
        row: Dict = {"symbol": sym, "base": sym.replace(".NS", "")}
        for m in ma_list:
            comp = _sma_distance_monthly(df, n=m)
            if comp is None:
                row[f"sma{m}"] = None
                row[f"dist{m}_pct"] = None
            else:
                c, s, d = comp
                row["close"] = c  # same for all m
                row[f"sma{m}"] = s
                row[f"dist{m}_pct"] = round(d, 3)
        return row

    # fan out
    rows: List[Dict] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(task, s): s for s in symbols}
        for fut in as_completed(futs):
            r = fut.result()
            if r:
                rows.append(r)

    # collect matches for each MA
    for m in ma_list:
        matches: List[Dict] = []
        for r in rows:
            d = r.get(f"dist{m}_pct")
            s = r.get(f"sma{m}")
            c = r.get("close")
            if d is None or s is None or c is None:
                continue
            if abs(d) <= (tol * 100.0):
                in50 = bool(in50_map.get(r["symbol"], False))
                matches.append({
                    "symbol": r["symbol"],
                    "display": f"{r['base']}" + (" (NIFTY50)" if in50 else ""),
                    "close": round(c, 2),
                    f"sma{m}": round(s, 2),
                    "distance_pct": d,  # already in percent
                    "in_nifty50": in50,
                })
        # sort by |distance| asc; then by close desc
        matches.sort(key=lambda x: (abs(x["distance_pct"]), -x["close"]))
        out[m] = matches[:max(1, limit)]

    return out

def get_universe(universe: str = "nifty100") -> List[str]:
    if load_universe_symbols:
        return load_universe_symbols(universe=universe)
    # minimal fallback: read screener/data/nifty100.csv directly
    base_dir = os.path.dirname(os.path.dirname(__file__))
    data_path = os.path.join(base_dir, "data", "nifty100.csv")
    if os.path.exists(data_path):
        df = pd.read_csv(data_path)
        col = [c for c in df.columns if c.lower() == "symbol"]
        if col:
            syms = df[col[0]].dropna().astype(str).tolist()
        else:
            syms = df.iloc[:, 0].dropna().astype(str).tolist()
        syms = [s.strip().upper() for s in syms if s and str(s).strip()]
        syms = [s if s.endswith(".NS") else f"{s}.NS" for s in syms]
        return sorted(set(syms))
    return []

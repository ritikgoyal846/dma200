# screener/services_volume.py
from __future__ import annotations

from typing import List, Dict, Tuple, Optional
import os
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
from django.core.cache import cache

# If you have Ticker model for display/NIFTY50 tag, we’ll use it; otherwise we fall back safely
try:
    from .models import Ticker  # optional
    HAS_TICKER = True
except Exception:
    HAS_TICKER = False

# --- Tunables ---
HISTORY_DAYS = 220             # ask yfinance for ~1yr (enough to cover 100-day windows cleanly)
MAX_WORKERS = 8                # concurrency for downloads
PER_SYMBOL_TIMEOUT = 12.0      # seconds (yfinance internal timeouts are limited; we guard at call sites)


# ---------- Universe loading ----------
def _read_symbol_list_from_csv(path: str) -> List[str]:
    """
    Read a CSV and return a list of symbols (ensuring .NS suffix).
    Accepts either a 'symbol' column or treats the first column as symbols.
    """
    if not os.path.exists(path):
        return []
    try:
        df = pd.read_csv(path)
        # accept either 'symbol' column or first column
        col = "symbol" if "symbol" in {c.lower(): c for c in df.columns} else None
        if col:
            syms = df[[c for c in df.columns if c.lower() == "symbol"][0]].dropna().astype(str).tolist()
        else:
            syms = df.iloc[:, 0].dropna().astype(str).tolist()
        syms = [s.strip().upper() for s in syms if s and str(s).strip()]
        # ensure .NS suffix
        syms = [s if s.endswith(".NS") else f"{s}.NS" for s in syms]
        return syms
    except Exception:
        return []


def load_universe_symbols(universe: str = "nifty100") -> List[str]:
    """
    Universe options:
      - 'nifty100' -> reads screener/data/nifty100.csv
      - 'tickers'  -> pulls from DB Ticker table (if available)
    """
    key = f"universe_{universe}_v2"
    syms = cache.get(key)
    if syms:
        return syms

    base_dir = os.path.dirname(os.path.dirname(__file__))  # .../screener/..
    data_dir = os.path.join(base_dir, "data")

    if universe.lower() == "nifty100":
        syms = _read_symbol_list_from_csv(os.path.join(data_dir, "nifty100.csv"))
    elif universe.lower() == "tickers" and HAS_TICKER:
        syms = list(Ticker.objects.values_list("symbol", flat=True))
    else:
        syms = []

    syms = sorted(set(syms))
    cache.set(key, syms, 60 * 60 * 6)  # 6 hours
    return syms


# ---------- Market data ----------
def fetch_history(symbol: str, period_days: int = HISTORY_DAYS) -> Optional[pd.DataFrame]:
    """
    Fetch ~1y of daily bars. Returns None if empty or missing Volume column.
    """
    try:
        df = yf.Ticker(symbol).history(period="1y", interval="1d", auto_adjust=False, actions=False)
        if df is None or df.empty:
            return None
        if "Volume" not in df.columns:
            # try to normalize if volume is lowercased
            cols = {c.lower(): c for c in df.columns}
            if "volume" in cols:
                df.rename(columns={cols["volume"]: "Volume"}, inplace=True)
            else:
                return None
        return df
    except Exception:
        return None


# ---------- Core calc ----------
def compute_volume_ratio(
    df: pd.DataFrame,
    window: int = 100,
    use_last_nonzero: bool = False,
) -> Optional[Tuple[int, int, float]]:
    """
    Compare today's volume vs the max of the previous `window` trading days (excluding 'today').

    Returns:
      (today_vol, prev_window_max, ratio)
      OR None if not enough data.

    If use_last_nonzero=True, use the latest non-zero volume bar as 'today'
    and exclude that bar from the prior window.
    """
    vol = df["Volume"].dropna().astype("int64")

    # need bars to compute a solid window
    if len(vol) < max(20, window + 5):
        return None

    if use_last_nonzero:
        nz = vol[vol > 0]
        if nz.empty:
            return None
        today_vol = int(nz.iloc[-1])
        idx = nz.index[-1]
        prior = vol.loc[:idx].iloc[:-1].tail(window)
    else:
        today_vol = int(vol.iloc[-1])
        prior = vol.iloc[:-1].tail(window)

    if len(prior) == 0:
        return None

    prev_max = int(prior.max())
    if prev_max <= 0:
        return None

    ratio = float(today_vol / prev_max)
    return today_vol, prev_max, ratio


def top_volume_spikes(
    symbols: List[str],
    factor: float = 3.0,
    limit: int = 5,
    window: int = 100,
    use_last_nonzero: bool = False,
) -> List[Dict]:
    """
    Return top `limit` symbols where today_vol >= factor * max(prior `window` volumes).

    Output row fields:
      symbol, display, today_volume, prev_window_max, ratio (percent), multiple (raw),
      window, in_nifty50
    """
    results: List[Dict] = []

    # Pre-warm optional NIFTY50 tag map if Ticker exists
    in50_map: Dict[str, bool] = {}
    if HAS_TICKER:
        try:
            in50_map = dict(Ticker.objects.values_list("symbol", "in_nifty50"))
        except Exception:
            in50_map = {}

    def task(sym: str) -> Optional[Dict]:
        df = fetch_history(sym)
        if df is None:
            return None
        comp = compute_volume_ratio(df, window=window, use_last_nonzero=use_last_nonzero)
        if comp is None:
            return None
        today_vol, prev_max, ratio = comp
        if ratio < factor:
            return None
        base = sym.replace(".NS", "")
        in50 = bool(in50_map.get(sym, False))
        return {
            "symbol": sym,
            "display": f"{base}" + (" (NIFTY50)" if in50 else ""),
            "today_volume": today_vol,
            "prev_window_max": prev_max,
            "ratio": round(100.0 * ratio, 2),   # percent (e.g., 250% = 2.5x)
            "multiple": round(ratio, 2),        # raw multiple (e.g., 2.5)
            "window": window,
            "in_nifty50": in50,
        }

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(task, s): s for s in symbols}
        for fut in as_completed(futs):
            row = fut.result()
            if row:
                results.append(row)

    # sort by multiple desc then by absolute today volume
    results.sort(key=lambda r: (r["multiple"], r["today_volume"]), reverse=True)
    return results[: max(1, limit)]

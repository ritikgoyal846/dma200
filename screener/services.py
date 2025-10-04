from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Optional
import pandas as pd
import numpy as np
import yfinance as yf

@dataclass
class ScanResult:
    symbol: str
    name: str
    close: float
    sma200: float
    distance_pct: float  # signed distance (close - sma)/sma
    in_nifty50: bool


def fetch_history(symbol: str, period: str = "420d") -> Optional[pd.DataFrame]:
    try:
        df = yf.Ticker(symbol).history(period=period, interval="1d", auto_adjust=False)
        if df is None or df.empty:
            return None
        return df
    except Exception:
        return None


def scan_at_200dma(tickers: List[dict], tol: float = 0.003) -> List[ScanResult]:
    """
    Find tickers where last close is within ±tol of 200‑DMA.
    tol=0.003 -> ±0.3% (default)
    """
    results: List[ScanResult] = []

    def worker(tk):
        symbol = tk["symbol"]
        name = tk.get("name", "")
        in50 = tk.get("in_nifty50", False)
        df = fetch_history(symbol)
        if df is None or df.shape[0] < 210:
            return None
        close = df["Close"].astype(float)
        sma = close.rolling(200).mean()
        last_close = float(close.iloc[-1])
        last_sma = float(sma.iloc[-1])
        if np.isnan(last_sma) or last_sma == 0:
            return None
        dist = (last_close - last_sma) / last_sma
        if abs(dist) <= tol:
            return ScanResult(
                symbol=symbol,
                name=name,
                close=round(last_close, 2),
                sma200=round(last_sma, 2),
                distance_pct=float(round(dist * 100, 3)),
                in_nifty50=in50,
            )
        return None

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(worker, tk) for tk in tickers]
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                results.append(res)

    # Sort by absolute distance (closest to 200DMA first)
    return sorted(results, key=lambda r: abs(r.distance_pct))
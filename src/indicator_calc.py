# src/indicator_calc.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
import math
import pandas as pd
import numpy as np


def _to_df(candles: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Normaliza candles [{t,o,h,l,c,v}, ...] em DataFrame com colunas:
    ['t','o','h','l','c','v'] e índice temporal (se possível).
    """
    if not isinstance(candles, list):
        return pd.DataFrame(columns=["t", "o", "h", "l", "c", "v"])

    rows = []
    for it in candles:
        if not isinstance(it, dict):
            continue
        c = it.get("c", None)
        # ignora registros sem close
        if c is None:
            continue
        rows.append({
            "t": it.get("t", None),
            "o": it.get("o", None),
            "h": it.get("h", None),
            "l": it.get("l", None),
            "c": c,
            "v": it.get("v", None),
        })

    if not rows:
        return pd.DataFrame(columns=["t", "o", "h", "l", "c", "v"])

    df = pd.DataFrame(rows, columns=["t", "o", "h", "l", "c", "v"]).copy()

    # tenta converter tipos numéricos
    for col in ["o", "h", "l", "c", "v"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # tenta interpretar timestamp se for string
    if "t" in df.columns:
        try:
            ts = pd.to_datetime(df["t"], errors="coerce", utc=True)
            if ts.notna().any():
                df.index = ts
        except Exception:
            pass

    # remove linhas sem close numérico
    df = df[pd.to_numeric(df["c"], errors="coerce").notna()]
    df = df.sort_index()
    return df


def _rsi_wilder(close: pd.Series, period: int = 14) -> pd.Series:
    if close is None or len(close) == 0:
        return pd.Series(dtype=float)

    delta = close.diff()
    gain = (delta.where(delta > 0, 0.0)).abs()
    loss = (-delta.where(delta < 0, 0.0)).abs()

    # Suavização de Wilder: ewm(alpha=1/period)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    # onde avg_loss == 0 (só altas), RSI = 100
    rsi = rsi.fillna(100.0).clip(0, 100)
    return rsi


def _atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=float)

    close = df["c"]
    high = df["h"]
    low = df["l"]

    prev_close = close.shift(1)
    tr1 = (high - low).abs()
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # Wilder
    atr = tr.ewm(alpha=1 / period, adjust=False).mean()
    return atr


def _bbands(close: pd.Series, period: int = 20, mult: float = 2.0):
    if close is None or len(close) == 0:
        return (pd.Series(dtype=float), pd.Series(dtype=float), pd.Series(dtype=float))
    ma = close.rolling(period, min_periods=1).mean()
    std = close.rolling(period, min_periods=1).std(ddof=0)
    upper = ma + mult * std
    lower = ma - mult * std
    return ma, lower, upper


def compute_indicators_dict(candles_or_series: Any) -> Dict[str, Optional[float]]:
    """
    Aceita lista de candles de um único ativo e devolve um dict de indicadores.
    Campos impossíveis aqui (ex.: funding, OI) retornam None (viram em branco no CSV).
    """
    # candles_or_series deve ser lista de candles deste símbolo
    df = _to_df(candles_or_series if isinstance(candles_or_series, list) else [])
    if df.empty:
        return {
            "RSI14": None,
            "ATR14": None,
            "ATR14_PCT": None,
            "BB_MA20": None,
            "BB_LOWER": None,
            "BB_UPPER": None,
            "FUNDING": None,
            "OI_CHG_3D_PCT": None,
        }

    close = df["c"]
    rsi = _rsi_wilder(close, period=14)
    atr = _atr(df, period=14)
    ma20, bb_lower, bb_upper = _bbands(close, period=20, mult=2.0)

    last_close = close.iloc[-1] if len(close) else np.nan
    last_rsi = float(rsi.iloc[-1]) if len(rsi) else None
    last_atr = float(atr.iloc[-1]) if len(atr) else None
    atr_pct = float(last_atr / last_close * 100.0) if (last_atr is not None and last_close and not math.isnan(last_close) and last_close != 0) else None
    last_ma20 = float(ma20.iloc[-1]) if len(ma20) else None
    last_bb_lower = float(bb_lower.iloc[-1]) if len(bb_lower) else None
    last_bb_upper = float(bb_upper.iloc[-1]) if len(bb_upper) else None

    return {
        "RSI14": last_rsi,
        "ATR14": last_atr,
        "ATR14_PCT": atr_pct,
        "BB_MA20": last_ma20,
        "BB_LOWER": last_bb_lower,
        "BB_UPPER": last_bb_upper,
        "FUNDING": None,        # não calculado aqui
        "OI_CHG_3D_PCT": None,  # não calculado aqui
    }


def compute_indicators_map(series_map: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Dict[str, Optional[float]]]:
    """
    Opcional: calcula indicadores para um dict {symbol: [candles]}.
    """
    out: Dict[str, Dict[str, Optional[float]]] = {}
    if not isinstance(series_map, dict):
        return out
    for sym, candles in series_map.items():
        out[sym] = compute_indicators_dict(candles)
    return out

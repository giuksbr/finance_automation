from __future__ import annotations
import pandas as pd
import numpy as np

"""
Indicadores mínimos para a pipeline pública:

- RSI(14)  (Wilder)
- ATR(14)  (proxy sem OHLC: TR = |close - close.shift(1)|; ATR = EMA(14) de TR)
- BB(20, 2): BB_MA20 e BB_LOWER (MA20 - 2*STD20)

Observações:
- Trabalhamos apenas com uma série de CLOSES (pd.Series[float]).
- Se a janela for insuficiente, retornamos None nos indicadores correspondentes.
- Mantemos resultados como floats (não np.nan) para JSON estável.
- Também retornamos "CLOSE" com o último preço para facilitar regras N-níveis.
"""

def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(alpha=1.0 / span, adjust=False, min_periods=span).mean()

def _rsi_wilder(close: pd.Series, period: int = 14) -> pd.Series:
    # diffs
    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = -delta.clip(upper=0.0)

    # Wilder smoothing (EMA com alpha=1/period)
    roll_up = _ema(up, period)
    roll_down = _ema(down, period)

    rs = roll_up / (roll_down.replace(0, np.nan))
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi

def _atr_proxy_from_close(close: pd.Series, period: int = 14) -> pd.Series:
    # Sem OHLC, usamos TR = |close - close.shift(1)|
    tr = (close - close.shift(1)).abs()
    atr = _ema(tr, period)
    return atr

def compute_indicators(close: pd.Series) -> dict:
    """
    Parâmetro:
      close: pd.Series de preços de fechamento em ordem temporal (antigo -> recente).
    Retorno:
      dict com chaves: RSI14, ATR14, BB_MA20, BB_LOWER, CLOSE
    """
    if not isinstance(close, pd.Series):
        close = pd.Series(close, dtype="float64")
    close = close.astype("float64")
    close = close.dropna().reset_index(drop=True)

    n = len(close)
    out = {"RSI14": None, "ATR14": None, "BB_MA20": None, "BB_LOWER": None, "CLOSE": None}

    if n == 0:
        return out

    out["CLOSE"] = float(close.iloc[-1])

    # RSI(14)
    try:
        if n >= 14 + 1:  # precisa de pelo menos 15 pontos para primeiro RSI completo
            rsi = _rsi_wilder(close, 14)
            val = rsi.iloc[-1]
            if pd.notna(val):
                out["RSI14"] = float(round(val, 2))
    except Exception:
        pass

    # ATR(14) proxy
    try:
        if n >= 14 + 1:
            atr = _atr_proxy_from_close(close, 14)
            val = atr.iloc[-1]
            if pd.notna(val):
                out["ATR14"] = float(round(val, 6))
    except Exception:
        pass

    # BB(20,2)
    try:
        if n >= 20:
            ma20 = close.rolling(window=20, min_periods=20).mean()
            std20 = close.rolling(window=20, min_periods=20).std(ddof=0)
            ma_val = ma20.iloc[-1]
            std_val = std20.iloc[-1]
            if pd.notna(ma_val) and pd.notna(std_val):
                out["BB_MA20"] = float(round(ma_val, 6))
                out["BB_LOWER"] = float(round(ma_val - 2.0 * std_val, 6))
    except Exception:
        pass

    return out

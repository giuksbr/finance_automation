from __future__ import annotations
import math

def pct_change_n(df, n: int):
    if len(df) <= n:
        return None
    last, prev = df["close"].iloc[-1], df["close"].iloc[-(n+1)]
    return (last/prev - 1.0) * 100.0

def n_levels_from_features(asset_type: str, chg7, chg10, rsi14, atr14, bb_lower, bb_ma20, close):
    levels = []
    chg7_ok = (chg7 is not None and chg7 <= -12.0)
    chg10_ok = (chg10 is not None and chg10 <= -12.0)
    rsi_mid = (rsi14 is not None and 38.0 <= rsi14 <= 55.0)
    bb_touch = (bb_lower is not None and close is not None and close <= bb_lower)
    m20_dev = False
    if atr14 is not None and bb_ma20 is not None and close is not None:
        m20_dev = abs(close - bb_ma20) >= 1.5 * atr14

    if (chg7 is not None and chg7 <= -22.0) or (chg10 is not None and chg10 <= -22.0):
        levels.append("N1")
    if (chg7_ok or chg10_ok) and ((rsi14 is not None and 38.0 <= rsi14 <= 50.0) or m20_dev):
        levels.append("N2")
    if ((chg7 is not None and chg7 <= -8.0) or (chg10 is not None and chg10 <= -8.0)) and ((rsi14 is not None and 40.0 <= rsi14 <= 55.0) or bb_touch):
        levels.append("N3")
    if asset_type == "cr":
        if ((chg7 is not None and chg7 <= -8.0) or (chg10 is not None and chg10 <= -10.0)) and (rsi_mid or bb_touch or m20_dev):
            levels.append("N3C")
    return levels

def confidence_from_levels(levels, sources):
    s = set(levels)
    if "N1" in s and ("N2" in s or "N3" in s):
        return "high"
    if len(set(sources)) >= 2 and s.intersection({"N2","N3","N3C"}):
        return "medium"
    return "low"

import numpy as np, pandas as pd

def pct_delta(a: float, b: float) -> float:
    if a == 0 or b == 0:
        return np.inf
    return abs(a-b)/((abs(a)+abs(b))/2.0)

def accept_close_eq(stooq_df: pd.DataFrame | None, yahoo_df: pd.DataFrame | None, thresholds) -> tuple[pd.DataFrame | None, str]:
    if stooq_df is not None and yahoo_df is not None and len(stooq_df)>0 and len(yahoo_df)>0:
        d = pct_delta(stooq_df["close"].iloc[-1], yahoo_df["close"].iloc[-1])
        return (stooq_df, "stooq|yahoo") if d <= thresholds.eq_delta_max else (None, "divergent")
    if stooq_df is not None and len(stooq_df)>0:
        return stooq_df, "stooq_only"
    if yahoo_df is not None and len(yahoo_df)>0:
        return yahoo_df, "yahoo_only"
    return None, "none"

def accept_close_cr(binance_df: pd.DataFrame | None, coingecko_df: pd.DataFrame | None, thresholds) -> tuple[pd.DataFrame | None, str]:
    if binance_df is not None and coingecko_df is not None and len(binance_df)>0 and len(coingecko_df)>0:
        d = pct_delta(binance_df["close"].iloc[-1], coingecko_df["close"].iloc[-1])
        return (binance_df, "binance|coingecko") if d <= thresholds.cr_delta_max else (None, "divergent")
    if binance_df is not None and len(binance_df)>0:
        return binance_df, "binance_only"
    if coingecko_df is not None and len(coingecko_df)>0:
        return coingecko_df, "coingecko_only"
    return None, "none"

def sanity_last7_abs_move_ok(df: pd.DataFrame, is_crypto: bool, thresholds) -> bool:
    if len(df) < 8:
        return False
    last, prev = df["close"].iloc[-1], df["close"].iloc[-8]
    chg = abs((last/prev) - 1.0)
    lim = thresholds.cr_abs_chg7d_max if is_crypto else thresholds.eq_abs_chg7d_max
    return chg <= lim

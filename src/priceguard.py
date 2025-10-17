from __future__ import annotations
import math
import pandas as pd

def _last_aligned_close(df: pd.DataFrame) -> tuple[str | None, float | None]:
    if df is None or df.empty:
        return (None, None)
    d = df.sort_values("Date").iloc[-1]
    return (str(d["Date"]), float(d["close"]))

def _abs_pct_delta(a: float, b: float) -> float:
    if a is None or b is None:
        return math.inf
    if a == 0:
        return math.inf
    return abs(a - b) / abs(a)

# --------------------------------------------------------------------------------------
# PRICEGUARD: EQUITIES / ETFs  (Stooq x Yahoo)
# - Se ambas as fontes existem e alinham a data e Δ ≤ eq_delta_max -> aceita e tag "stooq|yahoo"
# - Se apenas uma fonte válida -> aceita com tag "stooq_only" ou "yahoo_only"
# - Se nenhuma fonte útil -> retorna (None, None)
# --------------------------------------------------------------------------------------
def accept_close_eq(stooq_df: pd.DataFrame | None,
                    yahoo_df: pd.DataFrame | None,
                    thresholds) -> tuple[pd.DataFrame | None, str | None]:

    s_date, s_close = _last_aligned_close(stooq_df)
    y_date, y_close = _last_aligned_close(yahoo_df)

    both_available = (s_date is not None) and (y_date is not None)
    if both_available and (s_date == y_date):
        delta = _abs_pct_delta(s_close, y_close)
        if delta <= thresholds.eq_delta_max:
            # datas alinhadas e delta ok -> dupla-fonte
            # preferimos a série do Stooq como "aceita" (padrão), mas o tag reflete ambas
            accepted = stooq_df if stooq_df is not None else yahoo_df
            return (accepted, "stooq|yahoo")

    # fallback para single-fonte (se uma delas existir)
    if stooq_df is not None and not stooq_df.empty:
        return (stooq_df, "stooq_only")
    if yahoo_df is not None and not yahoo_df.empty:
        return (yahoo_df, "yahoo_only")

    return (None, None)

# --------------------------------------------------------------------------------------
# PRICEGUARD: CRYPTO  (Binance x CoinGecko)
# - Se ambas as fontes existem e alinham a data e Δ ≤ cr_delta_max -> aceita e tag "binance|coingecko"
# - Se apenas uma fonte válida -> aceita com tag "binance_only" ou "coingecko_only"
# - Se nenhuma fonte útil -> (None, None)
# --------------------------------------------------------------------------------------
def accept_close_cr(binance_df: pd.DataFrame | None,
                    coingecko_df: pd.DataFrame | None,
                    thresholds) -> tuple[pd.DataFrame | None, str | None]:

    b_date, b_close = _last_aligned_close(binance_df)
    c_date, c_close = _last_aligned_close(coingecko_df)

    both_available = (b_date is not None) and (c_date is not None)
    if both_available and (b_date == c_date):
        delta = _abs_pct_delta(b_close, c_close)
        if delta <= thresholds.cr_delta_max:
            # datas alinhadas e delta ok -> dupla-fonte
            # preferimos Binance como "aceita" (padrão), mas o tag reflete ambas
            accepted = binance_df if binance_df is not None else coingecko_df
            return (accepted, "binance|coingecko")

    # fallback single-fonte
    if binance_df is not None and not binance_df.empty:
        return (binance_df, "binance_only")
    if coingecko_df is not None and not coingecko_df.empty:
        return (coingecko_df, "coingecko_only")

    return (None, None)

# --------------------------------------------------------------------------------------
# SANITY: movimento absoluto nos últimos 7 dias
# - usado quando só 1 fonte existe: aceitamos o fechamento se o movimento 7d não for absurdo
# --------------------------------------------------------------------------------------
def sanity_last7_abs_move_ok(df: pd.DataFrame, is_crypto: bool, thresholds) -> bool:
    if df is None or df.empty or len(df) < 8:
        return False
    df = df.sort_values("Date").reset_index(drop=True)
    last = float(df["close"].iloc[-1])
    prev = float(df["close"].iloc[-8])
    if prev == 0:
        return False
    chg = abs(last / prev - 1.0)
    limit = thresholds.cr_abs_chg7d_max if is_crypto else thresholds.eq_abs_chg7d_max
    return chg <= limit

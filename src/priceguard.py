from __future__ import annotations
from typing import Tuple, Optional
import pandas as pd


# =========================
# Helpers genéricos
# =========================

def _pct(a: float | None, b: float | None) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return (a / b) - 1.0


def _chg7(df: pd.DataFrame) -> Optional[float]:
    if df is None or df.empty or "close" not in df.columns or len(df) < 8:
        return None
    c = df["close"].tolist()
    return _pct(c[-1], c[-8])


def sanity_last7_abs_move_ok(df: pd.DataFrame, abs_limit: float) -> bool:
    """
    Sanidade estrita para single-source:
      - >= 8 barras
      - close final > 0
      - |Δ7d| <= abs_limit  (ex.: 0.25 = 25%)
    """
    if df is None or df.empty or "close" not in df.columns or len(df) < 8:
        return False
    last = pd.to_numeric(df["close"].iloc[-1], errors="coerce")
    if pd.isna(last) or float(last) <= 0:
        return False
    chg7 = _chg7(df)
    if chg7 is None:
        return False
    return abs(chg7) <= abs_limit


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza DataFrame para colunas ['date','close'] ordenadas."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "close"])
    out = df.copy()
    if "date" not in out.columns or "close" not in out.columns:
        return pd.DataFrame(columns=["date", "close"])
    out = out.dropna(subset=["date", "close"]).copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out.dropna(subset=["date", "close"])
    out = out.sort_values("date").reset_index(drop=True)
    return out[["date", "close"]]


def _cfg_val(cfg: object, key: str, default: float) -> float:
    """
    Lê 'key' de:
      - dict (cfg[key] ou cfg['priceguard'][key])
      - objeto (getattr(cfg, key, ...) ou getattr(cfg.priceguard, key, ...))
    """
    # dict direto
    if isinstance(cfg, dict):
        if key in cfg:
            try:
                return float(cfg[key])
            except Exception:
                pass
        pg = cfg.get("priceguard", {})
        if isinstance(pg, dict) and key in pg:
            try:
                return float(pg[key])
            except Exception:
                pass

    # objeto com atributo direto
    try:
        val = getattr(cfg, key)
        return float(val)
    except Exception:
        pass

    # objeto com atributo 'priceguard' aninhado
    try:
        pg = getattr(cfg, "priceguard", None)
        if pg is not None:
            val = getattr(pg, key)
            return float(val)
    except Exception:
        pass

    return float(default)


# =========================
# EQ / ETF (stooq × yahoo)
# =========================

def accept_close_eq(
    stooq_df: pd.DataFrame | None,
    yahoo_df: pd.DataFrame | None,
    cfg: object,
) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    PriceGuard p/ EQ/ETF:
      - Duas fontes: aceitar se datas alinham e Δ <= eq_delta_max; marca 'stooq|yahoo'.
      - Uma fonte: aceitar com sanidade estrita (|Δ7d|<=single_abs_7d_max_eq); marca 'stooq_only' ou 'yahoo_only'.
      - Nenhuma: rejeita.
    Retorna (df_aceito ['date','close'], tag).
    """
    eq_delta_max = _cfg_val(cfg, "eq_delta_max", 0.008)              # 0,8%
    single_abs_7d_max = _cfg_val(cfg, "single_abs_7d_max_eq", 0.25)  # 25%

    stq = _prepare(stooq_df)
    yh = _prepare(yahoo_df)

    stq_empty = stq.empty
    yh_empty = yh.empty

    # 1) duas fontes
    if not stq_empty and not yh_empty:
        merged = pd.merge(stq, yh, on="date", how="inner", suffixes=("_stq", "_yh"))
        if merged.empty:
            return pd.DataFrame(columns=["date", "close"]), "datas_desalinhadas"

        last = merged.iloc[-1]
        cs, cy = float(last["close_stq"]), float(last["close_yh"])
        if cs <= 0 or cy <= 0:
            return pd.DataFrame(columns=["date", "close"]), "close_invalido"

        delta = abs(cy - cs) / cs
        if delta <= eq_delta_max:
            accepted = merged[["date", "close_yh"]].rename(columns={"close_yh": "close"})
            return accepted.reset_index(drop=True), "stooq|yahoo"
        return pd.DataFrame(columns=["date", "close"]), "divergencia"

    # 2) apenas stooq
    if not stq_empty and yh_empty:
        if sanity_last7_abs_move_ok(stq, single_abs_7d_max):
            return stq, "stooq_only"
        return pd.DataFrame(columns=["date", "close"]), "stooq_sanity_fail"

    # 3) apenas yahoo
    if stq_empty and not yh_empty:
        if sanity_last7_abs_move_ok(yh, single_abs_7d_max):
            return yh, "yahoo_only"
        return pd.DataFrame(columns=["date", "close"]), "yahoo_sanity_fail"

    # 4) nenhuma
    return pd.DataFrame(columns=["date", "close"]), None


# =========================
# CR (binance × coingecko)
# =========================

def accept_close_cr(
    binance_df: pd.DataFrame | None,
    coingecko_df: pd.DataFrame | None,
    cfg: object,
) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    PriceGuard p/ CR:
      - Duas fontes: aceitar se datas alinham e Δ <= cr_delta_max; tag 'binance|coingecko'.
      - Uma fonte: aceitar com sanidade estrita (|Δ7d|<=single_abs_7d_max_cr); tag 'binance_only' ou 'coingecko_only'.
      - Nenhuma: rejeita.
    """
    cr_delta_max = _cfg_val(cfg, "cr_delta_max", 0.0035)             # 0,35%
    single_abs_7d_max = _cfg_val(cfg, "single_abs_7d_max_cr", 0.40)  # 40%

    bn = _prepare(binance_df)
    cg = _prepare(coingecko_df)

    bn_empty = bn.empty
    cg_empty = cg.empty

    # 1) duas fontes
    if not bn_empty and not cg_empty:
        merged = pd.merge(bn, cg, on="date", how="inner", suffixes=("_bn", "_cg"))
        if merged.empty:
            return pd.DataFrame(columns=["date", "close"]), "datas_desalinhadas"

        last = merged.iloc[-1]
        cb, cc = float(last["close_bn"]), float(last["close_cg"])
        if cb <= 0 or cc <= 0:
            return pd.DataFrame(columns=["date", "close"]), "close_invalido"

        delta = abs(cc - cb) / cb
        if delta <= cr_delta_max:
            accepted = merged[["date", "close_bn"]].rename(columns={"close_bn": "close"})
            return accepted.reset_index(drop=True), "binance|coingecko"
        return pd.DataFrame(columns=["date", "close"]), "divergencia"

    # 2) apenas binance
    if not bn_empty and cg_empty:
        if sanity_last7_abs_move_ok(bn, single_abs_7d_max):
            return bn, "binance_only"
        return pd.DataFrame(columns=["date", "close"]), "binance_sanity_fail"

    # 3) apenas coingecko
    if bn_empty and not cg_empty:
        if sanity_last7_abs_move_ok(cg, single_abs_7d_max):
            return cg, "coingecko_only"
        return pd.DataFrame(columns=["date", "close"]), "coingecko_sanity_fail"

    # 4) nenhuma
    return pd.DataFrame(columns=["date", "close"]), None

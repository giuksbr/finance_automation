from __future__ import annotations
from typing import Tuple, Optional
import pandas as pd


def _pct(a: float | None, b: float | None) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return (a / b) - 1.0


def _chg7(df: pd.DataFrame) -> Optional[float]:
    if df is None or df.empty or "close" not in df.columns or len(df) < 8:
        return None
    c = df["close"].tolist()
    return _pct(c[-1], c[-8])


def _sanity_single_source(df: pd.DataFrame, abs_7d_max: float) -> bool:
    """
    Sanidade estrita p/ single-source (design):
      - >= 8 barras
      - close > 0
      - |Δ7d| <= abs_7d_max (ex.: 0.25 = 25%)
    """
    if df is None or df.empty or "close" not in df.columns or len(df) < 8:
        return False
    if pd.isna(df["close"].iloc[-1]) or float(df["close"].iloc[-1]) <= 0:
        return False
    chg7 = _chg7(df)
    if chg7 is None:
        return False
    return abs(chg7) <= abs_7d_max


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
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


def accept_close_eq(
    stooq_df: pd.DataFrame | None,
    yahoo_df: pd.DataFrame | None,
    cfg: dict,
) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    PriceGuard p/ EQ/ETF:
      - Duas fontes: aceitar se datas alinham e Δ <= eq_delta_max; marca 'stooq|yahoo'.
      - Uma fonte: aceitar com sanidade estrita; marca 'stooq_only' ou 'yahoo_only'.
      - Nenhuma: rejeita.
    Retorna (df_aceito ['date','close'], tag).
    """
    eq_delta_max = float(cfg.get("eq_delta_max", 0.008))          # 0,8%
    single_abs_7d_max = float(cfg.get("single_abs_7d_max", 0.25)) # 25%

    stq = _prepare(stooq_df)
    yh = _prepare(yahoo_df)

    stq_empty = stq.empty
    yh_empty = yh.empty

    # 1) Duas fontes presentes → checa data/Δ
    if not stq_empty and not yh_empty:
        # alinha por data
        merged = pd.merge(stq, yh, on="date", how="inner", suffixes=("_stq", "_yh"))
        if merged.empty:
            return pd.DataFrame(columns=["date", "close"]), "datas_desalinhadas"

        # usa a última data comum para o teste de Δ
        last = merged.iloc[-1]
        close_stq = float(last["close_stq"])
        close_yh = float(last["close_yh"])
        if close_stq <= 0 or close_yh <= 0:
            return pd.DataFrame(columns=["date", "close"]), "close_invalido"

        delta = abs(close_yh - close_stq) / close_stq
        if delta <= eq_delta_max:
            # aprovado — usamos a série do Yahoo (ou poderíamos tirar média)
            accepted = merged[["date", "close_yh"]].rename(columns={"close_yh": "close"})
            return accepted.reset_index(drop=True), "stooq|yahoo"
        else:
            return pd.DataFrame(columns=["date", "close"]), "divergencia"

    # 2) Apenas Stooq → sanidade estrita
    if not stq_empty and yh_empty:
        if _sanity_single_source(stq, single_abs_7d_max):
            return stq, "stooq_only"
        else:
            return pd.DataFrame(columns=["date", "close"]), "stooq_sanity_fail"

    # 3) Apenas Yahoo → sanidade estrita
    if stq_empty and not yh_empty:
        if _sanity_single_source(yh, single_abs_7d_max):
            return yh, "yahoo_only"
        else:
            return pd.DataFrame(columns=["date", "close"]), "yahoo_sanity_fail"

    # 4) Nenhuma
    return pd.DataFrame(columns=["date", "close"]), None

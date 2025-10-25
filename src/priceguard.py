# -*- coding: utf-8 -*-
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd


# ---------------------------
# Tipos auxiliares
# ---------------------------

SeriesLike = Union[pd.DataFrame, Dict[str, Any], List[Dict[str, Any]]]


@dataclass
class ClosePoint:
    ts_utc: Optional[datetime]
    close: Optional[float]

    @property
    def is_valid(self) -> bool:
        return self.ts_utc is not None and self.close is not None and not math.isnan(self.close)


# ---------------------------
# Normalização de entrada
# ---------------------------

def _as_dataframe(obj: SeriesLike) -> pd.DataFrame:
    """
    Normaliza diferentes formatos (DataFrame, dict, lista) para um DataFrame
    com colunas padronizadas:
      - t: datetime (UTC)
      - c: close (float)

    Formatos aceitos além de DataFrame:
      - {"candles": [{"t": "...", "c": ...}, ...]}
      - {"series": [{"t": "...", "c": ...}, ...]}
      - [{"t": "...", "c": ...}, ...]
      - dict com arrays paralelos: {"t": [...], "c": [...]}
      - DataFrame com colunas ('t','c') ou equivalentes ('close','timestamp', etc.)
    """
    if obj is None:
        return pd.DataFrame(columns=["t", "c"])

    # Já é DataFrame?
    if isinstance(obj, pd.DataFrame):
        df = obj.copy()
    else:
        payload = obj

        # dict com chave "candles"/"series"
        if isinstance(payload, dict):
            if "candles" in payload and isinstance(payload["candles"], list):
                df = pd.DataFrame(payload["candles"])
            elif "series" in payload and isinstance(payload["series"], list):
                df = pd.DataFrame(payload["series"])
            elif "t" in payload and "c" in payload and isinstance(payload["t"], Iterable):
                df = pd.DataFrame({"t": payload["t"], "c": payload["c"]})
            else:
                # pode ser um map {ts: close}
                try:
                    df = pd.DataFrame(
                        [{"t": k, "c": v} for k, v in payload.items()],
                        columns=["t", "c"],
                    )
                except Exception:
                    df = pd.DataFrame(columns=["t", "c"])
        # lista de dicts [{"t":..., "c":...}]
        elif isinstance(payload, list):
            df = pd.DataFrame(payload)
        else:
            df = pd.DataFrame(columns=["t", "c"])

    # Padroniza nomes de colunas comuns
    colmap = {}
    for src, dst in [
        ("timestamp", "t"),
        ("time", "t"),
        ("date", "t"),
        ("dt", "t"),
        ("Close", "c"),
        ("close", "c"),
        ("price", "c"),
        ("cl", "c"),
    ]:
        if src in df.columns and dst not in df.columns:
            colmap[src] = dst
    if colmap:
        df = df.rename(columns=colmap)

    # Tenta inferir t/c se nomes diferentes
    if "t" not in df.columns and "c" not in df.columns:
        possible_t = [c for c in df.columns if c.lower() in {"t", "time", "timestamp", "date", "dt"}]
        possible_c = [c for c in df.columns if c.lower() in {"c", "close", "price", "cl"}]
        if possible_t and possible_c:
            df = df.rename(columns={possible_t[0]: "t", possible_c[0]: "c"})
        else:
            return pd.DataFrame(columns=["t", "c"])

    # Filtra só t e c
    keep = [c for c in ["t", "c"] if c in df.columns]
    df = df[keep].copy()

    # Converte t -> datetime UTC
    if "t" in df.columns:
        def _to_utc(x):
            if pd.isna(x):
                return pd.NaT
            if isinstance(x, datetime):
                return x.astimezone(timezone.utc) if x.tzinfo else x.replace(tzinfo=timezone.utc)
            if isinstance(x, (int, float)):
                # heurística: > 1e12 assume ms
                if x > 1e12:
                    return datetime.fromtimestamp(x / 1000.0, tz=timezone.utc)
                return datetime.fromtimestamp(x, tz=timezone.utc)
            try:
                dt = pd.to_datetime(x, utc=True)
                if isinstance(dt, pd.Series):
                    return pd.NaT
                return dt.to_pydatetime()
            except Exception:
                return pd.NaT

        df["t"] = df["t"].apply(_to_utc)

    # Converte c -> float
    if "c" in df.columns:
        df["c"] = pd.to_numeric(df["c"], errors="coerce")

    # Ordena, dropa NaN e duplicados
    df = df.dropna(subset=["t", "c"])
    df = df.sort_values("t").drop_duplicates(subset=["t"], keep="last").reset_index(drop=True)

    if not {"t", "c"}.issubset(df.columns):
        return pd.DataFrame(columns=["t", "c"])

    return df


def _prepare(obj: SeriesLike) -> pd.DataFrame:
    """Compat: aceita DataFrame/dict/lista e retorna DataFrame padronizado ('t','c')."""
    return _as_dataframe(obj)


# ---------------------------
# Lógica de verificação de preço
# ---------------------------

def _last_close(df: pd.DataFrame) -> ClosePoint:
    if df is None or df.empty:
        return ClosePoint(ts_utc=None, close=None)
    row = df.iloc[-1]
    ts = row.get("t")
    cl = row.get("c")
    return ClosePoint(ts_utc=ts if isinstance(ts, datetime) else None,
                      close=float(cl) if pd.notna(cl) else None)


def _pct_diff(a: float, b: float) -> Optional[float]:
    try:
        if a is None or b is None:
            return None
        if b == 0:
            return None
        return abs(a - b) / abs(b) * 100.0
    except Exception:
        return None


def accept_close_eq(stooq_df: SeriesLike, yahoo_df: SeriesLike, threshold_pct: float) -> Tuple[pd.DataFrame, str]:
    """
    Decide qual série aceitar para equities (Stooq vs Yahoo) usando um limiar
    percentual sobre o último fechamento.

    Retorna (df_escolhido, tag), onde tag ∈ {"YH","STQ","BOTH","ONLY_YH","ONLY_STQ","NONE","MISMATCH"}.
    """
    stq = _prepare(stooq_df)
    yh = _prepare(yahoo_df)

    stq_pt = _last_close(stq)
    yh_pt = _last_close(yh)

    if not stq_pt.is_valid and not yh_pt.is_valid:
        return pd.DataFrame(columns=["t", "c"]), "NONE"
    if stq_pt.is_valid and not yh_pt.is_valid:
        return stq, "ONLY_STQ"
    if yh_pt.is_valid and not stq_pt.is_valid:
        return yh, "ONLY_YH"

    diff = _pct_diff(stq_pt.close, yh_pt.close)
    if diff is None:
        chosen = yh if (yh_pt.ts_utc or datetime.min) >= (stq_pt.ts_utc or datetime.min) else stq
        return chosen, "BOTH"
    if diff <= threshold_pct:
        chosen = yh if (yh_pt.ts_utc or datetime.min) >= (stq_pt.ts_utc or datetime.min) else stq
        return chosen, "BOTH"
    chosen = yh if (yh_pt.ts_utc or datetime.min) >= (stq_pt.ts_utc or datetime.min) else stq
    return chosen, "MISMATCH"


def accept_close_cr(binance_df: SeriesLike, coingecko_df: SeriesLike, threshold_pct: float) -> Tuple[pd.DataFrame, str]:
    """
    Decide qual série aceitar para cripto (Binance vs Coingecko).
    Semelhante a accept_close_eq.
    """
    bn = _prepare(binance_df)
    cg = _prepare(coingecko_df)

    bn_pt = _last_close(bn)
    cg_pt = _last_close(cg)

    if not bn_pt.is_valid and not cg_pt.is_valid:
        return pd.DataFrame(columns=["t", "c"]), "NONE"
    if bn_pt.is_valid and not cg_pt.is_valid:
        return bn, "ONLY_BINANCE"
    if cg_pt.is_valid and not bn_pt.is_valid:
        return cg, "ONLY_COINGECKO"

    diff = _pct_diff(bn_pt.close, cg_pt.close)
    if diff is None or diff <= threshold_pct:
        chosen = bn if (bn_pt.ts_utc or datetime.min) >= (cg_pt.ts_utc or datetime.min) else cg
        return chosen, "BOTH"
    chosen = bn if (bn_pt.ts_utc or datetime.min) >= (cg_pt.ts_utc or datetime.min) else cg
    return chosen, "MISMATCH"


# ---------------------------
# Sanidade de movimentos (usado pelo job.py)
# ---------------------------

def _pct_change_over_days(df: pd.DataFrame, days: int) -> Optional[float]:
    """
    Retorna variação percentual (último vs valor de ~N dias atrás).
    Requer pelo menos 2 pontos; se faltar janela, retorna None.
    """
    if df is None or df.empty or "t" not in df or "c" not in df:
        return None

    last = df.iloc[-1]
    t_last: datetime = last["t"]
    c_last: float = last["c"]

    if pd.isna(t_last) or pd.isna(c_last):
        return None

    lim = t_last - timedelta(days=days)
    # pega o ponto com timestamp <= lim mais próximo
    prev_df = df[df["t"] <= lim]
    if prev_df.empty:
        return None
    c_prev = prev_df.iloc[-1]["c"]
    if c_prev == 0 or pd.isna(c_prev):
        return None
    return (c_last - c_prev) / abs(c_prev) * 100.0


def sanity_last7_abs_move_ok(df_like: SeriesLike, max_abs_pct: float = 25.0) -> bool:
    """
    Verifica se o movimento absoluto em ~7 dias não excede 'max_abs_pct'.
    Retorna True se:
      - há dados e |pct_change_7d| <= max_abs_pct
      - ou dados insuficientes (não bloqueia)
    Retorna False apenas quando há dados suficientes e a variação excede o limite.
    """
    df = _prepare(df_like)
    pct7 = _pct_change_over_days(df, 7)
    if pct7 is None:
        return True  # não bloqueia por falta de janela
    return abs(pct7) <= max_abs_pct

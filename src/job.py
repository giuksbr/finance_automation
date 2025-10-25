# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import math
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional

import pandas as pd

from src.fetch_eq import fetch_stooq, fetch_yahoo
from src.fetch_cr import fetch_binance, fetch_coingecko
from src.priceguard import (
    accept_close_eq,
    accept_close_cr,
    sanity_last7_abs_move_ok,
    _prepare as _pg_prepare,  # <<< adicionamos isto
)

# -----------------------------------------------------------------------------
# Helper: normaliza qualquer coisa (DataFrame/dict/lista) para DataFrame ['t','c']
# -----------------------------------------------------------------------------
def _df(obj) -> pd.DataFrame:
    try:
        df = _pg_prepare(obj)
        if df is None:
            return pd.DataFrame(columns=["t", "c"])
        # garante colunas
        if "t" not in df.columns or "c" not in df.columns:
            return pd.DataFrame(columns=["t", "c"])
        return df
    except Exception:
        return pd.DataFrame(columns=["t", "c"])


# -----------------------------------------------------------------------------
# EQ
# -----------------------------------------------------------------------------
def collect_eq(symbols: List[str], days: int, threshold_pct: float) -> Tuple[Dict, Dict, Dict, Dict]:
    ohlcv: Dict[str, List[Dict]] = {}
    indicators: Dict[str, Dict] = {}
    src_tags: Dict[str, str] = {}
    chg: Dict[str, Dict] = {}

    for sym in symbols:
        # 1) Baixa bruto
        stq_raw = fetch_stooq(sym, days)
        yh_raw = fetch_yahoo(sym, days)

        # 2) Normaliza
        stq = _df(stq_raw)
        yh = _df(yh_raw)

        # 3) Escolha de fonte por guard
        chosen_df, tag = accept_close_eq(stq, yh, threshold_pct)
        src_df = chosen_df if not chosen_df.empty else (stq if not stq.empty else yh)
        src_tags[sym] = tag

        # 4) Guarda série em formato de candles simples
        if not src_df.empty:
            ohlcv[sym] = [{"t": r.t.isoformat(), "c": float(r.c)} for r in src_df.itertuples(index=False)]
        else:
            ohlcv[sym] = []

        # 5) Indicadores básicos (mínimo para não quebrar)
        if not src_df.empty:
            last = src_df.iloc[-1]
            close = float(last["c"])
            # RSI/ATR/BB podem já ser calculados em outro lugar; aqui só garantimos chaves
            indicators.setdefault("eq", {})
            indicators["eq"][sym] = {
                "CLOSE": close,
                # deixe None se não calcular aqui — o build/export usa estes campos quando presentes
                "RSI14": None,
                "ATR14": None,
                "BB_MA20": None,
                "BB_LOWER": None,
            }

            # variações (se houver janela)
            def pct_over(d):
                lim = last["t"] - timedelta(days=d)
                prev = src_df[src_df["t"] <= lim]
                if prev.empty:
                    return None
                p = float(prev.iloc[-1]["c"])
                if p == 0:
                    return None
                return (close - p) / abs(p) * 100.0

            chg[sym] = {
                "pct_chg_7d": pct_over(7),
                "pct_chg_10d": pct_over(10),
                "pct_chg_30d": pct_over(30),
            }
        else:
            indicators.setdefault("eq", {})
            indicators["eq"][sym] = {
                "CLOSE": None,
                "RSI14": None,
                "ATR14": None,
                "BB_MA20": None,
                "BB_LOWER": None,
            }
            chg[sym] = {"pct_chg_7d": None, "pct_chg_10d": None, "pct_chg_30d": None}

    return ohlcv, indicators, src_tags, chg


# -----------------------------------------------------------------------------
# CR
# -----------------------------------------------------------------------------
def collect_cr(symbols: List[str], days: int, threshold_pct: float) -> Tuple[Dict, Dict, Dict, Dict]:
    ohlcv: Dict[str, List[Dict]] = {}
    indicators: Dict[str, Dict] = {}
    src_tags: Dict[str, str] = {}
    chg: Dict[str, Dict] = {}

    for sym in symbols:
        bn_raw = fetch_binance(sym, days)
        cg_raw = fetch_coingecko(sym, days)

        bn = _df(bn_raw)
        cg = _df(cg_raw)

        chosen_df, tag = accept_close_cr(bn, cg, threshold_pct)
        src_df = chosen_df if not chosen_df.empty else (bn if not bn.empty else cg)
        src_tags[sym] = tag

        if not src_df.empty:
            ohlcv[sym] = [{"t": r.t.isoformat(), "c": float(r.c)} for r in src_df.itertuples(index=False)]
        else:
            ohlcv[sym] = []

        if not src_df.empty:
            last = src_df.iloc[-1]
            close = float(last["c"])

            indicators.setdefault("cr", {})
            indicators["cr"][sym] = {
                "CLOSE": close,
                "RSI14": None,
                "ATR14": None,
                "BB_MA20": None,
                "BB_LOWER": None,
            }

            def pct_over(d):
                lim = last["t"] - timedelta(days=d)
                prev = src_df[src_df["t"] <= lim]
                if prev.empty:
                    return None
                p = float(prev.iloc[-1]["c"])
                if p == 0:
                    return None
                return (close - p) / abs(p) * 100.0

            chg[sym] = {
                "pct_chg_7d": pct_over(7),
                "pct_chg_10d": pct_over(10),
                "pct_chg_30d": pct_over(30),
            }
        else:
            indicators.setdefault("cr", {})
            indicators["cr"][sym] = {
                "CLOSE": None,
                "RSI14": None,
                "ATR14": None,
                "BB_MA20": None,
                "BB_LOWER": None,
            }
            chg[sym] = {"pct_chg_7d": None, "pct_chg_10d": None, "pct_chg_30d": None}

    return ohlcv, indicators, src_tags, chg


# -----------------------------------------------------------------------------
# (restante do job.py permanece igual)
# -----------------------------------------------------------------------------

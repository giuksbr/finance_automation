#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Equities/ETFs: utilitários para OHLCV diário.

Exporta as funções esperadas por src.job:
- fetch_stooq(symbol_canonical: str, limit: int = 120) -> dict
- fetch_yahoo(symbol_canonical: str, limit: int = 120) -> dict  (usa stooq como fallback)

Formato de retorno (para cada função):
{
  "symbol": "NYSEARCA:VUG",
  "venue": "NYSEARCA",
  "series": {
    "c": [float, ...],            # closes
    "t": ["YYYY-MM-DDTHH:MM:SSZ", ...]  # timestamps UTC ISO8601 (00:00:00Z p/ equities)
  },
  "source": "stooq" | "yahoo_fallback_stooq"
}
"""

from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Dict, List, Tuple

import requests

STOOQ_DAILY = "https://stooq.com/q/d/l/?s={symbol}&i=d"


def _split_symbol(sym: str) -> Tuple[str, str]:
    if ":" in sym:
        ex, tic = sym.split(":", 1)
        return ex, tic
    return "", sym


def _canon_to_stooq(sym_canon: str) -> str:
    # Regra simples: tickers dos EUA -> .us
    # Ex.: "NASDAQ:NVDA" -> "nvda.us"
    _, tic = _split_symbol(sym_canon)
    return f"{tic}.us".lower().replace("/", "-")


def _fetch_stooq_arrays(sym_canon: str) -> Tuple[List[float], List[str]]:
    url = STOOQ_DAILY.format(symbol=_canon_to_stooq(sym_canon))
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    text = r.text
    reader = csv.DictReader(io.StringIO(text))
    closes, times = [], []
    for row in reader:
        try:
            c = float(row["Close"])
            d = row["Date"]  # YYYY-MM-DD
        except Exception:
            continue
        closes.append(c)
        times.append(f"{d}T00:00:00Z")  # usar meia-noite UTC para fechamento diário
    return closes, times


def fetch_stooq(symbol_canonical: str, limit: int = 120) -> Dict:
    """
    Baixa OHLCV diário via Stooq e entrega no formato que o src.job consome.
    """
    venue, _ = _split_symbol(symbol_canonical)
    closes, times = _fetch_stooq_arrays(symbol_canonical)
    # aplica limite (se houver)
    if limit and len(closes) > limit:
        closes = closes[-limit:]
        times = times[-limit:]

    return {
        "symbol": symbol_canonical,
        "venue": venue,
        "series": {"c": closes, "t": times},
        "source": "stooq",
    }


def fetch_yahoo(symbol_canonical: str, limit: int = 120) -> Dict:
    """
    Implementação compatível com src.job:
    - Para reduzir 429 e simplificar, usa Stooq como *fallback* imediato.
    - Se no futuro quiser Yahoo de verdade, é só trocar aqui.
    """
    data = fetch_stooq(symbol_canonical, limit=limit)
    data["source"] = "yahoo_fallback_stooq"
    return data


__all__ = ["fetch_stooq", "fetch_yahoo"]

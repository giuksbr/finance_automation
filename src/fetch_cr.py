#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Cripto: utilitário simples para klines diários (Binance Futures PERPETUAL),
caso src.job utilize diretamente.

Exporta:
- fetch_binance_1d(symbol_canonical: "BINANCE:BTCUSDT", limit=120) -> dict compatível
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Tuple

import requests

BINANCE_FAPI = "https://fapi.binance.com"


def _to_iso(ts_ms: int) -> str:
    return (
        datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def fetch_binance_1d(symbol_canonical: str, limit: int = 120) -> Dict:
    # "BINANCE:BTCUSDT" -> pair "BTCUSDT"
    pair = symbol_canonical.split(":", 1)[1].upper() if ":" in symbol_canonical else symbol_canonical.upper()
    url = f"{BINANCE_FAPI}/fapi/v1/continuousKlines"
    params = {"pair": pair, "contractType": "PERPETUAL", "interval": "1d", "limit": str(limit)}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    closes, times = [], []
    for row in data:
        closes.append(float(row[4]))   # close
        times.append(_to_iso(int(row[6])))  # close time

    return {
        "symbol": symbol_canonical,
        "venue": "BINANCE",
        "series": {"c": closes, "t": times},
        "source": "binance_futures",
    }


__all__ = ["fetch_binance_1d"]

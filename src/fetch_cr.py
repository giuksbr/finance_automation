#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Cripto (sem cache): utilitários de OHLC diário compatíveis com src.job.

Exporta as funções que src.job espera:
- fetch_binance(symbol_canonical: str, limit: int = 120) -> dict
- fetch_coingecko(symbol_canonical: str, limit: int = 120) -> dict

Formato de retorno:
{
  "symbol": "BINANCE:BTCUSDT",
  "venue": "BINANCE",
  "series": {
    "c": [float, ...],                         # closes
    "t": ["YYYY-MM-DDTHH:MM:SSZ", ...]         # timestamps ISO UTC (close time)
  },
  "source": "binance_spot" | "coingecko"
}
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

# ---- Helpers ---------------------------------------------------------------

def _to_iso_utc_from_ms(ms: int) -> str:
    return (
        datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )

def _to_iso_utc_from_sec(sec: int) -> str:
    return (
        datetime.fromtimestamp(sec, tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )

def _split_symbol(sym: str) -> Tuple[str, str]:
    # "BINANCE:BTCUSDT" -> ("BINANCE", "BTCUSDT")
    if ":" in sym:
        ex, rest = sym.split(":", 1)
        return ex, rest
    return "", sym

# ---- Binance (spot klines) -------------------------------------------------

BINANCE_SPOT = "https://api.binance.com"

def fetch_binance(symbol_canonical: str, limit: int = 120) -> Dict:
    """
    Usa /api/v3/klines (spot) com interval=1d.
    - symbol_canonical: "BINANCE:BTCUSDT", etc.
    - limit: nº de candles (máx aceito pela API é 1000).
    Retorno compatível com src.job.
    """
    venue, pair = _split_symbol(symbol_canonical)
    if not pair:
        raise ValueError(f"symbol_canonical inválido para Binance: {symbol_canonical}")

    url = f"{BINANCE_SPOT}/api/v3/klines"
    params = {"symbol": pair.upper(), "interval": "1d", "limit": str(max(1, min(limit, 1000)))}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    rows = r.json()

    closes: List[float] = []
    times: List[str] = []
    # Resposta: [ openTime, open, high, low, close, volume, closeTime, ... ]
    for row in rows:
        try:
            close = float(row[4])
            close_time_ms = int(row[6])
        except Exception:
            continue
        closes.append(close)
        times.append(_to_iso_utc_from_ms(close_time_ms))

    return {
        "symbol": symbol_canonical,
        "venue": venue or "BINANCE",
        "series": {"c": closes, "t": times},
        "source": "binance_spot",
    }

# ---- Coingecko -------------------------------------------------------------

# Endpoint: /coins/{id}/market_chart?vs_currency=usd&days=...
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

# Mapa mínimo de segurança se coingecko_map.json não estiver disponível:
_FALLBACK_ID_MAP: Dict[str, str] = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "LINK": "chainlink",
    "XRP": "ripple",
    "FET": "fetch-ai",
    "ADA": "cardano",
    "DOT": "polkadot",
    "ATOM": "cosmos",
    "ARB": "arbitrum",
    "OP": "optimism",
    "AAVE": "aave",
    "UNI": "uniswap",
    "LTC": "litecoin",
    "TRX": "tron",
    "TON": "the-open-network",
    "NEAR": "near",
    "INJ": "injective",
}

def _load_cg_map() -> Dict[str, str]:
    """
    Carrega coingecko_map.json (se existir) da raiz do repo. Formato esperado:
    { "BTC": "bitcoin", "ETH": "ethereum", ... }
    """
    p = Path("coingecko_map.json")
    if not p.exists():
        return {}
    try:
        with p.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
            if isinstance(data, dict):
                # normaliza chaves para upper
                return {k.upper(): str(v) for k, v in data.items()}
    except Exception:
        pass
    return {}

def _resolve_cg_id(symbol_canonical: str) -> Optional[str]:
    """
    De "BINANCE:BTCUSDT" -> "BTC" -> "bitcoin"
    """
    _, right = _split_symbol(symbol_canonical)  # "BTCUSDT"
    base = right.upper().replace("USDT", "").replace("USD", "")
    m = _load_cg_map()
    if base in m:
        return m[base]
    return _FALLBACK_ID_MAP.get(base)

def _days_from_limit(limit: int) -> int:
    """
    Coingecko 'market_chart' aceita dias (inteiro). Aproxima a partir do número de candles desejado.
    """
    # Se queremos ~N candles diários, pedir ligeiramente a mais para garantir cobertura
    n = max(1, int(limit))
    return max(1, min(10950, int(n * 1.2)))  # teto ~30 anos

def fetch_coingecko(symbol_canonical: str, limit: int = 120) -> Dict:
    """
    Usa /coins/{id}/market_chart?vs_currency=usd&days=...
    Retorno compatível com src.job.
    """
    coin_id = _resolve_cg_id(symbol_canonical)
    if not coin_id:
        raise ValueError(f"Não foi possível mapear Coingecko ID para '{symbol_canonical}'")

    days = _days_from_limit(limit)
    url = f"{COINGECKO_BASE}/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": str(days)}
    r = requests.get(url, params=params, timeout=25)
    r.raise_for_status()
    data = r.json()

    prices = data.get("prices") or []  # [[ts_ms, price], ...]
    closes: List[float] = []
    times: List[str] = []

    # Coingecko traz vários pontos intradiários; vamos decimar para 1 ponto/dia aproximado:
    # Estratégia: pegar o último ponto de cada dia (UTC) — simples e robusto.
    last_by_day: Dict[str, Tuple[int, float]] = {}
    for ts_ms, px in prices:
        # Normaliza para data UTC (YYYY-MM-DD)
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        day_key = dt.date().isoformat()
        last_by_day[day_key] = (int(ts_ms), float(px))

    # Ordena por data e monta vetores
    for day in sorted(last_by_day.keys()):
        ts_ms, px = last_by_day[day]
        closes.append(px)
        times.append(_to_iso_utc_from_ms(ts_ms))

    # aplica limit final
    if limit and len(closes) > limit:
        closes = closes[-limit:]
        times  = times[-limit:]

    venue, _ = _split_symbol(symbol_canonical)
    return {
        "symbol": symbol_canonical,
        "venue": venue or "BINANCE",
        "series": {"c": closes, "t": times},
        "source": "coingecko",
    }

# Alias opcional de compatibilidade (se algum módulo antigo chamar):
def fetch_binance_1d(symbol_canonical: str, limit: int = 120) -> Dict:
    return fetch_binance(symbol_canonical, limit=limit)

__all__ = ["fetch_binance", "fetch_coingecko", "fetch_binance_1d"]

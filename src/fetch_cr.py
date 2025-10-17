from __future__ import annotations
import requests
import pandas as pd
from datetime import datetime, timezone
from typing import Optional

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def _to_date_iso(ts_ms: int) -> str:
    # CoinGecko e Binance vêm em ms/seg; normalizamos para data UTC (YYYY-MM-DD)
    # Binance klines usam ms em k[0]; CoinGecko market_chart usa ms em "prices"
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date().isoformat()

def _ensure_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    # Garante colunas padrão esperadas pelo resto da pipeline
    cols = ["Date", "open", "high", "low", "close"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df[cols]

# -------------------------------------------------------------------
# Binance (1d klines)
# sym_can: "BINANCE:BTCUSDT"
# -------------------------------------------------------------------
def fetch_binance(sym_can: str, days: int) -> Optional[pd.DataFrame]:
    try:
        pair = sym_can.split(":")[1]  # <-- chave correta (ex.: "BTCUSDT")
    except Exception:
        return None

    url = f"https://api.binance.com/api/v3/klines?symbol={pair}&interval=1d&limit={max(10, days)}"
    r = requests.get(url, timeout=20)
    if r.status_code != 200:
        return None
    data = r.json()
    if not isinstance(data, list) or not data:
        return None

    rows = []
    for k in data:
        # kline: [open time(ms), open, high, low, close, volume, close time(ms), ...]
        rows.append({
            "Date": _to_date_iso(int(k[0])),
            "open": float(k[1]),
            "high": float(k[2]),
            "low":  float(k[3]),
            "close":float(k[4]),
        })
    df = pd.DataFrame(rows)
    df = _ensure_ohlc(df).sort_values("Date").reset_index(drop=True)
    return df.tail(days).reset_index(drop=True)

# -------------------------------------------------------------------
# CoinGecko (market_chart)
# sym_can: "BINANCE:BTCUSDT"
# cg_map:  dict com { "BTCUSDT": "bitcoin", ... }
# -------------------------------------------------------------------
def fetch_coingecko(sym_can: str, cg_map: dict, days: int) -> Optional[pd.DataFrame]:
    try:
        pair = sym_can.split(":")[1]              # <-- usar o PAR como chave do mapa
        coin_id = cg_map.get(pair)                # ex.: "BTCUSDT" -> "bitcoin"
    except Exception:
        return None
    if not coin_id:
        return None

    # CoinGecko aceita days inteiros; pedimos um pouco mais para segurança
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?vs_currency=usd&days={max(10, days)}&interval=daily"
    r = requests.get(url, timeout=20)
    if r.status_code != 200:
        return None
    j = r.json()
    prices = j.get("prices") or []
    if not prices:
        return None

    rows = []
    for ts_ms, px in prices:
        rows.append({
            "Date": _to_date_iso(int(ts_ms)),
            "open": float(px),
            "high": float(px),
            "low":  float(px),
            "close":float(px),
        })
    df = pd.DataFrame(rows)
    df = _ensure_ohlc(df).sort_values("Date").reset_index(drop=True)
    return df.tail(days).reset_index(drop=True)

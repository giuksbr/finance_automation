# -*- coding: utf-8 -*-
# src/fetch_cr.py
import json
import time
import typing as T
from urllib.request import urlopen, Request
from urllib.parse import urlencode

Candle = T.Dict[str, T.Union[str, float]]

def _get_json(url: str, headers=None, timeout=30):
    req = Request(url, headers=headers or {"User-Agent":"Mozilla/5.0"})
    with urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))

def _canon_to_binance_pair(symbol_canonical: str) -> str:
    # "BINANCE:BTCUSDT" -> "BTCUSDT"
    if ":" in symbol_canonical:
        return symbol_canonical.split(":",1)[1]
    return symbol_canonical

def _fetch_binance_one(symbol_canonical: str, days: int=120) -> T.List[Candle]:
    pair = _canon_to_binance_pair(symbol_canonical)
    qs = urlencode({"symbol":pair, "interval":"1d", "limit": max(days, 120)})
    url = f"https://api.binance.com/api/v3/klines?{qs}"
    arr = _get_json(url)
    out: T.List[Candle] = []
    for k in arr:
        # [openTime, open, high, low, close, volume, closeTime, ...]
        tms = int(k[0]) // 1000
        import time as _t
        iso = _t.strftime("%Y-%m-%dT%H:%M:%SZ", _t.gmtime(tms))
        out.append({
            "t": iso,
            "o": float(k[1]),
            "h": float(k[2]),
            "l": float(k[3]),
            "c": float(k[4]),
            "v": float(k[5]),
        })
    return out[-days:] if days and len(out) > days else out

def _normalize_input(symbols=None, symbol_canonical=None) -> T.List[str]:
    if symbols and isinstance(symbols, (list, tuple)):
        return list(symbols)
    if symbol_canonical and isinstance(symbol_canonical, str):
        return [symbol_canonical]
    if isinstance(symbols, str):
        return [symbols]
    return []

def fetch_binance(symbols=None, symbol_canonical=None, days: int=120, **kwargs) -> T.Dict[str, T.List[Candle]]:
    syms = _normalize_input(symbols, symbol_canonical)
    out: T.Dict[str, T.List[Candle]] = {}
    for sc in syms:
        try:
            out[sc] = _fetch_binance_one(sc, days=days)
            time.sleep(0.2)
        except Exception:
            out[sc] = []
    return out

def fetch_coingecko(symbols=None, symbol_canonical=None, days: int=120, **kwargs) -> T.Dict[str, T.List[Candle]]:
    """Shim compatível: usa Binance por baixo."""
    return fetch_binance(symbols=symbols, symbol_canonical=symbol_canonical, days=days)

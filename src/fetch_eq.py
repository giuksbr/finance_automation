# -*- coding: utf-8 -*-
# src/fetch_eq.py
import csv
import io
import time
import typing as T
from urllib.request import urlopen, Request
from urllib.parse import urlencode

# Candle padronizado:
# t: ISO "YYYY-MM-DDT00:00:00Z", o/h/l/c/v: float
Candle = T.Dict[str, T.Union[str, float]]

def _canon_to_stooq(symbol_canonical: str) -> str:
    # "NYSEARCA:VUG" -> "VUG.US"
    # "NASDAQ:BRK.B" -> "BRK.B.US"
    if ":" not in symbol_canonical:
        return f"{symbol_canonical}.US"
    _, tick = symbol_canonical.split(":", 1)
    return f"{tick}.US"

def _get(url: str, headers: T.Optional[T.Dict[str,str]]=None, timeout=20) -> bytes:
    req = Request(url, headers=headers or {"User-Agent":"Mozilla/5.0"})
    with urlopen(req, timeout=timeout) as r:
        return r.read()

def _parse_stooq_csv(raw: bytes) -> T.List[Candle]:
    # CSV: Date,Open,High,Low,Close,Volume
    out: T.List[Candle] = []
    s = raw.decode("utf-8", errors="ignore")
    f = io.StringIO(s)
    rdr = csv.DictReader(f)
    for row in rdr:
        d = row.get("Date") or row.get("date")
        o = row.get("Open") or row.get("open")
        h = row.get("High") or row.get("high")
        l = row.get("Low")  or row.get("low")
        c = row.get("Close") or row.get("close")
        v = row.get("Volume") or row.get("volume") or "0"
        if not (d and c):
            continue
        ts = f"{d}T00:00:00Z"
        try:
            out.append({
                "t": ts,
                "o": float(o or c),
                "h": float(h or c),
                "l": float(l or c),
                "c": float(c),
                "v": float(v or 0.0),
            })
        except:
            continue
    return out

def _fetch_stooq_one(symbol_canonical: str, days: int=120) -> T.List[Candle]:
    st = _canon_to_stooq(symbol_canonical).lower()
    url = f"https://stooq.com/q/d/l/?{urlencode({'s': st, 'i': 'd'})}"
    raw = _get(url)
    candles = _parse_stooq_csv(raw)
    return candles[-days:] if days and len(candles) > days else candles

def _normalize_input(symbols=None, symbol_canonical=None) -> T.List[str]:
    if symbols and isinstance(symbols, (list, tuple)):
        return list(symbols)
    if symbol_canonical and isinstance(symbol_canonical, str):
        return [symbol_canonical]
    if isinstance(symbols, str):
        return [symbols]
    return []

def fetch_stooq(symbols=None, symbol_canonical=None, days: int=120, **kwargs) -> T.Dict[str, T.List[Candle]]:
    """Equities via Stooq. Retorna {canonical: [candles]}."""
    syms = _normalize_input(symbols, symbol_canonical)
    out: T.Dict[str, T.List[Candle]] = {}
    for sc in syms:
        try:
            out[sc] = _fetch_stooq_one(sc, days=days)
            time.sleep(0.2)
        except Exception:
            out[sc] = []
    return out

def fetch_yahoo(symbols=None, symbol_canonical=None, days: int=120, **kwargs) -> T.Dict[str, T.List[Candle]]:
    """Shim compatível (usa Stooq por baixo)."""
    return fetch_stooq(symbols=symbols, symbol_canonical=symbol_canonical, days=days)

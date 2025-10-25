#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Enriquece o OHLCV local garantindo:
- séries diárias para todos os símbolos do universo (eq/cr),
- campos derivados por símbolo: pct_chg_7d/10d/30d e last_close_ts_utc,
- grava novo arquivo em public/ e atualiza pointer_signals_v1.json.

Fonte dos dados:
- Crypto: Binance Futures (klines 1d)
- Equity/ETF: Stooq (CSV diário)

Requisitos: requests, csv (built-in)
"""

from __future__ import annotations

import csv
import io
import json
import time
import math
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

import requests

ROOT = Path(__file__).resolve().parents[1]
PUB = ROOT / "public"

BINANCE_FAPI = "https://fapi.binance.com"
STOOQ_DAILY = "https://stooq.com/q/d/l/?s={symbol}&i=d"

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _read_json(p: Path) -> dict:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def _write_json_atomic(p: Path, data: dict) -> None:
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":"), indent=2), encoding="utf-8")
    tmp.replace(p)

def _latest_glob(pattern: str) -> Optional[Path]:
    files = sorted(PUB.glob(pattern))
    return files[-1] if files else None

def _load_pointer_paths() -> dict:
    ptr_file = PUB / "pointer_signals_v1.json"
    out = {"signals": None, "ohlcv": None, "indicators": None}
    if ptr_file.exists():
        try:
            data = _read_json(ptr_file)
            for k in ("signals", "ohlcv", "indicators"):
                v = data.get(k)
                if isinstance(v, str) and v:
                    out[k] = (ROOT / v).resolve() if v.startswith("public/") else Path(v).resolve()
        except Exception:
            pass
    if not out["signals"]:
        out["signals"] = _latest_glob("n_signals_v1_*.json") or (PUB / "n_signals_v1_latest.json")
    if not out["ohlcv"]:
        out["ohlcv"] = _latest_glob("ohlcv_cache_*.json") or (PUB / "ohlcv_cache.json")
    if not out["indicators"]:
        out["indicators"] = _latest_glob("indicators_*.json") or (PUB / "indicators.json")
    return out

def _split_symbol(sym: str) -> Tuple[str, str]:
    if ":" in sym:
        ex, tic = sym.split(":", 1)
        return ex, tic
    return "", sym

def _pct(now: Optional[float], prev: Optional[float]) -> Optional[float]:
    try:
        if now is None or prev is None or prev == 0:
            return None
        return (float(now)/float(prev) - 1.0) * 100.0
    except Exception:
        return None

def _to_iso(ts: int | float | str) -> Optional[str]:
    try:
        if ts is None:
            return None
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(float(ts), tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if isinstance(ts, str) and ts.isdigit():
            return datetime.fromtimestamp(int(ts), tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if isinstance(ts, str) and ts.endswith("Z"):
            return ts
    except Exception:
        return None
    return None

def _binance_klines_1d(symbol: str, limit: int = 120) -> Tuple[List[float], List[str]]:
    """
    Futures kline: /fapi/v1/continuousKlines?pair=BTCUSDT&contractType=PERPETUAL&interval=1d&limit=xxx
    Retorna closes, timestamps (UTC ISOZ)
    """
    pair = symbol.upper()
    url = f"{BINANCE_FAPI}/fapi/v1/continuousKlines"
    params = {"pair": pair, "contractType": "PERPETUAL", "interval": "1d", "limit": str(limit)}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    closes, times = [], []
    for row in data:
        # kline array fields: [0 openTime,1 open,2 high,3 low,4 close,5 volume,6 closeTime, ...]
        close = float(row[4])
        close_time_ms = int(row[6])
        closes.append(close)
        times.append(_to_iso(close_time_ms // 1000))
    return closes, times

def _stooq_csv(ticker_canon: str) -> Tuple[List[float], List[str]]:
    """
    Stooq espera 'nvda.us', 'spy.us', etc. Converter venue conhecido para sufixo .us quando EUA.
    """
    exch, tic = _split_symbol(ticker_canon)
    # regra simples: venue EUA → .us ; caso contrário, tenta .us mesmo (Stooq cobre as principais dos EUA)
    stq = f"{tic}.us".lower().replace("/", "-")
    url = STOOQ_DAILY.format(symbol=stq)
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    text = r.text
    reader = csv.DictReader(io.StringIO(text))
    closes, times = [], []
    for row in reader:
        # Data no formato YYYY-MM-DD
        try:
            c = float(row["Close"])
            d = row["Date"]
        except Exception:
            continue
        closes.append(c)
        # usar 00:00:00Z para equities (fechamento do dia)
        times.append(f"{d}T00:00:00Z")
    return closes, times

def _ensure_series(section: Dict[str, Any], sym: str, asset_type: str, min_len: int = 31) -> Tuple[List[float], List[str], bool]:
    """
    Garante que exista série (closes,times) para o símbolo.
    Retorna (closes, times, created_or_extended: bool)
    """
    created = False
    node = section.get(sym) or {}
    series = node.get("series") or {}
    closes = series.get("c") or series.get("close") or []
    times = series.get("t") or series.get("ts") or []

    if isinstance(closes, list) and len(closes) >= min_len and isinstance(times, list) and len(times) == len(closes):
        return closes, times, created

    # Buscar da fonte
    try:
        if asset_type == "cr":
            pair = sym.split(":", 1)[1]  # BINANCE:BTCUSDT -> BTCUSDT
            closes, times = _binance_klines_1d(pair, limit=max(min_len, 120))
        else:
            closes, times = _stooq_csv(sym)
        if len(closes) >= min_len and len(times) == len(closes):
            node["series"] = {"c": closes, "t": times}
            section[sym] = node
            created = True
    except Exception as e:
        # mantém como estava
        pass

    return closes, times, created

def _derived_from_series(closes: List[float], times: List[str]) -> Dict[str, Any]:
    last = closes[-1] if closes else None
    ts = times[-1] if times else None
    c7  = closes[-8]  if len(closes) >= 8  else None
    c10 = closes[-11] if len(closes) >= 11 else None
    c30 = closes[-31] if len(closes) >= 31 else None
    return {
        "last_close": last,
        "last_close_ts_utc": ts,
        "pct_chg_7d": _pct(last, c7),
        "pct_chg_10d": _pct(last, c10),
        "pct_chg_30d": _pct(last, c30),
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-len", type=int, default=31, help="mínimo de candles por série")
    ap.add_argument("--throttle-ms", type=int, default=0, help="sleep entre downloads (0=desligado)")
    args = ap.parse_args()

    ptr = _load_pointer_paths()
    sig_path = Path(ptr["signals"])
    ohlcv_path = Path(ptr["ohlcv"]) if ptr["ohlcv"] else None

    if not sig_path.exists():
        print(f"[erro] signals não encontrado: {sig_path}")
        return

    signals = _read_json(sig_path)
    symbols: List[Tuple[str, str]] = []  # (asset_type, symbol_canonical)
    for s in signals.get("signals", []):
        at = s.get("asset_type")
        sc = s.get("symbol_canonical")
        if at and sc:
            # normalizar asset_type para eq/cr
            at_norm = "cr" if at in ("cr", "crypto") else "eq"
            symbols.append((at_norm, sc))

    # Carregar cache atual (pode estar vazio)
    cache = {"eq": {}, "cr": {}, "generated_at_utc": None}
    if ohlcv_path and ohlcv_path.exists():
        try:
            cache = _read_json(ohlcv_path)
            if "eq" not in cache: cache["eq"] = {}
            if "cr" not in cache: cache["cr"] = {}
        except Exception:
            pass

    changed = False
    for at, sym in symbols:
        sec = cache[at]
        closes, times, created = _ensure_series(sec, sym, at, min_len=args.min_len)
        if created:
            changed = True
        # gerar derivados mesmo que já existisse série
        if closes and times and len(closes) >= 2 and len(times) == len(closes):
            derived = _derived_from_series(closes, times)
            node = sec.get(sym) or {}
            node["derived"] = derived
            sec[sym] = node
            changed = True
        if args.throttle_ms > 0:
            time.sleep(args.throttle_ms / 1000.0)

    cache["generated_at_utc"] = _now_utc_iso()

    # Gravar novo arquivo em public/
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_ohlcv = PUB / f"ohlcv_cache_{ts}.json"
    _write_json_atomic(out_ohlcv, cache)

    # Atualizar pointer
    ptr_file = PUB / "pointer_signals_v1.json"
    if ptr_file.exists():
        try:
            pdata = _read_json(ptr_file)
        except Exception:
            pdata = {}
    else:
        pdata = {}
    pdata["ohlcv"] = f"public/{out_ohlcv.name}"
    _write_json_atomic(ptr_file, pdata)

    print(f"[ok] ohlcv enriquecido: {out_ohlcv}")

if __name__ == "__main__":
    main()

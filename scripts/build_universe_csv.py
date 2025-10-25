#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
from typing import Any, Dict, List, Optional

from src.pointer_utils import prefer_local_from_pointer as _prefer_local_from_pointer


OUT_CSV = "public/n_signals_universe_latest.csv"

FIELDS = [
    "symbol_canonical","asset_type","venue","window_used",
    "price_now_close","price_now_close_at_utc",
    "pct_chg_7d","pct_chg_10d","pct_chg_30d",
    "rsi14","atr14","atr14_pct","bb_ma20","bb_lower","bb_upper",
    "funding","oi_chg_3d_pct","priceguard","window_status","sources_used"
]


def _fmt(x):
    return "" if x is None else x


def _last_close_ts(candles: List[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(candles, list) or not candles:
        return None
    for row in reversed(candles):
        t = row.get("t"); c = row.get("c")
        if t and c is not None:
            return t
    return None


def _pct(a, b):
    try:
        a = float(a); b = float(b)
        if b == 0:
            return None
        return (a/b - 1.0) * 100.0
    except Exception:
        return None


def _pct_chg_window(candles: List[Dict[str, Any]], days: int) -> Optional[float]:
    if not isinstance(candles, list) or len(candles) < 2:
        return None
    closes = [r.get("c") for r in candles if r.get("c") is not None]
    if len(closes) < 2:
        return None
    now = closes[-1]
    idx = max(0, len(closes) - 1 - days)
    if idx >= len(closes):
        return None
    then = closes[idx]
    return _pct(now, then)


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    os.makedirs("public", exist_ok=True)

    # pointer com fallback automático
    ptr = {}
    if os.path.exists("public/pointer_signals_v1.json"):
        with open("public/pointer_signals_v1.json", "r", encoding="utf-8") as f:
            ptr = json.load(f)

    ohl_path = _prefer_local_from_pointer(ptr, "ohlcv_url", "ohlcv", "public/ohlcv_cache_*.json")
    ind_path = _prefer_local_from_pointer(ptr, "indicators_url", "indicators", "public/indicators_*.json")
    try:
        sig_path = _prefer_local_from_pointer(ptr, "signals_url", "signals", "public/n_signals_v1_*.json")
    except FileNotFoundError:
        sig_path = _prefer_local_from_pointer(ptr, "signals_url", "signals", "public/n_signals_*.json")

    ohl = _load_json(ohl_path)   # esperado: {"eq": {...}, "cr": {...}} com séries por símbolo
    ind = _load_json(ind_path)   # esperado: {"eq": {...}, "cr": {...}} com indicadores por símbolo
    sig = _load_json(sig_path)   # esperado: {"signals":[...]} ou lista legacy

    # determinar universo base
    if isinstance(sig, dict) and "signals" in sig:
        universe = sig.get("universe", sig.get("signals", []))
    elif isinstance(sig, list):
        universe = sig
    else:
        universe = []

    rows = []

    # index para lookup rápido de indicadores por símbolo
    ind_eq = (ind.get("eq") if isinstance(ind, dict) else {}) or {}
    ind_cr = (ind.get("cr") if isinstance(ind, dict) else {}) or {}

    ohl_eq = (ohl.get("eq") if isinstance(ohl, dict) else {}) or {}
    ohl_cr = (ohl.get("cr") if isinstance(ohl, dict) else {}) or {}

    for item in universe:
        # item pode ser dict já no formato v1 ou apenas string de símbolo
        if isinstance(item, dict):
            sym = item.get("symbol_canonical") or item.get("symbol") or ""
            asset_type = item.get("asset_type") or item.get("type") or ""
            venue = item.get("venue") or (sym.split(":")[0] if ":" in sym else "")
        else:
            sym = str(item)
            asset_type = "eq" if sym.startswith(("NYSE", "NASDAQ", "NYSEARCA")) else "crypto"
            venue = sym.split(":")[0] if ":" in sym else ""

        is_crypto = sym.startswith("BINANCE:")
        series = (ohl_cr if is_crypto else ohl_eq).get(sym, {}).get("candles") or (ohl_cr if is_crypto else ohl_eq).get(sym)
        candles = series if isinstance(series, list) else []
        ts_utc = _last_close_ts(candles)

        ind_map = (ind_cr if is_crypto else ind_eq).get(sym) or {}
        price_now = None
        if candles:
            # tenta último close não-nulo
            for r in reversed(candles):
                if r.get("c") is not None:
                    price_now = r.get("c")
                    break

        row = {
            "symbol_canonical": sym,
            "asset_type": ("crypto" if is_crypto else "eq") if not asset_type else asset_type,
            "venue": venue,
            "window_used": "7d",
            "price_now_close": _fmt(price_now),
            "price_now_close_at_utc": _fmt(ts_utc),
            "pct_chg_7d": _fmt(_pct_chg_window(candles, 7)),
            "pct_chg_10d": _fmt(_pct_chg_window(candles, 10)),
            "pct_chg_30d": _fmt(_pct_chg_window(candles, 30)),
            "rsi14": _fmt(ind_map.get("RSI14")),
            "atr14": _fmt(ind_map.get("ATR14")),
            "atr14_pct": _fmt(ind_map.get("ATR14_PCT")),
            "bb_ma20": _fmt(ind_map.get("BB_MA20")),
            "bb_lower": _fmt(ind_map.get("BB_LOWER")),
            "bb_upper": _fmt(ind_map.get("BB_UPPER")),
            "funding": _fmt(ind_map.get("FUNDING")),
            "oi_chg_3d_pct": _fmt(ind_map.get("OI_CHG_3D_PCT")),
            "priceguard": "OK",          # se você quiser, integre com priceguard aqui
            "window_status": "TARGET",   # idem
            "sources_used": "+".join(["yahoo","stooq","nasdaq"]) if not is_crypto else "+".join(["binance","coingecko"]),
        }
        rows.append(row)

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print(f"[ok] CSV gerado: {OUT_CSV} (linhas={len(rows)}, header=1, total={len(rows)+1})")

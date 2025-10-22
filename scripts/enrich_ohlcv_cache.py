#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Re-hidrata o arquivo public/ohlcv_cache_*.json adicionando séries colunar:
  - "c": lista de closes (floats)
  - "t": lista de timestamps (epoch segundos, UTC)

EQ/ETF  -> Yahoo Finance (primário) + Stooq CSV (fallback, **.us**)
CRYPTO  -> Binance (primário)

Uso:
  python scripts/enrich_ohlcv_cache.py [--limit 40] [--dry-run]

Requisitos:
  pip install requests
"""

from __future__ import annotations
import csv
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import requests

PUBLIC_DIR = Path("public")
OHL_PAT = "ohlcv_cache_*.json"

# ========== Helpers de mapeamento ==========

def _to_yahoo_symbol(symbol_canonical: str) -> str:
    """
    "VENUE:TICKER" -> Yahoo: usa parte após ":" e troca "." por "-".
    NYSE:BRK.B -> BRK-B
    """
    ticker = symbol_canonical.split(":", 1)[1] if ":" in symbol_canonical else symbol_canonical
    return ticker.replace(".", "-").strip()

def _to_stooq_symbol_us(symbol_canonical: str) -> str:
    """
    Stooq para ativos dos EUA requer sufixo '.us' e minúsculas:
      VUG -> vug.us
      IVW -> ivw.us
      IUSG -> iusg.us
    """
    ticker = symbol_canonical.split(":", 1)[1] if ":" in symbol_canonical else symbol_canonical
    return f"{ticker.lower().strip()}.us"

def _to_binance_pair(symbol_canonical: str) -> str:
    """'BINANCE:BTCUSDT' -> 'BTCUSDT'."""
    return symbol_canonical.split(":", 1)[1].strip() if ":" in symbol_canonical else symbol_canonical.strip()

# ========== Utils ==========

def _iso_to_epoch_seconds(iso_yyyy_mm_dd: str) -> int:
    dt = datetime.strptime(iso_yyyy_mm_dd, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())

# ========== Fetchers ==========

def fetch_yahoo_daily_colunar(yahoo_symbol: str, limit: int = 40) -> Optional[Tuple[List[float], List[int]]]:
    """
    Yahoo /v8/finance/chart (diário) -> (closes, timestamps_epoch) ou None
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}"
    params = {"interval": "1d", "range": "2mo"}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        res = data.get("chart", {}).get("result") or []
        if not res:
            return None
        res0 = res[0]
        ts = res0.get("timestamp") or []
        quotes = (res0.get("indicators", {}).get("quote") or [])
        if not quotes:
            return None
        closes = quotes[0].get("close") or []
        series = [(t, c) for t, c in zip(ts, closes) if c is not None]
        if not series:
            return None
        series = series[-limit:]
        t_out = [int(t) for t, _ in series]
        c_out = [float(c) for _, c in series]
        return c_out, t_out
    except Exception:
        return None

def fetch_stooq_daily_colunar(stooq_symbol_us: str, limit: int = 40) -> Optional[Tuple[List[float], List[int]]]:
    """
    Fallback Stooq CSV diário (ex.: s=vug.us, i=d):
      https://stooq.com/q/d/l/?s=vug.us&i=d
    """
    url = "https://stooq.com/q/d/l/"
    params = {"s": stooq_symbol_us, "i": "d"}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        txt = r.text
        if not txt or txt.strip() == "" or txt.lower().startswith("404"):
            return None
        c_list: List[float] = []
        t_list: List[int] = []
        reader = csv.DictReader(io.StringIO(txt))
        for row in reader:
            d = row.get("Date")
            c = row.get("Close")
            if not d or not c:
                continue
            try:
                c_val = float(c)
                t_val = _iso_to_epoch_seconds(d)
            except Exception:
                continue
            c_list.append(c_val)
            t_list.append(t_val)
        if not c_list:
            return None
        c_list = c_list[-limit:]
        t_list = t_list[-limit:]
        return c_list, t_list
    except Exception:
        return None

def fetch_binance_daily_colunar(pair: str, limit: int = 40) -> Optional[Tuple[List[float], List[int]]]:
    """Binance klines diário."""
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": pair, "interval": "1d", "limit": str(limit)}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        arr = r.json()
        if not isinstance(arr, list) or not arr:
            return None
        closes, ts = [], []
        for k in arr:
            try:
                closes.append(float(k[4]))        # close
                ts.append(int(k[6] // 1000))      # closeTime (ms -> s)
            except Exception:
                continue
        if not closes or len(closes) != len(ts):
            return None
        return closes, ts
    except Exception:
        return None

# ========== Núcleo ==========

def find_latest_ohlcv_path() -> Optional[Path]:
    files = sorted(PUBLIC_DIR.glob(OHL_PAT), reverse=True)
    return files[0] if files else None

def enrich_cache_file(path: Path, limit: int = 40, dry_run: bool = False) -> Tuple[int, int, int]:
    data: Dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))

    total = 0
    changed = 0
    failed = 0

    for sec in ("eq", "cr"):
        bucket = data.get(sec)
        if not isinstance(bucket, dict):
            continue

        for sym, obj in bucket.items():
            total += 1

            # já tem série? (qualquer formato mínimo)
            if isinstance(obj, dict) and (isinstance(obj.get("c"), list) or isinstance(obj.get("close"), list)):
                continue

            got: Optional[Tuple[List[float], List[int]]] = None

            if sec == "eq":
                # primário: Yahoo
                ysym = _to_yahoo_symbol(sym)
                got = fetch_yahoo_daily_colunar(ysym, limit=limit)

                # fallback: Stooq .us
                if not got:
                    ssym = _to_stooq_symbol_us(sym)  # <<<<<<<<<< SUFIXO .us AQUI
                    got = fetch_stooq_daily_colunar(ssym, limit=limit)
            else:
                pair = _to_binance_pair(sym)
                got = fetch_binance_daily_colunar(pair, limit=limit)

            if not got:
                failed += 1
                continue

            closes, ts = got
            if isinstance(obj, dict):
                obj["c"] = closes
                obj["t"] = ts
                obj["count"] = len(closes)
            else:
                bucket[sym] = {
                    "window": (data.get("window") or "7d"),
                    "count": len(closes),
                    "c": closes,
                    "t": ts,
                }
            changed += 1

    if changed and not dry_run:
        path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

    return total, changed, failed

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=40, help="nº máx de barras por símbolo (default: 40)")
    ap.add_argument("--dry-run", action="store_true", help="não grava; só reporta")
    args = ap.parse_args()

    p = find_latest_ohlcv_path()
    if not p:
        print("[err] nenhum public/ohlcv_cache_*.json encontrado")
        raise SystemExit(2)

    total, changed, failed = enrich_cache_file(p, limit=args.limit, dry_run=args.dry_run)
    tag = p.name
    print(f"[ok] {tag}: enriquecidos {changed}/{total} • falharam {failed}")
    if failed:
        print("     dica: se ainda faltar EQ/ETF, testaremos outro fallback YF CSV e/ou Nasdaq.")

if __name__ == "__main__":
    main()

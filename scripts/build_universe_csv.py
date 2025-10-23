#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera public/n_signals_universe_latest.csv (UTF-8) com 1 linha por ativo,
usando LF (\n) como terminador de linha (obrigatório para o RAW do GitHub
contar corretamente as linhas).

Cabeçalho fixo (ordem exata):
symbol_canonical,asset_type,venue,window_used,price_now_close,price_now_close_at_utc,
pct_chg_7d,pct_chg_10d,pct_chg_30d,rsi14,atr14,atr14_pct,bb_ma20,bb_lower,bb_upper,
funding,oi_chg_3d_pct,priceguard,window_status,sources_used
"""

from __future__ import annotations
import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ROOT / "public"

URL_JSON_LATEST = PUBLIC / "n_signals_v1_latest.json"
OUT_CSV = PUBLIC / "n_signals_universe_latest.csv"

HEADER = [
    "symbol_canonical",
    "asset_type",
    "venue",
    "window_used",
    "price_now_close",
    "price_now_close_at_utc",
    "pct_chg_7d",
    "pct_chg_10d",
    "pct_chg_30d",
    "rsi14",
    "atr14",
    "atr14_pct",
    "bb_ma20",
    "bb_lower",
    "bb_upper",
    "funding",
    "oi_chg_3d_pct",
    "priceguard",
    "window_status",
    "sources_used",
]

def _read_json(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def _to_num(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None

def _list_sources(v: Any) -> str:
    if isinstance(v, list) and v:
        return "+".join(str(x) for x in v)
    return ""

def _venue_from_symbol(symbol_canonical: str) -> str:
    # "VENUE:TICKER" (EQ/ETF) | "BINANCE:PAIR" (CR)
    if ":" in symbol_canonical:
        return symbol_canonical.split(":", 1)[0]
    return ""

def _row_from_item(u: Dict[str, Any]) -> List[str]:
    asset_type = u.get("asset_type") or ""
    symbol = u.get("symbol_canonical") or ""
    venue = _venue_from_symbol(symbol)

    window_used = u.get("window_used") or ""
    price_now_close = _to_num(u.get("price_now_close"))
    price_now_close_at_utc = u.get("price_now_close_at_utc") or ""

    pct7 = _to_num(u.get("pct_chg_7d"))
    pct10 = _to_num(u.get("pct_chg_10d"))
    pct30 = _to_num(u.get("pct_chg_30d"))

    rsi14 = _to_num(u.get("rsi14"))
    atr14 = _to_num(u.get("atr14"))
    atr14_pct = _to_num(u.get("atr14_pct"))
    bb_ma20 = _to_num(u.get("bb_ma20"))
    bb_lower = _to_num(u.get("bb_lower"))
    bb_upper = _to_num(u.get("bb_upper"))

    der = u.get("derivatives") or {}
    funding = _to_num(der.get("funding")) if asset_type == "crypto" else None
    oi3d = _to_num(der.get("oi_chg_3d_pct")) if asset_type == "crypto" else None

    val = u.get("validation") or {}
    priceguard = val.get("priceguard") or ""
    window_status = val.get("window_status") or (u.get("window_status") or "")
    sources_used = _list_sources(val.get("sources_used") or u.get("sources_used"))

    def _fmt(x: Optional[float]) -> str:
        return "" if x is None else str(x)

    return [
        symbol,
        asset_type,
        venue,
        window_used,
        _fmt(price_now_close),
        price_now_close_at_utc,
        _fmt(pct7),
        _fmt(pct10),
        _fmt(pct30),
        _fmt(rsi14),
        _fmt(atr14),
        _fmt(atr14_pct),
        _fmt(bb_ma20),
        _fmt(bb_lower),
        _fmt(bb_upper),
        _fmt(funding),
        _fmt(oi3d),
        priceguard,
        window_status,
        sources_used,
    ]

def main() -> None:
    if not URL_JSON_LATEST.exists():
        print(f"[erro] não encontrei {URL_JSON_LATEST}")
        sys.exit(1)

    data = _read_json(URL_JSON_LATEST)
    universe = data.get("universe") or []
    if not isinstance(universe, list):
        print("[erro] formato inesperado: .universe não é lista")
        sys.exit(2)

    # Escreve o CSV com LF por linha
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=",", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        w.writerow(HEADER)
        count = 0
        for u in universe:
            row = _row_from_item(u)
            w.writerow(row)
            count += 1

    print(f"[ok] CSV gerado: {OUT_CSV} (linhas={count}, header=1, total={count+1})")
    print("[dica] confira no RAW do GitHub; agora cada linha termina com LF (\\n).")

if __name__ == "__main__":
    main()

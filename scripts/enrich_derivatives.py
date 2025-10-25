#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Enriquece indicadores para CRIPTO com:
- funding (última taxa) via /fapi/v1/premiumIndex
- oi_chg_3d_pct via /futures/data/openInterestHist (period=1d, últimos 4 pontos)

Atualiza public/indicators_*.json preservando os campos existentes (RSI/ATR/BB).
"""

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

ROOT = Path(__file__).resolve().parents[1]
PUB = ROOT / "public"

BINANCE_FAPI = "https://fapi.binance.com"

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
    out = {"signals": None, "indicators": None}
    if ptr_file.exists():
        try:
            data = _read_json(ptr_file)
            for k in ("signals", "indicators"):
                v = data.get(k)
                if isinstance(v, str) and v:
                    out[k] = (ROOT / v).resolve() if v.startswith("public/") else Path(v).resolve()
        except Exception:
            pass
    if not out["signals"]:
        out["signals"] = _latest_glob("n_signals_v1_*.json") or (PUB / "n_signals_v1_latest.json")
    if not out["indicators"]:
        out["indicators"] = _latest_glob("indicators_*.json") or (PUB / "indicators.json")
    return out

def _binance_funding(symbol: str) -> Optional[float]:
    # premiumIndex traz lastFundingRate
    url = f"{BINANCE_FAPI}/fapi/v1/premiumIndex"
    r = requests.get(url, params={"symbol": symbol}, timeout=10)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        data = data[0] if data else {}
    val = data.get("lastFundingRate")
    return float(val) if val is not None else None

def _binance_oi_chg_3d_pct(symbol: str) -> Optional[float]:
    """
    Usa openInterestHist diário (period=1d) últimos 4 pontos
    """
    url = f"{BINANCE_FAPI}/futures/data/openInterestHist"
    params = {"symbol": symbol, "period": "1d", "limit": "4"}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    arr = r.json()
    if not isinstance(arr, list) or len(arr) < 2:
        return None
    # arr[-1] vs arr[0] ou arr[-4] se houver 4 pontos
    first = float(arr[-4]["sumOpenInterest"]) if len(arr) >= 4 else float(arr[0]["sumOpenInterest"])
    last = float(arr[-1]["sumOpenInterest"])
    if first == 0:
        return None
    return (last/first - 1.0) * 100.0

def main():
    ptr = _load_pointer_paths()
    sig_path = Path(ptr["signals"])
    ind_path = Path(ptr["indicators"]) if ptr["indicators"] else None

    if not sig_path.exists():
        print(f"[erro] signals não encontrado: {sig_path}")
        return

    signals = _read_json(sig_path)
    indicators = {}
    if ind_path and ind_path.exists():
        indicators = _read_json(ind_path)
    if "eq" not in indicators: indicators["eq"] = {}
    if "cr" not in indicators: indicators["cr"] = {}

    changed = False
    for s in signals.get("signals", []):
        sc = s.get("symbol_canonical")
        at = s.get("asset_type")
        if not sc or not at:
            continue
        if at not in ("crypto", "cr"):
            continue
        pair = sc.split(":", 1)[1]  # BINANCE:BTCUSDT -> BTCUSDT
        try:
            f = _binance_funding(pair)
        except Exception:
            f = None
        try:
            oi = _binance_oi_chg_3d_pct(pair)
        except Exception:
            oi = None

        node = indicators["cr"].get(sc) or {}
        # preserva técnicos existentes
        node["funding"] = f
        node["oi_chg_3d_pct"] = oi
        indicators["cr"][sc] = node
        changed = True

    if changed:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out_ind = PUB / f"indicators_{ts}.json"
        indicators["generated_at_utc"] = _now_utc_iso()
        _write_json_atomic(out_ind, indicators)

        # atualizar pointer
        ptr_file = PUB / "pointer_signals_v1.json"
        try:
            pdata = _read_json(ptr_file)
        except Exception:
            pdata = {}
        pdata["indicators"] = f"public/{out_ind.name}"
        _write_json_atomic(ptr_file, pdata)
        print(f"[ok] indicators enriquecido (derivatives): {out_ind}")
    else:
        print("[ok] indicators sem alterações (derivatives)")

if __name__ == "__main__":
    main()

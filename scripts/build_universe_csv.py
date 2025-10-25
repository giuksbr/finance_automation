#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera public/n_signals_universe_latest.csv no layout solicitado:

symbol_canonical,asset_type,venue,window_used,price_now_close,price_now_close_at_utc,
pct_chg_7d,pct_chg_10d,pct_chg_30d,rsi14,atr14,atr14_pct,bb_ma20,bb_lower,bb_upper,
funding,oi_chg_3d_pct,priceguard,window_status,sources_used
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
PUB = ROOT / "public"
OUT_CSV = PUB / "n_signals_universe_latest.csv"

# ----------------------------
# util
# ----------------------------
def _read_json(p: Path) -> dict:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def _latest_glob(pattern: str) -> Path | None:
    files = sorted(PUB.glob(pattern))
    return files[-1] if files else None

def _load_pointer_paths() -> dict:
    """
    Lê public/pointer_signals_v1.json, preferindo caminhos locais.
    Faz fallback por glob quando necessário.
    """
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

    # Fallbacks por glob
    if not out["signals"]:
        out["signals"] = _latest_glob("n_signals_v1_*.json") or (PUB / "n_signals_v1_latest.json")
    if not out["ohlcv"]:
        out["ohlcv"] = _latest_glob("ohlcv_cache_*.json")
    if not out["indicators"]:
        out["indicators"] = _latest_glob("indicators_*.json")

    return out

def _split_symbol(sym: str) -> tuple[str, str]:
    if ":" in sym:
        ex, ticker = sym.split(":", 1)
        return ex, ticker
    return "", sym

def _fmt(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, float):
        return f"{x:.12g}"
    return str(x)

def _pct(a_now: float | None, a_prev: float | None) -> float | None:
    try:
        if a_now is None or a_prev is None or a_prev == 0:
            return None
        return (float(a_now) / float(a_prev) - 1.0) * 100.0
    except Exception:
        return None

# ----------------------------
# Normalização de OHLCV (compatível com vários formatos)
# ----------------------------
def _to_iso(ts: Any) -> Optional[str]:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if isinstance(ts, str) and ts.isdigit():
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if isinstance(ts, str) and ts.endswith("Z"):
        return ts
    return None

def _ensure_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def _extract_arrays_from_any(sec: Any) -> Tuple[List[float], List[Optional[str]]]:
    """
    Dado um nó de OHLCV (dict/list/qualquer), tenta extrair arrays de closes e times.
    Retorna (closes, times) com mesmo comprimento. Se não conseguir, retorna listas vazias.
    """
    closes: List[float] = []
    times: List[Optional[str]] = []

    def push_arrays(cand_c: List[Any], cand_t: List[Any]) -> bool:
        nonlocal closes, times
        if not cand_c:
            return False
        try:
            c = [float(x) for x in cand_c]
        except Exception:
            return False
        if cand_t and len(cand_t) == len(c):
            t = [_to_iso(tv) for tv in cand_t]
        else:
            t = [None] * len(c)
        closes, times = c, t
        return True

    if isinstance(sec, dict):
        ser = sec.get("series")
        if isinstance(ser, dict):
            c = _ensure_list(ser.get("c") or ser.get("close") or ser.get("closes") or ser.get("C"))
            t = _ensure_list(ser.get("t") or ser.get("ts") or ser.get("timestamp") or ser.get("T"))
            if push_arrays(c, t): return closes, times
        # campos diretos
        c = _ensure_list(sec.get("c") or sec.get("close") or sec.get("closes") or sec.get("C"))
        t = _ensure_list(sec.get("t") or sec.get("ts") or sec.get("timestamp") or sec.get("T"))
        if push_arrays(c, t): return closes, times
        # lista de pontos dentro de series
        if isinstance(ser, list):
            cand_c, cand_t = [], []
            for pt in ser:
                if isinstance(pt, dict):
                    cand_c.append(pt.get("c") or pt.get("close") or pt.get("C"))
                    cand_t.append(pt.get("t") or pt.get("ts") or pt.get("timestamp") or pt.get("T"))
            if push_arrays(cand_c, cand_t): return closes, times

    if isinstance(sec, list):
        cand_c, cand_t = [], []
        for pt in sec:
            if isinstance(pt, dict):
                cand_c.append(pt.get("c") or pt.get("close") or pt.get("C"))
                cand_t.append(pt.get("t") or pt.get("ts") or pt.get("timestamp") or pt.get("T"))
        if push_arrays(cand_c, cand_t): return closes, times

    return [], []

def _close_n_days_ago(closes: List[float], days: int) -> float | None:
    if not closes or len(closes) <= days:
        return None
    try:
        return float(closes[-(days+1)])
    except Exception:
        return None

def _last_close_ts(times: List[Optional[str]]) -> Optional[str]:
    if not times:
        return None
    t = times[-1]
    return t if isinstance(t, str) else None

def _atr_pct(atr, close) -> float | None:
    try:
        if atr is None or close in (None, 0):
            return None
        return float(atr) / float(close) * 100.0
    except Exception:
        return None

def _ensure_bb_upper(bb_ma20, bb_lower) -> float | None:
    try:
        if bb_ma20 is None or bb_lower is None:
            return None
        return 2.0 * float(bb_ma20) - float(bb_lower)
    except Exception:
        return None

def _sources_used(asset_type: str) -> str:
    return "binance+coingecko" if asset_type == "cr" else "yahoo+stooq+nasdaq"

# ----------------------------
# main
# ----------------------------
def main():
    paths = _load_pointer_paths()
    sig_path = paths.get("signals")
    ohlcv_path = paths.get("ohlcv")

    if not sig_path or not Path(sig_path).exists():
        print(f"[erro] signals JSON não encontrado: {sig_path}", file=sys.stderr)
        sys.exit(2)

    # OHLCV é opcional p/ não derrubar o CSV (pct_chg ficam vazios)
    ohlcv: Dict[str, Any] = {}
    if ohlcv_path and Path(ohlcv_path).exists():
        ohlcv = _read_json(Path(ohlcv_path))
    else:
        print(f"[aviso] ohlcv_cache não encontrado; variações pct ficarão nulas: {ohlcv_path}", file=sys.stderr)

    signals = _read_json(Path(sig_path))
    rows: list[str] = []

    header = [
        "symbol_canonical","asset_type","venue","window_used",
        "price_now_close","price_now_close_at_utc",
        "pct_chg_7d","pct_chg_10d","pct_chg_30d",
        "rsi14","atr14","atr14_pct",
        "bb_ma20","bb_lower","bb_upper",
        "funding","oi_chg_3d_pct",
        "priceguard","window_status","sources_used"
    ]
    rows.append(",".join(header))

    o_eq = (ohlcv.get("eq") if isinstance(ohlcv, dict) else None) or {}
    o_cr = (ohlcv.get("cr") if isinstance(ohlcv, dict) else None) or {}

    for s in signals.get("signals", []):
        sym: str = s.get("symbol_canonical")
        asset_type: str = s.get("asset_type")
        venue, _ = _split_symbol(sym or "")
        win = s.get("window_used") or "7d"

        # dados diretos do payload v1
        close_now = s.get("price_now_close")
        rsi14 = s.get("rsi14")
        atr14 = s.get("atr14")
        atr14_pct = s.get("atr14_pct")
        bb_ma20 = s.get("bb_ma20")
        bb_lower = s.get("bb_lower")
        bb_upper = s.get("bb_upper") or _ensure_bb_upper(bb_ma20, bb_lower)
        funding = s.get("funding")
        oi_chg_3d_pct = s.get("oi_chg_3d_pct")

        # candles (para calcular pct_chg_* e timestamp)
        sec: Any = None
        if asset_type == "cr":
            sec = o_cr.get(sym)
        else:
            sec = o_eq.get(sym)

        closes: List[float] = []
        times: List[Optional[str]] = []
        if sec is not None:
            closes, times = _extract_arrays_from_any(sec)

        ts_utc = _last_close_ts(times) if times else (s.get("price_now_close_at_utc") or None)
        c7  = _close_n_days_ago(closes, 7)
        c10 = _close_n_days_ago(closes, 10)
        c30 = _close_n_days_ago(closes, 30)
        pct7  = _pct(close_now, c7)
        pct10 = _pct(close_now, c10)
        pct30 = _pct(close_now, c30)
        atr14_pct = atr14_pct or _atr_pct(atr14, close_now)

        priceguard = "OK"
        window_status = "TARGET"
        src_used = _sources_used(asset_type)

        row = [
            _fmt(sym), _fmt(asset_type), _fmt(venue), _fmt(win),
            _fmt(close_now), _fmt(ts_utc),
            _fmt(pct7), _fmt(pct10), _fmt(pct30),
            _fmt(rsi14), _fmt(atr14), _fmt(atr14_pct),
            _fmt(bb_ma20), _fmt(bb_lower), _fmt(bb_upper),
            _fmt(funding), _fmt(oi_chg_3d_pct),
            _fmt(priceguard), _fmt(window_status), _fmt(src_used),
        ]
        rows.append(",".join(row))

    OUT_CSV.write_text("\n".join(rows) + "\n", encoding="utf-8")
    print(f"[ok] CSV gerado: {OUT_CSV} (linhas={len(rows)-1}, header=1, total={len(rows)})")

if __name__ == "__main__":
    main()

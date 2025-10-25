#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, Optional, Tuple, List
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
from pathlib import Path

RAW_BASE = "https://raw.githubusercontent.com/giuksbr/finance_automation/main"
OUT_DIR = "public"
POINTER_V1 = os.path.join(OUT_DIR, "pointer_signals_v1.json")
SCHEMA_VERSION = "1.0"
BRT = ZoneInfo("America/Sao_Paulo")

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def now_brt_iso() -> str:
    return datetime.now(BRT).replace(microsecond=0).isoformat()

def brt_date_today() -> str:
    return datetime.now(BRT).strftime("%Y-%m-%d")

def utc_timestamp_suffix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def _read_text(path_or_url: str) -> str:
    try:
        if path_or_url.startswith(("http://", "https://")):
            from urllib.parse import urlparse
            parsed = urlparse(path_or_url)
            basename = os.path.basename(parsed.path)
            if basename:
                local_candidate = os.path.join("public", basename)
                if os.path.exists(local_candidate):
                    with open(local_candidate, "r", encoding="utf-8") as f:
                        return f.read()
            r = requests.get(path_or_url, timeout=30)
            r.raise_for_status()
            return r.text
        with open(path_or_url, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        bn = os.path.basename(path_or_url)
        if bn:
            candidate = os.path.join("public", bn)
            if os.path.exists(candidate):
                with open(candidate, "r", encoding="utf-8") as f:
                    return f.read()
        raise

def _read_json(path_or_url: str) -> Any:
    return json.loads(_read_text(path_or_url))

def _write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def _latest_public(pattern: str) -> Optional[str]:
    files = sorted(Path("public").glob(pattern))
    return str(files[-1]) if files else None

# ---------- OHLCV normalize ----------
def _normalize_sections(ohl: Dict[str, Any]) -> Dict[str, Any]:
    for k in ("eq", "cr"):
        if k not in ohl or ohl[k] is None:
            ohl[k] = {}
        if isinstance(ohl[k], list):
            ohl[k] = {"data": ohl[k]}
        if not isinstance(ohl[k], dict):
            ohl[k] = {"data": ohl[k]}
    return ohl

def _ensure_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

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

def _extract_arrays_from_any(sec: Any) -> Tuple[List[float], List[Optional[str]]]:
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
        c = _ensure_list(sec.get("c") or sec.get("close") or sec.get("closes") or sec.get("C"))
        t = _ensure_list(sec.get("t") or sec.get("ts") or sec.get("timestamp") or sec.get("T"))
        if push_arrays(c, t): return closes, times
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

def _extract_pct_chg(closes: List[float], win: int) -> Optional[float]:
    if not closes or len(closes) < win + 1:
        return None
    old = closes[-(win + 1)]
    new = closes[-1]
    if old == 0:
        return None
    return (new - old) / old * 100.0

def _merge_eq_cr(ohl: Dict[str, Any]) -> Dict[str, Any]:
    out = {"eq": {}, "cr": {}}
    eq = ohl.get("eq", {}) or {}
    cr = ohl.get("cr", {}) or {}
    if isinstance(eq, dict):
        out["eq"] = eq
    if isinstance(cr, dict):
        out["cr"] = cr
    return out

# ---------- indicators normalize ----------
FIELD_ALIASES = {
    "rsi14": {"RSI14", "rsi14"},
    "atr14": {"ATR14", "atr14"},
    "bb_ma20": {"BB_MA20", "bb_ma20"},
    "bb_lower": {"BB_LOWER", "bb_lower"},
    "bb_upper": {"BB_UPPER", "bb_upper"},
    "close": {"CLOSE", "close"},
    "atr14_pct": {"ATR14_PCT", "atr14_pct"},
}

def _normalize_indicator_fields(node: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not isinstance(node, dict):
        return out
    for k, v in node.items():
        out[str(k).lower()] = v
    for target, aliases in FIELD_ALIASES.items():
        if target not in out:
            for a in aliases:
                if a.lower() in out:
                    out[target] = out[a.lower()]
                    break
    return out

def _normalize_indicators(ind: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for bucket in ("eq", "cr"):
        node = ind.get(bucket)
        if isinstance(node, dict):
            out[bucket] = {}
            for sym, sym_node in node.items():
                out[bucket][sym] = _normalize_indicator_fields(sym_node)
    for k, v in ind.items():
        if k in ("eq", "cr"):
            continue
        if isinstance(v, dict):
            out[k] = _normalize_indicator_fields(v)
    return out

def _load_pointer_v1() -> Dict[str, Any]:
    if os.path.exists(POINTER_V1):
        try:
            with open(POINTER_V1, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def _prefer_local_from_pointer(ptr: Dict[str, Any], key_url: str, key_path: str) -> str:
    # 1) caminho local explícito
    lp = ptr.get(key_path)
    if isinstance(lp, str) and lp and os.path.exists(lp):
        return lp
    # 2) glob fallback
    if key_path == "ohlcv":
        gl = _latest_public("ohlcv_cache_*.json")
        if gl: return gl
    if key_path == "indicators":
        gl = _latest_public("indicators_*.json")
        if gl: return gl
    if key_path == "signals":
        gl = _latest_public("n_signals_*.json")
        if gl: return gl
    # 3) URL remota → tenta usar public/<basename>
    url = ptr.get(key_url)
    if isinstance(url, str) and url:
        bn = os.path.basename(url)
        candidate = os.path.join("public", bn)
        if os.path.exists(candidate):
            return candidate
        return url
    raise FileNotFoundError(f"Pointer sem {key_url}/{key_path} válidos ou arquivo local inexistente.")

# ---------- payload ----------
def build_payload(*, with_universe: bool = False) -> Dict[str, Any]:
    ptr = _load_pointer_v1()

    ohl_url_or_path = _prefer_local_from_pointer(ptr, "ohlcv_url", "ohlcv")
    ind_url_or_path = _prefer_local_from_pointer(ptr, "indicators_url", "indicators")
    sig_url_or_path = _prefer_local_from_pointer(ptr, "signals_url", "signals")

    ohl = _normalize_sections(_read_json(ohl_url_or_path))
    ind_raw = _read_json(ind_url_or_path)
    sig_raw = _read_json(sig_url_or_path)

    indicators: Dict[str, Any] = _normalize_indicators(ind_raw)
    merged = _merge_eq_cr(ohl)

    universe_rows: List[Dict[str, Any]] = []
    signals_rows: List[Dict[str, Any]] = []

    def take_ind(sym: str, bucket: str) -> Dict[str, Any] | None:
        node = None
        if isinstance(indicators.get(bucket), dict):
            node = indicators[bucket].get(sym)
        return node or indicators.get(sym)

    for asset_type in ("eq", "cr"):
        bucket = merged.get(asset_type, {}) or {}
        if not isinstance(bucket, dict):
            continue
        for sym, sec in bucket.items():
            if sec is None:
                continue
            closes, times = _extract_arrays_from_any(sec)
            price_now_close = closes[-1] if closes else None
            price_now_close_at_utc = times[-1] if times else None

            pct_chg_7d  = _extract_pct_chg(closes, 7) if closes else None
            pct_chg_10d = _extract_pct_chg(closes, 10) if closes else None
            pct_chg_30d = _extract_pct_chg(closes, 30) if closes else None

            ind = take_ind(sym, asset_type) or {}
            rsi14 = ind.get("rsi14")
            atr14 = ind.get("atr14")
            atr14_pct = ind.get("atr14_pct")
            bb_ma20 = ind.get("bb_ma20")
            bb_lower = ind.get("bb_lower")
            bb_upper = ind.get("bb_upper")
            if price_now_close is None and ind.get("close") is not None:
                try: price_now_close = float(ind["close"])
                except Exception: pass
            if atr14_pct is None and atr14 is not None and price_now_close not in (None, 0):
                try: atr14_pct = float(atr14)/float(price_now_close)*100.0
                except Exception: pass

            features = {
                "chg_7d_pct": pct_chg_7d,
                "chg_10d_pct": pct_chg_10d,
                "chg_30d_pct": pct_chg_30d,
                "price_now_close_at_utc": price_now_close_at_utc,
            }

            row = {
                "symbol_canonical": sym,
                "asset_type": asset_type,
                "window_used": str(sec.get("window")) if isinstance(sec, dict) and sec.get("window") else "7d",
                "price_now_close": price_now_close,
                "price_now_close_at_utc": price_now_close_at_utc,
                "pct_chg_7d": pct_chg_7d,
                "pct_chg_10d": pct_chg_10d,
                "pct_chg_30d": pct_chg_30d,
                "rsi14": rsi14,
                "atr14": atr14,
                "atr14_pct": atr14_pct,
                "bb_ma20": bb_ma20,
                "bb_lower": bb_lower,
                "bb_upper": bb_upper,
                "funding": (sig_raw.get(sym, {}) if isinstance(sig_raw, dict) else {}).get("funding"),
                "oi_chg_3d_pct": (sig_raw.get(sym, {}) if isinstance(sig_raw, dict) else {}).get("oi_chg_3d_pct"),
                "features": features,
            }
            signals_rows.append(row)
            if with_universe:
                universe_rows.append({"symbol": sym, "asset_type": asset_type})

    payload: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": now_utc_iso(),
        "generated_at_brt": now_brt_iso(),
        "brt_date": brt_date_today(),
        "signals": signals_rows,
    }
    if with_universe:
        payload["universe"] = universe_rows
    return payload

def _write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def write_payload_files(payload: Dict[str, Any],
                        *,
                        write_latest: bool = False) -> Tuple[str, Optional[str]]:
    ts = utc_timestamp_suffix()
    path_ts = os.path.join(OUT_DIR, f"n_signals_v1_{ts}.json")
    _write_json(path_ts, payload)
    path_latest = None
    if write_latest:
        path_latest = os.path.join(OUT_DIR, "n_signals_v1_latest.json")
        _write_json(path_latest, payload)
    return path_ts, path_latest

def _resolve_raw_base_url() -> str:
    # tenta descobrir URL raw correta do repo
    try:
        from src.update_pointer_signals_v1 import resolve_raw_base_url  # type: ignore
        rb = resolve_raw_base_url()
        if isinstance(rb, str) and rb:
            return rb
    except Exception:
        pass
    return RAW_BASE

def _merge_local_paths_into_pointer(ptr_file: str) -> None:
    # Enriquecer pointer com caminhos locais (se existirem) — idempotente
    ohl = _latest_public("ohlcv_cache_*.json")
    ind = _latest_public("indicators_*.json")
    sig = _latest_public("n_signals_*.json")
    base = {}
    if os.path.exists(ptr_file):
        try:
            with open(ptr_file, "r", encoding="utf-8") as f:
                base = json.load(f)
        except Exception:
            base = {}
    base["updated_at_brt"] = now_brt_iso()
    base["updated_at_utc"] = now_utc_iso()
    if ohl: base["ohlcv"] = ohl
    if ind: base["indicators"] = ind
    if sig: base["signals"] = sig
    _write_json(ptr_file, base)

def update_pointer_signals_v1(path_ts: str, raw_base: str) -> str:
    bn = os.path.basename(path_ts)
    ptr = {}
    if os.path.exists(POINTER_V1):
        try:
            with open(POINTER_V1, "r", encoding="utf-8") as f:
                ptr = json.load(f)
        except Exception:
            ptr = {}
    ptr["schema_version"] = SCHEMA_VERSION
    ptr["updated_at_brt"] = now_brt_iso()
    ptr["updated_at_utc"] = now_utc_iso()
    ptr["signals_v1_url"] = f"{raw_base}/public/{bn}"
    _write_json(POINTER_V1, ptr)

    # (NOVO) merge com caminhos locais
    _merge_local_paths_into_pointer(POINTER_V1)
    return POINTER_V1

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Exporta payload v1 e atualiza pointer_signals_v1.json")
    p.add_argument("--with-universe", action="store_true")
    p.add_argument("--write-latest", action="store_true")
    p.add_argument("--update-pointer", action="store_true")
    return p.parse_args()

def main() -> None:
    args = parse_args()
    payload = build_payload(with_universe=args.with_universe)
    path_ts, path_latest = write_payload_files(payload, write_latest=args.write_latest)
    if args.update_pointer:
        raw_base = _resolve_raw_base_url()
        pointer_path = update_pointer_signals_v1(path_ts, raw_base=raw_base)
        print(f"pointer_signals_v1 atualizado: {pointer_path}")
    print(f"signals v1 gerado: {path_ts}")
    if path_latest:
        print(f"signals v1 latest atualizado: {path_latest}")

if __name__ == "__main__":
    main()

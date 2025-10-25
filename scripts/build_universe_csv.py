#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera public/n_signals_universe_latest.csv com o layout solicitado:

symbol_canonical,asset_type,venue,window_used,price_now_close,price_now_close_at_utc,
pct_chg_7d,pct_chg_10d,pct_chg_30d,rsi14,atr14,atr14_pct,bb_ma20,bb_lower,bb_upper,
funding,oi_chg_3d_pct,priceguard,window_status,sources_used
"""

import json
import sys
import re
import glob
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
PUB = ROOT / "public"
OUT_CSV = PUB / "n_signals_universe_latest.csv"

def _read_json(p: Path) -> dict:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def _latest_glob(pattern: str) -> Path | None:
    files = sorted(PUB.glob(pattern))
    return files[-1] if files else None

def _load_pointer_paths() -> dict:
    ptr = PUB / "pointer_signals_v1.json"
    if ptr.exists():
        data = _read_json(ptr)
        # Estrutura esperada: {"ohlcv":"public/ohlcv_cache_...json","indicators":"public/...json","signals":"public/...json"}
        out = {}
        for k in ("ohlcv", "indicators", "signals"):
            v = data.get(k)
            if isinstance(v, str):
                out[k] = (ROOT / v).resolve() if v.startswith("public/") else (ROOT / v).resolve()
        return out
    # fallback por glob
    return {
        "signals": _latest_glob("n_signals_v1_*.json") or (PUB / "n_signals_v1_latest.json"),
        "ohlcv": _latest_glob("ohlcv_cache_*.json"),
        "indicators": _latest_glob("indicators_*.json"),
    }

def _split_symbol(sym: str) -> tuple[str, str]:
    # "EXCHANGE:SYMBOL" -> ("EXCHANGE","SYMBOL")
    if ":" in sym:
        ex, ticker = sym.split(":", 1)
        return ex, ticker
    return "", sym

def _pct(a_now: float, a_prev: float) -> float | None:
    try:
        if a_prev is None or a_prev == 0:
            return None
        return (a_now / a_prev - 1.0) * 100.0
    except Exception:
        return None

def _last_close_ts(candles: list) -> str | None:
    # candles é lista de dicts com "t" (utc iso) e "c" (close)
    if not candles:
        return None
    t = candles[-1].get("t")
    if isinstance(t, str):
        # normaliza para Z
        try:
            dt = datetime.fromisoformat(t.replace("Z", "+00:00")).astimezone(timezone.utc)
            return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z")
        except Exception:
            return t
    return None

def _close_n_days_ago(candles: list, days: int) -> float | None:
    # Assumimos candles diários ordenados, pegamos índice - (days)
    if not candles or len(candles) <= days:
        return None
    try:
        return float(candles[-(days+1)]["c"])
    except Exception:
        return None

def _ensure_bb_upper(bb_ma20, bb_lower) -> float | None:
    try:
        if bb_ma20 is None or bb_lower is None:
            return None
        # std = (ma - lower)/2  => upper = ma + 2*std = 2*ma - lower
        return 2.0 * float(bb_ma20) - float(bb_lower)
    except Exception:
        return None

def _atr_pct(atr, close) -> float | None:
    try:
        if atr is None or close in (None, 0):
            return None
        return float(atr) / float(close) * 100.0
    except Exception:
        return None

def _sources_used(asset_type: str) -> str:
    return "binance+coingecko" if asset_type == "cr" else "yahoo+stooq+nasdaq"

def main():
    paths = _load_pointer_paths()
    sig_path = paths.get("signals") or (PUB / "n_signals_v1_latest.json")
    ohlcv_path = paths.get("ohlcv")

    if not sig_path or not Path(sig_path).exists():
        print(f"[erro] signals JSON não encontrado: {sig_path}", file=sys.stderr)
        sys.exit(2)
    if not ohlcv_path or not Path(ohlcv_path).exists():
        print(f"[aviso] ohlcv_cache não encontrado; variações pct ficarão nulas: {ohlcv_path}", file=sys.stderr)
        ohlcv = {}
    else:
        ohlcv = _read_json(Path(ohlcv_path))

    signals = _read_json(Path(sig_path))
    rows = []
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

    # OHLCV estrutura esperada: {"eq":{"EXC:SYM":[{t, c},...]}, "cr":{...}}
    o_eq = (ohlcv.get("eq") if isinstance(ohlcv, dict) else None) or {}
    o_cr = (ohlcv.get("cr") if isinstance(ohlcv, dict) else None) or {}

    for s in signals.get("signals", []):
        sym = s.get("symbol_canonical")
        asset_type = s.get("asset_type")
        venue, _ = _split_symbol(sym or "")
        win = s.get("window_used") or "7d"

        close_now = s.get("price_now_close")
        rsi14 = s.get("rsi14")
        atr14 = s.get("atr14")
        atr14_pct = s.get("atr14_pct")
        bb_ma20 = s.get("bb_ma20")
        bb_lower = s.get("bb_lower")
        bb_upper = s.get("bb_upper")
        funding = s.get("funding")
        oi_chg_3d_pct = s.get("oi_chg_3d_pct")

        # Deriva bb_upper se vier nulo e tivermos ma e lower
        if bb_upper is None:
            bb_upper = _ensure_bb_upper(bb_ma20, bb_lower)

        # Deriva atr14_pct se vier nulo
        if atr14_pct is None:
            atr14_pct = _atr_pct(atr14, close_now)

        # Recupera candles e timestamp do último close
        candles = None
        key = sym
        if asset_type == "cr":
            candles = o_cr.get(key) or []
        else:
            candles = o_eq.get(key) or []
        ts_utc = _last_close_ts(candles) if candles else None

        # Calcula pct changes (percentuais em %)
        c7 = _close_n_days_ago(candles, 7)
        c10 = _close_n_days_ago(candles, 10)
        c30 = _close_n_days_ago(candles, 30)
        pct7 = _pct(close_now, c7) if close_now is not None else None
        pct10 = _pct(close_now, c10) if close_now is not None else None
        pct30 = _pct(close_now, c30) if close_now is not None else None

        # Defaults simples para status
        priceguard = "OK"
        window_status = "TARGET"
        src_used = _sources_used(asset_type)

        def f(x):
            if x is None:
                return ""
            if isinstance(x, float):
                # evita notação científica em CSV
                return f"{x:.12g}"
            return str(x)

        row = [
            f(sym), f(asset_type), f(venue), f(win),
            f(close_now), f(ts_utc),
            f(pct7), f(pct10), f(pct30),
            f(rsi14), f(atr14), f(atr14_pct),
            f(bb_ma20), f(bb_lower), f(bb_upper),
            f(funding), f(oi_chg_3d_pct),
            f(priceguard), f(window_status), f(src_used),
        ]
        rows.append(",".join(row))

    OUT_CSV.write_text("\n".join(rows) + "\n", encoding="utf-8")
    print(f"[ok] CSV gerado: {OUT_CSV} (linhas={len(rows)-1}, header=1, total={len(rows)})")

if __name__ == "__main__":
    main()

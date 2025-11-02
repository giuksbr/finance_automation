#!/usr/bin/env python3
import json, csv, os, glob, math
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone

OUT_CSV = "public/n_signals_universe_latest.csv"

FIELDS = [
    "symbol_canonical","asset_type","venue","window_used",
    "price_now_close","price_now_close_at_utc",
    "pct_chg_7d","pct_chg_10d","pct_chg_30d",
    "rsi14","atr14","atr14_pct",
    "bb_ma20","bb_lower","bb_upper",
    "funding","oi_chg_3d_pct",
    "priceguard","window_status","sources_used"
]

def _fmt(x: Any) -> str:
    """String segura para CSV, retorna '' quando None/NaN."""
    if x is None: return ""
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)): return ""
    return f"{x}"

def _latest(pattern: str) -> Optional[str]:
    files = sorted(glob.glob(pattern))
    return files[-1] if files else None

def _load_json(path: Optional[str]) -> Any:
    if not path or not os.path.exists(path): return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _canon_parts(sc: str) -> Tuple[str, str]:
    # ex: "NASDAQ:NVDA" → ("NASDAQ","NVDA")
    if ":" in sc:
        p, s = sc.split(":", 1)
        return p, s
    return "", sc

def _is_crypto(sc: str) -> bool:
    return sc.startswith("BINANCE:")

def _pct_chg_window(candles: List[Dict[str, Any]], window_days: int) -> Optional[float]:
    """% change usando close da última barra vs close de N dias atrás."""
    if not candles or len(candles) < 2: return None
    closes = [c.get("c") for c in candles if isinstance(c, dict)]
    closes = [float(x) for x in closes if x is not None]
    if len(closes) < window_days + 1: return None
    last = closes[-1]; ref = closes[-(window_days+1)]
    try:
        return (last / ref - 1.0) * 100.0
    except ZeroDivisionError:
        return None

def _last_close_ts(candles: List[Dict[str, Any]]) -> Optional[str]:
    if not candles: return None
    t = candles[-1].get("t")
    if not t: return None
    # normaliza para ISO-UTC
    try:
        dt = datetime.fromisoformat(t.replace("Z","+00:00")).astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return t  # devolve como veio

def _detect_asset_type(sc: str) -> str:
    if _is_crypto(sc): return "crypto"
    # heurística básica para equities/etfs
    return "eq"

def _sources_used(sc: str) -> str:
    return "binance+coingecko" if _is_crypto(sc) else "yahoo+stooq+nasdaq"

def main():
    os.makedirs("public", exist_ok=True)

    # Pointer preferencial
    ptr = _load_json("public/pointer_signals_v1.json") or {}
    ohl_path = ptr.get("ohlcv") or _latest("public/ohlcv_cache_*.json")
    ind_path = ptr.get("indicators") or _latest("public/indicators_*.json")
    sig_path = ptr.get("signals") or _latest("public/n_signals_v1_*.json") or _latest("public/n_signals_*.json")

    if not (ohl_path and ind_path):
        print(f"[aviso] ohlcv/indicators não encontrados; pct e indicadores ficarão em branco: {ohl_path} {ind_path}")

    ohl = _load_json(ohl_path) or {}
    ind = _load_json(ind_path) or {}
    sig = _load_json(sig_path) or {}

    # Universo: preferir watchlists_local.json; senão, usar chaves de indicators
    wl = _load_json("out/watchlists_local.json") or {}
    eq_list = [d.get("symbol_canonical") for d in (wl.get("eq") or []) if isinstance(d, dict)]
    cr_list = [d.get("symbol_canonical") for d in (wl.get("cr") or []) if isinstance(d, dict)]
    universe = [s for s in (eq_list + cr_list) if s]

    if not universe:
        # fallback: todas chaves que aparecem em indicators
        universe = list(ind.keys()) if isinstance(ind, dict) else []
    universe = sorted(set(universe))

    rows: List[Dict[str, str]] = []
    for sc in universe:
        venue, _ = _canon_parts(sc)
        is_cr = _is_crypto(sc)
        asset_type = _detect_asset_type(sc)

        candles = (ohl or {}).get(sc) or []
        ind_map = (ind or {}).get(sc) or {}

        price_now = None
        ts_utc = None
        if candles:
            price_now = candles[-1].get("c")
            ts_utc = _last_close_ts(candles)

        # indicadores básicos
        rsi14 = ind_map.get("RSI14")
        atr14 = ind_map.get("ATR14")
        bb_ma20 = ind_map.get("BB_MA20")
        bb_lower = ind_map.get("BB_LOWER")
        bb_upper = ind_map.get("BB_UPPER")  # <-- mapeado corretamente

        # ATR em % (se não vier pronto)
        atr14_pct = ind_map.get("ATR14_PCT")
        if atr14_pct is None and (atr14 is not None) and (price_now not in (None, 0)):
            try:
                atr14_pct = (float(atr14) / float(price_now)) * 100.0
            except Exception:
                atr14_pct = None

        row = {
            "symbol_canonical": sc,
            "asset_type": asset_type,
            "venue": venue,
            "window_used": "7d",
            "price_now_close": _fmt(price_now),
            "price_now_close_at_utc": _fmt(ts_utc),
            "pct_chg_7d": _fmt(_pct_chg_window(candles, 7)),
            "pct_chg_10d": _fmt(_pct_chg_window(candles, 10)),
            "pct_chg_30d": _fmt(_pct_chg_window(candles, 30)),
            "rsi14": _fmt(rsi14),
            "atr14": _fmt(atr14),
            "atr14_pct": _fmt(atr14_pct),
            "bb_ma20": _fmt(bb_ma20),
            "bb_lower": _fmt(bb_lower),
            "bb_upper": _fmt(bb_upper),
            # crypto-only (se não houver, ficam em branco)
            "funding": _fmt(ind_map.get("FUNDING")),
            "oi_chg_3d_pct": _fmt(ind_map.get("OI_CHG_3D_PCT")),
            # status e fontes (placeholder/estáticos aceitáveis)
            "priceguard": "OK",
            "window_status": "TARGET",
            "sources_used": _sources_used(sc),
        }
        rows.append(row)

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"[ok] CSV gerado: {OUT_CSV} (linhas={len(rows)}, header=1, total={len(rows)+1})")

if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-
# scripts/stage4_full_collect.py
import json, sys, os
from pathlib import Path
from datetime import datetime, timezone

from src.fetch_eq import fetch_stooq
from src.fetch_cr import fetch_binance
from src.indicator_calc import compute_indicators_dict

OUT_DIR = Path("public")

def _now_tag():
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def _pct(a, b):
    try:
        if b is None or b == 0 or a is None: return None
        return (a/b - 1.0) * 100.0
    except:
        return None

def _last_ts(candles):
    return candles[-1]["t"] if candles else None

def _w_pct(candles, w):
    return _pct(candles[-1]["c"], candles[-(w+1)]["c"]) if candles and len(candles) >= w+1 else None

def main():
    # 1) feed
    feed_path = Path("out/last_good_feed.json")
    if not feed_path.exists():
        print("ERR: out/last_good_feed.json não existe", file=sys.stderr); sys.exit(1)
    feed = json.loads(feed_path.read_text(encoding="utf-8"))
    if feed.get("schema_version") != "2.0":
        print("ERR: schema_version != 2.0", file=sys.stderr); sys.exit(1)

    # 2) watchlists
    wl_path = Path("out/watchlists_local.json")
    if not wl_path.exists():
        print("ERR: out/watchlists_local.json não existe. Rode scripts/build_watchlists_from_feed.py", file=sys.stderr); sys.exit(2)
    wl = json.loads(wl_path.read_text(encoding="utf-8"))
    eq_syms = [x["symbol_canonical"] for x in wl.get("eq",[])]
    cr_syms = [x["symbol_canonical"] for x in wl.get("cr",[])]

    print(f"[collect] eq={len(eq_syms)} cr={len(cr_syms)}")

    # 3) coleta candles
    eq = fetch_stooq(symbols=eq_syms, days=120) if eq_syms else {}
    cr = fetch_binance(symbols=cr_syms, days=120) if cr_syms else {}

    total_eq = sum(1 for v in (eq or {}).values() if v)
    total_cr = sum(1 for v in (cr or {}).values() if v)
    print(f"[collect] candles OK: eq_nonempty={total_eq} cr_nonempty={total_cr}")
    if total_eq==0 and total_cr==0:
        print("ERR: Nenhum candle coletado (rede/endpoints?)", file=sys.stderr); sys.exit(3)

    # 4) indicadores
    ind_eq = compute_indicators_dict(eq)
    ind_cr = compute_indicators_dict(cr)
    print(f"[collect] indicators OK: eq={len(ind_eq)} cr={len(ind_cr)}")

    # 5) salvar artefatos
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = _now_tag()

    ohl_path = OUT_DIR / f"ohlcv_cache_{ts}.json"
    ohl_path.write_text(json.dumps({"eq":eq,"cr":cr}, ensure_ascii=False, indent=2), encoding="utf-8")

    ind_path = OUT_DIR / f"indicators_{ts}.json"
    ind_path.write_text(json.dumps({"eq":ind_eq,"cr":ind_cr}, ensure_ascii=False, indent=2), encoding="utf-8")

    # 6) signals (linhas por ativo)
    # meta venue/asset_type
    meta = {}
    def add_meta(lst, atype):
        for it in lst:
            sc = it.get("symbol_canonical")
            if sc and sc not in meta:
                meta[sc] = {"asset_type": atype, "venue": it.get("venue","")}
    add_meta(feed["universe"]["watchlists"]["avenue"]["whitelist"], "eq")
    add_meta(feed["universe"]["watchlists"]["avenue"]["candidate_pool"], "eq")
    add_meta(feed["universe"]["watchlists"]["binance"]["whitelist"], "crypto")
    add_meta(feed["universe"]["watchlists"]["binance"]["candidate_pool"], "crypto")

    rows = []
    joined = {}
    joined.update(eq or {})
    joined.update(cr or {})

    for sc, candles in joined.items():
        is_cr = sc in (cr or {})
        inds = (ind_cr if is_cr else ind_eq).get(sc, {})
        last_ts = _last_ts(candles)
        last_c  = candles[-1]["c"] if candles else None
        m = meta.get(sc, {})
        row = {
            "symbol_canonical": sc,
            "asset_type": m.get("asset_type") or ("crypto" if is_cr else "eq"),
            "venue": m.get("venue") or ("BINANCE" if is_cr else "NYSE/NASDAQ"),
            "window_used": "7d",
            "price_now_close": last_c,
            "price_now_close_at_utc": last_ts,
            "pct_chg_7d": _w_pct(candles, 7),
            "pct_chg_10d": _w_pct(candles, 10),
            "pct_chg_30d": _w_pct(candles, 30),
            "rsi14": inds.get("RSI14"),
            "atr14": inds.get("ATR14"),
            "atr14_pct": inds.get("ATR14_PCT"),
            "bb_ma20": inds.get("BB_MA20"),
            "bb_lower": inds.get("BB_LOWER"),
            "bb_upper": inds.get("BB_UPPER"),
            "funding": "",
            "oi_chg_3d_pct": "",
            "priceguard": "OK",
            "window_status": "TARGET",
            "sources_used": "binance" if is_cr else "stooq",
        }
        rows.append(row)

    sig_path = OUT_DIR / f"n_signals_{ts}.json"
    sig_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    v1 = {
        "brt_generated_at": datetime.now(timezone.utc).astimezone().isoformat(),
        "universe": {"counts": {"eq": len(eq), "cr": len(cr)}},
        "signals": rows,
        "sources": {"ohlcv": str(ohl_path), "indicators": str(ind_path)},
    }
    v1_path = OUT_DIR / f"n_signals_v1_{ts}.json"
    (OUT_DIR / "n_signals_v1_latest.json").write_text(json.dumps(v1, ensure_ascii=False, indent=2), encoding="utf-8")
    v1_path.write_text(json.dumps(v1, ensure_ascii=False, indent=2), encoding="utf-8")

    pointer = {
        "ohlcv": str(ohl_path),
        "indicators": str(ind_path),
        "signals": str(v1_path),
        "ohlcv_url": None, "indicators_url": None, "signals_url": None
    }
    (OUT_DIR / "pointer_signals_v1.json").write_text(json.dumps(pointer, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[collect] OK: ohlcv={ohl_path.name} indicators={ind_path.name} signals_v1={v1_path.name}")
    print("[collect] pointer_signals_v1.json atualizado")

if __name__=="__main__":
    main()

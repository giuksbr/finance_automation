from __future__ import annotations
import os, math, datetime as dt, pytz
import pandas as pd
import yaml

from .feed import load_config, fetch_feed, extract_watchlists
from .fetch_eq import fetch_stooq, fetch_yahoo
from .fetch_cr import fetch_binance, fetch_coingecko
from .indicators import rsi, atr, bollinger_bands
from .signals import pct_change_n, n_levels_from_features, confidence_from_levels
from .priceguard import accept_close_eq, accept_close_cr, sanity_last7_abs_move_ok
from .storage import repo_copy_to_public, repo_build_raw_urls, publish_pointer_local

def now_brt_iso():
    return dt.datetime.now(pytz.timezone("America/Sao_Paulo")).strftime("%Y-%m-%dT%H:%M:%S%z")

def write_json(path, payload):
    import json
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

class PGThresholds:
    def __init__(self, eq_delta_max, cr_delta_max, eq_abs_chg7d_max, cr_abs_chg7d_max):
        self.eq_delta_max = eq_delta_max
        self.cr_delta_max = cr_delta_max
        self.eq_abs_chg7d_max = eq_abs_chg7d_max
        self.cr_abs_chg7d_max = cr_abs_chg7d_max

def process_series(df, target_days):
    df = df.sort_values("Date").reset_index(drop=True)
    window_size = min(max(target_days, min(len(df), target_days+3)), len(df))
    return df.tail(window_size).reset_index(drop=True), window_size

def main():
    cfg = load_config()
    out_dir = cfg.get("out_dir", "out")
    os.makedirs(out_dir, exist_ok=True)
    thresholds = PGThresholds(**cfg["priceguard"])

    feed = fetch_feed(cfg["feed_url"])
    wl = extract_watchlists(feed)

    import json
    with open("coingecko_map.json", "r", encoding="utf-8") as f:
        cg_map = json.load(f)

    ohlcv_payload = {"generated_at_brt": now_brt_iso(), "eq": {}, "cr": {}, "errors": []}
    ind_payload = {"generated_at_brt": now_brt_iso(), "eq": {}, "cr": {}, "errors": []}
    sig_list = []
    errors = []

def compute_indicators_and_signal(asset_type: str, sym_can: str, df_win: pd.DataFrame, df_full: pd.DataFrame, used_tag: str):
    import math
    # Indicadores com df_full (até 30 dias)
    close_full = df_full["close"]
    rsi14 = rsi(close_full, 14).iloc[-1] if len(df_full) >= 14 else None
    atr14 = atr(df_full, 14).iloc[-1] if len(df_full) >= 14 else None
    ma20, bb_up, bb_lo = bollinger_bands(close_full, 20, 2.0)
    bb_ma20 = ma20.iloc[-1] if len(close_full) >= 20 else None
    bb_lower = bb_lo.iloc[-1] if len(close_full) >= 20 else None

    # Variações com df_win (janela alvo 7–10)
    chg7 = pct_change_n(df_win, 7)
    chg10 = pct_change_n(df_win, 10)
    last_close = float(df_win["close"].iloc[-1])

    bucket = ind_payload["eq"] if asset_type=="eq" else ind_payload["cr"]
    def _safe(v, nd=2):
        if v is None: return None
        if isinstance(v, float) and math.isnan(v): return None
        return round(float(v), nd)
    bucket[sym_can] = {
        "RSI14": _safe(rsi14, 2),
        "ATR14": _safe(atr14, 6),
        "BB_MA20": _safe(bb_ma20, 6),
        "BB_LOWER": _safe(bb_lower, 6),
    }

    lvls = n_levels_from_features(asset_type, chg7, chg10,
                                  None if rsi14 is None else float(rsi14),
                                  None if atr14 is None else float(atr14),
                                  None if bb_lower is None else float(bb_lower),
                                  None if bb_ma20 is None else float(bb_ma20),
                                  last_close)
    if lvls:
        srcs = used_tag.split("|")
        sig = {
            "symbol_canonical": sym_can,
            "window_used": ("7d" if chg7 is not None else ("10d" if chg10 is not None else used_tag)),
            "features": {
                "chg_7d_pct": None if chg7 is None else round(float(chg7), 2),
                "chg_10d_pct": None if chg10 is None else round(float(chg10), 2),
                "rsi14": None if rsi14 is None else round(float(rsi14), 2),
                "atr14": None if atr14 is None else round(float(atr14), 6),
                "bb_lower": None if bb_lower is None else round(float(bb_lower), 6),
                "bb_ma20": None if bb_ma20 is None else round(float(bb_ma20), 6),
                "close": round(last_close, 6),
            },
            "derivatives": {},
            "levels": lvls,
            "confidence": "low",
            "sources": srcs,
        }
        from .signals import confidence_from_levels
        sig["confidence"] = confidence_from_levels(sig["levels"], sig["sources"])
        return sig
    return None

    for sym in wl.get("eq", []):
        try:
            stq = fetch_stooq(sym, cfg.get("window_fallback_days", 10))
            yh = fetch_yahoo(sym, cfg.get("window_fallback_days", 10))
            accepted, src_tag = accept_close_eq(stq, yh, thresholds)
            if accepted is None:
                cand = stq if stq is not None else yh
                if cand is not None and sanity_last7_abs_move_ok(cand, False, thresholds):
                    accepted, src_tag = cand, src_tag + "|sanity_ok"
                else:
                    errors.append(f"PRICEGUARD_FAIL:{sym}")
                    continue
            df_full = accepted.sort_values("Date").reset_index(drop=True)
            df_win, used_n = process_series(df_full, cfg.get("window_days", 7))
            ohlcv_payload["eq"][sym] = {"window": f"{used_n}d", "count": int(len(df_win))}
            sig = compute_indicators_and_signal("eq", sym, df_win, df_full, src_tag)
            if sig: sig_list.append(sig)
        except Exception:
            errors.append(f"HIST_FETCH_FAIL:{sym}")

    for sym in wl.get("cr", []):
        try:
            bn = fetch_binance(sym, cfg.get("window_fallback_days", 10))
            cg = fetch_coingecko(sym, cg_map, cfg.get("window_fallback_days", 10))
            accepted, src_tag = accept_close_cr(bn, cg, thresholds)
            if accepted is None:
                cand = bn if bn is not None else cg
                if cand is not None and sanity_last7_abs_move_ok(cand, True, thresholds):
                    accepted, src_tag = cand, src_tag + "|sanity_ok"
                else:
                    errors.append(f"PRICEGUARD_FAIL:{sym}")
                    continue
            df_full = accepted.sort_values("Date").reset_index(drop=True)
            df_win, used_n = process_series(df_full, cfg.get("window_days", 7))
            ohlcv_payload["cr"][sym] = {"window": f"{used_n}d", "count": int(len(df_win))}
            sig = compute_indicators_and_signal("cr", sym, df_win, df_full, src_tag)
            if sig: sig_list.append(sig)
        except Exception:
            errors.append(f"HIST_FETCH_FAIL:{sym}")

    write_json(os.path.join(out_dir, "ohlcv_cache.json"), ohlcv_payload)
    write_json(os.path.join(out_dir, "indicators.json"), ind_payload)
    write_json(os.path.join(out_dir, "n_signals.json"), sig_list)

    st = cfg["storage"]
    if st.get("backend") == "repo":
        public_dir = st.get("public_dir", "public")
        raw_base = st.get("raw_base_url", "https://raw.githubusercontent.com/<owner>/<repo>/main/")
        ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        files_map = {
            "ohlcv_cache.json": f"ohlcv_cache_{ts}.json",
            "indicators.json": f"indicators_{ts}.json",
            "n_signals.json": f"n_signals_{ts}.json",
        }
        repo_copy_to_public(public_dir, out_dir, files_map)
        urls = repo_build_raw_urls(raw_base, public_dir, list(files_map.values()))
        pointer_local = os.path.join(out_dir, "pointer.json")
        exp = (dt.datetime.utcnow() + dt.timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
        publish_pointer_local(pointer_local, urls[files_map["ohlcv_cache.json"]], urls[files_map["indicators.json"]], urls[files_map["n_signals.json"]], exp)
        import shutil; shutil.copyfile(pointer_local, os.path.join(public_dir, "pointer.json"))
        print("Publicação concluída em", public_dir)
    else:
        print("storage.backend!=repo → saída somente em 'out/'")

if __name__ == "__main__":
    main()

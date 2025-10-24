from __future__ import annotations
import os, json, yaml, pytz
import pandas as pd
from datetime import datetime, timezone

from src.feed import fetch_feed, extract_watchlists
from src.fetch_eq import fetch_stooq, fetch_yahoo
from src.fetch_cr import fetch_binance, fetch_coingecko
from src.priceguard import accept_close_eq, accept_close_cr, sanity_last7_abs_move_ok
from src.indicators import compute_indicators

# ----------------------------------------------------------------------------------------------------------------------
# Utils / Config
# ----------------------------------------------------------------------------------------------------------------------

class Thresholds:
    def __init__(self, cfg: dict):
        pg = cfg["priceguard"]
        self.eq_delta_max = float(pg["eq_delta_max"])
        self.cr_delta_max = float(pg["cr_delta_max"])
        self.eq_abs_chg7d_max = float(pg["eq_abs_chg7d_max"])
        self.cr_abs_chg7d_max = float(pg["cr_abs_chg7d_max"])

def _load_cfg() -> dict:
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _ensure_dirs():
    os.makedirs("out", exist_ok=True)
    os.makedirs("public", exist_ok=True)

def _ts_stamp() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def now_brt_iso() -> str:
    tz = pytz.timezone("America/Sao_Paulo")
    now = datetime.now(tz)
    # formata 2025-10-17T16:39:05-03:00
    s = now.strftime("%Y-%m-%dT%H:%M:%S%z")
    return s[:-2] + ":" + s[-2:]

def _write_json(path: str, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def _normalize_sources(tag: str | list | None) -> list[str]:
    if isinstance(tag, list):
        return [str(x) for x in tag if x]
    if isinstance(tag, str):
        parts = [p for p in tag.split("|") if p]
        return parts if parts else ([tag] if tag else [])
    return []

def _pct(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or b == 0:
        return None
    return (a / b - 1.0) * 100.0

def _chg7_chg10_from_df(df: pd.DataFrame) -> tuple[float|None, float|None, float|None]:
    """
    Retorna (chg7%, chg10%, close) usando a coluna 'close' do DF aceito (ordem temporal).
    """
    if df is None or df.empty or "close" not in df.columns:
        return (None, None, None)
    closes = pd.Series(df["close"].tolist(), dtype="float64")
    n = len(closes)
    close_last = float(closes.iloc[-1])
    chg7 = chg10 = None
    if n >= 8:
        chg7 = _pct(close_last, float(closes.iloc[-8]))
    if n >= 11:
        chg10 = _pct(close_last, float(closes.iloc[-11]))
    return (chg7, chg10, close_last)

# ----------------------------------------------------------------------------------------------------------------------
# Coleta + PriceGuard + Indicadores + CHGs
# ----------------------------------------------------------------------------------------------------------------------

def collect_eq(symbols: list[str], days: int, th: Thresholds):
    """
    Retorna tupla:
      ohlcv_eq: {sym: {"window": "7d|Xd", "count": n}}
      ind_eq  : {sym: {... indic ...}}
      src_eq  : {sym: ["stooq","yahoo"] | ["stooq_only"] | ["yahoo_only"]}
      chg_eq  : {sym: {"chg7": float|None, "chg10": float|None, "close": float|None}}
    """
    ohlcv_eq, ind_eq, src_eq, chg_eq = {}, {}, {}, {}

    for sym in symbols:
        stq = fetch_stooq(sym, days)
        yh  = fetch_yahoo(sym, days)
        accepted, tag = accept_close_eq(stq, yh, th)

        # fallback sanity se só 1 fonte
        if accepted is None or accepted.empty:
            src_df = stq if (stq is not None and not stq.empty) else yh
            if src_df is not None and not src_df.empty and sanity_last7_abs_move_ok(src_df, False, th):
                accepted = src_df
                tag = tag or ("stooq_only" if (stq is not None and not stq.empty) else "yahoo_only")
            else:
                continue

        # janela
        count = len(accepted)
        win = "7d" if count >= 7 else f"{count}d"
        ohlcv_eq[sym] = {"window": win, "count": count}

        # fontes
        src_eq[sym] = _normalize_sources(tag)

        # indicadores
        try:
            close = pd.Series(accepted["close"].tolist(), dtype="float64")
            feats = compute_indicators(close)
        except Exception:
            feats = {"RSI14": None, "ATR14": None, "BB_MA20": None, "BB_LOWER": None, "CLOSE": None}
        ind_eq[sym] = feats

        # chgs
        ch7, ch10, close_last = _chg7_chg10_from_df(accepted)
        chg_eq[sym] = {"chg7": ch7, "chg10": ch10, "close": close_last}

    return ohlcv_eq, ind_eq, src_eq, chg_eq


def collect_cr(symbols: list[str], days: int, th: Thresholds, cg_map: dict):
    """
    Retorna tupla:
      ohlcv_cr: {sym: {"window": "7d|Xd", "count": n}}
      ind_cr  : {sym: {...}}
      src_cr  : {sym: ["binance","coingecko"] | ["binance_only"] | ["coingecko_only"]}
      chg_cr  : {sym: {"chg7": float|None, "chg10": float|None, "close": float|None}}
    """
    ohlcv_cr, ind_cr, src_cr, chg_cr = {}, {}, {}, {}

    for sym in symbols:
        bn = fetch_binance(sym, days)
        cg = fetch_coingecko(sym, cg_map, days)
        accepted, tag = accept_close_cr(bn, cg, th)

        # fallback sanity se só 1 fonte
        if accepted is None or accepted.empty:
            src_df = bn if (bn is not None and not bn.empty) else cg
            if src_df is not None and not src_df.empty and sanity_last7_abs_move_ok(src_df, True, th):
                accepted = src_df
                tag = tag or ("binance_only" if (bn is not None and not bn.empty) else "coingecko_only")
            else:
                continue

        # janela
        count = len(accepted)
        win = "7d" if count >= 7 else f"{count}d"
        ohlcv_cr[sym] = {"window": win, "count": count}

        # fontes
        src_cr[sym] = _normalize_sources(tag)

        # indicadores
        try:
            close = pd.Series(accepted["close"].tolist(), dtype="float64")
            feats = compute_indicators(close)
        except Exception:
            feats = {"RSI14": None, "ATR14": None, "BB_MA20": None, "BB_LOWER": None, "CLOSE": None}
        ind_cr[sym] = feats

        # chgs
        ch7, ch10, close_last = _chg7_chg10_from_df(accepted)
        chg_cr[sym] = {"chg7": ch7, "chg10": ch10, "close": close_last}

    return ohlcv_cr, ind_cr, src_cr, chg_cr

# ----------------------------------------------------------------------------------------------------------------------
# Sinais N-níveis (usa chg_7d/10 + indicadores)
# ----------------------------------------------------------------------------------------------------------------------

def build_signals(ohl_eq, ind_eq, src_eq, chg_eq,
                  ohl_cr, ind_cr, src_cr, chg_cr) -> list[dict]:
    signals: list[dict] = []

    # thresholds mínimos (triagem)
    N1_MIN = -22.0
    N2_MIN = -12.0
    N3_MIN = -8.0

    def mk(sym: str, feats: dict, ch: dict, srcs: list[str], is_cr: bool):
        if feats is None or ch is None:
            return None
        # indicadores completos
        need = ("RSI14","ATR14","BB_MA20","BB_LOWER")
        if any(feats.get(k) is None for k in need):
            return None
        # dupla-fonte obrigatória para CR
        if is_cr and len(srcs) < 2:
            return None

        chg7, chg10, close = ch.get("chg7"), ch.get("chg10"), ch.get("close")
        if chg7 is None:
            return None

        rsi, atr, bbm, bbl = feats["RSI14"], feats["ATR14"], feats["BB_MA20"], feats["BB_LOWER"]
        levels = []

        # N1
        if chg7 <= N1_MIN:
            levels.append("N1")
        # N2
        if chg7 <= N2_MIN and ((rsi is not None and 38 <= rsi <= 50) or (atr is not None and bbm is not None and close is not None and abs(close - bbm) >= 1.5*atr)):
            levels.append("N2")
        # N3
        if chg7 <= N3_MIN and ((rsi is not None and 40 <= rsi <= 55) or (close is not None and bbl is not None and close <= bbl)):
            levels.append("N3")
        # N3C para cripto (fallback sem derivativos)
        if is_cr and chg7 <= N3_MIN:
            levels.append("N3C")

        if not levels:
            return None

        return {
            "symbol_canonical": sym,
            "window_used": "7d",
            "features": {
                "chg_7d_pct": round(chg7, 2) if chg7 is not None else None,
                "chg_10d_pct": round(chg10, 2) if chg10 is not None else None,
                "rsi14": round(rsi, 2) if rsi is not None else None,
                "atr14": round(atr, 6) if atr is not None else None,
                "bb_ma20": round(bbm, 6) if bbm is not None else None,
                "bb_lower": round(bbl, 6) if bbl is not None else None,
                "close": round(close, 6) if close is not None else None,
            },
            "derivatives": {},
            "levels": levels,
            "confidence": "low",
            "sources": srcs
        }

    # EQ (não exigimos dupla-fonte para EQ aqui; consumidora pode filtrar depois se quiser)
    for sym in sorted(ind_eq.keys()):
        item = mk(sym, ind_eq[sym], chg_eq.get(sym, {}), src_eq.get(sym, []), is_cr=False)
        if item:
            signals.append(item)

    # CR (dupla-fonte obrigatória na criação do sinal)
    for sym in sorted(ind_cr.keys()):
        item = mk(sym, ind_cr[sym], chg_cr.get(sym, {}), src_cr.get(sym, []), is_cr=True)
        if item:
            signals.append(item)

    return signals

# ----------------------------------------------------------------------------------------------------------------------
# Publicação
# ----------------------------------------------------------------------------------------------------------------------

def publish(ohl: dict, ind: dict, sigs: list[dict], base_url_raw: str):
    _ensure_dirs()
    gen_brt = now_brt_iso()

    ohl_json = {
        "generated_at_brt": gen_brt,
        "eq": ohl.get("eq", {}),
        "cr": ohl.get("cr", {}),
        "errors": ohl.get("errors", []),
    }
    ind_json = {
        "generated_at_brt": gen_brt,
        "eq": ind.get("eq", {}),
        "cr": ind.get("cr", {}),
        "errors": ind.get("errors", []),
    }

    ts = _ts_stamp()
    out_ohl = f"public/ohlcv_cache_{ts}.json"
    out_ind = f"public/indicators_{ts}.json"
    out_sig = f"public/n_signals_{ts}.json"

    _write_json(out_ohl, ohl_json)
    _write_json(out_ind, ind_json)
    _write_json(out_sig, sigs)

    pointer = {
        "ohlcv_path": f"public/ohlcv_cache_{ts}.json",
        "indicators_path": f"public/indicators_{ts}.json",
        "signals_path": f"public/n_signals_{ts}.json",
        "ohlcv_url": f"{base_url_raw.rstrip('/')}/public/ohlcv_cache_{ts}.json",
        "indicators_url": f"{base_url_raw.rstrip('/')}/public/indicators_{ts}.json",
        "signals_url": f"{base_url_raw.rstrip('/')}/public/n_signals_{ts}.json",
        "expires_at_utc": (datetime.utcnow() + pd.Timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    _write_json("public/pointer.json", pointer)
    print("Publicação concluída em public")

# ----------------------------------------------------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------------------------------------------------

def main():
    cfg = _load_cfg()
    th  = Thresholds(cfg)
    days = int(cfg.get("window_fallback_days", 30))

    feed = fetch_feed(cfg["feed_url"])
    wl   = extract_watchlists(feed)
    eq_syms = wl.get("eq", []) or []
    cr_syms = wl.get("cr", []) or []

    # mapa CoinGecko
    try:
        with open("coingecko_map.json","r",encoding="utf-8") as f:
            cg_map = json.load(f)
    except Exception:
        cg_map = {}

    # coleta
    ohl_eq, ind_eq, src_eq, chg_eq = collect_eq(eq_syms, days, th)
    ohl_cr, ind_cr, src_cr, chg_cr = collect_cr(cr_syms, days, th, cg_map)

    # montar objetos de publicação
    ohl = {"eq": ohl_eq, "cr": ohl_cr, "errors": []}
    ind = {"eq": ind_eq, "cr": ind_cr, "errors": []}

    # sinais
    signals = build_signals(ohl_eq, ind_eq, src_eq, chg_eq,
                            ohl_cr, ind_cr, src_cr, chg_cr)

    publish(ohl, ind, signals, cfg["storage"]["raw_base_url"])

if __name__ == "__main__":
    main()

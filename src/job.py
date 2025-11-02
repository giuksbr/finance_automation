# src/job.py
from __future__ import annotations
import os, json, time, argparse, datetime as dt, inspect
from typing import Dict, Any, List, Tuple, Optional

from src.pointer_utils import write_pointer, latest
from src.export_signals_v1 import build_payload as build_payload_v1
from src.priceguard import sanity_last7_abs_move_ok  # mantém compat
from src.fetch_eq import fetch_stooq, fetch_yahoo
from src.fetch_cr import fetch_binance, fetch_coingecko

PUB = "public"
OUT = "out"

def _now_ts_brt_like() -> str:
    return dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

def _ts_tag() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())

def _save_json(obj: Any, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

# ---------- helpers de introspecção/flex ----------

def _sig_params(fn) -> List[str]:
    try:
        return list(inspect.signature(fn).parameters.keys())
    except Exception:
        return []

def _expects_single_symbol(fn) -> bool:
    """True se a função pede 'symbol_canonical' (ou 'symbol') e não um array 'symbols'/'tickers'."""
    ps = set(_sig_params(fn))
    if "symbol_canonical" in ps or ("symbol" in ps and "symbols" not in ps and "tickers" not in ps):
        return True
    return False

def _call_eq_fetch(fn, symbols: List[str], days: int) -> Dict[str, Any]:
    """
    Chama um fetcher de EQs aceitando tanto assinatura 'em lote'
    quanto 'um-a-um'. Retorna {sym: [candles...]}.
    """
    ps = set(_sig_params(fn))
    out: Dict[str, Any] = {}

    # Tenta caminho 'em lote' primeiro se houver 'symbols' ou 'tickers'
    try:
        if "symbols" in ps:
            res = fn(symbols=symbols, days=days) if "days" in ps else fn(symbols=symbols)
            if isinstance(res, dict):
                out.update(res)
                return out
        if "tickers" in ps:
            res = fn(tickers=symbols, days=days) if "days" in ps else fn(tickers=symbols)
            if isinstance(res, dict):
                out.update(res)
                return out
    except Exception:
        pass

    # Caminho 'um-a-um'
    for sym in symbols:
        try:
            if "symbol_canonical" in ps:
                res = fn(symbol_canonical=sym, days=days) if "days" in ps else fn(symbol_canonical=sym)
            elif "symbol" in ps:
                res = fn(symbol=sym, days=days) if "days" in ps else fn(symbol=sym)
            else:
                # fallback brutal: tenta sem nome
                res = fn(sym)  # pode falhar; daí cai no except
            if res:
                # esperado: dict {sym: [candles...]} ou lista diretamente
                if isinstance(res, dict):
                    # se vier {'NYSE:TSM': [...]} ok; se vier {'candles': [...]} normaliza
                    if sym in res:
                        out[sym] = res[sym]
                    elif "candles" in res:
                        out[sym] = res["candles"]
                elif isinstance(res, list):
                    out[sym] = res
        except Exception:
            continue
    return out

def _call_cr_fetch(fn, symbols: List[str], days: int) -> Dict[str, Any]:
    """Mesma ideia do _call_eq_fetch, mas para cripto."""
    ps = set(_sig_params(fn))
    out: Dict[str, Any] = {}

    try:
        if "symbols" in ps:
            res = fn(symbols=symbols, days=days) if "days" in ps else fn(symbols=symbols)
            if isinstance(res, dict):
                out.update(res)
                return out
        if "tickers" in ps:
            res = fn(tickers=symbols, days=days) if "days" in ps else fn(tickers=symbols)
            if isinstance(res, dict):
                out.update(res)
                return out
    except Exception:
        pass

    for sym in symbols:
        try:
            if "symbol_canonical" in ps:
                res = fn(symbol_canonical=sym, days=days) if "days" in ps else fn(symbol_canonical=sym)
            elif "symbol" in ps:
                res = fn(symbol=sym, days=days) if "days" in ps else fn(symbol=sym)
            else:
                res = fn(sym)
            if res:
                if isinstance(res, dict):
                    if sym in res:
                        out[sym] = res[sym]
                    elif "candles" in res:
                        out[sym] = res["candles"]
                elif isinstance(res, list):
                    out[sym] = res
        except Exception:
            continue
    return out

# ---------- feed & universo ----------

def _load_feed() -> Dict[str, Any]:
    urls = [
        "https://raw.githubusercontent.com/giuksbr/finance_feed/main/feed.json",
        "https://raw.githubusercontent.com/giuksbr/finance_feed/refs/heads/main/feed.json",
        "https://cdn.jsdelivr.net/gh/giuksbr/finance_feed@main/feed.json",
        "https://api.github.com/repos/giuksbr/finance_feed/contents/feed.json?ref=main",
    ]
    try:
        import requests
        s = requests.Session()
        for u in urls:
            try:
                r = s.get(u, timeout=6)
                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, dict) and "universe" in data:
                        return data
            except Exception:
                pass
    except Exception:
        pass

    # fallback local
    p = os.path.join(OUT, "last_good_feed.json")
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def _extract_watchlists(feed: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    eq_syms: List[str] = []
    cr_syms: List[str] = []
    wl = (feed.get("universe") or {}).get("watchlists") or {}

    av = wl.get("avenue") or {}
    bn = wl.get("binance") or {}

    for it in (av.get("whitelist") or []):
        sym = it.get("symbol_canonical")
        if isinstance(sym, str) and ":" in sym:
            eq_syms.append(sym)
    for it in (av.get("candidate_pool") or []):
        sym = it.get("symbol_canonical")
        if isinstance(sym, str) and ":" in sym:
            eq_syms.append(sym)

    for it in (bn.get("whitelist") or []):
        sym = it.get("symbol_canonical")
        if isinstance(sym, str) and ":" in sym:
            cr_syms.append(sym)
    for it in (bn.get("candidate_pool") or []):
        sym = it.get("symbol_canonical")
        if isinstance(sym, str) and ":" in sym:
            cr_syms.append(sym)

    eq_syms = sorted(set(eq_syms))
    cr_syms = sorted(set(cr_syms))
    return eq_syms, cr_syms

# ---------- coleta & indicadores ----------

def _collect_eq(eq_syms: List[str], days: int = 120) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    stq = _call_eq_fetch(fetch_stooq, eq_syms, days) or {}
    yh  = _call_eq_fetch(fetch_yahoo,  eq_syms, days) or {}

    ohl: Dict[str, List[Dict[str, Any]]] = {}
    for sym in eq_syms:
        series = stq.get(sym) or yh.get(sym) or []
        if isinstance(series, list) and series:
            ohl[sym] = series

    from src.indicator_calc import compute_indicators_dict
    ind: Dict[str, Dict[str, Any]] = {}
    for sym, candles in ohl.items():
        ind[sym] = compute_indicators_dict(candles)
    return ohl, ind

def _collect_cr(cr_syms: List[str], days: int = 120) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    bn = _call_cr_fetch(fetch_binance,    cr_syms, days) or {}
    cg = _call_cr_fetch(fetch_coingecko,  cr_syms, days) or {}

    ohl: Dict[str, List[Dict[str, Any]]] = {}
    for sym in cr_syms:
        series = bn.get(sym) or cg.get(sym) or []
        if isinstance(series, list) and series:
            ohl[sym] = series

    from src.indicator_calc import compute_indicators_dict
    ind: Dict[str, Dict[str, Any]] = {}
    for sym, candles in ohl.items():
        ind[sym] = compute_indicators_dict(candles)
    return ohl, ind

def _merge_ohlcv(eq_ohl: Dict[str, Any], cr_ohl: Dict[str, Any]) -> Dict[str, Any]:
    return {"eq": eq_ohl, "cr": cr_ohl}

def _merge_ind(eq_ind: Dict[str, Any], cr_ind: Dict[str, Any]) -> Dict[str, Any]:
    return {"eq": eq_ind, "cr": cr_ind}

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=180, help="dias de histórico p/ bootstrap")
    args = ap.parse_args()

    os.makedirs(PUB, exist_ok=True)
    ts_tag = _ts_tag()

    # 0) Feed & universo
    feed = _load_feed()
    eq_syms, cr_syms = _extract_watchlists(feed)

    # 1) Bootstrap se public/ está vazio (ou sem artefatos)
    need_bootstrap = not (latest(f"{PUB}/ohlcv_cache_*.json") and latest(f"{PUB}/indicators_*.json"))
    if need_bootstrap:
        print(f"[{_now_ts_brt_like()}] Bootstrap inicial: coletando preços/indicadores…")
        eq_ohl, eq_ind = _collect_eq(eq_syms, days=args.days)
        cr_ohl, cr_ind = _collect_cr(cr_syms, days=args.days)
        ohl_all = _merge_ohlcv(eq_ohl, cr_ohl)
        ind_all = _merge_ind(eq_ind, cr_ind)
        ohl_path = f"{PUB}/ohlcv_cache_{ts_tag}.json"
        ind_path = f"{PUB}/indicators_{ts_tag}.json"
        _save_json(ohl_all, ohl_path)
        _save_json(ind_all, ind_path)
    else:
        ohl_path = latest(f"{PUB}/ohlcv_cache_*.json")
        ind_path = latest(f"{PUB}/indicators_*.json")

    # 2) Signals “legacy” mínimo (casco)
    sig_legacy_path = latest(f"{PUB}/n_signals_*.json")
    if not sig_legacy_path:
        sig_legacy_path = f"{PUB}/n_signals_{ts_tag}.json"
        _save_json({"signals": [], "universe": []}, sig_legacy_path)

    # 3) Pointer
    write_pointer(ohl_path=ohl_path, ind_path=ind_path, sig_path=sig_legacy_path)
    print(f"[{_now_ts_brt_like()}] pointer_signals_v1 atualizado: {ohl_path} | {ind_path} | {sig_legacy_path}")

    # 4) Export v1
    payload = build_payload_v1(with_universe=True)
    out_ver = f"{PUB}/n_signals_v1_{ts_tag}.json"
    out_latest = f"{PUB}/n_signals_v1_latest.json"
    _save_json(payload, out_ver)
    _save_json(payload, out_latest)
    write_pointer(ohl_path=payload["sources"]["ohlcv"],
                  ind_path=payload["sources"]["indicators"],
                  sig_path=out_ver)
    print(f"[{_now_ts_brt_like()}] export v1: {out_ver} + latest")

    # 5) CSV final
    os.system("python scripts/build_universe_csv.py")
    print(f"[{_now_ts_brt_like()}] Publicação concluída em public")

if __name__ == "__main__":
    main()

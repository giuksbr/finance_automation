#!/usr/bin/env python3
from __future__ import annotations
import os, sys, json, glob, traceback
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone

# Imports do projeto
try:
    from src.fetch_eq import fetch_stooq, fetch_yahoo
    from src.fetch_cr import fetch_binance, fetch_coingecko
    from src.indicator_calc import compute_indicators_dict
except Exception as e:
    print(f"[collect][ERRO] Imports do projeto falharam: {e}")
    raise

# pointer writer opcional
def _write_pointer(ohl_path: Optional[str], ind_path: Optional[str], sig_path: Optional[str] = None,
                   out_path: str = "public/pointer_signals_v1.json") -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    payload = {
        "ohlcv": ohl_path if ohl_path else None,
        "indicators": ind_path if ind_path else None,
        "signals": sig_path if sig_path else None,
        "ohlcv_url": None,
        "indicators_url": None,
        "signals_url": None,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def _read_lines(path: str) -> List[str]:
    if not os.path.exists(path): return []
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]

def _read_watchlists_json(path: str) -> Tuple[List[str], List[str]]:
    if not os.path.exists(path): return ([], [])
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    eq = [x.get("symbol_canonical") for x in (obj.get("eq") or []) if x.get("symbol_canonical")]
    cr = [x.get("symbol_canonical") for x in (obj.get("cr") or []) if x.get("symbol_canonical")]
    return (eq, cr)

def _flex_call(fn, **kwargs):
    """
    Tenta chamar fetchers com assinaturas diferentes:
      - fn(symbols=..., days=...)
      - fn(symbol_canonical=..., days=...)
      - fn(symbols=...)
      - fn(symbol_canonical=...)
      - fn(list, days=...)
      - fn(list)
    Retorna o resultado na primeira assinatura que funcionar.
    """
    order = [
        lambda: fn(symbols=kwargs["symbols"], days=kwargs.get("days", 60)),
        lambda: fn(symbol_canonical=kwargs["symbols"], days=kwargs.get("days", 60)),
        lambda: fn(symbols=kwargs["symbols"]),
        lambda: fn(symbol_canonical=kwargs["symbols"]),
        lambda: fn(kwargs["symbols"], days=kwargs.get("days", 60)),
        lambda: fn(kwargs["symbols"]),
    ]
    errors = []
    for attempt in order:
        try:
            return attempt()
        except TypeError as te:
            errors.append(str(te))
            continue
    # última tentativa: chamada simples sem kwargs
    try:
        return fn()
    except Exception as e:
        errors.append(str(e))
        raise TypeError(f"Todas as tentativas de chamada falharam: {errors}")

def _as_list_of_candles(x: Any) -> List[Dict[str, Any]]:
    """
    Normaliza retorno do fetcher para lista de candles [{t,o,h,l,c,v}, ...]
    Aceita None/dict/list e filtra registros sem 'c'.
    """
    if x is None:
        return []
    if isinstance(x, list):
        out = []
        for it in x:
            if isinstance(it, dict) and "c" in it:
                out.append({
                    "t": it.get("t"),
                    "o": it.get("o"),
                    "h": it.get("h"),
                    "l": it.get("l"),
                    "c": it.get("c"),
                    "v": it.get("v"),
                })
        return out
    if isinstance(x, dict):
        # Alguns fetchers retornam {"series":[...]}; tenta extrair
        for k in ("series", "data", "candles"):
            if isinstance(x.get(k), list):
                return _as_list_of_candles(x.get(k))
    return []

def _nonempty(d: Dict[str, List[Dict[str, Any]]]) -> int:
    return sum(1 for v in d.values() if isinstance(v, list) and len(v) > 0)

def _merge_pref(a: Dict[str, List[Dict[str, Any]]],
                b: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Une dois maps de séries preferindo 'a' quando não vazio, senão 'b'.
    """
    out: Dict[str, List[Dict[str, Any]]] = {}
    keys = set(a.keys()) | set(b.keys())
    for k in keys:
        va = a.get(k) or []
        vb = b.get(k) or []
        out[k] = va if va else vb
    return out

def main():
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.makedirs(os.path.join(ROOT, "public"), exist_ok=True)

    # 1) Carregar universo local (S1 já gerou)
    eq_syms = _read_lines("out/syms_eq.txt")
    cr_syms = _read_lines("out/syms_cr.txt")
    if not eq_syms and not cr_syms:
        # fallback para JSON
        eq_syms, cr_syms = _read_watchlists_json("out/watchlists_local.json")

    print(f"[collect] eq={len(eq_syms)} cr={len(cr_syms)}")

    # 2) Buscar OHLCV (EQ via Stooq→Yahoo; CR via Binance→Coingecko)
    days = 90

    eq_stooq_map: Dict[str, List[Dict[str, Any]]] = {}
    eq_yahoo_map: Dict[str, List[Dict[str, Any]]] = {}

    cr_binance_map: Dict[str, List[Dict[str, Any]]] = {}
    cr_coingecko_map: Dict[str, List[Dict[str, Any]]] = {}

    # EQ: Stooq
    try:
        print("[collect] Coletando EQ via Stooq…")
        stq_raw = _flex_call(fetch_stooq, symbols=eq_syms, days=days)
        # stq_raw pode ser dict{sym:list} ou list (1-sym); normaliza
        tmp: Dict[str, List[Dict[str, Any]]] = {}
        if isinstance(stq_raw, dict):
            for k, v in stq_raw.items():
                tmp[k] = _as_list_of_candles(v)
        elif isinstance(stq_raw, list) and len(eq_syms) == 1:
            tmp[eq_syms[0]] = _as_list_of_candles(stq_raw)
        eq_stooq_map = tmp
        print(f"[collect] stooq ok={_nonempty(eq_stooq_map)} / {len(eq_syms)}")
    except Exception as e:
        print(f"[collect][WARN] Stooq falhou: {e}")

    # EQ: Yahoo (fallback)
    try:
        print("[collect] Coletando EQ via Yahoo (fallback)…")
        yh_raw = _flex_call(fetch_yahoo, symbols=eq_syms, days=days)
        tmp: Dict[str, List[Dict[str, Any]]] = {}
        if isinstance(yh_raw, dict):
            for k, v in yh_raw.items():
                tmp[k] = _as_list_of_candles(v)
        elif isinstance(yh_raw, list) and len(eq_syms) == 1:
            tmp[eq_syms[0]] = _as_list_of_candles(yh_raw)
        eq_yahoo_map = tmp
        print(f"[collect] yahoo ok={_nonempty(eq_yahoo_map)} / {len(eq_syms)}")
    except Exception as e:
        print(f"[collect][WARN] Yahoo falhou: {e}")

    # CR: Binance
    try:
        print("[collect] Coletando CR via Binance…")
        bz_raw = _flex_call(fetch_binance, symbols=cr_syms, days=days)
        tmp: Dict[str, List[Dict[str, Any]]] = {}
        if isinstance(bz_raw, dict):
            for k, v in bz_raw.items():
                tmp[k] = _as_list_of_candles(v)
        elif isinstance(bz_raw, list) and len(cr_syms) == 1:
            tmp[cr_syms[0]] = _as_list_of_candles(bz_raw)
        cr_binance_map = tmp
        print(f"[collect] binance ok={_nonempty(cr_binance_map)} / {len(cr_syms)}")
    except Exception as e:
        print(f"[collect][WARN] Binance falhou: {e}")

    # CR: Coingecko (fallback)
    try:
        print("[collect] Coletando CR via Coingecko (fallback)…")
        cg_raw = _flex_call(fetch_coingecko, symbols=cr_syms, days=days)
        tmp: Dict[str, List[Dict[str, Any]]] = {}
        if isinstance(cg_raw, dict):
            for k, v in cg_raw.items():
                tmp[k] = _as_list_of_candles(v)
        elif isinstance(cg_raw, list) and len(cr_syms) == 1:
            tmp[cr_syms[0]] = _as_list_of_candles(cg_raw)
        cr_coingecko_map = tmp
        print(f"[collect] coingecko ok={_nonempty(cr_coingecko_map)} / {len(cr_syms)}")
    except Exception as e:
        print(f"[collect][WARN] Coingecko falhou: {e}")

    # 3) Merge preferencial
    eq_series = _merge_pref(eq_stooq_map, eq_yahoo_map)
    cr_series = _merge_pref(cr_binance_map, cr_coingecko_map)

    # Filtra universo total
    all_syms = list(dict.fromkeys(eq_syms + cr_syms))
    ohlcv: Dict[str, List[Dict[str, Any]]] = {}
    for s in all_syms:
        if s in eq_series and eq_series[s]:
            ohlcv[s] = eq_series[s]
        elif s in cr_series and cr_series[s]:
            ohlcv[s] = cr_series[s]
        else:
            ohlcv[s] = []

    nonempty = sum(1 for v in ohlcv.values() if v)
    print(f"[collect] candles OK: {nonempty} símbolos com dados")

    ts = _now_tag()
    ohl_path = os.path.join("public", f"ohlcv_cache_{ts}.json")
    with open(ohl_path, "w", encoding="utf-8") as f:
        json.dump(ohlcv, f, ensure_ascii=False, indent=2)

    # 4) Indicadores por símbolo
    indicators: Dict[str, Dict[str, Any]] = {}
    for sym, candles in ohlcv.items():
        indicators[sym] = compute_indicators_dict(candles)

    ind_path = os.path.join("public", f"indicators_{ts}.json")
    with open(ind_path, "w", encoding="utf-8") as f:
        json.dump(indicators, f, ensure_ascii=False, indent=2)

    print(f"[collect] gravado: {os.path.basename(ohl_path)} | {os.path.basename(ind_path)}")

    # 5) Pointer
    _write_pointer(ohl_path=ohl_path, ind_path=ind_path, sig_path=None)
    print("[collect] pointer_signals_v1.json atualizado")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        print("[collect][ERRO] Falha na coleta:")
        traceback.print_exc()
        sys.exit(1)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coleta candles para EQ (Stooq→Yahoo fallback) e CR (Binance→Coingecko fallback)
a partir de out/watchlists_local.json. Gera:

  public/ohlcv_cache_<TS>.json
  public/indicators_<TS>.json   (placeholder: campos em branco/None)
  public/pointer_signals_v1.json (atualizado)

Regras:
- Se algum campo não puder ser obtido: deixar em branco ("" ou None).
- Logs detalhados para troubleshooting.
"""

from __future__ import annotations
import json, sys, os, math
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ROOT / "public"
OUTDIR = PUBLIC
OUTDIR.mkdir(parents=True, exist_ok=True)

WATCH = ROOT / "out" / "watchlists_local.json"
POINTER = PUBLIC / "pointer_signals_v1.json"

# --- util de log/tempo
def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def _isoz(dt: Optional[datetime]=None) -> str:
    dt = dt or datetime.now(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")

def log(msg: str) -> None:
    print(f"[collect] {msg}")

# --- carga de watchlists
def _load_watch() -> Tuple[List[str], List[str]]:
    if not WATCH.exists():
        raise SystemExit("[collect][ERRO] out/watchlists_local.json ausente. Rode S1 (build_watchlists_from_feed.py).")
    with WATCH.open("r", encoding="utf-8") as f:
        wl = json.load(f)
    eq = [ (it or {}).get("symbol_canonical") for it in (wl.get("eq") or []) ]
    cr = [ (it or {}).get("symbol_canonical") for it in (wl.get("cr") or []) ]
    eq = [s for s in eq if s]
    cr = [s for s in cr if s]
    return eq, cr

# --- normalização de retornos (lista de candles com 't' e 'c')
def _norm_one_series(x: Any) -> List[Dict[str, Any]]:
    """
    Aceita:
      - lista de dicts (candles)
      - pandas.DataFrame-like (tem .to_dict('records'))
      - dict com chaves 't','c' em listas (agrega)
      - None/vazio -> []
    Cada candle precisa de ao menos 't' (ISO Z ou timestamp) e 'c' (close).
    """
    if x is None:
        return []
    if isinstance(x, list):
        out = []
        for row in x:
            if not isinstance(row, dict):
                continue
            t = row.get("t")
            c = row.get("c")
            if t is None or c is None:
                continue
            out.append({"t": t, "c": c, "o": row.get("o"), "h": row.get("h"), "l": row.get("l"), "v": row.get("v")})
        return out
    if hasattr(x, "to_dict"):
        try:
            recs = x.to_dict("records")  # type: ignore[attr-defined]
            return _norm_one_series(recs)
        except Exception:
            return []
    if isinstance(x, dict):
        # formato colunar: {"t":[...], "c":[...], ...}
        if "t" in x and "c" in x and isinstance(x["t"], list) and isinstance(x["c"], list):
            L = min(len(x["t"]), len(x["c"]))
            out = []
            for i in range(L):
                out.append({
                    "t": x["t"][i],
                    "c": x["c"][i],
                    "o": (x.get("o") or [None]*L)[i] if isinstance(x.get("o"), list) else None,
                    "h": (x.get("h") or [None]*L)[i] if isinstance(x.get("h"), list) else None,
                    "l": (x.get("l") or [None]*L)[i] if isinstance(x.get("l"), list) else None,
                    "v": (x.get("v") or [None]*L)[i] if isinstance(x.get("v"), list) else None,
                })
            return out
    return []

def _merge_pref(a: List[Dict[str, Any]], b: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Se A tem dados, fica com A; senão usa B."""
    return a if a else b

# --- chamadas flexíveis aos fetchers (aceita variações de assinatura)
def _flex_fetch_batch(fn, symbols: List[str], days: Optional[int]=None) -> Dict[str, List[Dict[str, Any]]]:
    """
    Tenta múltiplas assinaturas:
      1) fn(symbols=symbols, days=days)
      2) fn(symbols=symbols)
      3) fn(symbols)
      4) fallback 1-a-1: fn(symbol), agrega em dict
    Converte cada série com _norm_one_series.
    """
    # tentativa 1
    for attempt in (
        {"symbols": symbols, "days": days},
        {"symbols": symbols},
        {"_pos": True},  # chamada posicional com a lista inteira
    ):
        try:
            if attempt.get("_pos"):
                raw = fn(symbols)
            else:
                # remove None
                kwargs = {k: v for k, v in attempt.items() if k != "_pos" and v is not None}
                raw = fn(**kwargs)
            if isinstance(raw, dict):
                return {k: _norm_one_series(v) for k, v in raw.items()}
            if isinstance(raw, list):
                # alguns fetchers podem retornar lista pra um único símbolo
                if len(symbols) == 1:
                    return {symbols[0]: _norm_one_series(raw)}
            # dataframe? impossível mapear sem chave -> ignora
        except TypeError:
            continue
        except Exception:
            continue

    # fallback por símbolo
    out: Dict[str, List[Dict[str, Any]]] = {}
    for s in symbols:
        got = None
        for sig in (
            {"symbol": s, "days": days},
            {"symbol": s},
            {"symbol_canonical": s, "days": days},
            {"symbol_canonical": s},
            {"_pos": True},
        ):
            try:
                if sig.get("_pos"):
                    got = fn(s)  # posicional
                else:
                    kw = {k: v for k, v in sig.items() if k != "_pos" and v is not None}
                    got = fn(**kw)
                ser = _norm_one_series(got)
                if ser:
                    out[s] = ser
                    break
            except TypeError:
                continue
            except Exception:
                continue
    return out

def main() -> None:
    from src.fetch_eq import fetch_stooq, fetch_yahoo  # type: ignore
    from src.fetch_cr import fetch_binance, fetch_coingecko  # type: ignore

    eq_syms, cr_syms = _load_watch()
    log(f"eq={len(eq_syms)} cr={len(cr_syms)}")

    # Coleta EQ: Stooq → Yahoo fallback
    log("Coletando EQ via Stooq…")
    stq = _flex_fetch_batch(fetch_stooq, eq_syms, days=120)
    log(f"stooq ok={sum(1 for v in stq.values() if v)} / {len(eq_syms)}")

    log("Coletando EQ via Yahoo (fallback)…")
    yh = _flex_fetch_batch(fetch_yahoo, eq_syms, days=120)
    log(f"yahoo ok={sum(1 for v in yh.values() if v)} / {len(eq_syms)}")

    # Merge preferindo Stooq
    eq_map: Dict[str, List[Dict[str, Any]]] = {}
    for s in eq_syms:
        eq_map[s] = _merge_pref(stq.get(s) or [], yh.get(s) or [])

    # Coleta CR: Binance → Coingecko fallback
    log("Coletando CR via Binance…")
    bz = _flex_fetch_batch(fetch_binance, cr_syms, days=120)
    log(f"binance ok={sum(1 for v in bz.values() if v)} / {len(cr_syms)}")

    log("Coletando CR via Coingecko (fallback)…")
    cg = _flex_fetch_batch(fetch_coingecko, cr_syms, days=120)
    log(f"coingecko ok={sum(1 for v in cg.values() if v)} / {len(cr_syms)}")

    cr_map: Dict[str, List[Dict[str, Any]]] = {}
    for s in cr_syms:
        cr_map[s] = _merge_pref(bz.get(s) or [], cg.get(s) or [])

    # Junta EQ + CR em um único mapa OHLCV
    ohlcv: Dict[str, List[Dict[str, Any]]] = {}
    for s, v in (eq_map or {}).items():
        if v: ohlcv[s] = v
    for s, v in (cr_map or {}).items():
        if v: ohlcv[s] = v

    nonempty = sum(1 for v in ohlcv.values() if v)
    log(f"candles OK: {nonempty} símbolos com dados")

    if nonempty == 0:
        print("ERRO: Nenhum candle coletado. Verifique os fetchers (assinaturas/retornos).", file=sys.stderr)
        raise SystemExit(2)

    # Indicadores placeholders (em branco)
    ind: Dict[str, Dict[str, Any]] = {}
    for s in ohlcv.keys():
        ind[s] = {
            "RSI14": None,
            "ATR14": None,
            "ATR14_PCT": None,
            "BB_MA20": None,
            "BB_LOWER": None,
            "BB_UPPER": None,
            "FUNDING": None,
            "OI_CHG_3D_PCT": None
        }

    ts = _ts()
    p_ohl = PUBLIC / f"ohlcv_cache_{ts}.json"
    p_ind = PUBLIC / f"indicators_{ts}.json"

    p_ohl.write_text(json.dumps(ohlcv, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    p_ind.write_text(json.dumps(ind, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    log(f"gravado: {p_ohl.name} | {p_ind.name}")

    # Atualiza pointer
    pointer = {
        "ohlcv": f"public/{p_ohl.name}",
        "indicators": f"public/{p_ind.name}",
        "signals": None,
        "ohlcv_url": None,
        "indicators_url": None,
        "signals_url": None
    }
    POINTER.write_text(json.dumps(pointer, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    log("pointer_signals_v1.json atualizado")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        print(f"[collect][FATAL] {e}", file=sys.stderr)
        raise SystemExit(1)

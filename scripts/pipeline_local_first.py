#!/usr/bin/env python3
import os, sys, json, math, time, importlib, traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple
import numpy as np
import pandas as pd
from pathlib import Path
from subprocess import run, CalledProcessError

OUT_DIR = Path("public")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _now_tag() -> str:
    # igual ao padrão do projeto: UTC -> BRT tag friendly
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def _iso(ts) -> str:
    if isinstance(ts, (int,float)) and ts>1e10:  # ms -> s
        ts = ts/1000.0
    if isinstance(ts,(int,float)):
        return datetime.utcfromtimestamp(ts).replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z")
    if isinstance(ts,str):
        # tenta normalizar
        try:
            return datetime.fromisoformat(ts.replace("Z","+00:00")).astimezone(timezone.utc).isoformat().replace("+00:00","Z")
        except Exception:
            return ts
    return None

def _norm_candle_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    # aceita variantes de chaves comuns
    t = d.get("t") or d.get("time") or d.get("timestamp") or d.get("timestamp_utc")
    o = d.get("o") or d.get("open")
    h = d.get("h") or d.get("high")
    l = d.get("l") or d.get("low")
    c = d.get("c") or d.get("close") or d.get("price") or d.get("adj_close")
    v = d.get("v") or d.get("volume")
    return {"t": _iso(t), "o": _to_float(o), "h": _to_float(h), "l": _to_float(l), "c": _to_float(c), "v": _to_float(v)}

def _to_float(x):
    try:
        return float(x) if x is not None and not (isinstance(x,str) and x.strip()=="") else None
    except Exception:
        return None

def _flex_call_variants(fn, symbols: List[str], days: int|None) -> Dict[str, List[Dict[str,Any]]]:
    """
    Chama um fetcher desconhecido tentando várias assinaturas:
      1) fn(symbols=symbols, days=days)
      2) fn(symbols=symbols)
      3) fn(days=days)  # raríssimo
      4) loop: fn(symbol_canonical=s, days=days) para cada s
      5) loop: fn(symbol_canonical=s) para cada s
      6) loop: fn(s)  para cada s
    Retorna { "SYM": [ {t,o,h,l,c,v}, ... ], ... }
    """
    def _merge(dst, src):
        for k,v in (src or {}).items(): dst[k]=v

    # tenta em bloco
    for kwargs in (
        {"symbols": symbols, "days": days},
        {"symbols": symbols},
        {"days": days},
    ):
        try:
            out = fn(**{k:v for k,v in kwargs.items() if v is not None or k!="days"})
            if out: return _normalize_out(out)
        except TypeError:
            pass
        except Exception:
            # não aborta — tenta outras
            continue

    # tenta 1 a 1
    acc={}
    for s in symbols:
        for kwargs in (
            {"symbol_canonical": s, "days": days},
            {"symbol_canonical": s},
            {"": s}, # chamada posicional pura
        ):
            try:
                if "" in kwargs:
                    out = fn(kwargs[""])
                else:
                    out = fn(**{k:v for k,v in kwargs.items() if k and (v is not None or k!="days")})
                norm = _normalize_out(out, sym_hint=s)
                if norm:
                    acc[s]=norm.get(s, [])
                    break
            except TypeError:
                continue
            except Exception:
                continue
    return acc

def _normalize_out(out: Any, sym_hint: str|None=None) -> Dict[str, List[Dict[str,Any]]]:
    """
    Tenta normalizar a saída do fetcher para {sym: [candles]}.
    Aceita:
     - dict[str, list[dict]]
     - list[dict] (usa sym_hint)
     - pandas.DataFrame (usa colunas e sym_hint)
    """
    res: Dict[str, List[Dict[str,Any]]] = {}
    if out is None:
        return res
    # dict de séries
    if isinstance(out, dict):
        for k,v in out.items():
            series=[]
            if isinstance(v, pd.DataFrame):
                v = v.to_dict(orient="records")
            if isinstance(v, list):
                for row in v:
                    if isinstance(row, dict):
                        series.append(_norm_candle_dict(row))
            res[str(k)]=[r for r in series if r.get("t") and r.get("c") is not None]
        return res
    # lista de candles
    if isinstance(out, list):
        series=[]
        for row in out:
            if isinstance(row, dict):
                series.append(_norm_candle_dict(row))
        if sym_hint:
            res[sym_hint]=[r for r in series if r.get("t") and r.get("c") is not None]
        return res
    # DataFrame único
    if isinstance(out, pd.DataFrame):
        records = out.to_dict(orient="records")
        series=[_norm_candle_dict(r) for r in records]
        if sym_hint:
            res[sym_hint]=[r for r in series if r.get("t") and r.get("c") is not None]
        return res
    return res

def _indicators_for_series(series: List[Dict[str,Any]]) -> Dict[str, float|None]:
    """
    Calcula RSI14, ATR14, ATR14_PCT, BB_MA20/LOWER/UPPER na ponta.
    """
    if not series: return {}
    df = pd.DataFrame(series)
    # garante ordem crescente no tempo
    try:
        df["t_dt"] = pd.to_datetime(df["t"], utc=True)
        df = df.sort_values("t_dt").reset_index(drop=True)
    except Exception:
        pass

    close = df["c"].astype(float)
    high  = df["h"].astype(float) if "h" in df else close
    low   = df["l"].astype(float) if "l" in df else close

    # RSI14
    window=14
    delta = close.diff()
    up = delta.clip(lower=0.0).rolling(window=window, min_periods=window).mean()
    down = (-delta.clip(upper=0.0)).rolling(window=window, min_periods=window).mean()
    rs = up / down.replace(0,np.nan)
    rsi = 100 - (100/(1+rs))
    rsi_last = float(rsi.iloc[-1]) if rsi.notna().any() else None

    # ATR14 (True Range)
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low).abs(),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    atr = tr.rolling(window=window, min_periods=window).mean()
    atr_last = float(atr.iloc[-1]) if atr.notna().any() else None
    c_last = float(close.iloc[-1]) if close.notna().any() else None
    atr_pct_last = float(atr_last / c_last * 100.0) if (atr_last is not None and c_last and c_last!=0) else None

    # Bollinger 20
    bb_n = 20
    ma = close.rolling(bb_n, min_periods=bb_n).mean()
    sd = close.rolling(bb_n, min_periods=bb_n).std()
    bb_ma_last = float(ma.iloc[-1]) if ma.notna().any() else None
    bb_lo_last = float((ma - 2*sd).iloc[-1]) if (ma.notna().any() and sd.notna().any()) else None
    bb_up_last = float((ma + 2*sd).iloc[-1]) if (ma.notna().any() and sd.notna().any()) else None

    return {
        "RSI14": rsi_last,
        "ATR14": atr_last,
        "ATR14_PCT": atr_pct_last,
        "BB_MA20": bb_ma_last,
        "BB_LOWER": bb_lo_last,
        "BB_UPPER": bb_up_last,
    }

def main():
    print("[pipeline] Iniciando pipeline local-first…")
    # Carrega watchlists
    wl_path = Path("out/watchlists_local.json")
    if not wl_path.exists():
        print("ERRO: out/watchlists_local.json não encontrado. Rode scripts/build_watchlists_from_feed.py", file=sys.stderr)
        sys.exit(2)
    wl = json.loads(wl_path.read_text(encoding="utf-8"))
    eq_syms = [d["symbol_canonical"] for d in wl.get("eq",[])]
    cr_syms = [d["symbol_canonical"] for d in wl.get("cr",[])]

    # Importa fetchers dos módulos existentes
    try:
        fetch_eq_mod = importlib.import_module("src.fetch_eq")
        fetch_cr_mod = importlib.import_module("src.fetch_cr")
        fetch_yahoo   = getattr(fetch_eq_mod, "fetch_yahoo")
        fetch_stooq   = getattr(fetch_eq_mod, "fetch_stooq")
        fetch_binance = getattr(fetch_cr_mod, "fetch_binance")
        fetch_coingecko = getattr(fetch_cr_mod, "fetch_coingecko")
    except Exception as e:
        print(f"ERRO: não consegui importar fetchers: {e}", file=sys.stderr)
        sys.exit(3)

    # Busca candles de EQ
    print("[pipeline] Buscando candles EQ…")
    eq_yh = _flex_call_variants(fetch_yahoo, eq_syms, days=120)
    eq_st = _flex_call_variants(fetch_stooq, eq_syms, days=120)

    # Escolhe melhor fonte (maior número de candles por símbolo)
    eq_final: Dict[str, List[Dict[str,Any]]] = {}
    for s in eq_syms:
        a = eq_yh.get(s, [])
        b = eq_st.get(s, [])
        eq_final[s] = a if len(a) >= len(b) else b

    # Busca candles de CR
    print("[pipeline] Buscando candles CR…")
    cr_bz = _flex_call_variants(fetch_binance, cr_syms, days=120)
    cr_cg = _flex_call_variants(fetch_coingecko, cr_syms, days=120)

    cr_final: Dict[str, List[Dict[str,Any]]] = {}
    for s in cr_syms:
        a = cr_bz.get(s, [])
        b = cr_cg.get(s, [])
        cr_final[s] = a if len(a) >= len(b) else b

    # Remove símbolos sem dados
    eq_final = {k:v for k,v in eq_final.items() if v}
    cr_final = {k:v for k,v in cr_final.items() if v}

    if not eq_final and not cr_final:
        print("ERRO: Nenhum candle coletado. Verifique os fetchers (assinaturas/retornos).", file=sys.stderr)
        sys.exit(4)

    # Calcula indicadores por símbolo
    print("[pipeline] Calculando indicadores…")
    ind_map: Dict[str, Dict[str, float|None]] = {}
    for s, series in {**eq_final, **cr_final}.items():
        ind_map[s] = _indicators_for_series(series)

    # Grava artefatos
    ts = _now_tag()
    ohl_path = OUT_DIR / f"ohlcv_cache_{ts}.json"
    ind_path = OUT_DIR / f"indicators_{ts}.json"
    with open(ohl_path,"w",encoding="utf-8") as f:
        json.dump({"eq": eq_final, "cr": cr_final}, f, ensure_ascii=False)
    with open(ind_path,"w",encoding="utf-8") as f:
        json.dump(ind_map, f, ensure_ascii=False, indent=2)

    # Pointer
    ptr_path = OUT_DIR / "pointer_signals_v1.json"
    pointer = {
        "ohlcv": str(ohl_path),
        "indicators": str(ind_path),
        "signals": None,
        "ohlcv_url": None,
        "indicators_url": None,
        "signals_url": None
    }
    with open(ptr_path,"w",encoding="utf-8") as f:
        json.dump(pointer, f, ensure_ascii=False, indent=2)
    print(f"[pipeline] pointer_signals_v1 atualizado: {ohl_path} | {ind_path}")

    # Export v1
    try:
        print("[pipeline] Exportando signals v1…")
        run([sys.executable, "-m", "src.export_signals_v1"], check=True)
    except CalledProcessError as e:
        print(f"[pipeline][WARN] export_signals_v1 falhou: {e}", file=sys.stderr)

    # CSV final
    try:
        print("[pipeline] Gerando CSV…")
        run([sys.executable, "scripts/build_universe_csv.py"], check=True)
    except CalledProcessError as e:
        print(f"[pipeline][WARN] build_universe_csv falhou: {e}", file=sys.stderr)

    print("[pipeline] Concluído.")

if __name__ == "__main__":
    main()

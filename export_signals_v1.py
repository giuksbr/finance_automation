#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
export_signals_v1.py
--------------------
Gera `public/n_signals_v1_<TS>Z.json` e opcionalmente:
- `public/n_signals_v1_latest.json`
- atualiza `public/pointer_signals_v1.json`

Pontos chave deste arquivo:
- Lê o pointer principal: public/pointer.json (com URLs de OHLCV/INDIC/SIGNALS)
- Normaliza o OHLCV para garantir dicts em ohl["eq"] e ohl["cr"]
- Lê séries em dois formatos: (A) colunar {"c":[...], "t":[...]} e (B) lista de objetos
- Extrai: price_now_close, price_now_close_at_utc, pct_chg_7d/10d/30d
- Concilia indicadores (RSI, ATR, BB*) com os preços/variações
- Mantém/propaga derivatives já existentes do último run (quando possível)
- Escreve run_id, clock e demais campos sugeridos

CLI:
  python -m src.export_signals_v1 --with-universe --write-latest --update-pointer
"""

import argparse
import json
import os
from typing import Any, Dict, Optional, Tuple, List
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests


RAW_BASE = "https://raw.githubusercontent.com/giuksbr/finance_automation/main"
POINTER_PATH = "public/pointer.json"
OUT_DIR = "public"
SCHEMA_VERSION = "1.0"


def _prefer_path(pointer: Dict[str, Any], key_url: str, key_path: str) -> str:
    # se existir caminho local no pointer, usar
    p = pointer.get(key_path)
    if isinstance(p, str) and os.path.exists(p):
        return p
    # caso contrário, usa URL/arquivo original
    return pointer.get(key_url)


# ---------- utils de tempo ----------

BRT = ZoneInfo("America/Sao_Paulo")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def now_brt_iso() -> str:
    return datetime.now(BRT).replace(microsecond=0).isoformat()


def brt_date_today() -> str:
    return datetime.now(BRT).date().isoformat()


def to_iso_utc(ts: Any) -> Optional[str]:
    """Converte número epoch (s|ms), string ISO (com/sem Z), ou datetime p/ ISO-UTC."""
    try:
        if ts is None:
            return None
        if isinstance(ts, (int, float)):
            # heurística: epoch ms se muito grande
            if ts > 10**12:
                ts = ts / 1000.0
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if isinstance(ts, str):
            s = ts.strip()
            if s.endswith("Z"):
                s = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if hasattr(ts, "tzinfo"):
            dt = ts.astimezone(timezone.utc)
            return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return None
    return None


# ---------- IO helpers ----------


def _url_to_local(path_or_url: str) -> Optional[str]:
    try:
        if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
            # tenta mapear para public/<basename>
            basename = path_or_url.rsplit("/", 1)[-1]
            cand = os.path.join("public", basename)
            if os.path.exists(cand):
                return cand
        return None
    except Exception:
        return None

def _read_text(path_or_url: str) -> str:
    local_pref = _url_to_local(path_or_url)
    if local_pref:
        with open(local_pref, "r", encoding="utf-8") as f:
            return f.read()
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        r = requests.get(path_or_url, timeout=30)
        r.raise_for_status()
        return r.text
    with open(path_or_url, "r", encoding="utf-8") as f:
        return f.read()


def _read_json(path_or_url: str) -> Any:
    return json.loads(_read_text(path_or_url))


def _write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


# ---------- normalização OHLCV ----------

def _normalize_sections(ohl: Dict[str, Any]) -> Dict[str, Any]:
    """Garante que ohl['eq'] e ohl['cr'] sejam dicts (não str, não None)."""
    for k in ("eq", "cr"):
        v = ohl.get(k)
        if isinstance(v, str):
            try:
                ohl[k] = json.loads(v)
            except Exception:
                ohl[k] = {}
        elif v is None or not isinstance(v, dict):
            ohl[k] = {}
    return ohl


# ---------- leitura unificada de séries ----------

def _series_from_node(node: Any) -> Tuple[Optional[float], Optional[str], Optional[float], Optional[float], Optional[float]]:
    """
    Retorna: (last_close, last_close_at_iso_utc, pct7, pct10, pct30)
    Aceita:
      A) colunar: {"c":[...floats...], "t":[...epoch(s|ms)/iso...]}
      B) lista de objetos: [{"close"| "c"| "C"| "Close":...,"time"| "timestamp"| "t"| "Date": ...}, ...]
    """
    # pct em cima de vetor 'c'
    def pct(arr: List[float], days: int) -> Optional[float]:
        if not isinstance(arr, list) or len(arr) <= days:
            return None
        try:
            c_now = float(arr[-1])
            c_then = float(arr[-1 - days])
            return (c_now / c_then - 1.0) * 100.0
        except Exception:
            return None

    # Formato A: colunar
    if isinstance(node, dict) and ("c" in node) and ("t" in node) and isinstance(node["c"], list) and isinstance(node["t"], list):
        c = node["c"]
        t = node["t"]
        last_close = float(c[-1]) if c else None
        last_ts = to_iso_utc(t[-1]) if t else None
        return last_close, last_ts, pct(c, 7), pct(c, 10), pct(c, 30)

    # Formato B: lista de candles
    if isinstance(node, list) and node:
        def get_close(i: int) -> Optional[float]:
            x = node[i]
            if not isinstance(x, dict):
                return None
            for k in ("close", "c", "Close", "C"):
                if k in x and x[k] is not None:
                    try:
                        return float(x[k])
                    except Exception:
                        return None
            return None

        def get_time(i: int) -> Any:
            x = node[i]
            if not isinstance(x, dict):
                return None
            for k in ("time", "timestamp", "t", "Date", "date"):
                if k in x:
                    return x[k]
            return None

        # precisa de pelo menos 31 pontos para pct_30
        if len(node) >= 31:
            try:
                c_arr = [get_close(i) for i in range(len(node))]
                c_arr = [v for v in c_arr if v is not None]
                last = node[-1]
                last_close = float(last.get("close") or last.get("c") or last.get("Close") or last.get("C"))
                last_ts = to_iso_utc(get_time(len(node) - 1))
                return last_close, last_ts, pct(c_arr, 7), pct(c_arr, 10), pct(c_arr, 30)
            except Exception:
                pass

    return None, None, None, None, None


# ---------- indicadores -> mapa ----------

def _indicators_map(ind_any: Any) -> Dict[str, Dict[str, Any]]:
    """Aceita indicadores agrupados ({"eq":[...], "cr":[...]}) OU flat (lista). Retorna mapa por símbolo."""
    rows: List[Dict[str, Any]] = []
    if isinstance(ind_any, dict):
        if isinstance(ind_any.get("eq"), list):
            rows.extend(ind_any.get("eq") or [])
        if isinstance(ind_any.get("cr"), list):
            rows.extend(ind_any.get("cr") or [])
        # fallback: alguns dumps podem estar "flat" dentro do root
        if not rows:
            if isinstance(ind_any.get("rows"), list):
                rows.extend(ind_any.get("rows") or [])
    elif isinstance(ind_any, list):
        rows = ind_any

    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        sym = r.get("symbol_canonical") or r.get("symbol") or r.get("sym")
        if not sym:
            continue

        def g(*keys):
            for kk in keys:
                if kk in r and r[kk] is not None:
                    return r[kk]
            return None

        out[sym] = {
            "rsi14": g("RSI14", "rsi14"),
            "atr14": g("ATR14", "atr14"),
            "bb_ma20": g("BB_MA20", "bb_ma20"),
            "bb_lower": g("BB_LOWER", "bb_lower"),
            "bb_upper": g("BB_UPPER", "bb_upper"),
        }
    return out


# ---------- signals (raw) -> set de símbolos ----------

def _extract_watchlists(feed_any: Any) -> Tuple[List[str], List[str]]:
    """
    Procura watchlists de eq e cr (tanto como lista de strings quanto lista de dicts).
    Retorna (eq_symbols, cr_symbols)
    """
    eq: List[str] = []
    cr: List[str] = []
    if isinstance(feed_any, dict):
        for k in ("eq", "stocks", "equities"):
            v = feed_any.get(k)
            if isinstance(v, list):
                for item in v:
                    if isinstance(item, str):
                        eq.append(item)
                    elif isinstance(item, dict):
                        s = item.get("symbol_canonical") or item.get("symbol") or item.get("sym")
                        if s:
                            eq.append(s)
        for k in ("cr", "crypto", "coins"):
            v = feed_any.get(k)
            if isinstance(v, list):
                for item in v:
                    if isinstance(item, str):
                        cr.append(item)
                    elif isinstance(item, dict):
                        s = item.get("symbol_canonical") or item.get("symbol") or item.get("sym")
                        if s:
                            cr.append(s)
    # unique, preserve order
    def uniq(lst):
        seen = set()
        out = []
        for x in lst:
            if x not in seen:
                out.append(x)
                seen.add(x)
        return out

    return uniq(eq), uniq(cr)


# ---------- payload builder ----------

def build_payload(with_universe: bool = True) -> Dict[str, Any]:
    # pointer principal
    pointer = _read_json(POINTER_PATH)
    ohl_url = pointer.get("ohlcv_url")
    ind_url = pointer.get("indicators_url")
    raw_signals_url = pointer.get("signals_url")

    # lê fontes
    ohl = _normalize_sections(_read_json(ohl_url))
    ind = _read_json(ind_url)
    raw_signals = _read_json(raw_signals_url)

    # metadados de horário
    generated_at_brt = (
        raw_signals.get("generated_at_brt")
        if isinstance(raw_signals, dict) else None
    ) or ind.get("generated_at_brt") or ohl.get("generated_at_brt") or now_brt_iso()

    # clock
    clock = {
        "market_day_brt": brt_date_today(),
        "is_trading_day_us": bool(pointer.get("is_trading_day_us", True)),
        "is_trading_day_crypto": True,
    }

    # indicadores por símbolo
    ind_map = _indicators_map(ind)

    # tenta preservar derivatives do latest anterior
    prev_latest_map: Dict[str, Dict[str, Any]] = {}
    prev_latest_path = os.path.join(OUT_DIR, "n_signals_v1_latest.json")
    if os.path.exists(prev_latest_path):
        try:
            prev = _read_json(prev_latest_path)
            for it in prev.get("universe", []):
                sym = it.get("symbol_canonical")
                if sym and "derivatives" in it:
                    prev_latest_map[sym] = it["derivatives"]
        except Exception:
            pass

    universe: List[Dict[str, Any]] = []
    if with_universe:
        # partir do universo observado em OHLCV
        # (chaves em ohl["eq"] e ohl["cr"])
        for asset_type, sec in (("eq", ohl.get("eq", {})), ("crypto", ohl.get("cr", {}))):
            if not isinstance(sec, dict):
                continue
            for sym, node in sec.items():
                # extrai preços/variações
                last_close, last_ts, p7, p10, p30 = _series_from_node(node)

                ind_row = ind_map.get(sym, {}) if isinstance(ind_map, dict) else {}

                # fontes/priceguard
                if asset_type == "eq":
                    sources_used = ["yahoo", "stooq", "nasdaq"]
                else:
                    sources_used = ["binance", "coingecko"]

                priceguard = "OK" if (last_close is not None and p7 is not None) else ("PART" if last_close is not None else "FAIL")
                window_used = node.get("window") if isinstance(node, dict) else "7d"  # best effort

                item = {
                    "symbol_canonical": sym,
                    "asset_type": "eq" if asset_type == "eq" else "crypto",
                    "venue": sym.split(":")[0] if ":" in sym else None,
                    "window_used": window_used or "7d",

                    "price_now_close": last_close,
                    "price_now_close_at_utc": last_ts,

                    "pct_chg_7d": p7,
                    "pct_chg_10d": p10,
                    "pct_chg_30d": p30,

                    "rsi14": _safe_float(ind_row.get("rsi14")),
                    "atr14": _safe_float(ind_row.get("atr14")),
                    "bb_ma20": _safe_float(ind_row.get("bb_ma20")),
                    "bb_lower": _safe_float(ind_row.get("bb_lower")),
                    "bb_upper": _safe_float(ind_row.get("bb_upper")),

                    "levels": [],
                    "confidence": "low",

                    "validation": {
                        "priceguard": priceguard,
                        "window_status": "TARGET" if (window_used or "7d") == "7d" else "SHORT_WINDOW",
                        "sources_used": sources_used,
                    },
                }

                # derivações previamente conhecidas (cripto)
                if asset_type == "crypto":
                    if sym in prev_latest_map:
                        item["derivatives"] = prev_latest_map[sym]

                universe.append(item)

    payload = {
        "schema_version": SCHEMA_VERSION,
        "run_id": f"n_signals_v1_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        "generated_at_brt": generated_at_brt,
        "clock": clock,
        "signals": [],   # no momento derivamos tudo do universo; sinais específicos podem ser adicionados aqui
        "universe": universe,
    }
    return payload


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


# ---------- pointer_signals_v1 ----------

def update_pointer_signals_v1(latest_rel_path: str, generated_at_brt: str) -> Dict[str, Any]:
    expires_at_utc = datetime.now(timezone.utc).replace(microsecond=0)
    # 24h de validade
    expires_at_utc = (expires_at_utc).isoformat().replace("+00:00", "Z")

    pointer_obj = {
        "version": "1.0",
        "generated_at_brt": generated_at_brt,
        "signals_url": f"{RAW_BASE}/{latest_rel_path}",
        "expires_at_utc": expires_at_utc,
    }
    _write_json(os.path.join(OUT_DIR, "pointer_signals_v1.json"), pointer_obj)
    return pointer_obj


# ---------- main ----------

def main():
    parser = argparse.ArgumentParser(description="Exporta n_signals_v1 a partir de OHLC/INDIC/SIGNALS do pointer.")
    parser.add_argument("--with-universe", action="store_true", help="inclui bloco universe")
    parser.add_argument("--write-latest", action="store_true", help="também escreve n_signals_v1_latest.json")
    parser.add_argument("--update-pointer", action="store_true", help="atualiza public/pointer_signals_v1.json")
    args = parser.parse_args()

    payload = build_payload(with_universe=args.with_universe)

    # caminho versionado
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_ver = os.path.join(OUT_DIR, f"n_signals_v1_{ts}.json")
    _write_json(out_ver, payload)
    print(f"[ok] gerado {out_ver}")

    # latest
    latest_rel = "public/n_signals_v1_latest.json"
    latest_abs = os.path.join(OUT_DIR, "n_signals_v1_latest.json")
    if args.write_latest:
        _write_json(latest_abs, payload)
        print(f"[ok] gerado {latest_abs}")

    # pointer
    if args.update_pointer:
        pointer_obj = update_pointer_signals_v1(latest_rel, payload.get("generated_at_brt"))
        print(f"[ok] pointer atualizado: public/pointer_signals_v1.json")
        print(f"[info] pointer signals_url: {pointer_obj['signals_url']}")


if __name__ == "__main__":
    main()

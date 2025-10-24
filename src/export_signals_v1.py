#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
export_signals_v1.py
--------------------
Gera `public/n_signals_v1_<TS>Z.json` e opcionalmente:
- `public/n_signals_v1_latest.json`
- atualiza `public/pointer_signals_v1.json`

Local-first:
- Preferência por arquivos locais quando a URL do pointer aponta para "public/<arquivo>".

Correção desta versão:
- Normaliza APENAS os nomes de CAMPOS dos indicadores (rsi14/atr14/bb_*/close)
  preservando as chaves de SÍMBOLOS (ex.: "NYSEARCA:VUG") para evitar mismatch.
- Fallback: se OHLCV não tiver série, usa `indicators.close` como `price_now_close`.
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

BRT = ZoneInfo("America/Sao_Paulo")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def now_brt_iso() -> str:
    return datetime.now(BRT).replace(microsecond=0).isoformat()


def brt_date_today() -> str:
    return datetime.now(BRT).strftime("%Y-%m-%d")


def utc_timestamp_suffix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# ---------- leitura/gravação ----------

def _read_text(path_or_url: str) -> str:
    """
    Lê texto de caminho local ou URL. Para URLs de raw que apontem para /public/<basename>,
    tenta primeiro ./public/<basename>. Só faz GET se o arquivo local não existir.
    """
    try:
        if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
            from urllib.parse import urlparse
            parsed = urlparse(path_or_url)
            basename = os.path.basename(parsed.path)
            if basename:
                local_candidate = os.path.join("public", basename)
                if os.path.exists(local_candidate):
                    with open(local_candidate, "r", encoding="utf-8") as f:
                        return f.read()
            r = requests.get(path_or_url, timeout=30)
            r.raise_for_status()
            return r.text
        with open(path_or_url, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        try:
            bn = os.path.basename(path_or_url)
            if bn:
                candidate = os.path.join("public", bn)
                if os.path.exists(candidate):
                    with open(candidate, "r", encoding="utf-8") as f:
                        return f.read()
        except Exception:
            pass
        raise


def _read_json(path_or_url: str) -> Any:
    return json.loads(_read_text(path_or_url))


def _write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


# ---------- normalização OHLCV ----------

def _normalize_sections(ohl: Dict[str, Any]) -> Dict[str, Any]:
    """Garante que ohl['eq'] e ohl['cr'] sejam dicts."""
    for k in ("eq", "cr"):
        if k not in ohl or ohl[k] is None:
            ohl[k] = {}
        if isinstance(ohl[k], list):
            ohl[k] = {"data": ohl[k]}
        if not isinstance(ohl[k], dict):
            ohl[k] = {"data": ohl[k]}
    return ohl


def _ensure_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def _series_to_list(sec: Any) -> List[Dict[str, Any]]:
    """
    Converte para lista de objetos com chaves: c, t (opcional).
    Aceita:
      A) colunar: {"c":[...], "t":[...]}
      B) lista de objetos: [{"c":..., "t":...}, ...]
    """
    if isinstance(sec, dict) and "c" in sec and "t" in sec:
        c = _ensure_list(sec.get("c"))
        t = _ensure_list(sec.get("t"))
        out = []
        for i, cv in enumerate(c):
            tv = t[i] if i < len(t) else None
            if isinstance(tv, (int, float)):
                tv = datetime.fromtimestamp(tv, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            elif isinstance(tv, str) and tv.isdigit():
                tv2 = int(tv)
                tv = datetime.fromtimestamp(tv2, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            out.append({"c": cv, "t": tv})
        return out
    if isinstance(sec, list):
        return sec
    return _ensure_list(sec)


def _extract_pct_chg(arr: List[float], win: int) -> Optional[float]:
    if not arr or len(arr) < win + 1:
        return None
    try:
        old = float(arr[-(win + 1)])
        new = float(arr[-1])
        if old == 0:
            return None
        return (new - old) / old * 100.0
    except Exception:
        return None


def _merge_eq_cr(ohl: Dict[str, Any]) -> Dict[str, Any]:
    out = {"eq": {}, "cr": {}}
    eq = ohl.get("eq", {}) or {}
    cr = ohl.get("cr", {}) or {}
    if isinstance(eq, dict):
        out["eq"] = eq
    if isinstance(cr, dict):
        out["cr"] = cr
    return out


# ---------- indicadores: normalização (somente campos, preserva símbolos) ----------

FIELD_ALIASES = {
    "rsi14": {"RSI14", "rsi14"},
    "atr14": {"ATR14", "atr14"},
    "bb_ma20": {"BB_MA20", "bb_ma20"},
    "bb_lower": {"BB_LOWER", "bb_lower"},
    "bb_upper": {"BB_UPPER", "bb_upper"},
    "close": {"CLOSE", "close"},
    "atr14_pct": {"ATR14_PCT", "atr14_pct"},  # se existir
}

def _normalize_indicator_fields(node: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recebe o dict de UM símbolo e retorna um novo dict com campos minúsculos.
    Não altera o nome do símbolo; apenas os nomes dos campos internos.
    """
    out: Dict[str, Any] = {}
    if not isinstance(node, dict):
        return out

    # primeiro, copia todos com lower-case de chave
    for k, v in node.items():
        out[str(k).lower()] = v

    # garante aliases explícitos
    for target, aliases in FIELD_ALIASES.items():
        if target not in out:
            for a in aliases:
                if a.lower() in out:
                    out[target] = out[a.lower()]
                    break
    return out


def _normalize_indicators(ind: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza APENAS os campos internos, preservando chaves 'eq'/'cr' e símbolos.
    Estruturas aceitas:
      - ind["eq"][SYM], ind["cr"][SYM]
      - ind[SYM] (top-level)
    """
    out: Dict[str, Any] = {}

    # mantém 'eq'/'cr' se existirem
    for bucket in ("eq", "cr"):
        node = ind.get(bucket)
        if isinstance(node, dict):
            out[bucket] = {}
            for sym, sym_node in node.items():
                out[bucket][sym] = _normalize_indicator_fields(sym_node)

    # símbolos no topo (fora de eq/cr)
    for k, v in ind.items():
        if k in ("eq", "cr"):
            continue
        if isinstance(v, dict):
            out[k] = _normalize_indicator_fields(v)

    return out


# ---------- pointer & escolha local-first ----------

def _load_pointer(path: str = POINTER_PATH) -> Dict[str, Any]:
    return _read_json(path)


def _prefer_local_from_pointer(ptr: Dict[str, Any], key_url: str, key_path: str) -> str:
    lp = ptr.get(key_path)
    if isinstance(lp, str) and lp and os.path.exists(lp):
        return lp
    url = ptr.get(key_url)
    if isinstance(url, str) and url:
        bn = os.path.basename(url)
        candidate = os.path.join("public", bn)
        if os.path.exists(candidate):
            return candidate
        return url
    lp2 = ptr.get(key_path)
    if isinstance(lp2, str):
        bn = os.path.basename(lp2)
        candidate = os.path.join("public", bn)
        if os.path.exists(candidate):
            return candidate
    raise FileNotFoundError(f"Pointer sem {key_url}/{key_path} válidos ou arquivo local inexistente.")


# ---------- métricas a partir das séries ----------

def _extract_close_ts_from_colunar(sec: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
    c = _ensure_list(sec.get("c"))
    t = _ensure_list(sec.get("t"))
    if not c:
        return None, None
    last_c = c[-1]
    ts = None
    if t and len(t) == len(c):
        ts = t[-1]
        if isinstance(ts, (int, float)):
            ts = datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        elif isinstance(ts, str) and ts.isdigit():
            ts2 = int(ts)
            ts = datetime.fromtimestamp(ts2, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return last_c, ts


def _extract_close_ts_from_list(sec: List[Dict[str, Any]]) -> Tuple[Optional[float], Optional[str]]:
    if not sec:
        return None, None
    last = sec[-1]
    last_c = last.get("c")
    ts = last.get("t")
    if isinstance(ts, (int, float)):
        ts = datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    elif isinstance(ts, str) and ts.isdigit():
        ts2 = int(ts)
        ts = datetime.fromtimestamp(ts2, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return last_c, ts


def _extract_metrics_from_series(sec: Any) -> Tuple[Optional[float], Optional[str], Optional[float], Optional[float], Optional[float], List[Dict[str, Any]]]:
    series_list = _series_to_list(sec)
    closes = [x.get("c") for x in series_list if isinstance(x, dict) and "c" in x]
    close_now = None
    if closes:
        try:
            close_now = float(closes[-1])
        except Exception:
            close_now = None

    if isinstance(sec, dict) and "c" in sec and "t" in sec:
        close_ts_utc = _extract_close_ts_from_colunar(sec)[1]
        chg_7d = _extract_pct_chg(_ensure_list(sec.get("c")), 7)
        chg_10d = _extract_pct_chg(_ensure_list(sec.get("c")), 10)
        chg_30d = _extract_pct_chg(_ensure_list(sec.get("c")), 30)
    elif isinstance(sec, list):
        close_ts_utc = _extract_close_ts_from_list(sec)[1]
        chg_7d = _extract_pct_chg([float(x.get("c")) for x in sec if isinstance(x, dict) and x.get("c") is not None], 7)
        chg_10d = _extract_pct_chg([float(x.get("c")) for x in sec if isinstance(x, dict) and x.get("c") is not None], 10)
        chg_30d = _extract_pct_chg([float(x.get("c")) for x in sec if isinstance(x, dict) and x.get("c") is not None], 30)
    else:
        close_ts_utc = None
        chg_7d = chg_10d = chg_30d = None

    return close_now, close_ts_utc, chg_7d, chg_10d, chg_30d, series_list


# ---------- construção do payload ----------

def build_payload(*, with_universe: bool = False) -> Dict[str, Any]:
    ptr = _load_pointer()

    ohl_url_or_path = _prefer_local_from_pointer(ptr, "ohlcv_url", "ohlcv_path")
    ind_url_or_path = _prefer_local_from_pointer(ptr, "indicators_url", "indicators_path")
    sig_url_or_path = _prefer_local_from_pointer(ptr, "signals_url", "signals_path")

    ohl = _normalize_sections(_read_json(ohl_url_or_path))
    ind_raw = _read_json(ind_url_or_path)
    sig = _read_json(sig_url_or_path)

    # normaliza SOMENTE campos de indicadores; preserva símbolos
    ind: Dict[str, Any] = _normalize_indicators(ind_raw)

    merged = _merge_eq_cr(ohl)

    universe_rows: List[Dict[str, Any]] = []
    signals_rows: List[Dict[str, Any]] = []

    for asset_type in ("eq", "cr"):
        bucket = merged.get(asset_type, {}) or {}
        if not isinstance(bucket, dict):
            continue
        for sym, sec in bucket.items():
            if sec is None:
                continue

            # 1) tenta extrair de séries OHLCV (se existirem)
            close_now, close_ts_utc, chg_7d, chg_10d, chg_30d, series_list = _extract_metrics_from_series(sec)

            # 2) indicadores do símbolo (ind[asset_type][sym] ou ind[sym])
            ind_node = None
            if isinstance(ind.get(asset_type), dict):
                ind_node = ind[asset_type].get(sym)
            if ind_node is None:
                ind_node = ind.get(sym)

            rsi14 = atr14 = atr14_pct = bb_ma20 = bb_lower = bb_upper = None
            close_from_ind = None
            if isinstance(ind_node, dict):
                rsi14 = ind_node.get("rsi14")
                atr14 = ind_node.get("atr14")
                atr14_pct = ind_node.get("atr14_pct")
                bb_ma20 = ind_node.get("bb_ma20")
                bb_lower = ind_node.get("bb_lower")
                bb_upper = ind_node.get("bb_upper")
                close_from_ind = ind_node.get("close")

            # 3) fallback de preço: se não houver série, usa CLOSE do indicators
            if close_now is None and close_from_ind is not None:
                try:
                    close_now = float(close_from_ind)
                except Exception:
                    pass

            # funding e oi_chg_3d_pct (se vieram em sig)
            funding = None
            oi_chg_3d_pct = None
            if isinstance(sig, dict):
                sn = sig.get(sym) or (sig.get(asset_type, {}).get(sym) if isinstance(sig.get(asset_type), dict) else None)
                if isinstance(sn, dict):
                    funding = sn.get("funding")
                    oi_chg_3d_pct = sn.get("oi_chg_3d_pct")

            row = {
                "symbol_canonical": sym,
                "asset_type": asset_type,
                "window_used": str(sec.get("window")) if isinstance(sec, dict) and sec.get("window") else "7d",
                "price_now_close": close_now,
                "price_now_close_at_utc": close_ts_utc,
                "pct_chg_7d": chg_7d,
                "pct_chg_10d": chg_10d,
                "pct_chg_30d": chg_30d,
                "rsi14": rsi14,
                "atr14": atr14,
                "atr14_pct": atr14_pct,
                "bb_ma20": bb_ma20,
                "bb_lower": bb_lower,
                "bb_upper": bb_upper,
                "funding": funding,
                "oi_chg_3d_pct": oi_chg_3d_pct,
                "source_sections_len": len(series_list) if isinstance(series_list, list) else None,
            }
            signals_rows.append(row)

            if with_universe:
                universe_rows.append({
                    "symbol": sym,
                    "asset_type": asset_type,
                })

    payload: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": now_utc_iso(),
        "generated_at_brt": now_brt_iso(),
        "brt_date": brt_date_today(),
        "signals": signals_rows,
    }
    if with_universe:
        payload["universe"] = universe_rows
    return payload


# ---------- escrita dos artefatos ----------

def write_payload_files(payload: Dict[str, Any],
                        *,
                        write_latest: bool = False) -> Tuple[str, Optional[str]]:
    ts = utc_timestamp_suffix()
    path_ts = os.path.join(OUT_DIR, f"n_signals_v1_{ts}.json")
    _write_json(path_ts, payload)

    path_latest = None
    if write_latest:
        path_latest = os.path.join(OUT_DIR, "n_signals_v1_latest.json")
        _write_json(path_latest, payload)

    return path_ts, path_latest


def update_pointer_signals_v1(path_ts: str, raw_base: str = RAW_BASE) -> str:
    bn = os.path.basename(path_ts)
    pointer_obj = {
        "schema_version": SCHEMA_VERSION,
        "updated_at_brt": now_brt_iso(),
        "updated_at_utc": now_utc_iso(),
        "signals_v1_url": f"{raw_base}/public/{bn}",
    }
    out = os.path.join(OUT_DIR, "pointer_signals_v1.json")
    _write_json(out, pointer_obj)
    return out


# ---------- CLI ----------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Exporta payload v1 e atualiza pointer_signals_v1.json")
    p.add_argument("--with-universe", action="store_true", help="Inclui bloco universe no payload")
    p.add_argument("--write-latest", action="store_true", help="Escreve também public/n_signals_v1_latest.json")
    p.add_argument("--update-pointer", action="store_true", help="Atualiza public/pointer_signals_v1.json")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    payload = build_payload(with_universe=args.with_universe)
    path_ts, path_latest = write_payload_files(payload, write_latest=args.write_latest)

    if args.update_pointer:
        raw_base = RAW_BASE
        try:
            from src.update_pointer_signals_v1 import resolve_raw_base_url  # type: ignore
            rb = resolve_raw_base_url()
            if isinstance(rb, str) and rb:
                raw_base = rb
        except Exception:
            pass
        pointer_path = update_pointer_signals_v1(path_ts, raw_base=raw_base)
        print(f"pointer_signals_v1 atualizado: {pointer_path}")

    print(f"signals v1 gerado: {path_ts}")
    if path_latest:
        print(f"signals v1 latest atualizado: {path_latest}")


if __name__ == "__main__":
    main()

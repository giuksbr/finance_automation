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
- Constrói payload v1 com "universe" opcional e "signals"
- Atualiza pointer_signals_v1.json (raw_base configurável em update_pointer_signals_v1.py)

Mudança principal desta versão (local-first):
- _read_text(path_or_url): se `path_or_url` for URL de raw e o arquivo existir em ./public/<basename>,
  **lê localmente** ao invés de fazer GET. Só acessa a web se o arquivo local não existir.

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

# ---------- utils de tempo ----------

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
    Lê texto de um caminho local ou URL. Para URLs (http/https) que pareçam apontar
    para arquivos dentro de "public/", tenta primeiro ./public/<basename> (se existir)
    para evitar chamadas web desnecessárias (ex.: 429). Só faz GET se não existir local.
    """
    # Mapeia URL -> arquivo local (./public/<basename>) quando apropriado
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
            # Se não existir local, faz a requisição normalmente
            r = requests.get(path_or_url, timeout=30)
            r.raise_for_status()
            return r.text
        # Caminho local direto
        with open(path_or_url, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        # Tentativa final: se o basename existir em public/, usa-o
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
    """Garante que ohl['eq'] e ohl['cr'] sejam dicts (não str, não None)."""
    for k in ("eq", "cr"):
        if k not in ohl or ohl[k] is None:
            ohl[k] = {}
        if isinstance(ohl[k], list):
            # converte lista para dict com chave "data"
            ohl[k] = {"data": ohl[k]}
        if not isinstance(ohl[k], dict):
            # se vier tipo inesperado, embrulha
            ohl[k] = {"data": ohl[k]}
    return ohl


def _ensure_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def _extract_close_ts_from_colunar(sec: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
    """
    Formato A (colunar): {"c":[...], "t":[...]}
      - fecha (close) = último item de "c"
      - timestamp UTC (string ISO ou epoch em "t" correspondente)
    """
    c = _ensure_list(sec.get("c"))
    t = _ensure_list(sec.get("t"))
    if not c or len(c) == 0:
        return None, None
    last_c = c[-1]
    ts = None
    if t and len(t) == len(c):
        ts = t[-1]
        # normaliza timestamp → ISO Z se for epoch numérico
        if isinstance(ts, (int, float)):
            ts = datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        elif isinstance(ts, str) and ts.isdigit():
            ts2 = int(ts)
            ts = datetime.fromtimestamp(ts2, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return last_c, ts


def _extract_close_ts_from_list(sec: List[Dict[str, Any]]) -> Tuple[Optional[float], Optional[str]]:
    """
    Formato B (lista de objetos): [{"c":..., "t":...}, ...]
      - fecha (close) = "c" do último item
      - timestamp UTC idem
    """
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


def _series_to_list(sec: Any) -> List[Dict[str, Any]]:
    """
    Converte formato A ou B em lista de objetos com chaves: c, t (opcional).
    """
    if isinstance(sec, dict) and "c" in sec and "t" in sec:
        c = _ensure_list(sec.get("c"))
        t = _ensure_list(sec.get("t"))
        out = []
        for i, cv in enumerate(c):
            tv = t[i] if i < len(t) else None
            if isinstance(tv, (int, float)):
                tv = datetime.fromtimestamp(tv, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            elif isinstance(tv, str) and tv and tv.isdigit():
                tv2 = int(tv)
                tv = datetime.fromtimestamp(tv2, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            out.append({"c": cv, "t": tv})
        return out
    if isinstance(sec, list):
        # já está em lista de objetos
        return sec
    # formato desconhecido: embrulha
    return _ensure_list(sec)


# ---------- leitura do pointer ----------

def _load_pointer(path: str = POINTER_PATH) -> Dict[str, Any]:
    """
    Lê pointer principal (public/pointer.json).
    Espera algo como:
      {
        "ohlcv_url": ".../public/ohlcv_cache_<TS>.json",
        "indicators_url": ".../public/indicators_<TS>.json",
        "signals_url": ".../public/n_signals_<TS>.json",
        # (opcional) caminhos locais adicionados pelo job:
        "ohlcv_path": "public/ohlcv_cache_<TS>.json",
        "indicators_path": "public/indicators_<TS>.json",
        "signals_path": "public/n_signals_<TS>.json"
      }
    """
    return _read_json(path)


def _prefer_local_from_pointer(ptr: Dict[str, Any], key_url: str, key_path: str) -> str:
    """
    Dado um pointer e as chaves (URL e PATH local), retorna:
      1) o PATH local se existir,
      2) senão, a URL do pointer,
      3) fallback final: se a URL apontar para /public/<bn> e existir public/<bn>, usa local.
    """
    # 1) path local do pointer
    lp = ptr.get(key_path)
    if isinstance(lp, str) and lp and os.path.exists(lp):
        return lp

    # 2) URL do pointer
    url = ptr.get(key_url)
    if isinstance(url, str) and url:
        # Se houver arquivo local com o mesmo basename, usa local
        bn = os.path.basename(url)
        candidate = os.path.join("public", bn)
        if os.path.exists(candidate):
            return candidate
        return url

    # 3) fallback extremo: se tiver só basename via path
    lp2 = ptr.get(key_path)
    if isinstance(lp2, str):
        bn = os.path.basename(lp2)
        candidate = os.path.join("public", bn)
        if os.path.exists(candidate):
            return candidate

    raise FileNotFoundError(f"Pointer sem {key_url}/{key_path} válidos ou arquivo local inexistente.")


# ---------- construção do payload ----------

def _merge_eq_cr(ohl: Dict[str, Any]) -> Dict[str, Any]:
    """
    Consolida estruturas para iteração única: devolve dict {"eq":{...}, "cr":{...}}
    """
    out = {"eq": {}, "cr": {}}
    eq = ohl.get("eq", {}) or {}
    cr = ohl.get("cr", {}) or {}
    if isinstance(eq, dict):
        out["eq"] = eq
    if isinstance(cr, dict):
        out["cr"] = cr
    return out


def _extract_metrics_from_series(sec: Any) -> Tuple[Optional[float], Optional[str], Optional[float], Optional[float], Optional[float], List[Dict[str, Any]]]:
    """
    Retorna:
      close_now, close_ts_utc, chg_7d, chg_10d, chg_30d, series_list
    """
    series_list = _series_to_list(sec)
    closes = [x.get("c") for x in series_list if isinstance(x, dict) and "c" in x]
    close_now = None
    if closes:
        try:
            close_now = float(closes[-1])
        except Exception:
            close_now = None

    # extração do timestamp e pct_chg pelo formato original, se disponível
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


def build_payload(*, with_universe: bool = False) -> Dict[str, Any]:
    """
    Monta o payload v1 a partir dos artefatos apontados por public/pointer.json.
    Agora com comportamento local-first: sempre tenta abrir ./public/<arquivo> antes
    de acessar as URLs do pointer.
    """
    ptr = _load_pointer()

    ohl_url_or_path = _prefer_local_from_pointer(ptr, "ohlcv_url", "ohlcv_path")
    ind_url_or_path = _prefer_local_from_pointer(ptr, "indicators_url", "indicators_path")
    sig_url_or_path = _prefer_local_from_pointer(ptr, "signals_url", "signals_path")

    ohl = _normalize_sections(_read_json(ohl_url_or_path))
    ind = _read_json(ind_url_or_path)
    sig = _read_json(sig_url_or_path)

    merged = _merge_eq_cr(ohl)

    # universe opcional
    universe_rows: List[Dict[str, Any]] = []

    signals_rows: List[Dict[str, Any]] = []
    # Percorre eq + cr
    for asset_type in ("eq", "cr"):
        bucket = merged.get(asset_type, {}) or {}
        if not isinstance(bucket, dict):
            continue
        for sym, sec in bucket.items():
            if sec is None:
                continue
            # extrai métricas dos preços (close_now, timestamp, variações)
            close_now, close_ts_utc, chg_7d, chg_10d, chg_30d, series_list = _extract_metrics_from_series(sec)

            # indicadores: tentamos buscar por asset_type->sym ou por sym direto
            ind_node = None
            if isinstance(ind.get(asset_type), dict):
                ind_node = ind[asset_type].get(sym)
            if ind_node is None:
                ind_node = ind.get(sym)

            # campos de indicadores (podem não existir)
            rsi14 = None
            atr14 = None
            atr14_pct = None
            bb_ma20 = None
            bb_lower = None
            bb_upper = None
            if isinstance(ind_node, dict):
                rsi14 = ind_node.get("rsi14")
                atr14 = ind_node.get("atr14")
                atr14_pct = ind_node.get("atr14_pct")
                bb_ma20 = ind_node.get("bb_ma20")
                bb_lower = ind_node.get("bb_lower")
                bb_upper = ind_node.get("bb_upper")

            # funding e oi_chg_3d_pct (se vieram em sig)
            funding = None
            oi_chg_3d_pct = None
            if isinstance(sig, dict):
                sn = sig.get(sym) or sig.get(asset_type, {}).get(sym) if isinstance(sig.get(asset_type), dict) else None
                if isinstance(sn, dict):
                    funding = sn.get("funding")
                    oi_chg_3d_pct = sn.get("oi_chg_3d_pct")

            row = {
                "symbol_canonical": sym,
                "asset_type": asset_type,
                "window_used": "7d",  # pode ser atualizado se você guardar essa info em outro lugar
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
                # guardamos referências de origem para troubleshooting
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
    """
    Escreve:
      - public/n_signals_v1_<TS>Z.json
      - (opcional) public/n_signals_v1_latest.json
    Retorna (path_ts, path_latest_or_none)
    """
    ts = utc_timestamp_suffix()
    path_ts = os.path.join(OUT_DIR, f"n_signals_v1_{ts}.json")
    _write_json(path_ts, payload)

    path_latest = None
    if write_latest:
        path_latest = os.path.join(OUT_DIR, "n_signals_v1_latest.json")
        _write_json(path_latest, payload)

    return path_ts, path_latest


def update_pointer_signals_v1(path_ts: str, raw_base: str = RAW_BASE) -> str:
    """
    Atualiza public/pointer_signals_v1.json com o novo arquivo gerado.
      {
        "signals_v1_url": "<RAW_BASE>/public/<basename>",
        "updated_at_brt": "...",
        "updated_at_utc": "...",
        "schema_version": "1.0"
      }
    O `raw_base` final pode ser ajustado pelo módulo src/update_pointer_signals_v1.py
    conforme config.yaml. Aqui usamos um default.
    """
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

    # Atualização do pointer v1 (se pedida)
    if args.update_pointer:
        # Permite override via src/update_pointer_signals_v1.py que lê config.yaml
        raw_base = RAW_BASE
        try:
            # import tardio para não criar dependência cíclica
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

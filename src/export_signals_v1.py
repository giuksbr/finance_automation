#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Exportador n_signals_v1:
- Lê public/pointer.json (ohlcv_url, indicators_url, signals_url)
- Consolida em n_signals_v1_* e/ou n_signals_v1_latest.json
- Acrescenta METADADOS pedidas:
  - Top-level: run_id, commit_sha, content_sha256, clock{market_day_brt, is_trading_day_us, is_trading_day_crypto}
  - Por item (signals/universe): price_now_close_at_utc, pct_chg_10d, pct_chg_30d, atr14_pct, liq{...} (se disponível)
- Compatível com: --with-universe --write-latest --update-pointer
"""
from __future__ import annotations

import os
import argparse
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    import requests
except Exception:
    requests = None  # fallback arquivo local

# ----------------------------
# Helpers de I/O
# ----------------------------

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PUBLIC_DIR = os.path.join(ROOT, "public")
POINTER_PATH = os.path.join(PUBLIC_DIR, "pointer.json")
POINTER_SIG_V1_PATH = os.path.join(PUBLIC_DIR, "pointer_signals_v1.json")

RAW_BASE_DEFAULT = "https://raw.githubusercontent.com/giuksbr/finance_automation/main"


def _read_text(path_or_url: str) -> str:
    if re.match(r"^https?://", path_or_url):
        if requests is None:
            raise RuntimeError("requests não disponível para leitura HTTP")
        r = requests.get(path_or_url, timeout=30)
        r.raise_for_status()
        return r.text
    with open(path_or_url, "r", encoding="utf-8") as f:
        return f.read()


def _read_json(path_or_url: str) -> Any:
    return json.loads(_read_text(path_or_url))


def _write_json(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"), sort_keys=False)


def _git_sha_short() -> Optional[str]:
    try:
        out = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, timeout=5)
        return out.decode().strip()
    except Exception:
        return None


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _to_iso_utc(ts: Union[int, float, str]) -> Optional[str]:
    """
    Converte epoch seconds (int/float) ou string YYYY-MM-DD[THH:MM:SS] em ISO UTC.
    Retorna None se não conseguir.
    """
    try:
        if isinstance(ts, (int, float)):
            return dt.datetime.utcfromtimestamp(int(ts)).replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00", "Z")
        s = str(ts)
        # Se vier 'YYYY-MM-DD' assumimos 00:00:00Z do DIA
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            d = dt.datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
            return d.isoformat().replace("+00:00", "Z")
        # Se vier ISO já com TZ/sem TZ, normalizamos para Z
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


# ----------------------------
# Extração / shapes esperados
# ----------------------------

def load_pointer() -> Dict[str, str]:
    if not os.path.exists(POINTER_PATH):
        raise FileNotFoundError(f"Pointer não encontrado em {POINTER_PATH}")
    p = _read_json(POINTER_PATH)
    for k in ("ohlcv_url", "indicators_url", "signals_url"):
        if k not in p:
            raise ValueError(f"Pointer inválido: falta chave {k}")
    return p


def extract_watchlists(feed: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """
    Retorna (eq_list, cr_list) a partir do feed.
    Aceita 'universe.watchlists.avenue/binance' e formatos legados.
    """
    eq: List[str] = []
    cr: List[str] = []

    uni = (feed or {}).get("universe", {})
    w = (uni or {}).get("watchlists", {})
    av = (w or {}).get("avenue", {})
    bn = (w or {}).get("binance", {})

    def _to_syms(x):
        # pode vir lista de strings ou lista de objetos {symbol: "..."}
        if not x:
            return []
        out = []
        for item in x:
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, dict):
                val = item.get("symbol") or item.get("symbol_canonical") or item.get("ticker") or item.get("pair")
                if val:
                    out.append(val)
        return out

    eq = _to_syms(av.get("whitelist", [])) + _to_syms(av.get("candidate_pool", []))
    cr = _to_syms(bn.get("whitelist", [])) + _to_syms(bn.get("candidate_pool", []))

    # dedup preservando ordem
    def _dedup(seq: List[str]) -> List[str]:
        seen = set()
        out = []
        for x in seq:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    return _dedup(eq), _dedup(cr)


# ----------------------------
# Transform helpers por símbolo
# ----------------------------

def last_close_and_ts_from_ohlcv(series: List[Dict[str, Any]]) -> Tuple[Optional[float], Optional[str]]:
    """
    Espera lista de candles [{date: 'YYYY-MM-DD' ou ISO ou epoch, close: float}, ...]
    Pega o último elemento.
    """
    if not series:
        return None, None
    last = series[-1]
    close = last.get("close")
    d = last.get("date") or last.get("time") or last.get("t")  # flexível
    iso = _to_iso_utc(d) if d is not None else None
    try:
        close = float(close) if close is not None else None
    except Exception:
        close = None
    return close, iso


def pct_chg_from_window(series: List[Dict[str, Any]], days: int) -> Optional[float]:
    """
    Calcula variação percentual entre último close e close de N dias atrás.
    Supõe 1 candle/dia; se faltar profundidade, retorna None.
    """
    n = len(series or [])
    if n < (days + 1):
        return None
    try:
        last = float(series[-1]["close"])
        prev = float(series[-(days + 1)]["close"])
        if prev == 0:
            return None
        return 100.0 * (last - prev) / prev
    except Exception:
        return None


def enrich_item_with_indicators(item: Dict[str, Any]) -> None:
    """Calcula atr14_pct quando possível."""
    price = item.get("price_now_close")
    atr14 = item.get("atr14")
    try:
        if price is not None and atr14 is not None:
            item["atr14_pct"] = round(100.0 * float(atr14) / float(price), 4)
    except Exception:
        pass


# ----------------------------
# Construção do payload
# ----------------------------

def build_payload(with_universe: bool = False) -> dict:
    pointer = _read_json(os.path.join(ROOT, "public", "pointer.json"))
    ohl = _read_json(pointer["ohlcv_url"])
    ind = _read_json(pointer["indicators_url"])

    # --- Ler sinais brutos do pointer (podem ser OBJ ou LISTA) ---
    raw_signals = _read_json(pointer["signals_url"])
    if isinstance(raw_signals, list):
        # Formato legado: lista pura de itens de sinal
        raw_signals_hdr = {}
        raw_signals_list = raw_signals
    elif isinstance(raw_signals, dict):
        raw_signals_hdr = raw_signals
        raw_signals_list = raw_signals.get("signals", [])
    else:
        raise TypeError(f"Formato inesperado em signals_url: {type(raw_signals)}")

    # --- Descobrir generated_at_brt do melhor lugar ---
    generated_at_brt = (
        raw_signals_hdr.get("generated_at_brt")
        if isinstance(raw_signals_hdr, dict) else None
    ) or ind.get("generated_at_brt") or ohl.get("generated_at_brt")

    # --- Extrair watchlists do feed ---
    feed = _read_json(DEFAULT_FEED_URL)
    eq_list, cr_list = _extract_watchlists(feed)

    # --- Montar universo (se solicitado) ---
    universe = []
    if with_universe:
        universe = _build_universe(eq_list, cr_list, ohl, ind)

    # --- Construir sinais já no contrato v1 (podem estar vazios) ---
    signals_v1 = _project_signals_v1(raw_signals_list, ohl, ind)

    # --- Cabeçalho v1 ---
    payload = {
        "schema_version": "1.0",
        "generated_at_brt": generated_at_brt,
        "universe_counts": { "avenue": len(eq_list), "binance": len(cr_list) },
        "sources": {
            "eq_primary": "stooq", "eq_backup": "yahoo", "eq_sanity": "nasdaq",
            "cr_primary": "binance", "cr_backup": "coingecko", "cr_sanity": "kraken"
        },
        "window_policy": { "target_days": 7, "fallback_days": 30 },
        "priceguard": { "eq_tolerance_pct": 0.8, "cr_tolerance_pct": 0.35, "mode": "dual_or_single_with_sanity" },
        "overlays": { "vix_gt_25": False, "dxy_up_3d_0p8": False, "usdt_depeg": False },
        "list_policy": {
            "watchlists": ["whitelist","candidate_pool"],
            "coverage": { "whitelist": "100%", "candidate_pool": "rotating" }
        },
        "signals": signals_v1,
        "errors": [],
        "telemetry": {
            "run_id": _make_run_id_v1(generated_at_brt),
            "eq_status": "OK", "cr_status": "OK",
            "fallback_used": { "eq": [], "cr": [] }
        }
    }

    if with_universe:
        payload["universe"] = universe

    return payload


# ----------------------------
# Pointer v1 (aponta para latest)
# ----------------------------

def update_pointer_signals_v1(latest_relpath: str, expires_hours: int = 24, raw_base: str = RAW_BASE_DEFAULT) -> Dict[str, Any]:
    # Normaliza // duplicadas
    latest_relpath = latest_relpath.lstrip("/")
    url = f"{raw_base}/{latest_relpath}"
    # generated_at_brt vem dentro do arquivo
    try:
        meta = _read_json(os.path.join(ROOT, latest_relpath))
        gen = meta.get("generated_at_brt")
    except Exception:
        gen = None

    exp = (dt.datetime.utcnow() + dt.timedelta(hours=expires_hours)).replace(tzinfo=dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    pointer = {
        "version": "1.0",
        "generated_at_brt": gen,
        "signals_url": url,
        "expires_at_utc": exp,
    }
    _write_json(POINTER_SIG_V1_PATH, pointer)
    return pointer


# ----------------------------
# Main CLI
# ----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--with-universe", action="store_true", help="Inclui array 'universe' no JSON final")
    ap.add_argument("--write-latest", action="store_true", help="Grava também public/n_signals_v1_latest.json")
    ap.add_argument("--update-pointer", action="store_true", help="Atualiza public/pointer_signals_v1.json para apontar para o latest")
    args = ap.parse_args()

    payload = build_payload(with_universe=args.with_universe)

    # content_sha256 (do conteúdo final) — escrevemos primeiro num buffer em memória
    b = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=False).encode("utf-8")
    payload["content_sha256"] = _sha256_bytes(b)

    # Agora escrevemos em disco (carimbando com o content_sha256 já inserido)
    tsz = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_name = f"n_signals_v1_{tsz}.json"
    out_path = os.path.join(PUBLIC_DIR, out_name)
    _write_json(out_path, payload)
    print(f"[ok] gerado {out_path}")

    latest_rel = "public/n_signals_v1_latest.json"
    if args.write_latest:
        latest_path = os.path.join(ROOT, latest_rel)
        _write_json(latest_path, payload)
        print(f"[ok] gerado {latest_path}")

    if args.update_pointer and args.write_latest:
        pointer = update_pointer_signals_v1(latest_relpath=latest_rel, expires_hours=24, raw_base=RAW_BASE_DEFAULT)
        print(f"[ok] pointer atualizado: {POINTER_SIG_V1_PATH}")
        # feedback útil
        print(json.dumps(pointer, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

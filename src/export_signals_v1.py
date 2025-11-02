# src/export_signals_v1.py
from __future__ import annotations

import argparse
import json
import os
import sys
import glob
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Dependências utilitárias do projeto
try:
    from src.pointer_utils import prefer_local_from_pointer as _prefer_local_from_pointer, write_pointer
except Exception:
    # Fallback mínimo se pointer_utils não estiver disponível por algum motivo
    def _prefer_local_from_pointer(ptr: Dict[str, Any], key_url: str, key_path: str, pattern: str) -> str:
        # (1) caminho local informado no pointer
        p_local = (ptr or {}).get(key_path)
        if isinstance(p_local, str) and p_local and os.path.exists(p_local):
            return p_local
        # (2) url informada (devolve a string; consumo externo pode tratar)
        p_url = (ptr or {}).get(key_url)
        if isinstance(p_url, str) and p_url:
            return p_url
        # (3) fallback glob local
        candidates = sorted(glob.glob(pattern))
        if candidates:
            return candidates[-1]
        raise FileNotFoundError(f"Pointer sem {key_url}/{key_path} válidos e nenhum match para '{pattern}'.")

    def write_pointer(ohl_path: Optional[str], ind_path: Optional[str], sig_path: Optional[str],
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

def _load_json_maybe(path_or_url: Optional[str]) -> Dict[str, Any]:
    """
    Carrega JSON de arquivo local; se for URL, não baixa (devolve {}), pois
    o export v1 não faz rede. Campos impossíveis ficam em branco (null).
    """
    if not path_or_url:
        return {}
    if isinstance(path_or_url, str) and os.path.exists(path_or_url):
        with open(path_or_url, "r", encoding="utf-8") as f:
            return json.load(f)
    # URL ou arquivo inexistente → devolve vazio e loga.
    print(f"[export_v1][WARN] Fonte não-local ou ausente, ignorando: {path_or_url}")
    return {}

def _venue_and_type(symbol_canonical: str) -> (Optional[str], Optional[str]):
    """
    Deduz venue e tipo com heurística simples:
    - prefixo antes de ":" vira venue (e também ajuda no tipo)
    - BINANCE → crypto ; NASDAQ/NYSE/NYSEARCA → equity/etf conforme sufixo conhecido (não crítico)
    """
    if not isinstance(symbol_canonical, str) or ":" not in symbol_canonical:
        return (None, None)
    venue, _ = symbol_canonical.split(":", 1)
    asset_type = None
    if venue == "BINANCE":
        asset_type = "crypto"
    elif venue in ("NASDAQ", "NYSE"):
        asset_type = "equity"
    elif venue in ("NYSEARCA",):
        # pode ser equity/etf ─ deixamos None se não souber
        asset_type = None
    return (venue, asset_type)

def _num_or_null(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None

def build_payload(*, with_universe: bool = True) -> Dict[str, Any]:
    print("[export_v1] Iniciando…")

    # Pointer atual
    ptr_path = "public/pointer_signals_v1.json"
    pointer: Dict[str, Any] = {}
    if os.path.exists(ptr_path):
        try:
            with open(ptr_path, "r", encoding="utf-8") as f:
                pointer = json.load(f)
        except Exception as e:
            print(f"[export_v1][WARN] Falha lendo pointer ({e}); prosseguindo com glob…")

    # Resolve fontes (preferindo local)
    ohl_path = _prefer_local_from_pointer(pointer, "ohlcv_url", "ohlcv", "public/ohlcv_cache_*.json")
    ind_path = _prefer_local_from_pointer(pointer, "indicators_url", "indicators", "public/indicators_*.json")
    print(f"[export_v1] usando OHLCV: {os.path.basename(ohl_path) if ohl_path else '(vazio)'}")
    print(f"[export_v1] usando INDICATORS: {os.path.basename(ind_path) if ind_path else '(vazio)'}")

    # Carrega dados (somente local)
    ohl = _load_json_maybe(ohl_path)
    ind = _load_json_maybe(ind_path)

    # Monta universo: união de chaves presentes em OHLCV e IND
    symbols = sorted(set(ohl.keys()) | set(ind.keys()))
    print(f"[export_v1] ohlcv símbolos com dados: {len([k for k in ohl.keys()])}")
    print(f"[export_v1] indicadores símbolos: {len([k for k in ind.keys()])}")
    print(f"[export_v1] universe montado: total={len(symbols)}")

    universe: List[Dict[str, Any]] = []
    if with_universe:
        for sc in symbols:
            ind_row = ind.get(sc, {}) or {}
            venue, guessed_type = _venue_and_type(sc)
            # Campos de preço/variações ficam em branco (null) aqui.
            row = {
                "symbol_canonical": sc,
                "asset_type": guessed_type,            # pode ser None
                "venue": venue,                        # pode ser None
                "window_used": "7d",
                "price_now_close": None,
                "price_now_close_at_utc": None,
                "pct_chg_7d": None,
                "pct_chg_10d": None,
                "pct_chg_30d": None,
                # Indicadores (quando existirem)
                "rsi14": _num_or_null(ind_row.get("RSI14")),
                "atr14": _num_or_null(ind_row.get("ATR14")),
                "atr14_pct": _num_or_null(ind_row.get("ATR14_PCT")),
                "bb_ma20": _num_or_null(ind_row.get("BB_MA20")),
                "bb_lower": _num_or_null(ind_row.get("BB_LOWER")),
                "bb_upper": _num_or_null(ind_row.get("BB_UPPER")),
                # Derivativos/cripto (se houver)
                "funding": _num_or_null(ind_row.get("FUNDING")),
                "oi_chg_3d_pct": _num_or_null(ind_row.get("OI_CHG_3D_PCT")),
                # Sinalização
                "priceguard": None,
                "window_status": None,
                "sources_used": None,
            }
            universe.append(row)

    payload = {
        "schema": "n_signals_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "sources": {
            "ohlcv": ohl_path,
            "indicators": ind_path,
        },
        "universe_size": len(universe),
        "universe": universe,
    }
    return payload

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--with-universe", action="store_true", default=True)
    args = ap.parse_args()

    os.makedirs("public", exist_ok=True)

    payload = build_payload(with_universe=args.with_universe)

    ts = _now_tag()
    out_ver = f"public/n_signals_v1_{ts}.json"
    out_latest = "public/n_signals_v1_latest.json"

    with open(out_ver, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    with open(out_latest, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # Atualiza pointer (campo "signals")
    try:
        write_pointer(
            ohl_path=payload["sources"]["ohlcv"],
            ind_path=payload["sources"]["indicators"],
            sig_path=out_ver,
        )
        print(f"[export_v1] OK: v1 gerado: {os.path.abspath(out_ver)}")
        print(f"[export_v1] OK: v1 latest atualizado: {os.path.abspath(out_latest)}")
        print(f"[export_v1] OK: pointer_signals_v1.json atualizado")
        print(f"[export_v1] universe size = {payload.get('universe_size')}")
    except Exception as e:
        print(f"[export_v1][WARN] não foi possível atualizar pointer automaticamente: {e}")

if __name__ == "__main__":
    main()

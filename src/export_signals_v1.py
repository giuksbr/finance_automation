#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List

from src.pointer_utils import prefer_local_from_pointer as _prefer_local_from_pointer, write_pointer


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_json(path_or_url: str) -> Any:
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        # ambiente local costuma rodar offline; tente curl simples se disponível
        try:
            import subprocess, tempfile
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                subprocess.check_call(["curl", "-fsSL", path_or_url, "-o", tmp.name])
                with open(tmp.name, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            raise RuntimeError(f"Falha ao baixar {path_or_url}: {e}")
    else:
        with open(path_or_url, "r", encoding="utf-8") as f:
            return json.load(f)


def _detect_is_v1(obj: Dict[str, Any]) -> bool:
    # Heurística simples: payload V1 possui "signals" (lista de dicts) e pode conter "universe"
    return isinstance(obj, dict) and "signals" in obj


def _coerce_v1(obj: Any) -> Dict[str, Any]:
    """
    Se já for V1, retorna como está.
    Se for legacy (ex.: lista de sinais simples), envolve em {"signals":[...]}.
    """
    if isinstance(obj, dict) and _detect_is_v1(obj):
        return obj
    if isinstance(obj, list):
        return {"signals": obj, "universe": obj}
    if isinstance(obj, dict) and "universe" in obj and "signals" not in obj:
        # alguns dumps tinham apenas universe
        obj["signals"] = obj.get("universe", [])
        return obj
    # última tentativa: empacotar tudo como signals
    return {"signals": [obj], "universe": [obj]}


def build_payload(with_universe: bool = True) -> Dict[str, Any]:
    # resolve pointer
    ptr_path = "public/pointer_signals_v1.json"
    ptr = {}
    if os.path.exists(ptr_path):
        with open(ptr_path, "r", encoding="utf-8") as f:
            ptr = json.load(f)

    ohl_url_or_path = _prefer_local_from_pointer(ptr, "ohlcv_url", "ohlcv", "public/ohlcv_cache_*.json")
    ind_url_or_path = _prefer_local_from_pointer(ptr, "indicators_url", "indicators", "public/indicators_*.json")
    try:
        sig_url_or_path = _prefer_local_from_pointer(ptr, "signals_url", "signals", "public/n_signals_v1_*.json")
    except FileNotFoundError:
        sig_url_or_path = _prefer_local_from_pointer(ptr, "signals_url", "signals", "public/n_signals_*.json")

    # carregar jsons
    ohl = _load_json(ohl_url_or_path)
    ind = _load_json(ind_url_or_path)
    sig_raw = _load_json(sig_url_or_path)

    sig_v1 = _coerce_v1(sig_raw)
    payload: Dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "sources": {
            "ohlcv": ohl_url_or_path,
            "indicators": ind_url_or_path,
            "signals_src": sig_url_or_path,
        },
        "signals": sig_v1.get("signals", []),
    }
    if with_universe:
        payload["universe"] = sig_v1.get("universe", sig_v1.get("signals", []))

    return payload


def main():
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

    # atualizar pointer (campo "signals")
    try:
        write_pointer(
            ohl_path=payload["sources"]["ohlcv"],
            ind_path=payload["sources"]["indicators"],
            sig_path=out_ver,
        )
        print(f"signals v1 gerado: {out_ver}")
        print(f"signals v1 latest atualizado: {out_latest}")
    except Exception as e:
        print(f"[warn] não foi possível atualizar pointer automaticamente: {e}")


if __name__ == "__main__":
    main()

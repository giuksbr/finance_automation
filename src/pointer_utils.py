# src/pointer_utils.py
from __future__ import annotations
import os, glob, json
from typing import Optional, Dict, Any

def latest(pattern: str) -> Optional[str]:
    files = sorted(glob.glob(pattern))
    return files[-1] if files else None

def prefer_local_from_pointer(
    ptr: Optional[Dict[str, Any]],
    key_url: str,
    key_path: str,
    pattern: str,
) -> str:
    p_local = (ptr or {}).get(key_path)
    if isinstance(p_local, str) and p_local and os.path.exists(p_local):
        return p_local

    p_url = (ptr or {}).get(key_url)
    if isinstance(p_url, str) and p_url:
        return p_url

    p_glob = latest(pattern)
    if p_glob:
        return p_glob

    raise FileNotFoundError(
        f"Pointer sem {key_url}/{key_path} válidos e nenhum match para '{pattern}'."
    )

def write_pointer(
    ohl_path: Optional[str],
    ind_path: Optional[str],
    sig_path: Optional[str],
    out_path: str = "public/pointer_signals_v1.json",
) -> None:
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

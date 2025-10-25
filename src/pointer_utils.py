from __future__ import annotations

import glob
import json
import os
from typing import Optional, Dict, Any


def prefer_local_from_pointer(
    ptr: Dict[str, Any],
    key_url: str,
    key_path: str,
    pattern: str,
) -> str:
    """
    Ordem de preferência:
      1) caminho local em 'key_path' se existir
      2) url em 'key_url'
      3) fallback: arquivo local mais novo que combine com 'pattern'
      4) FileNotFoundError se nada existir
    """
    # (1) caminho local informado:
    p_local = (ptr or {}).get(key_path)
    if isinstance(p_local, str) and p_local and os.path.exists(p_local):
        return p_local

    # (2) url informada:
    p_url = (ptr or {}).get(key_url)
    if isinstance(p_url, str) and p_url:
        return p_url

    # (3) fallback glob
    candidates = sorted(glob.glob(pattern))
    if candidates:
        return candidates[-1]

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

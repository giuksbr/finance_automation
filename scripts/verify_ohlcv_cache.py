#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Valida o ohlcv_cache mais recente:
 - Garante que eq/cr existam e sejam dicts
 - Amostra 3 símbolos para checar se têm "c","t" e pontos suficientes
"""

from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Any, List, Tuple

PUBLIC_DIR = Path("public")

def latest(pattern: str) -> Path | None:
    files = sorted(PUBLIC_DIR.glob(pattern), reverse=True)
    return files[0] if files else None

def sample_keys(d: Dict[str, Any], n: int = 3) -> List[str]:
    return list(d.keys())[:n]

def main():
    p = latest("ohlcv_cache_*.json")
    if not p:
        print("[err] nenhum ohlcv_cache_* encontrado")
        return
    data = json.loads(p.read_text())

    for sec in ("eq","cr"):
        bucket = data.get(sec)
        if not isinstance(bucket, dict):
            print(f"[fail] {p.name}: seção {sec} ausente ou inválida")
            continue
        ks = sample_keys(bucket, 3)
        for k in ks:
            v = bucket.get(k, {})
            if not isinstance(v, dict):
                print(f"[warn] {sec}:{k} não é dict")
                continue
            c = v.get("c"); t = v.get("t")
            if not (isinstance(c, list) and isinstance(t, list) and len(c) >= 8 and len(t) == len(c)):
                print(f"[fail] {sec}:{k} séries insuficientes/ausentes (c/t)")
            else:
                print(f"[ok]   {sec}:{k}  pontos={len(c)} último_close={c[-1]}")

if __name__ == "__main__":
    main()

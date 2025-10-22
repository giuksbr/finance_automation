#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Enriquece derivatives (funding, oi_chg_3d_pct) para cripto no n_signals_v1_latest.json.
Esta versão supõe que você já tem funções/fonte para buscar esses números.
Aqui, está mockado com valores fictícios se ausentes — ajuste para sua fonte real.
"""

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict

OUT_DIR = "public"
LATEST = os.path.join(OUT_DIR, "n_signals_v1_latest.json")


def _read_json(p: str) -> Any:
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(p: str, obj: Any) -> None:
    with open(p, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def fetch_derivatives_for(sym: str) -> Dict[str, float]:
    """
    TODO: troque por sua fonte real.
    Retorna dict com keys: funding, oi_chg_3d_pct.
    """
    # Placeholders apenas para exemplo (não use em produção):
    import random
    return {
        "funding": round(random.uniform(-0.00015, 0.00015), 8),
        "oi_chg_3d_pct": round(random.uniform(-15, 15), 6),
    }


def main():
    if not os.path.exists(LATEST):
        print("[warn] latest não encontrado, abortando.")
        return

    data = _read_json(LATEST)
    changed = 0
    for it in data.get("universe", []):
        if it.get("asset_type") == "crypto":
            sym = it.get("symbol_canonical")
            d = it.get("derivatives") or {}
            if "funding" not in d or "oi_chg_3d_pct" not in d:
                vals = fetch_derivatives_for(sym)
                d.update(vals)
                it["derivatives"] = d
                changed += 1
                print(f"[ok] {sym}: funding={vals['funding']} oi3d={vals['oi_chg_3d_pct']}")

    if changed == 0:
        print("[info] nada para atualizar.")
        return

    # grava versão nova + latest
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_ver = os.path.join(OUT_DIR, f"n_signals_v1_{ts}.json")
    _write_json(LATEST, data)
    _write_json(out_ver, data)
    print(f"[save] atualizado: {LATEST} + {out_ver}")


if __name__ == "__main__":
    main()

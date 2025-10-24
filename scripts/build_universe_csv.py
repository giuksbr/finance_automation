#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_universe_csv.py
Gera um CSV simples a partir de public/n_signals_v1_latest.json.

Regra:
- Exporta SEMPRE colunas: symbol, asset_type, price_now_close, rsi14, atr14, atr14_pct, bb_ma20, bb_lower
- Só adiciona pct_chg_7d/10d/30d e price_now_close_at_utc se existir (não nulo) para algum ativo.
- Evita colunas “tudo null”.
"""

import json
import os
import csv
from typing import Any, Dict, List

SRC = os.path.join("public", "n_signals_v1_latest.json")
DST = os.path.join("public", "n_signals_universe_latest.csv")


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main() -> None:
    if not os.path.exists(SRC):
        print(f"[warn] fonte não encontrada: {SRC}")
        return

    obj = _load_json(SRC)
    rows = obj.get("signals") or []
    if not isinstance(rows, list):
        rows = []

    # sempre-exportadas
    base_cols = ["symbol_canonical", "asset_type", "price_now_close", "rsi14", "atr14", "atr14_pct", "bb_ma20", "bb_lower"]

    # candidatas
    maybes = ["price_now_close_at_utc", "pct_chg_7d", "pct_chg_10d", "pct_chg_30d"]

    # decide quais maybes existem em pelo menos um ativo
    extra_cols: List[str] = []
    for c in maybes:
        if any((isinstance(r, dict) and r.get(c) is not None) for r in rows):
            extra_cols.append(c)

    cols = base_cols + extra_cols

    os.makedirs(os.path.dirname(DST), exist_ok=True)
    with open(DST, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            if not isinstance(r, dict):
                continue
            line = [r.get(c) for c in cols]
            w.writerow(line)

    print(f"[ok] CSV gerado: {os.path.abspath(DST)} (linhas={len(rows)}, header=1, total={len(rows)+1})")


if __name__ == "__main__":
    main()

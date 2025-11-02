# -*- coding: utf-8 -*-
# scripts/stage3_compute_indicators.py
import json, sys
from pathlib import Path
from src.fetch_eq import fetch_stooq
from src.fetch_cr import fetch_binance
from src.indicator_calc import compute_indicators_dict

def _pick_one(m):
    for k,v in m.items():
        if v: return {k:v}
    return {}

def main():
    # usa os mesmos samples do stage2
    p = Path("out/watchlists_local.json")
    if not p.exists():
        print("ERR: out/watchlists_local.json não existe. Rode stage2.", file=sys.stderr)
        sys.exit(2)
    wl = json.loads(p.read_text(encoding="utf-8"))
    eq_sm = [x["symbol_canonical"] for x in wl.get("eq",[])][:4]
    cr_sm = [x["symbol_canonical"] for x in wl.get("cr",[])][:3]

    eq_map = fetch_stooq(symbols=eq_sm, days=60)
    cr_map = fetch_binance(symbols=cr_sm, days=60)

    eq_one = _pick_one(eq_map)
    cr_one = _pick_one(cr_map)
    if not eq_one and not cr_one:
        print("ERR: sem candles p/ indicadores.", file=sys.stderr); sys.exit(3)

    ind_eq = compute_indicators_dict(eq_one) if eq_one else {}
    ind_cr = compute_indicators_dict(cr_one) if cr_one else {}
    print(json.dumps({"eq_has": bool(ind_eq), "cr_has": bool(ind_cr), "eq_keys": list(ind_eq.keys()), "cr_keys": list(ind_cr.keys())}, indent=2))
    print("[indicators] OK")

if __name__=="__main__":
    main()

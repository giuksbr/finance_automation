# -*- coding: utf-8 -*-
# scripts/stage2_fetch_smoke.py
import json, sys
from pathlib import Path
from src.fetch_eq import fetch_stooq
from src.fetch_cr import fetch_binance

def _load_syms():
    p = Path("out/watchlists_local.json")
    if not p.exists():
        print("ERR: out/watchlists_local.json não existe. Rode scripts/build_watchlists_from_feed.py", file=sys.stderr)
        sys.exit(2)
    data = json.loads(p.read_text(encoding="utf-8"))
    eq = [x["symbol_canonical"] for x in data.get("eq",[])]
    cr = [x["symbol_canonical"] for x in data.get("cr",[])]
    return eq[:4], cr[:3]

def _check_map(name, m):
    if not isinstance(m, dict):
        print(f"ERR {name}: retorno não é dict", file=sys.stderr); sys.exit(3)
    keys = list(m.keys())
    total = len(keys)
    nonempty = sum(1 for k,v in m.items() if isinstance(v, list) and len(v)>0)
    # valida o primeiro candle se houver
    sample_ok = True
    for v in m.values():
        if v:
            c=v[-1]
            sample_ok = all(k in c for k in ("t","o","h","l","c"))
            break
    print(f"[smoke] {name}: total={total} com_dados={nonempty} shape_ok={sample_ok}")
    if total==0 or nonempty==0:
        print(f"ERR {name}: nada coletado (verifique rede/endpoints).", file=sys.stderr)
        sys.exit(4)

def main():
    eq_sm, cr_sm = _load_syms()
    print(f"[smoke] sample eq={eq_sm} cr={cr_sm}")

    eq_map = fetch_stooq(symbols=eq_sm, days=60)
    _check_map("stooq", eq_map)

    cr_map = fetch_binance(symbols=cr_sm, days=60)
    _check_map("binance", cr_map)

    print("[smoke] OK")

if __name__=="__main__":
    main()

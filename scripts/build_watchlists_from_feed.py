#!/usr/bin/env python3
import json, sys, os, re
from typing import List, Dict

FEED = "out/last_good_feed.json"
OUT_EQ_TXT = "out/syms_eq.txt"
OUT_CR_TXT = "out/syms_cr.txt"
OUT_WL_JSON = "out/watchlists_local.json"

def _canon_to_ticker(sc: str) -> str:
    # Mantém o canonical (padrão REPO) — serve para chaves em artifacts
    return sc.strip() if sc else ""

def uniq(xs: List[str]) -> List[str]:
    seen, out = set(), []
    for x in xs:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def main():
    if not os.path.exists(FEED):
        print(f"ERRO: {FEED} ausente.", file=sys.stderr)
        sys.exit(1)

    with open(FEED, "r", encoding="utf-8") as f:
        data = json.load(f)

    if data.get("schema_version") != "2.0":
        print("ERRO: schema_version != 2.0", file=sys.stderr)
        sys.exit(1)

    uni = data.get("universe") or {}
    wl = (uni.get("watchlists") or {})
    ave = (wl.get("avenue") or {})
    binx = (wl.get("binance") or {})

    eq_syms = []
    for k in ("whitelist", "candidate_pool"):
        for it in (ave.get(k) or []):
            sc = (it or {}).get("symbol_canonical")
            if sc: eq_syms.append(_canon_to_ticker(sc))
    eq_syms = uniq([s for s in eq_syms if s])

    cr_syms = []
    for k in ("whitelist", "candidate_pool"):
        for it in (binx.get(k) or []):
            sc = (it or {}).get("symbol_canonical")
            if sc: cr_syms.append(_canon_to_ticker(sc))
    cr_syms = uniq([s for s in cr_syms if s])

    os.makedirs("out", exist_ok=True)
    with open(OUT_EQ_TXT, "w", encoding="utf-8") as f:
        for s in eq_syms: f.write(s + "\n")
    with open(OUT_CR_TXT, "w", encoding="utf-8") as f:
        for s in cr_syms: f.write(s + "\n")

    wl_json = {
        "eq": [{"symbol_canonical": s} for s in eq_syms],
        "cr": [{"symbol_canonical": s} for s in cr_syms],
    }
    with open(OUT_WL_JSON, "w", encoding="utf-8") as f:
        json.dump(wl_json, f, ensure_ascii=False, indent=2)

    print(f"ok: eq={len(eq_syms)} cr={len(cr_syms)}")
    print(f"out: {OUT_EQ_TXT}, {OUT_CR_TXT}, {OUT_WL_JSON}")

if __name__ == "__main__":
    main()

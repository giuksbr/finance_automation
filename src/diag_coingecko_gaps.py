from __future__ import annotations
import requests, yaml, json, re
import pandas as pd

def load_cfg():
    with open("config.yaml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def fetch_json(url:str):
    r=requests.get(url,timeout=20); r.raise_for_status(); return r.json()

def main():
    cfg = load_cfg()
    feed = fetch_json(cfg["feed_url"])

    # suporta feed.watchlists ou universe.watchlists
    wlroot = feed.get("watchlists") or feed.get("universe", {}).get("watchlists") or {}
    cr = []
    for key in ("whitelist","candidate_pool"):
        for it in wlroot.get("binance",{}).get(key,[]):
            sym = it.get("symbol_canonical","")
            if sym.startswith("BINANCE:"):
                cr.append(sym.split(":")[1])
    cr = sorted(set(cr))

    with open("coingecko_map.json","r",encoding="utf-8") as f:
        cg = json.load(f)

    missing = [p for p in cr if p not in cg]
    if not missing:
        print("[ok] coingecko_map.json tem todos os pares do feed (binance).")
        return

    print("== Pares BINANCE do feed sem id no coingecko_map.json ==")
    for p in missing:
        base = re.sub(r"USDT$","",p)  # tentativa simples
        print(f"- {p}  →  sugestão de rascunho: \"{p}\": \"{base.lower()}\"")
    print()
    print("Ajuste manualmente o coingecko_map.json (ids exatos do CoinGecko).")
    print("Depois rode:  python -m src.job  e verifique se as fontes viram binance|coingecko.")

if __name__ == "__main__":
    main()

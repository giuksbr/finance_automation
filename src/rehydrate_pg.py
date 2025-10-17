from __future__ import annotations
import json, yaml, requests, pandas as pd
from datetime import datetime, timezone
from src.priceguard import accept_close_cr, accept_close_eq
from src.fetch_cr import fetch_binance, fetch_coingecko
from src.fetch_eq import fetch_stooq, fetch_yahoo

class T:
    def __init__(self, cfg): 
        pg=cfg["priceguard"]
        self.eq_delta_max=float(pg["eq_delta_max"])
        self.cr_delta_max=float(pg["cr_delta_max"])
        self.eq_abs_chg7d_max=float(pg["eq_abs_chg7d_max"])
        self.cr_abs_chg7d_max=float(pg["cr_abs_chg7d_max"])

def load_cfg():
    with open("config.yaml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def jget(url):
    r=requests.get(url,timeout=20); r.raise_for_status(); return r.json()

def main():
    cfg = load_cfg()
    p = jget(cfg["storage"]["raw_base_url"].rstrip("/") + "/public/pointer.json")
    sig = jget(p["signals_url"])
    if not sig:
        print("Nenhum sinal."); return

    with open("coingecko_map.json","r",encoding="utf-8") as f:
        cg_map=json.load(f)

    th = T(cfg)
    days = int(cfg.get("window_fallback_days", 30))

    rows=[]
    for r in sig:
        sym = r["symbol_canonical"]
        src_saved = r.get("sources")
        if sym.startswith("BINANCE:"):
            bn = fetch_binance(sym.split(":")[1], days)
            cg = fetch_coingecko(sym, cg_map, days)
            acc, tag = accept_close_cr(bn, cg, th)
        else:
            stq = fetch_stooq(sym, days)
            yh  = fetch_yahoo(sym, days)
            acc, tag = accept_close_eq(stq, yh, th)
        rows.append({"symbol": sym, "sources_saved": src_saved, "sources_now": tag or "<none>"})
    df = pd.DataFrame(rows)
    print(df.to_string(index=False))

if __name__=="__main__":
    main()

from __future__ import annotations
import requests, yaml, pandas as pd

def load_cfg():
    with open("config.yaml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def jget(url):
    r=requests.get(url,timeout=20); r.raise_for_status(); return r.json()

def main():
    cfg = load_cfg()
    p = jget(cfg["storage"]["raw_base_url"].rstrip("/") + "/public/pointer.json")
    ind = jget(p["indicators_url"])
    eq = ind.get("eq",{}); cr = ind.get("cr",{})

    def scan(di, asset_type):
        rows=[]
        for sym, vals in di.items():
            rows.append({
                "symbol": sym, "asset": asset_type,
                "RSI14": vals.get("RSI14"),
                "ATR14": vals.get("ATR14"),
                "BB_MA20": vals.get("BB_MA20"),
                "BB_LOWER": vals.get("BB_LOWER"),
            })
        return rows

    rows = scan(eq,"eq") + scan(cr,"cr")
    df = pd.DataFrame(rows)
    if df.empty:
        print("Sem indicadores."); return

    df["missing"] = df[["RSI14","ATR14","BB_MA20","BB_LOWER"]].isna().sum(axis=1)
    print("Top com indicadores faltantes:")
    print(df.sort_values(["missing","symbol"], ascending=[False,True]).head(20).to_string(index=False))

if __name__=="__main__":
    main()

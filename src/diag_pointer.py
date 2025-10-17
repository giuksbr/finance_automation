from __future__ import annotations
import requests, yaml, json

def load_cfg():
    with open("config.yaml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def jget(url):
    r=requests.get(url,timeout=20); r.raise_for_status(); return r.json()

def main():
    cfg = load_cfg()
    pointer_url = cfg["storage"]["raw_base_url"].rstrip("/") + "/public/pointer.json"
    p = jget(pointer_url)
    print("POINTER:", pointer_url)
    print(json.dumps(p, indent=2, ensure_ascii=False))
    ohl, ind, sig = p["ohlcv_url"], p["indicators_url"], p["signals_url"]

    o = jget(ohl)
    i = jget(ind)
    s = jget(sig)

    def cnt(di):
        return len(di.get("eq",{})), len(di.get("cr",{}))
    eq_o, cr_o = cnt(o)
    eq_i, cr_i = cnt(i)
    print("\nResumo OHLCV:", o.get("generated_at_brt"), "| eq:", eq_o, "cr:", cr_o, "| errors:", len(o.get("errors",[])))
    print("Resumo INDIC :", i.get("generated_at_brt"), "| eq:", eq_i, "cr:", cr_i, "| errors:", len(i.get("errors",[])))
    print("Resumo SIGNS :", "n_signals:", len(s))

    # primeiro(s) sinais
    for k in range(min(5, len(s))):
        row = s[k]
        print(f"\nSinal[{k}]:", row.get("symbol_canonical"), "| sources:", row.get("sources"), "| levels:", row.get("levels"), "| window_used:", row.get("window_used"))

if __name__=="__main__":
    main()

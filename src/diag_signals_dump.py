from __future__ import annotations
import requests, yaml, pandas as pd

def load_cfg():
    with open("config.yaml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def jget(url):
    r=requests.get(url,timeout=20); r.raise_for_status(); return r.json()

def to_src_list(s):
    if isinstance(s, list): return [str(x) for x in s if x]
    if isinstance(s, str):  return [p for p in s.split("|") if p] or ([s] if s else [])
    return []

def feat_ok(f):
    return bool(f) and all(f.get(k) is not None for k in ("rsi14","atr14","bb_ma20","bb_lower"))

def main():
    cfg = load_cfg()
    p = jget(cfg["storage"]["raw_base_url"].rstrip("/") + "/public/pointer.json")
    sig = jget(p["signals_url"])

    if not sig:
        print("Nenhum sinal no momento."); return

    rows=[]
    for r in sig:
        sym = r.get("symbol_canonical")
        srcs = to_src_list(r.get("sources"))
        f = r.get("features") or {}
        reasons=[]
        if len(srcs) < 2: reasons.append("single_source")
        if not feat_ok(f): reasons.append("ind_incompletos")
        rows.append({
            "symbol": sym,
            "level": ",".join(r.get("levels",[])),
            "sources": "|".join(srcs) if srcs else "",
            "rsi14": f.get("rsi14"), "atr14": f.get("atr14"),
            "bb_ma20": f.get("bb_ma20"), "bb_lower": f.get("bb_lower"),
            "close": f.get("close"),
            "discard_reasons": ",".join(reasons) if reasons else ""
        })
    df = pd.DataFrame(rows)
    print(df.to_string(index=False))

    print("\nResumo por motivo de descarte:")
    if "discard_reasons" in df.columns:
        print(df["discard_reasons"].value_counts(dropna=False).to_string())

if __name__=="__main__":
    main()

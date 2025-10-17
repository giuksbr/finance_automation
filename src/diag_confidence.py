from __future__ import annotations
import requests, yaml, json, pandas as pd
from typing import Any

def load_cfg():
    with open("config.yaml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def fetch_json(url:str):
    r=requests.get(url,timeout=20); r.raise_for_status(); return r.json()

def _to_list_sources(s: Any) -> list[str]:
    if isinstance(s, list):
        return [str(x) for x in s if x is not None]
    if isinstance(s, str):
        parts = [p for p in s.split("|") if p]
        return parts if parts else ([s] if s else [])
    return []

def main():
    cfg = load_cfg()
    pointer = cfg["storage"]["raw_base_url"].rstrip("/") + "/public/pointer.json"
    p = fetch_json(pointer)
    sig = fetch_json(p["signals_url"])

    df = pd.DataFrame(sig)
    if df.empty:
        print("[info] Nenhum sinal no momento.")
        return

    df["src_list"] = df["sources"].apply(_to_list_sources)
    dual = df["src_list"].apply(lambda x: len(x) >= 2)

    print("== Cobertura de fontes nos sinais ==")
    print(f"Total sinais     : {len(df)}")
    print(f"Dupla-fonte      : {dual.sum()}")
    print(f"Single-fonte     : {(~dual).sum()}")
    print()

    sgl = df[~dual].copy()
    if sgl.empty:
        print("[ok] todos os sinais são dupla-fonte.")
        return

    rows=[]
    for _, r in sgl.iterrows():
        sym = r["symbol_canonical"]
        srcs = r["src_list"]
        src = "cr" if sym.startswith("BINANCE:") else "eq"
        if src == "cr":
            reason = "divergência binance vs coingecko (> cr_delta_max), data desalinhada, ou CG sem dado no horário"
        else:
            reason = "divergência stooq vs yahoo (> eq_delta_max), data desalinhada, ou Yahoo sem dado no horário"
        rows.append({"symbol": sym, "asset_type": src, "sources": "|".join(srcs) if srcs else "", "provavel_causa": reason})

    out = pd.DataFrame(rows).sort_values(["asset_type","symbol"])
    print("== Sinais single-fonte (priorize corrigir estes) ==")
    print(out.to_string(index=False))
    print()
    print("Dicas:")
    print("- CR: ids corretos no coingecko_map.json e observar timing do último candle.")
    print("- EQ: conferir ticker no Yahoo e timing do último candle.")
    print("- Se alinharem e Δ ≤ limite, a pipeline agora marca corretamente como dupla-fonte.")

if __name__ == "__main__":
    main()

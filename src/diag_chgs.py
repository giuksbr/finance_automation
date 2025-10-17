from __future__ import annotations
import requests, yaml, pandas as pd

def load_cfg():
    with open("config.yaml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def jget(u):
    r=requests.get(u,timeout=20); r.raise_for_status(); return r.json()

def gate(chg7):
    """
    Retorna a 'maior' janela atingida apenas pelo critério de queda (sem RSI/BB/ATR):
    N1: <= -22, N2: <= -12, N3: <= -8
    """
    if chg7 is None: return "-"
    if chg7 <= -22: return "N1"
    if chg7 <= -12: return "N2"
    if chg7 <= -8:  return "N3"
    return "-"

def as_list(x):
    if isinstance(x, list): return x
    if isinstance(x, str) and x: return x.split("|")
    return []

def main():
    cfg = load_cfg()
    pointer = jget(cfg["storage"]["raw_base_url"].rstrip("/") + "/public/pointer.json")
    sig_url = pointer["signals_url"]
    ind_url = pointer["indicators_url"]

    sig = jget(sig_url)
    ind = jget(ind_url)
    eq_ind = ind.get("eq",{}); cr_ind = ind.get("cr",{})

    rows=[]
    # Como o n_signals.json já vem filtrado, vamos também olhar os INDICATORS + chg presentes nos próprios sinais (quando houver)
    for r in sig:
        sym = r["symbol_canonical"]
        f = r.get("features",{})
        chg7 = f.get("chg_7d_pct")
        chg10 = f.get("chg_10d_pct")
        srcs = as_list(r.get("sources"))
        rows.append({
            "symbol": sym,
            "chg7": chg7, "chg10": chg10,
            "pass_chg": gate(chg7),
            "sources": "|".join(srcs) if srcs else ""
        })

    df = pd.DataFrame(rows)
    if df.empty:
        print("[info] Pointer atual não tem sinais; para saber 'quão perto' está, gere um CSV simples lendo ohlcv_cache no job (ou mantenha este script só como placeholder).")
        return

    print(df.sort_values(["pass_chg","chg7"], ascending=[True, True]).to_string(index=False))

if __name__=="__main__":
    main()

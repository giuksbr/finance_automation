from __future__ import annotations
import yaml, requests
import pandas as pd

from src.feed import fetch_feed, extract_watchlists
from src.fetch_eq import fetch_stooq, fetch_yahoo
from src.priceguard import accept_close_eq

def _load_cfg() -> dict:
    with open("config.yaml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def _pct(a: float|None, b: float|None) -> float|None:
    if a is None or b is None or b == 0: return None
    return (a/b - 1.0)*100.0

def _chg7(df: pd.DataFrame|None) -> float|None:
    if df is None or df.empty or "close" not in df.columns or len(df)<8:
        return None
    c = df["close"].tolist()
    return _pct(c[-1], c[-8])

def _why_reject(stq: pd.DataFrame|None, yh: pd.DataFrame|None, eq_delta_max: float) -> str:
    if (stq is None or stq.empty) and (yh is None or yh.empty):
        return "sem_dados_stooq_e_yahoo"
    # se só 1 fonte tem dados, a aceitação depende da sanidade (vale olhar >7 barras):
    if stq is not None and not stq.empty and (yh is None or yh.empty):
        return "yahoo_vazio"
    if yh is not None and not yh.empty and (stq is None or stq.empty):
        return "stooq_vazio"
    # duas fontes -> provavelmente delta > limite ou datas desalinhadas
    return f"divergencia_>={eq_delta_max*100:.2f}%_ou_datas_desalinhadas"

def main():
    cfg = _load_cfg()
    feed = fetch_feed(cfg["feed_url"])
    wl = extract_watchlists(feed)
    eq_syms = wl.get("eq", []) or []

    if not eq_syms:
        print("[erro] feed sem EQ — ver src/diag_feed.py primeiro.")
        return

    # thresholds
    eq_delta_max = float(cfg["priceguard"]["eq_delta_max"])

    rows = []
    for sym in eq_syms:
        stq = fetch_stooq(sym, int(cfg.get("window_fallback_days", 30)))
        yh  = fetch_yahoo(sym, int(cfg.get("window_fallback_days", 30)))
        accepted, tag = accept_close_eq(stq, yh, cfg["priceguard"])
        acc_len = 0 if (accepted is None or accepted.empty) else len(accepted)

        stq_len = 0 if (stq is None or stq.empty) else len(stq)
        yh_len  = 0 if (yh  is None or yh.empty)  else len(yh)
        chg7_stq = _chg7(stq)
        chg7_yh  = _chg7(yh)

        reason = ""
        if acc_len == 0:
            reason = _why_reject(stq, yh, eq_delta_max)

        rows.append({
            "symbol": sym,
            "stooq_len": stq_len,
            "yahoo_len": yh_len,
            "chg7_stq": None if chg7_stq is None else round(chg7_stq,2),
            "chg7_yh":  None if chg7_yh  is None else round(chg7_yh,2),
            "accepted_len": acc_len,
            "tag": tag or "",
            "reject_reason": reason
        })

    df = pd.DataFrame(rows)
    if df.empty:
        print("[info] nada coletado.")
        return

    # visão geral
    print("== Resumo por motivo de rejeição ==")
    print(df.assign(rej=df["reject_reason"].replace("", "aceito")).groupby("rej").size().sort_values(ascending=False))

    print("\n== Top problemáticos (primeiros 15) ==")
    cols = ["symbol","stooq_len","yahoo_len","chg7_stq","chg7_yh","accepted_len","tag","reject_reason"]
    print(df.sort_values(["accepted_len","yahoo_len","stooq_len"], ascending=[True, True, True])[cols].head(15).to_string(index=False))

    # dicas rápidas
    print("\nDicas:")
    print("- Se muitos estiverem com 'yahoo_vazio': ver mapeamento do ticker no Yahoo e checar bloqueio/timing do último candle.")
    print("- Se 'divergencia_*': compare os fechamentos de ontem/hoje; se Δ > 0,8% pode ser diferença de ajuste (ETF) ou delay.")
    print("- Se 'stooq_vazio': verificar mapeamento stooq (ex.: BRK.B -> brk-b.us).")

if __name__ == "__main__":
    main()

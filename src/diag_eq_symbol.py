from __future__ import annotations
import io, yaml, pandas as pd, requests
from datetime import datetime, timezone

def load_cfg():
    with open("config.yaml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def stooq_ticker(sym_can: str) -> str:
    # NYSE:BRK.B → brk-b.us ; NASDAQ:NVDA → nvda.us ; NYSEARCA:VUG → vug.us
    exc, tick = sym_can.split(":")
    t = tick.lower().replace(".","-") + ".us"
    return t

def fetch_stooq(sym_can: str, days: int):
    t = stooq_ticker(sym_can)
    url=f"https://stooq.com/q/d/l/?s={t}&i=d"
    r = requests.get(url, timeout=15)
    if r.status_code!=200 or "No data" in r.text:
        return None
    df = pd.read_csv(io.StringIO(r.text))
    if df.empty: return None
    df["Date"] = pd.to_datetime(df["Date"]).dt.date.astype(str)
    df = df.rename(columns={"Open":"open","High":"high","Low":"low","Close":"close"})
    return df.tail(days)

def fetch_yahoo(sym_can: str, days: int):
    # usando query1.finance.yahoo.com (histórico CSV) para evitar dependência
    exc, tick = sym_can.split(":")
    qs = tick.replace("/","-")
    # últimos ~60 dias em segundos (padrão)
    import time
    period2 = int(time.time())
    period1 = period2 - 90*86400
    url = f"https://query1.finance.yahoo.com/v7/finance/download/{qs}?period1={period1}&period2={period2}&interval=1d&events=history&includeAdjustedClose=true"
    r = requests.get(url, timeout=20)
    if r.status_code!=200 or "Not Found" in r.text:
        return None
    df = pd.read_csv(io.StringIO(r.text))
    if df.empty: return None
    df = df.rename(columns={"Date":"Date","Open":"open","High":"high","Low":"low","Close":"close"})
    df["Date"] = pd.to_datetime(df["Date"]).dt.date.astype(str)
    return df.tail(days)

def last(df):
    if df is None or df.empty: return ("<vazio>", float("nan"))
    d = df.iloc[-1]
    return (str(d["Date"]), float(d["close"]))

def main():
    import sys, math
    if len(sys.argv) < 2 or ":" not in sys.argv[1]:
        print("Uso: python -m src.diag_eq_symbol EXCHANGE:TICKER  (ex.: NYSE:NVO)")
        return
    sym = sys.argv[1]
    cfg = load_cfg()
    days = int(cfg.get("window_fallback_days", 30))
    limit = float(cfg["priceguard"]["eq_delta_max"])  # 0.008 = 0,8%

    stq = fetch_stooq(sym, days)
    yh  = fetch_yahoo(sym,  days)

    sd, sc = last(stq)
    yd, yc = last(yh)
    print(f"=== {sym} (janela {days}d) ===")
    print(f"Stooq : date={sd} close={sc}")
    print(f"Yahoo : date={yd} close={yc}")

    aligned = (sd == yd)
    print(f"Datas alinhadas? {aligned}")
    if not math.isnan(sc) and not math.isnan(yc):
        delta = abs(yc - sc) / sc if sc!=0 else float("inf")
        print(f"Δ |close_YH - close_STQ| / STQ = {delta:.6f} (limite eq_delta_max={limit:.6f})")
        if not aligned:
            print("Motivo provável do FAIL: data desalinhada.")
        elif delta > limit:
            print("Motivo provável do FAIL: divergência acima do limite (PriceGuard).")
        else:
            print("As fontes parecem convergir — se ainda saiu stooq_only, foi falta de dado pontual no horário da coleta.")
    else:
        print("Não foi possível calcular delta (fonte vazia).")

if __name__ == "__main__":
    main()

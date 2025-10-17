from __future__ import annotations
import io, requests, pandas as pd
from datetime import datetime, timezone

# ---------- helpers de mapeamento ----------
def stooq_ticker_from_symbol_canonical(sym_can: str) -> str:
    # EX: NASDAQ:NVDA -> nvda.us | NYSEARCA:VUG -> vug.us | NYSE:BRK.B -> brk-b.us
    exch, tick = sym_can.split(":")
    return tick.lower().replace(".", "-") + ".us"

def yahoo_symbol_from_symbol_canonical(sym_can: str) -> str:
    # Para Yahoo, geralmente o ticker "cru" funciona; BRK.B vira BRK-B
    exch, tick = sym_can.split(":")
    return tick.replace(".", "-").replace("/", "-")

# ---------- FETCHERS ----------
def fetch_stooq(sym_can: str, days: int) -> pd.DataFrame | None:
    t = stooq_ticker_from_symbol_canonical(sym_can)
    url = f"https://stooq.com/q/d/l/?s={t}&i=d"
    r = requests.get(url, timeout=15)
    if r.status_code != 200 or "No data" in r.text:
        return None
    df = pd.read_csv(io.StringIO(r.text))
    if df.empty:
        return None
    df = df.rename(columns={"Date": "Date", "Open": "open", "High": "high", "Low": "low", "Close": "close"})
    df["Date"] = pd.to_datetime(df["Date"]).dt.date.astype(str)
    return df.tail(days).reset_index(drop=True)

def fetch_yahoo(sym_can: str, days: int) -> pd.DataFrame | None:
    """
    Usa a API JSON v8 (chart) do Yahoo, que é mais resiliente do que o CSV.
    Ex: https://query1.finance.yahoo.com/v8/finance/chart/NVO?range=90d&interval=1d
    """
    sym = yahoo_symbol_from_symbol_canonical(sym_can)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?range=90d&interval=1d&includePrePost=false"
    r = requests.get(url, timeout=20)
    if r.status_code != 200:
        return None
    j = r.json()
    res = j.get("chart", {}).get("result")
    if not res:
        return None
    result = res[0]
    ts = result.get("timestamp") or []
    ind = result.get("indicators", {}).get("quote", [])
    if not ts or not ind:
        return None
    q = ind[0]
    close = q.get("close") or []
    open_ = q.get("open") or []
    high = q.get("high") or []
    low  = q.get("low") or []

    rows = []
    for i, t in enumerate(ts):
        # timestamp UTC -> date ISO (YYYY-MM-DD)
        d = datetime.fromtimestamp(int(t), tz=timezone.utc).date().isoformat()
        c = close[i] if i < len(close) else None
        o = open_[i] if i < len(open_) else None
        h = high[i]  if i < len(high)  else None
        l = low[i]   if i < len(low)   else None
        if c is None:
            continue
        rows.append({"Date": d, "open": o, "high": h, "low": l, "close": c})
    if not rows:
        return None
    df = pd.DataFrame(rows).dropna(subset=["close"])
    return df.tail(days).reset_index(drop=True)

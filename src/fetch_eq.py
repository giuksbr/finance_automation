import io, requests, pandas as pd

from .mapping import stooq_ticker_from_symbol_canonical, yahoo_symbol_from_symbol_canonical

def fetch_stooq(sym_can: str, limit_days: int) -> pd.DataFrame | None:
    t = stooq_ticker_from_symbol_canonical(sym_can)
    url = f"https://stooq.com/q/d/l/?s={t}&i=d"
    r = requests.get(url, timeout=10)
    if r.status_code != 200 or not r.text or "No data" in r.text:
        return None
    df = pd.read_csv(io.StringIO(r.text))
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").tail(limit_days)
    df = df.rename(columns={"Open":"open","High":"high","Low":"low","Close":"close","Volume":"volume"})
    return df[["Date","open","high","low","close","volume"]].reset_index(drop=True)

def fetch_yahoo(sym_can: str, limit_days: int) -> pd.DataFrame | None:
    import time
    symbol = yahoo_symbol_from_symbol_canonical(sym_can)
    end = int(time.time())
    start = end - 60*60*24*40
    url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={start}&period2={end}&interval=1d&events=history&includeAdjustedClose=true"
    r = requests.get(url, timeout=10)
    if r.status_code != 200 or "Date,Open,High,Low,Close,Adj Close,Volume" not in r.text:
        return None
    df = pd.read_csv(io.StringIO(r.text))
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").tail(limit_days)
    df = df.rename(columns={"Open":"open","High":"high","Low":"low","Close":"close","Volume":"volume"})
    return df[["Date","open","high","low","close","volume"]].reset_index(drop=True)

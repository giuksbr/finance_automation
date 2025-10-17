import requests, pandas as pd

from .mapping import binance_pair_from_symbol_canonical, coingecko_id_from_pair

def fetch_binance(sym_can: str, limit_days: int) -> pd.DataFrame | None:
    pair = binance_pair_from_symbol_canonical(sym_can)
    url = f"https://api.binance.com/api/v3/klines?symbol={pair}&interval=1d&limit={limit_days}"
    r = requests.get(url, timeout=10)
    if r.status_code != 200:
        return None
    arr = r.json()
    if not arr:
        return None
    rows = []
    for k in arr:
        rows.append({
            "Date": pd.to_datetime(k[6], unit="ms"),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
        })
    df = pd.DataFrame(rows).sort_values("Date").reset_index(drop=True)
    return df

def fetch_coingecko(sym_can: str, cg_map: dict, limit_days: int) -> pd.DataFrame | None:
    pair = binance_pair_from_symbol_canonical(sym_can)
    coin_id = coingecko_id_from_pair(pair, cg_map)
    if not coin_id:
        return None
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?vs_currency=usd&days={limit_days}&interval=daily"
    r = requests.get(url, timeout=10)
    if r.status_code != 200:
        return None
    js = r.json()
    prices = js.get("prices", [])
    if not prices:
        return None
    rows = []
    for ts, price in prices:
        rows.append({"Date": pd.to_datetime(ts, unit="ms"), "close": float(price)})
    df = pd.DataFrame(rows).sort_values("Date").reset_index(drop=True)
    df["open"] = df["close"]; df["high"] = df["close"]; df["low"] = df["close"]; df["volume"] = 0.0
    return df[["Date","open","high","low","close","volume"]]

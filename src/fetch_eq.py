from __future__ import annotations
import io
from typing import Optional, Tuple

import pandas as pd
import requests


# ==========================================================
# Helpers de mapeamento
# ==========================================================

def _split_symbol(sym: str) -> Tuple[str, str]:
    """
    "NASDAQ:NVDA" -> ("NASDAQ", "NVDA")
    """
    parts = sym.strip().upper().split(":")
    if len(parts) != 2:
        return "", sym.strip().upper()
    return parts[0], parts[1]


def _to_stooq_ticker(sym: str) -> Optional[str]:
    """
    Regras Stooq (US):
      - ticker minúsculo
      - "." vira "-" (ex.: BRK.B -> brk-b)
      - sufixo ".us"
    """
    _, tick = _split_symbol(sym)
    if not tick:
        return None
    t = tick.lower().replace(".", "-")
    return f"{t}.us"


def _to_yahoo_symbol(sym: str) -> Optional[str]:
    """
    Yahoo:
      - Ticker sem venue
      - "." vira "-" para classes (BRK.B -> BRK-B)
      - ETFs/ARCA usam o mesmo ticker (VUG)
    """
    _, tick = _split_symbol(sym)
    if not tick:
        return None
    return tick.replace(".", "-")


# ==========================================================
# Stooq
# ==========================================================

_STQ_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Connection": "keep-alive",
}

def _stooq_fetch_csv(url: str) -> pd.DataFrame:
    try:
        r = requests.get(url, timeout=15, headers=_STQ_HEADERS)
        if r.status_code != 200 or ("No data" in r.text):
            return pd.DataFrame(columns=["date", "close"])
        df = pd.read_csv(io.StringIO(r.text))
        if "Date" not in df.columns or "Close" not in df.columns:
            return pd.DataFrame(columns=["date", "close"])
        df = df[["Date", "Close"]].rename(columns={"Date": "date", "Close": "close"})
        df = df.dropna().copy()
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["close"])
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df = df.dropna(subset=["date"])
        return df.reset_index(drop=True)
    except Exception:
        return pd.DataFrame(columns=["date", "close"])


def fetch_stooq(sym: str, days: int = 30) -> pd.DataFrame:
    """
    Stooq CSV diário:
      https://stooq.com/q/d/l/?s=<ticker>.us&i=d
    Fallback: http://stooq.com (sem TLS) — útil em ambientes com LibreSSL/TLS antigos.
    """
    ticker = _to_stooq_ticker(sym)
    if not ticker:
        return pd.DataFrame(columns=["date", "close"])

    url_https = f"https://stooq.com/q/d/l/?s={ticker}&i=d"
    df = _stooq_fetch_csv(url_https)
    if not df.empty:
        return df

    # fallback http
    url_http = f"http://stooq.com/q/d/l/?s={ticker}&i=d"
    df2 = _stooq_fetch_csv(url_http)
    return df2


# ==========================================================
# Yahoo v8 (chart API)
# ==========================================================

_YH_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

def _yahoo_fetch_chart(base: str, ysym: str) -> pd.DataFrame:
    # 3 meses para cobrir 30 dias úteis com folga
    url = f"{base}/{ysym}?range=3mo&interval=1d&includePrePost=false"
    try:
        r = requests.get(url, timeout=15, headers=_YH_HEADERS)
        r.raise_for_status()
        data = r.json()
        res = data.get("chart", {}).get("result", [])
        if not res:
            return pd.DataFrame(columns=["date", "close"])
        r0 = res[0]
        ts = r0.get("timestamp") or []
        ind = r0.get("indicators", {}) or {}
        adj = ind.get("adjclose", [])
        qts = ind.get("quote", [])

        closes = None
        if isinstance(adj, list) and adj and isinstance(adj[0], dict) and "adjclose" in adj[0]:
            closes = adj[0]["adjclose"]
        elif isinstance(qts, list) and qts and isinstance(qts[0], dict) and "close" in qts[0]:
            closes = qts[0]["close"]

        if not ts or not closes or len(ts) != len(closes):
            return pd.DataFrame(columns=["date", "close"])

        df = pd.DataFrame({"ts": ts, "close": closes})
        df = df.dropna(subset=["close"]).copy()
        df["date"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
        df = df[["date", "close"]].copy()
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["close"])
        return df.reset_index(drop=True)
    except Exception:
        return pd.DataFrame(columns=["date", "close"])


def fetch_yahoo(sym: str, days: int = 30) -> pd.DataFrame:
    """
    Yahoo chart v8 com fallback entre hosts query1 e query2.
    """
    ysym = _to_yahoo_symbol(sym)
    if not ysym:
        return pd.DataFrame(columns=["date", "close"])

    # host principal
    base1 = "https://query1.finance.yahoo.com/v8/finance/chart"
    df = _yahoo_fetch_chart(base1, ysym)
    if not df.empty:
        return df

    # fallback host alternativo
    base2 = "https://query2.finance.yahoo.com/v8/finance/chart"
    df2 = _yahoo_fetch_chart(base2, ysym)
    return df2

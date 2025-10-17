from __future__ import annotations
import io
import math
import time
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
    venue, tick = _split_symbol(sym)
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
    venue, tick = _split_symbol(sym)
    if not tick:
        return None
    return tick.replace(".", "-")


# ==========================================================
# Fetchers
# ==========================================================

def fetch_stooq(sym: str, days: int = 30) -> pd.DataFrame:
    """
    Stooq CSV diário:
      https://stooq.com/q/d/l/?s=<ticker>.us&i=d
    Retorna DataFrame com colunas: date (str 'YYYY-MM-DD'), close (float).
    days é só uma dica (pegamos tudo que voltar e o consumidor recorta).
    """
    ticker = _to_stooq_ticker(sym)
    if not ticker:
        return pd.DataFrame(columns=["date", "close"])

    url = f"https://stooq.com/q/d/l/?s={ticker}&i=d"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200 or ("No data" in r.text):
            return pd.DataFrame(columns=["date", "close"])
        df = pd.read_csv(io.StringIO(r.text))
        # Esperado: Date,Open,High,Low,Close,Volume
        if "Date" not in df.columns or "Close" not in df.columns:
            return pd.DataFrame(columns=["date", "close"])
        df = df[["Date", "Close"]].rename(columns={"Date": "date", "Close": "close"})
        # Sanitização básica
        df = df.dropna().copy()
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["close"])
        # Garantir string de data YYYY-MM-DD
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df = df.dropna(subset=["date"])
        return df.reset_index(drop=True)
    except Exception:
        return pd.DataFrame(columns=["date", "close"])


def fetch_yahoo(sym: str, days: int = 30) -> pd.DataFrame:
    """
    Yahoo v8 chart:
      https://query1.finance.yahoo.com/v8/finance/chart/<symbol>?range=2mo&interval=1d
    Prioriza adjclose (se existir), senão usa close.
    Retorna DataFrame com colunas: date (YYYY-MM-DD), close (float).
    """
    ysym = _to_yahoo_symbol(sym)
    if not ysym:
        return pd.DataFra

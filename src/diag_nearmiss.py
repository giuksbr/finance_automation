"""
diag_nearmiss.py
Mostra quem está "quase" disparando N1/N2/N3/N3C e por que não passou.

O script:
- Lê o feed (mesmo URL do config.yaml).
- Para cada símbolo (limite padrão 80), busca séries rápidas (8-11 candles) usando os mesmos fetchers (EQ: Yahoo v8; CR: Binance).
- Calcula chg_7d e chg_10d locais (independentes dos JSONs publicados).
- Carrega indicadores do pointer atual (RSI14/ATR14/BB20,2) para enriquecer.
- Avalia regras N1/N2/N3/N3C e sinaliza "near-miss" com razão do bloqueio.

Uso:
  python -m src.diag_nearmiss
  python -m src.diag_nearmiss 120     # aumentar o limite de símbolos
"""

from __future__ import annotations
import sys
import json
import io
import requests
import pandas as pd
from typing import Dict, Tuple, Optional

from src.feed import fetch_feed, extract_watchlists
from src.utils import load_config


# -----------------------------
# Helpers de coleta rápida
# -----------------------------
UA = {"User-Agent": "Mozilla/5.0 (diag_nearmiss)"}

def _series_close_eq(symbol: str) -> pd.Series:
    """
    Coleta rápida para EQ/ETF:
      - Yahoo v8 /query2 (3mo, 1d) e retorna os 11 últimos closes como Series
    """
    ticker = symbol.split(":")[1]
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}?range=3mo&interval=1d&includePrePost=false"
    r = requests.get(url, headers=UA, timeout=15)
    r.raise_for_status()
    data = r.json()
    res = data.get("chart", {}).get("result", [])
    if not res:
        return pd.Series([], dtype=float)
    close = res[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
    ser = pd.Series([c for c in close if c is not None], dtype=float)
    return ser.tail(11)

def _series_close_cr(symbol: str) -> pd.Series:
    """
    Coleta rápida para CR:
      - Binance Klines 1d (limit=11) e retorna closes
    """
    pair = symbol.split(":")[1]
    url = f"https://api.binance.com/api/v3/klines?symbol={pair}&interval=1d&limit=11"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    kl = r.json()
    closes = [float(k[4]) for k in kl]
    return pd.Series(closes, dtype=float).tail(11)


def _chg7_10(ser: pd.Series) -> Tuple[Optional[float], Optional[float]]:
    if ser is None or len(ser) < 11:
        return None, None
    chg7 = (ser.iloc[-1] / ser.iloc[-8] - 1.0) * 100.0
    chg10 = (ser.iloc[-1] / ser.iloc[-11] - 1.0) * 100.0
    return round(chg7, 2), round(chg10, 2)

# -----------------------------
# Regras N-níveis (mirror do papel)
# -----------------------------
def eval_n_levels(row: dict, asset_type: str) -> Tuple[list, list]:
    """
    Retorna (levels_aprovados, reasons_fail) considerando:
      N1: queda ≥22%–30% + volume ok (ignoramos volume) -> usamos só queda
      N2: −12%/7d + (RSI 38–50 OU desvio vs m20 ≥1,5×ATR14) -> usamos RSI OU desvio
      N3: −8%/7d  + (RSI 40–55 OU Close ≤ BB inferior)
      N3C: (apenas cr) fallback sem derivativos: preço/RSI/BB/ATR (~N3)

    Como aqui é DIAGNÓSTICO, checamos condições principais e anotamos faltas.
    """
    chg7 = row.get("chg_7d_pct")
    rsi = row.get("RSI14")
    close = row.get("close")
    bb_ma = row.get("BB_MA20")
    bb_lo = row.get("BB_LOWER")
    atr = row.get("ATR14")

    levels = []
    fails = []

    # N1: checamos só magnitude de queda (22% a 30%). Consideraremos >=22%.
    if chg7 is not None:
        if chg7 <= -22.0:
            levels.append("N1")
        else:
            fails.append(f"N1: queda {chg7}% > -22%")

    # N2
    n2_drop_ok = (chg7 is not None) and (chg7 <= -12.0)
    n2_rsi_ok = (rsi is not None) and (38.0 <= rsi <= 50.0)
    n2_dev_ok = None
    if close is not None and bb_ma is not None and atr is not None:
        n2_dev_ok = (abs(close - bb_ma) >= 1.5 * atr)
    n2_cond_ok = n2_drop_ok and (n2_rsi_ok or (n2_dev_ok is True))
    if n2_cond_ok:
        levels.append("N2")
    else:
        miss = []
        if not n2_drop_ok:
            miss.append(f"queda {chg7}% > -12%")
        if not n2_rsi_ok:
            miss.append(f"RSI {rsi} fora 38–50")
        if n2_dev_ok is False:
            miss.append("desvio < 1.5×ATR")
        fails.append("N2: " + "; ".join(miss))

    # N3
    n3_drop_ok = (chg7 is not None) and (chg7 <= -8.0)
    n3_rsi_ok = (rsi is not None) and (40.0 <= rsi <= 55.0)
    n3_bb_ok = None
    if close is not None and bb_lo is not None:
        n3_bb_ok = (close <= bb_lo)
    n3_cond_ok = n3_drop_ok and (n3_rsi_ok or (n3_bb_ok is True))
    if n3_cond_ok:
        levels.append("N3")
    else:
        miss = []
        if not n3_drop_ok:
            miss.append(f"queda {chg7}% > -8%")
        if not n3_rsi_ok:
            miss.append(f"RSI {rsi} fora 40–55")
        if n3_bb_ok is False:
            miss.append("close > BB inferior")
        fails.append("N3: " + "; ".join(miss))

    # N3C (apenas cr, fallback sem derivativos = similar a N3)
    if asset_type == "cr":
        if n3_cond_ok:
            levels.append("N3C")
        else:
            fails.append("N3C: (fallback) mesmas condições do N3 não atendidas")

    return levels, fails


def _load_pointer_urls(pointer_url: str) -> Dict[str, str]:
    p = requests.get(pointer_url, timeout=15).json()
    return {
        "ohlcv": p["ohlcv_url"],
        "ind": p["indicators_url"],
        "sig": p["signals_url"],
    }


def main():
    # Config / Feed
    cfg = load_config()
    feed_url = cfg.get("feed_url")
    pointer_url = cfg.get("pointer_url", "https://raw.githubusercontent.com/giuksbr/finance_automation/main/public/pointer.json")

    feed = fetch_feed(feed_url)
    wl = extract_watchlists(feed)
    eq_syms = wl.get("eq", [])
    cr_syms = wl.get("cr", [])

    # Limite opcional de símbolos
    limit = 80
    if len(sys.argv) >= 2:
        try:
            limit = int(sys.argv[1])
        except Exception:
            pass

    # Indicadores do pointer (para enriquecer)
    urls = _load_pointer_urls(pointer_url)
    ind = requests.get(urls["ind"], timeout=15).json()
    ind_eq = ind.get("eq", {})
    ind_cr = ind.get("cr", {})

    rows = []

    # EQ
    for s in eq_syms[:limit]:
        ser = _series_close_eq(s)
        chg7, chg10 = _chg7_10(ser)
        close = float(ser.iloc[-1]) if len(ser) else None
        indic = ind_eq.get(s, {})
        row = {
            "symbol": s, "asset": "eq",
            "chg_7d_pct": chg7, "chg_10d_pct": chg10, "close": close,
            "RSI14": indic.get("RSI14"),
            "ATR14": indic.get("ATR14"),
            "BB_MA20": indic.get("BB_MA20"),
            "BB_LOWER": indic.get("BB_LOWER"),
        }
        levels, fails = eval_n_levels(row, "eq")
        row["levels_hit"] = ",".join(levels) if levels else ""
        row["fail_reasons"] = " | ".join(fails)
        rows.append(row)

    # CR
    for s in cr_syms[:limit]:
        ser = _series_close_cr(s)
        chg7, chg10 = _chg7_10(ser)
        close = float(ser.iloc[-1]) if len(ser) else None
        indic = ind_cr.get(s, {})
        row = {
            "symbol": s, "asset": "cr",
            "chg_7d_pct": chg7, "chg_10d_pct": chg10, "close": close,
            "RSI14": indic.get("RSI14"),
            "ATR14": indic.get("ATR14"),
            "BB_MA20": indic.get("BB_MA20"),
            "BB_LOWER": indic.get("BB_LOWER"),
        }
        levels, fails = eval_n_levels(row, "cr")
        row["levels_hit"] = ",".join(levels) if levels else ""
        row["fail_reasons"] = " | ".join(fails)
        rows.append(row)

    df = pd.DataFrame(rows)

    # Ordena por pior queda 7d e mostra um top 20
    view = df.sort_values(["chg_7d_pct"], ascending=[True]).head(20).copy()

    # Resumo
    print("== Top quedas 7d e por que não virou N-level (top 20) ==\n")
    cols = ["symbol","asset","chg_7d_pct","RSI14","BB_MA20","BB_LOWER","ATR14","levels_hit","fail_reasons"]
    print(view[cols].to_string(index=False))

    # CSV opcional para revisar em planilha
    view.to_csv("nearmiss_review.csv", index=False)
    print("\n[ok] nearmiss_review.csv salvo (top 20).")


if __name__ == "__main__":
    main()

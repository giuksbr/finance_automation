from __future__ import annotations
import json, yaml, pandas as pd
import requests
from datetime import datetime, timezone
from typing import Tuple

def load_cfg():
    with open("config.yaml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def fetch_binance(pair: str, days: int) -> pd.DataFrame | None:
    # kline: [open time, o, h, l, c, v, close time, ...]
    url = f"https://api.binance.com/api/v3/klines?symbol={pair}&interval=1d&limit={days}"
    r = requests.get(url, timeout=20); r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or not data:
        return None
    rows = []
    for k in data:
        rows.append({
            "Date": datetime.fromtimestamp(k[0]/1000, tz=timezone.utc).date().isoformat(),
            "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4])
        })
    return pd.DataFrame(rows)

def fetch_coingecko(pair: str, cg_map: dict, days: int) -> pd.DataFrame | None:
    cid = cg_map.get(pair)
    if not cid:
        return None
    url = f"https://api.coingecko.com/api/v3/coins/{cid}/market_chart?vs_currency=usd&days={days}&interval=daily"
    r = requests.get(url, timeout=20); r.raise_for_status()
    j = r.json()
    prices = j.get("prices") or []
    if not prices:
        return None
    rows = []
    for t, px in prices:
        # CoinGecko retorna timestamp (ms) UTC do dia; normalizamos para date ISO
        rows.append({
            "Date": datetime.fromtimestamp(t/1000, tz=timezone.utc).date().isoformat(),
            "close": float(px)
        })
    # CoinGecko só traz close; copiamos para OHLC para harmonizar
    df = pd.DataFrame(rows)
    df["open"]=df["close"]; df["high"]=df["close"]; df["low"]=df["close"]
    return df

def last_aligned(df: pd.DataFrame) -> Tuple[str, float]:
    if df is None or df.empty:
        return ("<vazio>", float("nan"))
    d = df.iloc[-1]
    return (str(d["Date"]), float(d["close"]))

def main():
    import sys
    if len(sys.argv) < 2 or not sys.argv[1].startswith("BINANCE:"):
        print("Uso: python -m src.diag_cr_pair BINANCE:<PAIR>\nEx.: python -m src.diag_cr_pair BINANCE:FETUSDT")
        return
    sym_can = sys.argv[1]
    pair = sym_can.split(":")[1]

    cfg = load_cfg()
    days = int(cfg.get("window_fallback_days", 30))
    tg = cfg["priceguard"]
    cr_delta_max = float(tg["cr_delta_max"])  # 0.0035 = 0,35%

    # carrega mapa cg
    with open("coingecko_map.json","r",encoding="utf-8") as f:
        cg_map = json.load(f)

    bn = fetch_binance(pair, days)
    cg = fetch_coingecko(pair, cg_map, days)

    bd, bc = last_aligned(bn)
    cd, cc = last_aligned(cg)

    print(f"=== {sym_can} (janela {days}d) ===")
    print(f"Binance : date={bd} close={bc}")
    print(f"CG      : date={cd} close={cc}")

    # verificar alinhamento de datas
    aligned = (bd == cd)
    print(f"Datas alinhadas? {aligned}")

    # delta percentual (se possível)
    import math
    if all([not math.isnan(bc), not math.isnan(cc)]):
        delta = abs(cc - bc) / bc if bc != 0 else float("inf")
        print(f"Δ |close_CG - close_BN| / BN = {delta:.6f}  (limite cr_delta_max={cr_delta_max:.6f})")
        if not aligned:
            print("Motivo provável do FAIL: data desalinhada (CG e Binance têm último candle em dias diferentes).")
        elif delta > cr_delta_max:
            print("Motivo provável do FAIL: divergência de preço acima do limite (PriceGuard).")
        else:
            print("As fontes parecem convergir — se mesmo assim saiu binance_only, pode ter sido falta de dado pontual no horário de coleta.")
    else:
        print("Não foi possível calcular delta: alguma das fontes veio vazia.")

if __name__ == "__main__":
    main()

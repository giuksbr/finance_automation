from __future__ import annotations
import sys, io, json
import requests
from src.fetch_eq import _to_stooq_ticker, _to_yahoo_symbol, _STQ_HEADERS, _YH_HEADERS

def head(s: str, n=280):
    s = s if isinstance(s, str) else str(s)
    return s[:n].replace("\n", "\\n")

def test_stooq(sym: str):
    t = _to_stooq_ticker(sym)
    if not t:
        print("[stooq] ticker inválido")
        return
    for scheme in ("https", "http"):
        url = f"{scheme}://stooq.com/q/d/l/?s={t}&i=d"
        try:
            r = requests.get(url, timeout=15, headers=_STQ_HEADERS)
            print(f"[stooq] {url} -> {r.status_code}, {len(r.text)} bytes, sample='{head(r.text)}'")
            if r.status_code == 200 and "Date,Open,High,Low,Close,Volume" in r.text:
                break
        except Exception as e:
            print(f"[stooq] {url} EXC: {e}")

def test_yahoo(sym: str):
    y = _to_yahoo_symbol(sym)
    if not y:
        print("[yahoo] símbolo inválido")
        return
    for host in ("https://query1.finance.yahoo.com", "https://query2.finance.yahoo.com"):
        url = f"{host}/v8/finance/chart/{y}?range=3mo&interval=1d&includePrePost=false"
        try:
            r = requests.get(url, timeout=15, headers=_YH_HEADERS)
            txt = r.text
            samp = head(txt)
            print(f"[yahoo] {url} -> {r.status_code}, {len(txt)} bytes, sample='{samp}'")
            if r.status_code == 200:
                break
        except Exception as e:
            print(f"[yahoo] {url} EXC: {e}")

def main():
    syms = sys.argv[1:] or ["NYSEARCA:SPY","NASDAQ:NVDA","NYSE:BRK.B","NYSEARCA:VUG"]
    for s in syms:
        print(f"\n=== {s} ===")
        test_stooq(s)
        test_yahoo(s)

if __name__ == "__main__":
    main()

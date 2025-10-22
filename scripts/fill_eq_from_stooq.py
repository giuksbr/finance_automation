#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, json, glob, sys, csv
from datetime import datetime, timezone

import requests

STOOQ_URL = "https://stooq.com/q/d/l/?s={ticker}&i=d"
# Mapeamento do símbolo canonical -> ticker Stooq
SYMBOL_MAP = {
    "NYSE:BRK.B": "brk-b.us",
}

N_POINTS = 60  # baixa ~60 dias e usa tudo (o export calcula janelas)
PUBLIC_DIR = "public"

def _latest_ohlcv_path():
    files = sorted(glob.glob(os.path.join(PUBLIC_DIR, "ohlcv_cache_*.json")))
    if not files:
        print("[err] não achei public/ohlcv_cache_*.json", file=sys.stderr)
        sys.exit(1)
    return files[-1]

def _iso_utc(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)
    return ts.replace(microsecond=0).isoformat().replace("+00:00","Z")

def fetch_stooq_daily(ticker: str):
    url = STOOQ_URL.format(ticker=ticker)
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    it = csv.DictReader(r.text.splitlines())
    rows = [row for row in it if row.get("Date") and row.get("Close")]
    # ordena por data asc
    rows.sort(key=lambda x: x["Date"])
    # pega somente os últimos N_POINTS
    rows = rows[-N_POINTS:]
    closes = []
    times = []
    for row in rows:
        try:
            close = float(row["Close"])
        except Exception:
            continue
        # Stooq Date = YYYY-MM-DD (UTC)
        dt = datetime.fromisoformat(row["Date"]).replace(tzinfo=timezone.utc)
        closes.append(close)
        times.append(_iso_utc(dt))
    return closes, times

def main():
    ohl_path = _latest_ohlcv_path()
    print(f"[info] usando cache: {ohl_path}")
    data = json.load(open(ohl_path, "r", encoding="utf-8"))

    # garante estrutura mínima
    for k in ("eq", "cr"):
        if k not in data or not isinstance(data[k], dict):
            data[k] = {}

    changed = 0
    for sym, stooq_ticker in SYMBOL_MAP.items():
        print(f"[stooq] {sym} <- {stooq_ticker}")
        try:
            c, t = fetch_stooq_daily(stooq_ticker)
            if not c or not t or len(c) != len(t):
                print(f"[warn] sem série válida para {sym}")
                continue
            if sym not in data["eq"]:
                data["eq"][sym] = {}
            data["eq"][sym]["c"] = c
            data["eq"][sym]["t"] = t
            data["eq"][sym]["window"] = "7d"
            data["eq"][sym]["count"] = len(c)
            # metadados opcionais (podem existir ou não)
            meta = data["eq"][sym].get("meta", {})
            srcs = set(meta.get("sources", []))
            srcs.update(["stooq"])
            meta["sources"] = sorted(srcs)
            data["eq"][sym]["meta"] = meta
            changed += 1
            print(f"[ok] {sym}: pontos={len(c)} last_close={c[-1]}")
        except Exception as e:
            print(f"[err] {sym}: {e}")

    if changed:
        with open(ohl_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        print(f"[save] atualizado: {ohl_path} (símbolos alterados={changed})")
    else:
        print("[noop] nada alterado.")

if __name__ == "__main__":
    main()

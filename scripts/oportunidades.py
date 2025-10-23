#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
oportunidades.py
----------------
Gera o relatório “Oportunidades” usando apenas:
 - FEED: posições/PM/USDT
 - n_signals_v1_latest.json: preços/variações/indicadores/derivativos

Inclui:
 - Derivação N1/N2/N3/N3C/N-Lite/Nx
 - Precedências: winners (>=30%), cap BTC (>40% ex-USDT), lockout USDT (>20%)
 - LiveGuard opcional (Binance/Coinbase/Kraken para cripto; Yahoo/Stooq para EQ/ETF)
 - Frescor: exige n_signals do dia (<=8h em 09–21 BRT; <=12h fora)
"""

import json
from datetime import datetime, timezone, timedelta
import requests

# ---------- CONFIG ----------
URL_FEED = "https://raw.githubusercontent.com/giuksbr/finance_feed/refs/heads/main/feed.json"
URL_SIGS = "https://raw.githubusercontent.com/giuksbr/finance_automation/main/public/n_signals_v1_latest.json"

BRT = timezone(timedelta(hours=-3))
CR_LIVE_TOL = 0.0035   # 0,35% cripto
EQ_LIVE_TOL = 0.008    # 0,8%  equities/etfs
USE_LIVEGUARD = True   # validação ao vivo é opcional; se falhar/degradar, seguimos link-only

# ---------- UTILS ----------
def now_brt():
    return datetime.now(BRT)

def parse_iso_brt(s: str) -> datetime:
    # aceita 'Z' e offsets
    s2 = s.replace("Z", "+00:00") if s.endswith("Z") else s
    return datetime.fromisoformat(s2).astimezone(BRT)

def get_json(url: str, timeout: int = 25) -> dict:
    r = requests.get(url, timeout=timeout, headers={"Cache-Control": "no-cache"})
    r.raise_for_status()
    return r.json()

def pct_gain(cur, avg):
    if cur is None or avg in (None, 0):
        return None
    return (cur / avg - 1.0) * 100.0

def time_in_range(t: datetime, start="09:00", end="21:00") -> bool:
    sh, sm = map(int, start.split(":"))
    eh, em = map(int, end.split(":"))
    return (t.hour > sh or (t.hour == sh and t.minute >= sm)) and (t.hour < eh or (t.hour == eh and t.minute <= em))

# ---------- LiveGuard (degrada graciosamente) ----------
def lg_crypto(pair: str):  # "BTCUSDT"
    oks, vals = 0, []
    # Binance
    try:
        b = requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={pair}", timeout=6).json()
        vals.append(float(b["price"])); oks += 1
    except Exception:
        pass
    # Coinbase
    try:
        sym = pair.replace("USDT", "-USD")
        c = requests.get(f"https://api.coinbase.com/v2/prices/{sym}/spot", timeout=6).json()
        vals.append(float(c["data"]["amount"])); oks += 1
    except Exception:
        pass
    # Kraken (mapa de pares)
    try:
        m = {
            "BTCUSDT": "XXBTZUSD",
            "ETHUSDT": "XETHZUSD",
            "SOLUSDT": "SOLUSD",
            "LINKUSDT": "LINKUSD",
            "XRPUSDT": "XXRPZUSD",
            "FETUSDT": "FETUSD",
        }
        k = m.get(pair, "")
        if k:
            d = requests.get(f"https://api.kraken.com/0/public/Ticker?pair={k}", timeout=6).json()
            key = list(d["result"].keys())[0]
            vals.append(float(d["result"][key]["c"][0])); oks += 1
    except Exception:
        pass
    return (oks >= 2, sum(vals) / len(vals) if vals else None)

def lg_equity(ticker: str):  # "VUG", "IVW", "SOXX", etc.
    oks, vals = 0, []
    # Yahoo
    try:
        y = requests.get(f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={ticker}", timeout=6).json()
        q = y["quoteResponse"]["result"][0]
        p = q.get("regularMarketPrice") or q.get("postMarketPrice") or q.get("regularMarketPreviousClose")
        if p is not None:
            vals.append(float(p)); oks += 1
    except Exception:
        pass
    # Stooq (último close diário)
    try:
        t = ticker.lower().replace(".", "-") + ".us"
        csv = requests.get(f"https://stooq.com/q/d/l/?s={t}&i=d", timeout=6).text.strip().splitlines()
        if len(csv) >= 2:
            close = float(csv[-1].split(",")[4])
            vals.append(close); oks += 1
    except Exception:
        pass
    return (oks >= 1, sum(vals) / len(vals) if vals else None)

# ---------- Regras de N-níveis (v20251017A) ----------
def derive_levels(e: dict) -> set:
    """Retorna set de níveis para um item do universe."""
    lv = set()
    ch7  = e.get("pct_chg_7d")
    ch10 = e.get("pct_chg_10d")
    ch30 = e.get("pct_chg_30d")
    rsi  = e.get("rsi14")
    px   = e.get("price_now_close")
    ma20 = e.get("bb_ma20")
    bbL  = e.get("bb_lower")
    atr  = e.get("atr14")
    atrp = e.get("atr14_pct")
    if atr is None and atrp is not None and px is not None:
        atr = (atrp / 100.0) * px
    v    = e.get("validation", {})
    priceguard_ok = v.get("priceguard") in ("OK", "PART")

    # escolher variação (alvo 7d, fallback 10/30d)
    ch = ch7 if ch7 is not None else (ch10 if ch10 is not None else ch30)

    # N1: queda ≥22% + priceguard OK
    if ch is not None and ch <= -22 and priceguard_ok:
        lv.add("N1")
    # N2: -12% + (RSI 38–50 ou desvio MA20 ≥ 1.5*ATR)
    dev = ma20 - px if (ma20 is not None and px is not None) else None
    if ch is not None and ch <= -12 and priceguard_ok:
        cond = ((rsi is not None and 38 <= rsi <= 50) or (dev is not None and atr is not None and dev >= 1.5 * atr))
        if cond:
            lv.add("N2")
    # N3: -8% + (RSI 40–55 ou Close ≤ BB inferior)
    if ch is not None and ch <= -8 and priceguard_ok:
        cond = ((rsi is not None and 40 <= rsi <= 55) or (px is not None and bbL is not None and px <= bbL))
        if cond:
            lv.add("N3")
    # N3C: cripto 3d -8% (aprox com ch) + funding ≤ -0,02% + OI -8%
    if e.get("asset_type") == "crypto":
        der = e.get("derivatives") or {}
        if ch is not None and der.get("funding") is not None and der.get("oi_chg_3d_pct") is not None:
            if ch <= -8 and der["funding"] <= -0.0002 and der["oi_chg_3d_pct"] <= -8:
                lv.add("N3C")
    # N-Lite: janela curta/sem indicadores
    if v.get("window_status") == "SHORT_WINDOW" or (rsi is None and ma20 is None and atr is None):
        if ch is not None:
            if ch <= -22:
                lv.add("N_LITE_N1")
            elif ch <= -12:
                lv.add("N_LITE_N2")
            elif ch <= -8:
                lv.add("N_LITE_N3")
    # Near-miss
    if not lv:
        if rsi is not None and rsi < 35:
            lv.add("Nx_RSI_LOW")
        if px is not None and bbL is not None and (px - bbL) / px < 0.01:
            lv.add("Nx_NEAR_BB_LOWER")
    return lv

# ---------- CORE ----------
def main():
    # 1) Carregar fontes
    feed = get_json(URL_FEED)
    sigs = get_json(URL_SIGS)

    # 2) Frescor
    now = now_brt()
    sig_ts = parse_iso_brt(sigs["generated_at_brt"])
    # Mercado deve ser do dia; 8h na janela 09–21, 12h fora
    age_h = (now - sig_ts).total_seconds() / 3600.0
    if sig_ts.date() != now.date() or (time_in_range(now) and age_h > 8) or ((not time_in_range(now)) and age_h > 12):
        print(f"📊 Oportunidades — {now.strftime('%Y-%m-%d %H:%M')} (BRT) | LKG — n_signals desatualizado ({sigs['generated_at_brt']})")
        return

    # 3) Mapear universe
    uni = {u["symbol_canonical"]: u for u in sigs.get("universe", [])}

    # 4) Valorar posições Binance / BTC-cap / USDT%
    bin_pos = feed.get("binance", {}).get("positions", {})
    usdt = float(feed.get("binance", {}).get("usdt", 0.0))
    ex_val, btc_val = 0.0, 0.0
    winners = []
    for sym, p in bin_pos.items():
        if sym == "USDT":
            continue
        canon = p["symbol_canonical"]
        u = uni.get(canon)
        if not u:
            continue
        px = u.get("price_now_close")
        if px is None:
            continue
        mkt = px * float(p.get("quantity", 0.0))
        ex_val += mkt
        if sym == "BTC":
            btc_val += mkt
        prof = pct_gain(px, p.get("avg_cost_usd"))
        if prof is not None and prof >= 30.0:
            winners.append((canon, prof))
    total = ex_val + usdt
    usdt_pct = (usdt / total * 100.0) if total > 0 else None
    btc_cap = (btc_val / ex_val * 100.0) if ex_val > 0 else None

    # 5) LiveGuard (opcional; não bloqueia)
    lg_state = "SKIP"
    if USE_LIVEGUARD:
        try:
            b_ok, _ = lg_crypto("BTCUSDT")
            e_ok, _ = lg_crypto("ETHUSDT")
            v_ok, _ = lg_equity("VUG")
            lg_ok = (b_ok + e_ok + v_ok) >= 2
            lg_state = "OK" if lg_ok else "PART"
        except Exception:
            lg_state = "FAIL"

    # 6) Derivar níveis
    watch = list(uni.keys())
    levels = {canon: list(derive_levels(uni[canon])) for canon in watch}

    # 7) Precedências e Ações
    actions = []
    notes = []

    # Winners (giveback completo requer HWM detalhado)
    for canon, _pr in winners:
        actions.append(("Binance", canon, "Vender 25% (winner ≥30%)", "≥ US$100"))

    # BTC cap
    if btc_cap is not None and btc_cap > 40.0:
        actions.append(("Binance", "BINANCE:BTCUSDT", "Reduzir (cap >40% ex-USDT)", "US$150–200"))

    # Lockout USDT
    lockout = (usdt_pct is not None and usdt_pct > 20.0)
    if lockout:
        notes.append(f"USDT ~{usdt_pct:.1f}% ⇒ lockout ativo (sem compras; só rotação neutra R9 com N-nível).")

    # Sinais táticos (compras)
    for canon, lvs in levels.items():
        if not lvs:
            continue
        plat = "Binance" if canon.startswith("BINANCE:") else "Avenue"
        has_buy = any(x in lvs for x in ("N1", "N2", "N3", "N3C"))
        has_nlite = any(x.startswith("N_LITE") for x in lvs)
        if has_buy:
            if plat == "Binance" and lockout:
                notes.append(f"{canon} em {'/'.join(lvs)} — considerar R9 (vender winner e comprar na mesma sessão, USDT estável).")
            else:
                actions.append((plat, canon, f"Comprar ({'/'.join(lvs)})", "US$150–200" if plat == "Binance" else "US$300+"))
        elif has_nlite:
            notes.append(f"{canon} em {'/'.join([x for x in lvs if x.startswith('N_LITE')])} — triagem/alerta, não dispara ordem.")

    # 8) Cabeçalho e saída
    hdr = (
        f"📊 Oportunidades — {now.strftime('%Y-%m-%d %H:%M')} (BRT) | "
        f"Dados: FEED + n_signals | LiveGuard: {lg_state} | "
        f"USDT≈{(usdt_pct and round(usdt_pct,1)) or 'n/a'}% | "
        f"BTC cap≈{(btc_cap and round(btc_cap,1)) or 'n/a'}%"
    )
    print(hdr)

    print("\nSituação")
    if winners:
        print(f"- Winners detectados: {len(winners)}")
    if lockout:
        print(f"- Lockout USDT ativo (>20%).")
    print("- Amostra de níveis:")
    shown = 0
    for k, v in levels.items():
        if v:
            print(f"  • {k}: {','.join(v)}")
            shown += 1
        if shown >= 10:
            break

    print("\nAção")
    if not actions:
        print("| — | — | Sem AÇÃO | — |")
    else:
        print("| Plataforma | Ativo | Ação | Valor (US$) |")
        print("|---|---|---|---|")
        for plat, canon, acao, valor in actions:
            print(f"| {plat} | {canon} | {acao} | {valor} |")

    if notes:
        print("\nNotas")
        for n in notes:
            print(f"- {n}")

if __name__ == "__main__":
    main()

from __future__ import annotations
import re
import requests
from typing import Any, Dict, List, Optional


# -----------------------------
# Utils de parsing / normalização
# -----------------------------

VENUE_TICKER_RE = re.compile(r"^[A-Z0-9_.-]+:[A-Z0-9_.-]+$")

def _is_symbol_string(x: Any) -> bool:
    return isinstance(x, str) and VENUE_TICKER_RE.match(x) is not None

def _mk_symbol_canonical(venue: Optional[str], ticker: Optional[str]) -> Optional[str]:
    if not venue or not ticker:
        return None
    v = str(venue).strip().upper()
    t = str(ticker).strip().upper().replace(" ", "")
    # normalizações leves
    t = t.replace(".", ".")  # placeholder para futuras normalizações
    sc = f"{v}:{t}"
    return sc if _is_symbol_string(sc) else None

def _take_symbol_from_item(item: Any) -> Optional[str]:
    """
    Aceita:
      - string "VENUE:TICKER"
      - { "symbol_canonical": "VENUE:TICKER" }
      - { "venue": "BINANCE", "ticker": "BTCUSDT" }
      - { "exchange": "NASDAQ", "symbol": "NVDA" }  (normaliza -> NASDAQ:NVDA)
    """
    if _is_symbol_string(item):
        return item.upper()

    if isinstance(item, dict):
        sc = item.get("symbol_canonical")
        if _is_symbol_string(sc):
            return sc.upper()

        venue = item.get("venue") or item.get("exchange")
        ticker = item.get("ticker") or item.get("symbol")
        sc2 = _mk_symbol_canonical(venue, ticker)
        if sc2:
            return sc2

    return None

def _flat_symbols(arr: Any) -> List[str]:
    out: List[str] = []
    if isinstance(arr, list):
        for v in arr:
            sc = _take_symbol_from_item(v)
            if sc:
                out.append(sc)
    return out

def _get(d: Dict[str, Any], *path: str) -> Any:
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur

def _unique_preserve(seq: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for s in seq:
        if not _is_symbol_string(s):
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


# -----------------------------
# Fetch do feed
# -----------------------------

def fetch_feed(url: str) -> dict:
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()


# -----------------------------
# Extração das watchlists (cobre vários esquemas)
# -----------------------------

def extract_watchlists(feed: dict) -> dict:
    """
    Retorna {"eq":[...], "cr":[...]} a partir de várias formas possíveis:
      1) universe.watchlists.avenue.{whitelist|candidate_pool}
      2) universe.watchlists.binance.{whitelist|candidate_pool}
      3) watchlists.avenue.{whitelist|candidate_pool}
      4) watchlists.binance.{whitelist|candidate_pool}
      5) universe.watchlists.eq / universe.watchlists.cr (legado)
      6) watchlists.eq / watchlists.cr (legado)
      7) feed.avenue.watchlists.* / feed.binance.watchlists.* (fallback)
      8) feed.symbols (lista mista; separa por prefixo BINANCE:)
      9) itens como string "VENUE:TICKER" ou dicts com symbol_canonical / (venue,ticker) / (exchange,symbol)
    """
    eq: List[str] = []
    cr: List[str] = []

    # -------- formato principal atual: universe.watchlists.{avenue|binance}.{whitelist|candidate_pool}
    for venue, bucket in (("avenue", "eq"), ("binance", "cr")):
        for lst in ("whitelist", "candidate_pool"):
            arr = _get(feed, "universe", "watchlists", venue, lst)
            if arr:
                syms = _flat_symbols(arr)
                if bucket == "eq":
                    eq.extend([s for s in syms if not s.startswith("BINANCE:")])
                else:
                    cr.extend([s for s in syms if s.startswith("BINANCE:")])

    # -------- legado: watchlists.avenue/binance.{whitelist|candidate_pool}
    for venue, bucket in (("avenue", "eq"), ("binance", "cr")):
        for lst in ("whitelist", "candidate_pool"):
            arr = _get(feed, "watchlists", venue, lst)
            if arr:
                syms = _flat_symbols(arr)
                if bucket == "eq":
                    eq.extend([s for s in syms if not s.startswith("BINANCE:")])
                else:
                    cr.extend([s for s in syms if s.startswith("BINANCE:")])

    # -------- opcional: universe.watchlists.eq/cr (arrays diretos)
    arr_eq = _get(feed, "universe", "watchlists", "eq")
    if arr_eq:
        eq.extend(_flat_symbols(arr_eq))
    arr_cr = _get(feed, "universe", "watchlists", "cr")
    if arr_cr:
        cr.extend(_flat_symbols(arr_cr))

    # -------- legado: watchlists.eq/cr (arrays diretos)
    arr_eq2 = _get(feed, "watchlists", "eq")
    if arr_eq2:
        eq.extend(_flat_symbols(arr_eq2))
    arr_cr2 = _get(feed, "watchlists", "cr")
    if arr_cr2:
        cr.extend(_flat_symbols(arr_cr2))

    # -------- fallback: feed.avenue.watchlists.* / feed.binance.watchlists.*
    for venue, bucket in (("avenue", "eq"), ("binance", "cr")):
        for lst in ("whitelist", "candidate_pool"):
            arr = _get(feed, venue, "watchlists", lst)
            if arr:
                syms = _flat_symbols(arr)
                if bucket == "eq":
                    eq.extend([s for s in syms if not s.startswith("BINANCE:")])
                else:
                    cr.extend([s for s in syms if s.startswith("BINANCE:")])

    # -------- generic fallback: feed.symbols (misturado)
    arr_symbols = feed.get("symbols")
    if arr_symbols:
        syms = _flat_symbols(arr_symbols)
        eq.extend([s for s in syms if not s.startswith("BINANCE:")])
        cr.extend([s for s in syms if s.startswith("BINANCE:")])

    # normalizações finais
    eq = _unique_preserve(eq)
    cr = _unique_preserve(cr)

    # correção extra: se veio BINANCE:* dentro de eq, move pra cr
    if any(s.startswith("BINANCE:") for s in eq):
        extra_cr = [s for s in eq if s.startswith("BINANCE:")]
        eq = [s for s in eq if not s.startswith("BINANCE:")]
        cr = _unique_preserve(cr + extra_cr)

    return {"eq": eq, "cr": cr}

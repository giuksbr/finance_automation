from __future__ import annotations
import re
import requests
from typing import Any, Dict, List


def fetch_feed(url: str) -> dict:
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()


def _is_symbol_string(x: Any) -> bool:
    return isinstance(x, str) and ":" in x and re.match(r"^[A-Z0-9_.-]+:[A-Z0-9_.-]+$", x) is not None


def _flat_strings(lst: Any) -> List[str]:
    out: List[str] = []
    if isinstance(lst, list):
        for v in lst:
            if _is_symbol_string(v):
                out.append(v)
            elif isinstance(v, dict):
                # tenta achar symbol_canonical dentro de dicts {symbol_canonical: "..."}
                sc = v.get("symbol_canonical") if isinstance(v, dict) else None
                if _is_symbol_string(sc):
                    out.append(sc)
    return out


def _get_nested(dct: Dict[str, Any], *keys: str) -> Any:
    cur = dct
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur


def extract_watchlists(feed: dict) -> dict:
    """
    Extrai listas de símbolos para EQ (ações/ETFs) e CR (cripto) cobrindo:
      - feed["watchlists"]["avenue"]["whitelist"|"candidate_pool"]
      - feed["watchlists"]["binance"]["whitelist"|"candidate_pool"]
      - feed["universe"]["watchlists"]["eq"|"cr"]
      - arrays com dicts contendo "symbol_canonical"
      - strings diretas "VENUE:TICKER" / "BINANCE:PAIR"

    Retorna: {"eq":[...], "cr":[...]}
    """
    eq: List[str] = []
    cr: List[str] = []

    # 1) Formato "watchlists.avenue/binance.{whitelist|candidate_pool}"
    for venue_key, bucket in (("avenue", "eq"), ("binance", "cr")):
        for list_name in ("whitelist", "candidate_pool"):
            arr = _get_nested(feed, "watchlists", venue_key, list_name)
            if arr:
                syms = _flat_strings(arr)
                if bucket == "eq":
                    eq.extend([s for s in syms if not s.startswith("BINANCE:")])
                else:
                    cr.extend([s for s in syms if s.startswith("BINANCE:")])

    # 2) Formato "universe.watchlists.eq/cr"
    arr_eq = _get_nested(feed, "universe", "watchlists", "eq")
    if arr_eq:
        eq.extend(_flat_strings(arr_eq))
    arr_cr = _get_nested(feed, "universe", "watchlists", "cr")
    if arr_cr:
        cr.extend(_flat_strings(arr_cr))

    # 3) Formato plano "watchlists.eq/cr" (se existir)
    arr_eq2 = _get_nested(feed, "watchlists", "eq")
    if arr_eq2:
        eq.extend(_flat_strings(arr_eq2))
    arr_cr2 = _get_nested(feed, "watchlists", "cr")
    if arr_cr2:
        cr.extend(_flat_strings(arr_cr2))

    # 4) Varredura de fallback: se o feed tiver uma lista genérica "symbols"
    arr_symbols = feed.get("symbols")
    if arr_symbols:
        syms = _flat_strings(arr_symbols)
        eq.extend([s for s in syms if not s.startswith("BINANCE:")])
        cr.extend([s for s in syms if s.startswith("BINANCE:")])

    # Normalização: únicos e preserva ordem
    def unique_preserve(seq: List[str]) -> List[str]:
        seen = set()
        out = []
        for x in seq:
            if not _is_symbol_string(x):
                continue
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    eq = unique_preserve(eq)
    cr = unique_preserve(cr)

    # Filtro defensivo: qualquer coisa com prefixo BINANCE: é CR; o resto trata como EQ
    # (isso evita caso misturem venues no mesmo array)
    only_eq = [s for s in eq if not s.startswith("BINANCE:")]
    extra_cr = [s for s in eq if s.startswith("BINANCE:")]
    if extra_cr:
        cr = unique_preserve(cr + extra_cr)
        eq = only_eq

    return {"eq": eq, "cr": cr}

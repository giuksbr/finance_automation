# src/feed.py
import requests, yaml

def load_config():
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def fetch_feed(url: str) -> dict:
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()

def extract_watchlists(feed: dict) -> dict:
    """
    Suporta ambos formatos:
    - feed["watchlists"]{avenue/binance}
    - feed["universe"]["watchlists"]{avenue/binance}
    """
    wlroot = feed.get("watchlists") or feed.get("universe", {}).get("watchlists") or {}
    wl = {"eq": [], "cr": []}

    # Avenue (EQ/ETF)
    for key in ("whitelist", "candidate_pool"):
        for item in wlroot.get("avenue", {}).get(key, []):
            sym = item.get("symbol_canonical")
            if sym:
                wl["eq"].append(sym)

    # Binance (CR)
    for key in ("whitelist", "candidate_pool"):
        for item in wlroot.get("binance", {}).get(key, []):
            sym = item.get("symbol_canonical")
            if sym:
                wl["cr"].append(sym)
    return wl

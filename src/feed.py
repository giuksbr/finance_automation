import requests, yaml

def load_config():
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def fetch_feed(url: str) -> dict:
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()

def extract_watchlists(feed: dict) -> dict:
    wl = {"eq": [], "cr": []}
    for key in ("whitelist", "candidate_pool"):
        for item in feed.get("watchlists", {}).get("avenue", {}).get(key, []):
            sym = item.get("symbol_canonical")
            if sym:
                wl["eq"].append(sym)
    for key in ("whitelist", "candidate_pool"):
        for item in feed.get("watchlists", {}).get("binance", {}).get(key, []):
            sym = item.get("symbol_canonical")
            if sym:
                wl["cr"].append(sym)
    return wl

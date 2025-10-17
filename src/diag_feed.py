from __future__ import annotations
import requests, yaml, json
from src.feed import fetch_feed, extract_watchlists

def main():
    with open("config.yaml","r",encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    url = cfg["feed_url"]
    feed = fetch_feed(url)
    wl = extract_watchlists(feed)
    print("feed_url:", url)
    print("EQ:", len(wl.get("eq",[])), "| CR:", len(wl.get("cr",[])))
    print("Exemplos EQ:", wl.get("eq",[])[:10])
    print("Exemplos CR:", wl.get("cr",[])[:10])

    # dica: mostra chaves topo e caminhos relevantes
    print("\nChaves topo do feed:", sorted(list(feed.keys())))
    w = feed.get("watchlists") or {}
    u = feed.get("universe", {}).get("watchlists", {})
    print("watchlists.* keys:", list(w.keys()) if isinstance(w, dict) else type(w))
    if isinstance(w, dict):
        print("  - watchlists.avenue keys:", list(w.get("avenue",{}).keys()) if isinstance(w.get("avenue",{}),dict) else type(w.get("avenue")))
        print("  - watchlists.binance keys:", list(w.get("binance",{}).keys()) if isinstance(w.get("binance",{}),dict) else type(w.get("binance")))
    print("universe.watchlists keys:", list(u.keys()) if isinstance(u, dict) else type(u))

if __name__ == "__main__":
    main()

from __future__ import annotations
import yaml
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

    # Debug leve de caminhos relevantes
    uni = feed.get("universe", {})
    uwl = (uni or {}).get("watchlists", {})
    print("\nuniverse.watchlists keys:", list(uwl.keys()) if isinstance(uwl, dict) else type(uwl))
    for k in ("avenue","binance"):
        v = (uwl or {}).get(k, {})
        if isinstance(v, dict):
            print(f" - universe.watchlists.{k} keys:", list(v.keys()))
        else:
            print(f" - universe.watchlists.{k} type:", type(v))

if __name__ == "__main__":
    main()

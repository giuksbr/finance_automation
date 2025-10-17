def stooq_ticker_from_symbol_canonical(sym: str) -> str:
    try:
        venue, ticker = sym.split(":", 1)
    except ValueError:
        ticker = sym
    return f"{ticker.lower().replace('.', '-')}.us"

def yahoo_symbol_from_symbol_canonical(sym: str) -> str:
    try:
        venue, ticker = sym.split(":", 1)
    except ValueError:
        return sym
    return ticker.replace(".", "-")

def binance_pair_from_symbol_canonical(sym: str) -> str:
    try:
        venue, pair = sym.split(":", 1)
    except ValueError:
        return sym
    return pair

def coingecko_id_from_pair(pair: str, cg_map: dict) -> str | None:
    import re
    base = re.sub(r"(USDT|USD|BUSD|FDUSD|USDC)$", "", pair, flags=re.IGNORECASE).upper()
    return cg_map.get(base)

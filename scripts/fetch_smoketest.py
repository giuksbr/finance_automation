#!/usr/bin/env python3
import json, sys
from typing import Dict, Any, List

def _pad(xs: List[str], n: int) -> List[str]:
    return xs[:n] if len(xs) >= n else xs + [""]*(n-len(xs))

def main():
    # Amostras mínimas
    eq_sample = ["NYSEARCA:VUG", "NYSEARCA:IVW", "NYSEARCA:IUSG", "NYSEARCA:SGOV"]
    cr_sample = ["BINANCE:BTCUSDT", "BINANCE:ETHUSDT", "BINANCE:SOLUSDT"]

    print(f"[smoke] sample eq={eq_sample[:4]} cr={cr_sample[:3]}")
    try:
        from src.fetch_eq import fetch_yahoo, fetch_stooq
        from src.fetch_cr import fetch_binance, fetch_coingecko
        # Assinaturas aceitas: lista canonical → dict {canon:[{t,o,h,l,c,v},...]}
        st = fetch_stooq(eq_sample, 60) if callable(fetch_stooq) else {}
        yh = fetch_yahoo(eq_sample, 60) if callable(fetch_yahoo) else {}
        bz = fetch_binance(cr_sample, 60) if callable(fetch_binance) else {}
        cg = fetch_coingecko(cr_sample, 60) if callable(fetch_coingecko) else {}

        def shape_ok(d: Dict[str, Any]) -> bool:
            if not isinstance(d, dict) or not d: return False
            k, v = next(iter(d.items()))
            return isinstance(v, list) and (len(v) == 0 or isinstance(v[0], dict))

        st_ok = shape_ok(st); bz_ok = shape_ok(bz)
        print(f"[smoke] stooq: total={len(eq_sample)} com_dados={len(st)} shape_ok={st_ok}")
        print(f"[smoke] binance: total={len(cr_sample)} com_dados={len(bz)} shape_ok={bz_ok}")
        return 0
    except Exception as e:
        print(f"[smoke][ERR] {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())

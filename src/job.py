#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from src.pointer_utils import write_pointer


def _now_ts_brt_like() -> str:
    # apenas para logs com aparência próxima ao seu formato
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _latest(pattern: str) -> str | None:
    import glob
    files = sorted(glob.glob(pattern))
    return files[-1] if files else None


def main():
    ts = _now_ts_brt_like()
    print(f"[{ts}] Publicação concluída em public")

    # Tenta localizar os artefatos mais recentes e escrever pointer
    ohl = _latest("public/ohlcv_cache_*.json")
    ind = _latest("public/indicators_*.json")
    sig_v1 = _latest("public/n_signals_v1_*.json") or _latest("public/n_signals_*.json")

    if not (ohl and ind and sig_v1):
        print(f"[{ts}] WARN pointer: Não foi possível localizar todos (ohlcv/indicators/signals). "
              f"OHL='{ohl or ''}' IND='{ind or ''}' SIG='{sig_v1 or ''}'")
    else:
        write_pointer(ohl_path=ohl, ind_path=ind, sig_path=sig_v1)
        print(f"[{ts}] pointer_signals_v1 enriquecido: {ohl} | {ind} | {sig_v1}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERRO job] {e}")
        # ainda tenta reconstruir pointer com o que houver
        try:
            ohl = _latest("public/ohlcv_cache_*.json")
            ind = _latest("public/indicators_*.json")
            sig_v1 = _latest("public/n_signals_v1_*.json") or _latest("public/n_signals_*.json")
            write_pointer(ohl_path=ohl, ind_path=ind, sig_path=sig_v1)
            print("[job] pointer reconstruído no handler de erro.")
        except Exception as ee:
            print(f"[job] falhou reconstrução de pointer: {ee}")

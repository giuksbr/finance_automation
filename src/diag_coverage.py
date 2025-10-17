"""
diag_coverage.py
Verificação de cobertura ponta-a-ponta:
- Feed (universe.watchlists.*)  → esperado (EQ/CR)
- OHLCV do pointer               → coletado?
- Indicadores do pointer         → calculado?
- Sinais do pointer              → elegível? (opcional, não bloqueia cobertura)

Saídas:
- Resumo no stdout (com percentuais e gaps)
- CSVs auxiliares:
    missing_ohlcv.csv
    missing_indicators.csv
    short_window.csv
    single_source_signals.csv
    not_in_feed_but_in_pointer.csv   (sanidade)
Exit code:
- 0 se cobertura 100% para OHLCV e Indicadores (por classe).
- 2 se faltarem ativos em OHLCV ou Indicadores.
- 3 para erros de rede/formato.

Uso:
  python -m src.diag_coverage
  python -m src.diag_coverage --strict
  python -m src.diag_coverage --min-eq 100 --min-cr 100
"""

from __future__ import annotations

import sys
import json
import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import requests
import pandas as pd

try:
    import yaml
except Exception:
    yaml = None

from src.feed import fetch_feed, extract_watchlists


def load_config() -> dict:
    root = Path(__file__).resolve().parents[1]
    cfg_path = root / "config.yaml"
    cfg: dict = {}
    if cfg_path.exists() and yaml is not None:
        with cfg_path.open("r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    cfg.setdefault(
        "pointer_url",
        "https://raw.githubusercontent.com/giuksbr/finance_automation/main/public/pointer.json",
    )
    cfg.setdefault(
        "feed_url",
        "https://raw.githubusercontent.com/giuksbr/finance_feed/main/feed.json",
    )
    return cfg


def get_pointer_urls(pointer_url: str) -> Dict[str, str]:
    r = requests.get(pointer_url, timeout=20)
    r.raise_for_status()
    p = r.json()
    return {
        "ohlcv": p["ohlcv_url"],
        "ind": p["indicators_url"],
        "sig": p["signals_url"],
        "raw": p,
    }


def fetch_json(url: str) -> dict | list:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    # Alguns endpoints podem vir com BOM/espaços
    txt = r.text.strip()
    try:
        return json.loads(txt)
    except Exception as e:
        raise RuntimeError(f"JSON inválido em {url}: {e}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--strict", action="store_true", help="Falhar se cobertura < 100% em EQ ou CR.")
    ap.add_argument("--min-eq", type=int, default=100, help="Cobertura mínima de EQ em % (default 100).")
    ap.add_argument("--min-cr", type=int, default=100, help="Cobertura mínima de CR em % (default 100).")
    args = ap.parse_args()

    try:
        cfg = load_config()
        feed = fetch_feed(cfg["feed_url"])
        wl = extract_watchlists(feed)
        eq_expected: List[str] = wl.get("eq", [])
        cr_expected: List[str] = wl.get("cr", [])

        urls = get_pointer_urls(cfg["pointer_url"])
        ohl = fetch_json(urls["ohlcv"])
        ind = fetch_json(urls["ind"])
        sig = fetch_json(urls["sig"])

    except Exception as e:
        print(f"[erro] Falha ao ler feed/pointer: {e}", file=sys.stderr)
        sys.exit(3)

    # Normaliza dicionários
    ohl_eq = (ohl.get("eq") or {}) if isinstance(ohl, dict) else {}
    ohl_cr = (ohl.get("cr") or {}) if isinstance(ohl, dict) else {}
    ind_eq = (ind.get("eq") or {}) if isinstance(ind, dict) else {}
    ind_cr = (ind.get("cr") or {}) if isinstance(ind, dict) else {}
    signals: List[dict] = sig if isinstance(sig, list) else []

    # ---- Cobertura OHLCV / Indicadores
    def coverage(expected: List[str], got: Dict[str, dict]) -> Tuple[int, int, List[str]]:
        exp = set(expected)
        got_keys = set(got.keys())
        missing = sorted(list(exp - got_keys))
        return len(exp - (exp - got_keys)), len(exp), missing

    got_eq_ohl, tot_eq, miss_eq_ohl = coverage(eq_expected, ohl_eq)
    got_cr_ohl, tot_cr, miss_cr_ohl = coverage(cr_expected, ohl_cr)

    got_eq_ind, _, miss_eq_ind = coverage(eq_expected, ind_eq)
    got_cr_ind, _, miss_cr_ind = coverage(cr_expected, ind_cr)

    pct_eq_ohl = 0 if tot_eq == 0 else round(got_eq_ohl * 100 / tot_eq, 2)
    pct_cr_ohl = 0 if tot_cr == 0 else round(got_cr_ohl * 100 / tot_cr, 2)
    pct_eq_ind = 0 if tot_eq == 0 else round(got_eq_ind * 100 / tot_eq, 2)
    pct_cr_ind = 0 if tot_cr == 0 else round(got_cr_ind * 100 / tot_cr, 2)

    print("== Cobertura por etapa ==")
    print(f"Feed EQ: {tot_eq} | Feed CR: {tot_cr}")
    print(f"OHLCV  → EQ: {got_eq_ohl}/{tot_eq} ({pct_eq_ohl}%) | CR: {got_cr_ohl}/{tot_cr} ({pct_cr_ohl}%)")
    print(f"INDIC  → EQ: {got_eq_ind}/{tot_eq} ({pct_eq_ind}%) | CR: {got_cr_ind}/{tot_cr} ({pct_cr_ind}%)")

    # ---- Janela curta (SHORT_WINDOW) e contagem de barras úteis
    short_rows = []
    for sym, meta in ohl_eq.items():
        cnt = int(meta.get("count") or 0)
        win = meta.get("window") or ""
        if cnt < 7:
            short_rows.append({"symbol": sym, "asset": "eq", "count": cnt, "window": win})
    for sym, meta in ohl_cr.items():
        cnt = int(meta.get("count") or 0)
        win = meta.get("window") or ""
        if cnt < 7:
            short_rows.append({"symbol": sym, "asset": "cr", "count": cnt, "window": win})

    # ---- Sinais: fonte simples/dupla (não bloqueia cobertura, só diagnóstico)
    single_src = []
    for s in signals:
        sources = s.get("sources")
        if isinstance(sources, list):
            if len(sources) < 2:
                single_src.append({"symbol": s.get("symbol_canonical"), "levels": ",".join(s.get("levels", [])), "sources": sources})
        elif isinstance(sources, str):
            single_src.append({"symbol": s.get("symbol_canonical"), "levels": ",".join(s.get("levels", [])), "sources": [sources]})
        else:
            single_src.append({"symbol": s.get("symbol_canonical"), "levels": ",".join(s.get("levels", [])), "sources": []})

    # ---- Sanidade: itens no pointer que não estão no feed (provável lixo antigo)
    not_in_feed = []
    feed_all = set(eq_expected) | set(cr_expected)
    for sym in list(ohl_eq.keys()) + list(ohl_cr.keys()):
        if sym not in feed_all:
            not_in_feed.append({"symbol": sym, "where": "ohlcv"})
    for sym in list(ind_eq.keys()) + list(ind_cr.keys()):
        if sym not in feed_all:
            not_in_feed.append({"symbol": sym, "where": "indicators"})

    # ---- CSVs auxiliares
    def write_csv(name: str, rows: List[dict], cols: List[str] | None = None):
        if not rows:
            # Cria um CSV vazio com cabeçalho mínimo para evitar ruído
            Path(name).write_text("", encoding="utf-8")
            return
        df = pd.DataFrame(rows)
        if cols:
            df = df[cols]
        df.to_csv(name, index=False)

    write_csv("missing_ohlcv.csv", [{"symbol": s, "asset": "eq"} for s in miss_eq_ohl] + [{"symbol": s, "asset": "cr"} for s in miss_cr_ohl], ["symbol","asset"])
    write_csv("missing_indicators.csv", [{"symbol": s, "asset": "eq"} for s in miss_eq_ind] + [{"symbol": s, "asset": "cr"} for s in miss_cr_ind], ["symbol","asset"])
    write_csv("short_window.csv", short_rows, ["symbol","asset","count","window"])
    write_csv("single_source_signals.csv", single_src, ["symbol","levels","sources"])
    write_csv("not_in_feed_but_in_pointer.csv", not_in_feed, ["symbol","where"])

    # ---- Resumo visual
    def print_list(title: str, items: List[str], cap: int = 10):
        print(f"\n{title} ({len(items)})")
        if not items:
            print("  - nenhum")
            return
        for s in items[:cap]:
            print(f"  - {s}")
        if len(items) > cap:
            print(f"  ... (+{len(items)-cap})")

    print_list("Faltando OHLCV (EQ)", miss_eq_ohl)
    print_list("Faltando OHLCV (CR)", miss_cr_ohl)
    print_list("Faltando Indicadores (EQ)", miss_eq_ind)
    print_list("Faltando Indicadores (CR)", miss_cr_ind)

    if short_rows:
        print(f"\nSHORT_WINDOW (<7 barras úteis): {len(short_rows)}  → veja short_window.csv")
    if single_src:
        print(f"Signals single-source: {len(single_src)}        → veja single_source_signals.csv")
    if not_in_feed:
        print(f"Pointer contém símbolos fora do feed: {len(not_in_feed)} → veja not_in_feed_but_in_pointer.csv")

    # ---- Política de saída
    exit_code = 0
    if pct_eq_ohl < args.min_eq or pct_cr_ohl < args.min_cr or pct_eq_ind < args.min_eq or pct_cr_ind < args.min_cr:
        exit_code = 2
    if args.strict and exit_code == 0:
        # strict exige 100% mesmo que min_* tenha sido afrouxado
        if (pct_eq_ohl < 100) or (pct_cr_ohl < 100) or (pct_eq_ind < 100) or (pct_cr_ind < 100):
            exit_code = 2

    print("\nArquivos gerados:")
    for f in [
        "missing_ohlcv.csv",
        "missing_indicators.csv",
        "short_window.csv",
        "single_source_signals.csv",
        "not_in_feed_but_in_pointer.csv",
    ]:
        print(f" - {f}")

    if exit_code == 0:
        print("\n[ok] Cobertura atingiu os mínimos configurados.")
    else:
        print("\n[atenção] Cobertura abaixo do mínimo configurado.")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

from __future__ import annotations
import os, sys, json, yaml, requests, pandas as pd
from typing import List, Dict, Any

PRIO = {"N1": 1, "N2": 2, "N3C": 3, "N3": 4}
CONF = {"high": 0, "medium": 1, "low": 2}

def load_cfg():
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def safe_get_json(url: str):
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        txt = r.content.decode("utf-8", errors="ignore").strip().lstrip("\ufeff")
        return json.loads(txt)

def resolve_pointer_url(cfg) -> str:
    if len(sys.argv) > 1 and sys.argv[1].startswith("http"):
        return sys.argv[1]
    if os.getenv("POINTER_URL"):
        return os.getenv("POINTER_URL")
    return cfg["storage"]["raw_base_url"].rstrip("/") + "/public/pointer.json"

def get_pointer_urls(pointer_url: str) -> Dict[str, str]:
    p = safe_get_json(pointer_url)
    return {"ohlcv": p["ohlcv_url"], "ind": p["indicators_url"], "sig": p["signals_url"]}

def rank_signals(sig_list: List[dict]) -> pd.DataFrame:
    if not sig_list:
        return pd.DataFrame(columns=["symbol_canonical","levels","confidence","sources","features"])
    df = pd.DataFrame(sig_list)
    dfe = df.explode("levels", ignore_index=True)
    dfe["prio"] = dfe["levels"].map(PRIO).fillna(9).astype(int)
    dfe["conf_score"] = dfe["confidence"].map(CONF).fillna(9).astype(int)
    dfe = dfe.sort_values(["prio","conf_score","symbol_canonical"]).reset_index(drop=True)
    dfe = dfe.drop_duplicates(subset=["symbol_canonical"], keep="first")
    return dfe

def _to_list_sources(s: Any) -> list[str]:
    if isinstance(s, list):
        # já é lista (ex.: ["binance","coingecko"])
        return [str(x) for x in s if x is not None]
    if isinstance(s, str):
        # pode vir "binance|coingecko" ou "binance_only"
        parts = [p for p in s.split("|") if p]
        return parts if parts else ([s] if s else [])
    return []

def shortlist_confident(dfr: pd.DataFrame) -> pd.DataFrame:
    if dfr.empty: return dfr
    dfr = dfr.copy()
    dfr["src_list"] = dfr["sources"].apply(_to_list_sources)
    # 1) exige dupla-fonte real: len(src_list) >= 2
    dfr = dfr[dfr["src_list"].apply(lambda x: len(x) >= 2)]
    # 2) remove features incompletas (indicadores nulos)
    def ok_feat(f):
        return bool(f) and f.get("rsi14") is not None and f.get("atr14") is not None and f.get("bb_ma20") is not None and f.get("bb_lower") is not None
    dfr = dfr[dfr["features"].apply(ok_feat)]
    return dfr

def to_actions(df: pd.DataFrame) -> list[dict]:
    actions = []
    for _, r in df.iterrows():
        f = r.get("features", {}) or {}
        src_list = r.get("src_list") or _to_list_sources(r.get("sources"))
        actions.append({
            "symbol": r["symbol_canonical"],
            "level":  r["levels"],
            "confidence": r["confidence"],
            "sources": src_list,
            "metrics": {
                "chg_7d_pct": f.get("chg_7d_pct"),
                "chg_10d_pct": f.get("chg_10d_pct"),
                "rsi14": f.get("rsi14"),
                "atr14": f.get("atr14"),
                "bb_ma20": f.get("bb_ma20"),
                "bb_lower": f.get("bb_lower"),
                "close": f.get("close")
            },
            "suggestion": "review",
            "max_alloc_usd": None,
            "valid_until_brt": None
        })
    return actions

def main():
    cfg = load_cfg()
    pointer_url = resolve_pointer_url(cfg)
    urls = get_pointer_urls(pointer_url)

    _ = safe_get_json(urls["ohlcv"])  # não usado aqui, mas disponível se quiser auditar
    _ind = safe_get_json(urls["ind"]) # idem
    sig = safe_get_json(urls["sig"])

    dfr = rank_signals(sig)
    dfr = shortlist_confident(dfr)

    # montar tabela “review”
    proj = []
    for _, r in dfr.iterrows():
        f = r.get("features", {}) or {}
        src_list = r.get("src_list") or _to_list_sources(r.get("sources"))
        proj.append({
            "symbol_canonical": r["symbol_canonical"],
            "level": r["levels"],
            "confidence": r["confidence"],
            "sources": "|".join(src_list),
            "chg_7d_pct": f.get("chg_7d_pct"),
            "chg_10d_pct": f.get("chg_10d_pct"),
            "rsi14": f.get("rsi14"),
            "atr14": f.get("atr14"),
            "bb_ma20": f.get("bb_ma20"),
            "bb_lower": f.get("bb_lower"),
            "close": f.get("close"),
        })
    out = pd.DataFrame(proj)

    # ordenar por prioridade final
    if not out.empty:
        out["prio"] = out["level"].map(PRIO).fillna(9).astype(int)
        out["conf_score"] = out["confidence"].map(CONF).fillna(9).astype(int)
        out = out.sort_values(["prio","conf_score","symbol_canonical"]).reset_index(drop=True)

    # exportações
    out_csv = "oportunidades_review.csv"
    out.to_csv(out_csv, index=False)

    actions = to_actions(dfr)
    out_json = "acoes_publicar.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(actions, f, ensure_ascii=False, indent=2)

    print(f"[ok] {out_csv}: {len(out)} linhas")
    print(f"[ok] {out_json}: {len(actions)} ações (review)")
    if not out.empty:
        print(out.head(min(10, len(out))).to_string(index=False))

if __name__ == "__main__":
    main()

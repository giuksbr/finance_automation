#!/usr/bin/env bash
set -euo pipefail

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

log() { echo "[$(ts)] $*"; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# 0) Publicação base (OHLCV/INDIC/SIGNALS 'crus')
log "Executando: python -m src.job (publicar OHLCV/INDIC/SIGNALS 'crus')"
python -m src.job
log "OK: publicação inicial concluída"

# 1) Enriquecer OHLCV (colunar)
log "Enriquecendo OHLCV com séries colunar (c,t) — limit=500"
python scripts/enrich_ohlcv_cache.py --limit 500 || true

# 2) Stooq fallback para EQ/ETF (BRK.B etc.)
log "Preenchendo gaps de EQ/ETF via Stooq (.us) quando necessário"
python scripts/fill_eq_from_stooq.py || true

# 3) Verificar amostras
log "Verificando amostras de OHLCV"
python scripts/verify_ohlcv_cache.py || true

# 4) Publicar arquivos apontados pelo pointer (garante 200/OK)
log "Publicando os arquivos apontados pelo pointer.json (garante 200/OK online)"
jq -r '.ohlcv_url, .indicators_url, .signals_url' public/pointer.json \
| sed 's#.*/##' \
| xargs -I{} git add "public/{}" || true
git commit -m "publish: pointer targets $(ts)" || true
git push || true

# 5) Exportar n_signals_v1 e atualizar pointer
log "Exportando n_signals_v1 + atualizando pointer_signals_v1.json"
python -m src.export_signals_v1 --with-universe --write-latest --update-pointer

# 6) Pretty-print dos JSONs v1 antes do publish final (idempotente)
log "Formatando n_signals_v1 (pretty-print) antes do publish final"
python - <<'PY'
import json, pathlib, re
p = pathlib.Path("public")
for f in sorted(p.glob("n_signals_v1_*.json")) + [p/"n_signals_v1_latest.json"]:
    try:
        d = json.loads(f.read_text(encoding="utf-8"))
        f.write_text(json.dumps(d, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"[ok] formatado: {f}")
    except Exception as e:
        print(f"[warn] falhou formatar {f}: {e}")
PY

# 7) Commit/push dos v1 + pointer_signals_v1
log "Publicando n_signals_v1 + pointer_signals_v1.json"
git add public/n_signals_v1_*.json public/n_signals_v1_latest.json public/pointer_signals_v1.json
# pre-commit (idempotente) para garantir pretty-print
python - <<'PY'
import json, pathlib
p = pathlib.Path("public")
for f in sorted(p.glob("n_signals_v1_*.json")) + [p/"n_signals_v1_latest.json"]:
    d = json.loads(f.read_text(encoding="utf-8"))
    f.write_text(json.dumps(d, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print("[pre-commit] pretty-print n_signals_v1 JSONs…")
PY
git commit -m "publish: n_signals_v1 latest + pointer $(ts)" || true
git push || true
echo "[publish] feito."

# 8) Derivativos (CR)
log "Enriquecendo derivativos (funding / oi_chg_3d_pct) para CR"
python scripts/enrich_derivatives.py || true

# 9) Commit pós-derivativos (se houve)
log "Publicando após derivativos (se houve alterações)"
git add public/n_signals_v1_*.json public/n_signals_v1_latest.json || true
git commit -m "publish: n_signals_v1 post-derivatives $(ts)" || true
git push || true

# 10) Construir CSV do universo (LF por linha) + commit/push
log "Construindo CSV do universo a partir do n_signals_v1_latest.json (LF por linha)"
python scripts/build_universe_csv.py
git add public/n_signals_universe_latest.csv
git commit -m "publish: universe CSV (LF) $(ts)" || true
git push || true

# 11) Validações rápidas on-line
log "Validando HTTP 200 para pointer.json"
jq -r '.ohlcv_url, .indicators_url, .signals_url' public/pointer.json \
| xargs -n1 -I{} sh -c 'curl -fsSI "{}" | head -n1 && echo "{}"' | sed 'N;s/\n/ /'

log "Validando HTTP 200 para n_signals_v1_latest.json via pointer_signals_v1.json"
curl -fsSI "$(jq -r '.signals_url' public/pointer_signals_v1.json)" | head -n1

log "Validando EQ (close, pct7, close_at, priceguard)"
curl -fsSL "$(jq -r '.signals_url' public/pointer_signals_v1.json)" | \
jq '
  [.universe[] | select(.asset_type=="eq")
   | {c:(.price_now_close==null), p7:(.pct_chg_7d==null),
      t:(.price_now_close_at_utc==null), pg:.validation.priceguard}]
  | {total:length,
     close_missing: map(select(.c))|length,
     pct7_missing: map(select(.p7))|length,
     close_at_missing: map(select(.t))|length,
     pg_fail: map(select(.pg!="OK" and .pg!="PART"))|length }'

log "Validando CR (funding / oi_chg_3d_pct)"
curl -fsSL "$(jq -r '.signals_url' public/pointer_signals_v1.json)" | \
jq '
  [.universe[] | select(.asset_type=="crypto")
   | {f:(.derivatives.funding//null), o:(.derivatives.oi_chg_3d_pct//null)}]
  | {total:length,
     missing_funding: map(select(.f==null))|length,
     missing_oi3d: map(select(.o==null))|length }'

# 12) Snapshot fim (opcional)
log "Pipeline concluído."

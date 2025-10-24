#!/usr/bin/env bash
set -euo pipefail

BRANCH="${BRANCH:-main}"
REMOTE="${REMOTE:-origin}"
BRT_NOW="$(TZ=America/Sao_Paulo date +"%Y-%m-%d %H:%M:%S")"
UTC_NOW="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

log() {
  printf "[%s] %s\n" "$UTC_NOW" "$*" >&2
}

# ------ 0) Seed automático do fallback local do feed, se necessário ------
LOCAL_FEED="${FEED_LOCAL_PATH:-out/last_good_feed.json}"
need_seed=0

if [[ ! -f "$LOCAL_FEED" ]]; then
  need_seed=1
else
  # se existir jq, valida presença de watchlists; se não, assume que precisa
  if command -v jq >/dev/null 2>&1; then
    if ! jq -e '.watchlists // empty' "$LOCAL_FEED" >/dev/null 2>&1; then
      need_seed=1
    fi
  else
    # sem jq, se está muito pequeno (<200 bytes) assumimos inválido
    size=$(wc -c < "$LOCAL_FEED" 2>/dev/null || echo 0)
    if [[ "$size" -lt 200 ]]; then
      need_seed=1
    fi
  fi
fi

if [[ "$need_seed" -eq 1 ]]; then
  log "Seed do feed local necessário — executando scripts/seed_local_feed.sh"
  bash scripts/seed_local_feed.sh || true
fi

echo
log "Iniciando pipeline (branch=$BRANCH remote=$REMOTE)"

# ------ 1) Job principal (gera OHLCV/INDIC/SIGNALS básicos + pointer.json) ------
log "1/5 Rodando src.job (feed em tempo real; fallback local se 429)"
python -m src.job || {
  echo "[${UTC_NOW}] ERRO: src.job falhou — abortando pipeline." >&2
  exit 1
}

echo
# ------ 2) Export v1 (local-first ao ler pointer) ------
log "2/5 Exportando n_signals_v1 (universe + latest + pointer_signals_v1)"
python -m src.export_signals_v1 --with-universe --write-latest --update-pointer || {
  echo "[${UTC_NOW}] ERRO: export_signals_v1 falhou — abortando pipeline." >&2
  exit 1
}

echo
# ------ 3) CSV final (se existir o script) ------
if [[ -f "scripts/build_universe_csv.py" ]]; then
  log "3/5 (opcional) CSV: scripts/build_universe_csv.py"
  python scripts/build_universe_csv.py || {
    echo "[${UTC_NOW}] AVISO: falha ao gerar CSV — seguindo mesmo assim." >&2
  }
else
  log "3/5 CSV: pular (script ausente)"
fi

echo
# ------ 4) Validações locais rápidas ------
log "4/5 Validações locais rápidas"
if command -v jq >/dev/null 2>&1; then
  if [[ -f "public/n_signals_v1_latest.json" ]]; then
    jq -r '"signals_v1_latest: signals=" + ( .signals|length|tostring ) + ", universe=" + ( .universe|length|tostring ) + ", brt=" + .generated_at_brt' public/n_signals_v1_latest.json || true
  fi
  if [[ -f "public/pointer.json" ]]; then
    jq -r '"pointer: ohlcv=" + .ohlcv_path + ", indicators=" + .indicators_path + ", signals=" + .signals_path' public/pointer.json || true
  fi
else
  echo "[${UTC_NOW}] jq não encontrado — validações locais mínimas."
fi

echo
# ------ 5) Commit/push único ------
log "5/5 Commit e push únicos"
git add -A
if ! git diff --cached --quiet; then
  git commit -m "publish(local-first): $(date -u +"%Y%m%dT%H%M%SZ")"
  git push "${REMOTE}" "${BRANCH}"
  log "Publicação concluída em $BRANCH ($REMOTE)."
else
  log "Nada para publicar (sem mudanças)."
fi

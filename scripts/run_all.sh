#!/usr/bin/env bash
set -euo pipefail

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
log() { echo "[$(ts)] $*"; }

BRANCH="${BRANCH:-main}"
REMOTE="${REMOTE:-origin}"
PY="${PY:-python}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

log "Iniciando pipeline (branch=$BRANCH remote=$REMOTE)"

# 1) Geração base (feed em tempo real; resto local)
log "1/5 Rodando src.job (feed em tempo real; sem cache)"
${PY} -m src.job

# 2) Export n_signals_v1 (local-first: lê pointer local e arquivos gerados)
log "2/5 Exportando n_signals_v1 (universe + latest + pointer_signals_v1)"
${PY} -m src.export_signals_v1 --with-universe --write-latest --update-pointer

# 3) Validações locais — sem bater na web
log "3/5 Validação local dos artefatos"
LATEST_SIG="$(ls -t public/n_signals_v1_*.json | head -1)"
jq -r '"schema=\(.schema_version) generated=\(.generated_at_brt) signals=\((.signals//[])|length) universe=\((.universe//[])|length)"' "${LATEST_SIG}" || true

# 4) Preparar commit (único)
log "4/5 Preparando commit de publicação"
git add public/ohlcv_cache_*.json || true
git add public/indicators_*.json || true
git add public/n_signals_*.json || true
git add public/pointer.json public/pointer_signals_v1.json || true

if ! git diff --cached --quiet; then
  git commit -m "publish: atualiza ohlcv/indicators/signals + pointers"
  log "Commit criado"
else
  log "Nada para publicar (sem alterações)"
fi

# 5) Push único
if git rev-parse --abbrev-ref HEAD >/dev/null 2>&1; then
  log "5/5 Enviando para ${REMOTE} ${BRANCH}"
  git push "${REMOTE}" "HEAD:${BRANCH}"
  log "Pipeline concluído."
else
  log "[aviso] repositório Git não inicializado; pulando push"
fi

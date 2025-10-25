#!/usr/bin/env bash
set -euo pipefail

# ---------------------------
# Pipeline local-first
# - Busca feed em tempo real (retry/backoff) com fallback local
# - Gera OHLCV/indicators/signals locais
# - Exporta signals_v1
# - (NOVO) Preenche pointer_signals_v1.json com ohlcv/indicators/signals locais
# - Gera CSV final com pct_chg_* usando o OHLCV local do pointer
# - Faz um único commit/push
# ---------------------------

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

BRT_NOW="$(TZ=America/Sao_Paulo date -Iseconds)"
UTC_NOW="$(date -u -Iseconds)"
TSUTC="$(date -u +"%Y%m%dT%H%M%SZ")"

say() { echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] $*"; }

# ---------- helpers ----------
latest_public_file() {
  local pattern="$1"
  ls -1t public/$pattern 2>/dev/null | head -1 || true
}

write_pointer_signals_v1() {
  # Preenche public/pointer_signals_v1.json com caminhos locais
  local OHL="$(latest_public_file "ohlcv_cache_*.json")"
  local IND="$(latest_public_file "indicators_*.json")"
  local SIG="$(latest_public_file "n_signals_*.json")"   # signals bruto (não v1)

  if [[ -z "${OHL}" || -z "${IND}" || -z "${SIG}" ]]; then
    say "WARN pointer: Não foi possível localizar todos (ohlcv/indicators/signals). OHL='${OHL}' IND='${IND}' SIG='${SIG}'"
  fi

  # Se já existir, preserva signals_v1_url (se houver) e só atualiza campos locais
  local PTR_FILE="public/pointer_signals_v1.json"
  local TMP="$(mktemp)"
  if [[ -f "${PTR_FILE}" ]]; then
    # merge simples via jq se disponível; caso contrário, sobrescreve
    if command -v jq >/dev/null 2>&1; then
      jq \
        --arg ohlcv "${OHL:-null}" \
        --arg indicators "${IND:-null}" \
        --arg signals "${SIG:-null}" \
        --arg brt "${BRT_NOW}" \
        --arg utc "${UTC_NOW}" \
        '
        .updated_at_brt = $brt
        | .updated_at_utc = $utc
        | .ohlcv = ($ohlcv // .ohlcv)
        | .indicators = ($indicators // .indicators)
        | .signals = ($signals // .signals)
        ' "${PTR_FILE}" > "${TMP}" || true
      if [[ -s "${TMP}" ]]; then
        mv "${TMP}" "${PTR_FILE}"
      fi
    else
      cat > "${PTR_FILE}" <<JSON
{
  "updated_at_brt": "${BRT_NOW}",
  "updated_at_utc": "${UTC_NOW}",
  "ohlcv": "${OHL:-}",
  "indicators": "${IND:-}",
  "signals": "${SIG:-}"
}
JSON
    fi
  else
    cat > "${PTR_FILE}" <<JSON
{
  "updated_at_brt": "${BRT_NOW}",
  "updated_at_utc": "${UTC_NOW}",
  "ohlcv": "${OHL:-}",
  "indicators": "${IND:-}",
  "signals": "${SIG:-}"
}
JSON
  fi
  say "pointer_signals_v1 enriquecido: $(jq -r '.ohlcv+" | "+.indicators+" | "+.signals' "${PTR_FILE}" 2>/dev/null || echo "${PTR_FILE}")"
}

# ---------- 0) seed do feed local, se necessário ----------
SEED_SH="scripts/seed_local_feed.sh"
if [[ -x "${SEED_SH}" ]]; then
  say "Seed do feed local necessário — executando ${SEED_SH}"
  "${SEED_SH}" || true
fi

# ---------- 1) job principal (gera ohlcv/indicators/signals brutos) ----------
say "Iniciando pipeline (branch=$(git rev-parse --abbrev-ref HEAD) remote=$(git remote 2>/dev/null | head -1 || echo origin))"
say "1/5 Rodando src.job (feed em tempo real; fallback local se 429)"
python -m src.job || {
  say "ERRO: src.job falhou"; exit 2;
}
say "Publicação concluída em public"

# ---------- (NOVO) escrever pointer com caminhos LOCAIS ----------
write_pointer_signals_v1

# ---------- 2) Exportar v1 (gera n_signals_v1_<TS>.json + latest + atualiza pointer URL) ----------
say "2/5 Exportando n_signals_v1 (universe + latest + pointer_signals_v1)"
python -m src.export_signals_v1 --with-universe --write-latest --update-pointer

# Após export, re-enriquecer pointer com caminhos locais (mantendo a URL do v1 que o export escreveu)
write_pointer_signals_v1

# ---------- 3) CSV ----------
say "3/5 (opcional) CSV: scripts/build_universe_csv.py"
python "scripts/build_universe_csv.py" || {
  say "[aviso] falha ao gerar CSV — prosseguindo"; 
}

# ---------- 4) Validações locais rápidas ----------
say "4/5 Validações locais rápidas"
if command -v jq >/dev/null 2>&1; then
  SIG_LATEST="public/n_signals_v1_latest.json"
  if [[ -f "${SIG_LATEST}" ]]; then
    SIG_N=$(jq '.signals | length' "${SIG_LATEST}")
    UNI_N=$(jq '.universe | length' "${SIG_LATEST}" 2>/dev/null || echo 0)
    say "signals_v1_latest: signals=${SIG_N}, universe=${UNI_N}, brt=$(jq -r '.generated_at_brt' "${SIG_LATEST}" 2>/dev/null || echo "-")"
  fi
  PTR="public/pointer_signals_v1.json"
  if [[ -f "${PTR}" ]]; then
    OHL=$(jq -r '.ohlcv' "${PTR}" 2>/dev/null || echo "")
    IND=$(jq -r '.indicators' "${PTR}" 2>/dev/null || echo "")
    SIG=$(jq -r '.signals' "${PTR}" 2>/dev/null || echo "")
    say "pointer: ohlcv=${OHL}, indicators=${IND}, signals=${SIG}"
  fi
fi

# ---------- 5) Commit e push únicos ----------
say "5/5 Commit e push únicos"
git add public || true
git add scripts/*.py scripts/*.sh || true
git add src/*.py || true

if command -v jq >/dev/null 2>&1; then
  say "[pre-commit] pretty-print n_signals_v1 JSONs…"
  for f in public/n_signals_v1_*.json public/n_signals_v1_latest.json; do
    [[ -f "$f" ]] && jq '.' "$f" > "$f.tmp" && mv "$f.tmp" "$f" || true
  done
fi

git commit -m "publish(local-first): ${TSUTC}" || true

CUR_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
REMOTE="$(git remote 2>/dev/null | head -1 || echo origin)"
git push -u "${REMOTE}" "${CUR_BRANCH}" || {
  say "WARN: push falhou para ${REMOTE}/${CUR_BRANCH} (non-fast-forward?). Faça um pull --rebase ou publique neste branch."
}

say "Concluído."

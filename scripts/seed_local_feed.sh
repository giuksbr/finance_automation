#!/usr/bin/env bash
set -euo pipefail

# seeds a cópia local do feed: out/last_good_feed.json
# tenta múltiplas rotas (RAW refs/heads, RAW main, jsDelivr, API GitHub)
# respeita 429 com backoff leve

FEED_CANON="${FEED_CANON:-https://raw.githubusercontent.com/giuksbr/finance_feed/refs/heads/main/feed.json}"
FEED_RAW_MAIN="${FEED_RAW_MAIN:-https://raw.githubusercontent.com/giuksbr/finance_feed/main/feed.json}"
FEED_JSD="${FEED_JSD:-https://cdn.jsdelivr.net/gh/giuksbr/finance_feed@main/feed.json}"
FEED_API="${FEED_API:-https://api.github.com/repos/giuksbr/finance_feed/contents/feed.json?ref=main}"

OUT_PATH="${OUT_PATH:-out/last_good_feed.json}"
TMP_PATH="${OUT_PATH}.tmp"

mkdir -p "$(dirname "$OUT_PATH")"

has_jq=1
command -v jq >/dev/null 2>&1 || has_jq=0

log() {
  printf "[%s] seed_local_feed: %s\n" "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$*" >&2
}

# GET com tratamento básico de 429 (Retry-After) e backoff
http_get() {
  local url="$1"
  local tries=0
  local max_tries="${FEED_MAX_RETRIES:-3}"

  while :; do
    # -sS: quiet + show errors; -f: fail on >=400
    if curl -sSfL --connect-timeout 6 --max-time 15 -H "User-Agent: finance_automation/seed" "$url" -o "$TMP_PATH"; then
      return 0
    fi

    code=$?
    # Se curl falhou, tenta extrair HTTP code (nem sempre está disponível)
    http_code=$( (curl -sSL -w "%{http_code}" -o /dev/null "$url") || echo "000" )

    if [[ "$http_code" == "429" ]]; then
      # tenta respeitar Retry-After (não conseguimos ler facilmente aqui, então usamos backoff)
      sleep_s=$(( 2 ** tries ))
      if (( sleep_s > 15 )); then sleep_s=15; fi
      log "HTTP 429 em $url — aguardando ${sleep_s}s e tentando novamente ($((tries+1))/$((max_tries+1)))…"
      sleep "$sleep_s"
    else
      # outras falhas
      sleep_s=$(( 1 + tries ))
      if (( sleep_s > 8 )); then sleep_s=8; fi
      log "Falha HTTP ($http_code) em $url — retry em ${sleep_s}s ($((tries+1))/$((max_tries+1)))…"
      sleep "$sleep_s"
    fi

    if (( tries >= max_tries )); then
      return 1
    fi
    tries=$((tries+1))
  done
}

try_route() {
  local name="$1"
  local url="$2"

  log "Tentando rota $name → $url"
  if http_get "$url"; then
    # API GitHub retorna JSON com base64 quando usamos contents API:
    if [[ "$name" == "API" ]]; then
      if (( has_jq )); then
        local enc
        enc=$(jq -r '.content // empty' "$TMP_PATH" || true)
        if [[ -n "$enc" ]]; then
          # remove quebras de linha do base64
          echo "$enc" | tr -d '\n' | base64 --decode > "$TMP_PATH.decoded"
          mv "$TMP_PATH.decoded" "$TMP_PATH"
        fi
      else
        log "Aviso: sem jq — não foi possível decodificar base64 da API GitHub automaticamente."
        return 1
      fi
    fi

    # valida estrutura mínima (presence de "watchlists")
    if (( has_jq )); then
      if jq -e '.watchlists // empty' "$TMP_PATH" >/dev/null 2>&1; then
        mv "$TMP_PATH" "$OUT_PATH"
        log "✔ Seed concluído por $name → $OUT_PATH"
        return 0
      else
        log "Rota $name retornou JSON sem .watchlists — mantendo tentativa de outras rotas…"
        return 1
      fi
    else
      # sem jq: assume bom (melhor do que nada)
      mv "$TMP_PATH" "$OUT_PATH"
      log "✔ Seed (sem jq) por $name → $OUT_PATH"
      return 0
    fi
  fi

  log "Rota $name falhou."
  return 1
}

main() {
  # tenta na ordem
  try_route "CANON" "$FEED_CANON" && exit 0
  try_route "RAW_MAIN" "$FEED_RAW_MAIN" && exit 0
  try_route "JSD" "$FEED_JSD" && exit 0
  try_route "API" "$FEED_API" && exit 0

  log "✖ Não foi possível semear o feed local por nenhuma rota."
  exit 1
}

main "$@"

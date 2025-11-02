#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

mkdir -p public

latest() {
  local pattern="$1"
  ls -1t $pattern 2>/dev/null | head -1 || true
}

OHL="$(latest 'public/ohlcv_cache_*.json')"
IND="$(latest 'public/indicators_*.json')"
SIG_V1="$(latest 'public/n_signals_v1_*.json')"
SIG_LG="$(latest 'public/n_signals_*.json')"
SIG="${SIG_V1:-${SIG_LG:-}}"

jq -n \
  --arg ohl "${OHL:-}" \
  --arg ind "${IND:-}" \
  --arg sig "${SIG:-}" \
  '{
     "ohlcv": ( ($ohl|length)>0 ? $ohl : null ),
     "indicators": ( ($ind|length)>0 ? $ind : null ),
     "signals": ( ($sig|length)>0 ? $sig : null ),
     "ohlcv_url": null,
     "indicators_url": null,
     "signals_url": null
   }' > public/pointer_signals_v1.json

echo "[hotfix] pointer_signals_v1.json:"
cat public/pointer_signals_v1.json

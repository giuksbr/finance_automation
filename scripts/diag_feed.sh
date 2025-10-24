#!/usr/bin/env bash
set -euo pipefail

URL="${1:-https://raw.githubusercontent.com/giuksbr/finance_feed/main/feed.json}"

echo "[diag] tentando baixar: $URL"
tmp="$(mktemp)"
curl -sSfL "$URL" -o "$tmp" || { echo "[diag] falha no download"; exit 1; }

if ! command -v jq >/dev/null 2>&1; then
  echo "[diag] instale jq para diagnóstico mais rico (brew install jq)"
  cat "$tmp" | head -200
  exit 0
fi

echo "[diag] chaves de topo:"
jq -r 'keys | join(", ")' "$tmp" || true

echo "[diag] contagens candidatas:"
jq -r '
def len_or0: if type=="array" then length else 0 end;
"watchlists.eq=" + ((.watchlists.eq // []) | len | tostring),
"watchlists.cr=" + ((.watchlists.cr // []) | len | tostring),
"watchlists.whitelist=" + ((.watchlists.whitelist // []) | len | tostring),
"universe.eq=" + ((.universe.eq // []) | len | tostring),
"universe.cr=" + ((.universe.cr // []) | len | tostring),
"symbols.eq=" + ((.symbols.eq // []) | len | tostring),
"symbols.cr=" + ((.symbols.cr // []) | len | tostring),
"top.eq=" + ((.eq // []) | len | tostring),
"top.cr=" + ((.cr // []) | len | tostring),
"top.whitelist=" + ((.whitelist // []) | len | tostring),
"candidate_pool=" + ((.watchlists.candidate_pool // .candidate_pool // .universe.candidate_pool // []) | len | tostring)
' "$tmp" || true

rm -f "$tmp"

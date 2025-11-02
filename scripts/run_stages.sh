#!/usr/bin/env bash
set -euo pipefail

ts() { date -u +"[%Y-%m-%dT%H:%M:%SZ]"; }

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Garante pastas
mkdir -p out public

echo "$(ts) [S0] Validando feed local…"
if jq -e '.schema_version=="2.0"' out/last_good_feed.json >/dev/null; then
  echo "OK: feed schema 2.0"
else
  echo "$(ts) ERRO: out/last_good_feed.json inválido (schema_version)."
  exit 1
fi
echo "$(ts) OK: feed schema 2.0"

echo "$(ts) [S1] Extraindo símbolos das watchlists…"
python scripts/build_watchlists_from_feed.py
echo "$(ts) OK: watchlists geradas (out/syms_eq.txt, out/syms_cr.txt, out/watchlists_local.json)"

echo "$(ts) [S2] Smoke test dos coletores…"
python scripts/fetch_smoketest.py
echo "$(ts) [smoke] OK"

echo "$(ts) [S3] Coletando OHLCV + IND (local-first)…"
if [[ -f scripts/collect_local_first.py ]]; then
  python scripts/collect_local_first.py
else
  # Fallback para o job monolítico
  PYTHONPATH=. python -m src.job || true
fi

# Verificações rápidas pós-coleta
OHL="$(jq -r '.ohlcv' public/pointer_signals_v1.json 2>/dev/null || echo '')"
IND="$(jq -r '.indicators' public/pointer_signals_v1.json 2>/dev/null || echo '')"
if [[ -z "${OHL:-}" || -z "${IND:-}" || ! -s "${OHL:-/dev/null}" || ! -s "${IND:-/dev/null}" ]]; then
  echo "$(ts) [collect][ERRO] OHLCV/INDICATORS não resolvidos. Pointer:"
  cat public/pointer_signals_v1.json || true
  exit 1
fi

echo "$(ts) [S4] Exportando n_signals_v1 (robusto)…"
PYTHONPATH=. python -m src.export_signals_v1 || {
  echo "[export_v1][ERRO] Falhou a geração; verificando latest…"
  [[ -s "public/n_signals_v1_latest.json" ]] || exit 1
}
echo "$(ts) [export_v1] OK"

echo "$(ts) [S5] Construindo CSV final…"
python scripts/build_universe_csv.py
echo "$(ts) [csv] head:"
head -n 4 public/n_signals_universe_latest.csv || true
if ! grep -q 'null' public/n_signals_universe_latest.csv; then
  echo "$(ts) [csv] OK: sem 'null'"
else
  echo "$(ts) [csv][WARN] Há 'null' no CSV — revise cálculo."
fi

echo "$(ts) [DONE] Publicação concluída. Artefatos em ./public:"
ls -lh public

#!/usr/bin/env bash
# scripts/run_all.sh
# Pipeline completo: publica bases, enriquece OHLCV, exporta v1, publica e valida.
# Falha (exit != 0) se qualquer etapa crítica quebrar ou se houver faltas em EQ/CR.

set -euo pipefail

# -----------------------------
# Configurações
# -----------------------------
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Limites de enriquecimento OHLCV (pontos por ativo)
: "${ENRICH_LIMIT:=500}"

# Política de validação (você pode relaxar se quiser)
: "${ALLOW_CR_MISSING:=0}"   # 0 = estrito: falha se faltar funding/oi3d em qualquer CR

# Git identity silencioso (não altera global)
GIT_COMMITTER_NAME="${GIT_COMMITTER_NAME:-Giuliano Barros}"
GIT_COMMITTER_EMAIL="${GIT_COMMITTER_EMAIL:-giuliano@Giulianos-MacBook-Pro.local}"
export GIT_COMMITTER_NAME GIT_COMMITTER_EMAIL GIT_AUTHOR_NAME="$GIT_COMMITTER_NAME" GIT_AUTHOR_EMAIL="$GIT_COMMITTER_EMAIL"

# -----------------------------
# Helpers
# -----------------------------
ts_utc() { date -u +%Y-%m-%dT%H:%M:%SZ; }

say() { printf "[%s] %s\n" "$(ts_utc)" "$*"; }

die() { printf "[%s] ERROR: %s\n" "$(ts_utc)" "$*" >&2; exit 1; }

need() {
  command -v "$1" >/dev/null 2>&1 || die "comando requerido não encontrado: $1"
}

git_add_and_push() {
  local msg="$1"
  git add -A
  if ! git diff --cached --quiet; then
    git commit -m "$msg" || true
    git push || true
  else
    say "git: nada a commitar"
  fi
}

jq_val() {
  # jq_val <file_or_url> <jq_expr>
  local src="$1"; shift
  curl -fsSL "$src" | jq -r "$@"
}

# -----------------------------
# Etapas
# -----------------------------
step_publish_raw() {
  say "Executando: python -m src.job (publicar OHLCV/INDIC/SIGNALS 'crus')"
  python -m src.job
}

step_enrich_ohlcv() {
  say "Enriquecendo OHLCV com séries colunar (c,t) — limit=$ENRICH_LIMIT"
  python scripts/enrich_ohlcv_cache.py --limit "$ENRICH_LIMIT" || true

  say "Preenchendo gaps de EQ/ETF via Stooq (.us) quando necessário"
  python scripts/fill_eq_from_stooq.py || true

  say "Verificando amostras de OHLCV"
  python scripts/verify_ohlcv_cache.py
}

step_publish_pointer_targets() {
  say "Publicando os arquivos apontados pelo pointer.json (garante 200/OK online)"
  jq -r '.ohlcv_url, .indicators_url, .signals_url' public/pointer.json \
  | sed 's#.*/##' \
  | xargs -I{} git add "public/{}"
  git_add_and_push "publish: pointer targets $(ts_utc)"
}

step_export_v1_and_pointer() {
  say "Exportando n_signals_v1 + atualizando pointer_signals_v1.json"
  python -m src.export_signals_v1 --with-universe --write-latest --update-pointer

  say "Publicando n_signals_v1 + pointer_signals_v1.json"
  git add public/n_signals_v1_*.json public/n_signals_v1_latest.json public/pointer_signals_v1.json
  git_add_and_push "publish: n_signals_v1 latest + pointer $(ts_utc)"
}

step_enrich_derivatives() {
  say "Enriquecendo derivativos (funding / oi_chg_3d_pct) para CR"
  python scripts/enrich_derivatives.py || true

  say "Publicando após derivativos (se houve alterações)"
  git add public/n_signals_v1_*.json public/n_signals_v1_latest.json public/pointer_signals_v1.json
  git_add_and_push "publish: n_signals_v1 latest + pointer (derivs) $(ts_utc)"
}

# -----------------------------
# Validações finais
# -----------------------------
validate_online_sources() {
  say "Validando HTTP 200 para pointer.json"
  local OHL IND SIG
  OHL="$(jq -r '.ohlcv_url' public/pointer.json)"
  IND="$(jq -r '.indicators_url' public/pointer.json)"
  SIG="$(jq -r '.signals_url' public/pointer.json)"

  for U in "$OHL" "$IND" "$SIG"; do
    local code
    code="$(curl -fsSI "$U" | head -n1 | awk '{print $2}')"
    [[ "$code" == "200" ]] || die "pointer alvo sem HTTP 200: $U (status=$code)"
  done

  say "Validando HTTP 200 para n_signals_v1_latest.json via pointer_signals_v1.json"
  local PNT SIG_V1
  PNT="public/pointer_signals_v1.json"
  [[ -f "$PNT" ]] || die "arquivo não encontrado: $PNT"
  SIG_V1="$(jq -r '.signals_url' "$PNT")"
  local code2
  code2="$(curl -fsSI "$SIG_V1" | head -n1 | awk '{print $2}')"
  [[ "$code2" == "200" ]] || die "signals_url sem HTTP 200: $SIG_V1 (status=$code2)"
}

validate_eq_complete() {
  say "Validando EQ (close, pct7, close_at, priceguard)"
  local SIG_V1
  SIG_V1="$(jq -r '.signals_url' public/pointer_signals_v1.json)"

  local report
  report="$(curl -fsSL "$SIG_V1" | jq -r '
    [.universe[] | select(.asset_type=="eq")
     | {close_missing:(.price_now_close==null),
        pct7_missing:(.pct_chg_7d==null),
        close_at_missing:(.price_now_close_at_utc==null),
        pg:.validation.priceguard}]
    | {total:length,
       close_missing: map(select(.close_missing))|length,
       pct7_missing: map(select(.pct7_missing))|length,
       close_at_missing: map(select(.close_at_missing))|length,
       pg_fail: map(select(.pg=="FAIL"))|length }')"

  say "EQ summary: $report"

  local cm pm cam pf
  cm="$(jq -r '.close_missing' <<<"$report")"
  pm="$(jq -r '.pct7_missing'  <<<"$report")"
  cam="$(jq -r '.close_at_missing' <<<"$report")"
  pf="$(jq -r '.pg_fail' <<<"$report")"

  if [[ "$cm" != "0" || "$pm" != "0" || "$cam" != "0" || "$pf" != "0" ]]; then
    die "EQ inválido (faltas encontradas). Detalhe: $report"
  fi
}

validate_cr_derivs() {
  say "Validando CR (funding / oi_chg_3d_pct)"
  local SIG_V1
  SIG_V1="$(jq -r '.signals_url' public/pointer_signals_v1.json)"

  local report
  report="$(curl -fsSL "$SIG_V1" | jq -r '
    [.universe[] | select(.asset_type=="crypto")
     | {sym:.symbol_canonical, funding:(.derivatives.funding // null), oi3d:(.derivatives.oi_chg_3d_pct // null)}]
    | {total:length,
       missing_funding: map(select(.funding==null))|length,
       missing_oi3d:    map(select(.oi3d==null))|length }')"

  say "CR summary: $report"

  local mf mo
  mf="$(jq -r '.missing_funding' <<<"$report")"
  mo="$(jq -r '.missing_oi3d' <<<"$report")"

  if [[ "$ALLOW_CR_MISSING" == "0" ]]; then
    if [[ "$mf" != "0" || "$mo" != "0" ]]; then
      die "CR derivativos incompletos (funding/oi3d faltando). Detalhe: $report"
    fi
  else
    say "AVISO: CR com faltas, mas ALLOW_CR_MISSING=$ALLOW_CR_MISSING (não falha)."
  fi
}

validate_freshness() {
  say "Validando frescor/headers do v1"
  local SIG_V1 meta
  SIG_V1="$(jq -r '.signals_url' public/pointer_signals_v1.json)"
  meta="$(curl -fsSL "$SIG_V1" | jq -r '{run_id, generated_at_brt, clock, counts:{signals:(.signals//[]|length), universe:(.universe//[]|length)}}')"
  say "HEADERS: $meta"
}

# -----------------------------
# Main
# -----------------------------
main() {
  need python; need jq; need curl; need git

  step_publish_raw
  step_enrich_ohlcv
  step_publish_pointer_targets

  step_export_v1_and_pointer
  step_enrich_derivatives

  validate_online_sources
  validate_eq_complete
  validate_cr_derivs
  validate_freshness

  say "✅ Pipeline concluído com sucesso."
}

main "$@"

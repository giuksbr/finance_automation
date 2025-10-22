#!/usr/bin/env bash
set -euo pipefail

# 1) gerar sinais (ajuste flags se preferir)
python -m src.export_signals_v1 --with-universe

# 2) pegar o arquivo mais recente
LATEST="$(ls -t public/n_signals_v1_*.json | head -1)"

# 3) atualizar pointer para esse arquivo
python -m src.update_pointer_signals_v1

# 4) publicar ambos (sinais + pointer)
git add "$LATEST" public/pointer_signals_v1.json
git commit -m "publish(signals_v1): $LATEST + update pointer"
git push

# 5) validação simples
SIG_URL=$(jq -r '.signals_url' public/pointer_signals_v1.json)
echo "[info] signals_url => $SIG_URL"
curl -fsSL "$SIG_URL" | jq '{schema_version, generated_at_brt, signals:(.signals//[]|length), universe:(.universe//[]|length)}'

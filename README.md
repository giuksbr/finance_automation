![coverage-check](https://github.com/giuksbr/finance_automation/actions/workflows/coverage_check.yml/badge.svg)

# Pipeline automático de N-níveis (OHLCV 7d→10d + sinais) — **modo público (zero custo)**

Este starter roda localmente ou via GitHub Actions e **publica JSONs no próprio repositório** (pasta `public/`).
O consumo é por **URLs raw do GitHub** (públicas). Não há uso de AWS/R2.

- Lê o **feed canônico** (watchlists Whitelist + Candidate Pool).
- Coleta OHLCV 7→10 dias (EQ/ETF: Stooq, backup Yahoo | CR: Binance, backup CoinGecko).
- Aplica **PriceGuard 2/3** (EQ/ETF 0,8%; CR 0,35%; sanity se 1 fonte).
- Calcula **RSI(14)**, **ATR(14)**, **BB(20,2)**.
- Avalia **N1/N2/N3/N3C** (fallback sem derivativos).
- Publica `ohlcv_cache_*.json`, `indicators_*.json`, `n_signals_*.json` em `public/` + `public/pointer.json` (aponta para os mais recentes).

## Rodar local
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m src.job

git add public/ out/pointer.json
git commit -m "nlevels: primeira publicação"
git push
```

## Rodar no GitHub Actions
1) Ajuste `config.yaml` → `storage.raw_base_url` para o seu repo.  
2) (Opcional) Secret `FEED_URL` para sobrepor o feed.  
3) Workflow agenda 10:40/16:45 BRT (úteis) e permite run manual.  
4) Faz commit/push dos JSONs automaticamente.

## Estrutura
```
.
├─ .github/workflows/nlevels.yml
├─ public/
├─ config.yaml
├─ requirements.txt
├─ src/
│  ├─ job.py  ├─ feed.py  ├─ mapping.py  ├─ fetch_eq.py  ├─ fetch_cr.py
│  ├─ priceguard.py  ├─ indicators.py  ├─ signals.py  └─ storage.py
├─ tests/test_indicators.py
└─ .gitignore
```

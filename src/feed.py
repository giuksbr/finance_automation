#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
feed.py — leitura do feed JSON com fallback local.

Objetivo:
- Tenta baixar o feed em tempo real do GitHub (sem cache web).
- Se houver erro (ex.: 429 Too Many Requests), usa fallback local.
- Em sucesso remoto, salva uma cópia local "last-good" para futuros fallbacks.
- Mantém API pública fetch_feed(url) e (agora) extract_watchlists(feed) → **dict**.

Como define o caminho local:
1) Variável de ambiente FEED_LOCAL_PATH (se existir)
2) Chave `local_feed_path` no config.yaml (opcional)
3) Padrão: ./out/last_good_feed.json
"""

from __future__ import annotations

import os
import sys
import json
import time
import random
import logging
from typing import Any, Dict, List, Tuple, Optional

import requests

try:
    import yaml  # type: ignore
except Exception:
    yaml = None  # yaml é opcional; se ausente, usamos defaults


# ---------- Config & Consts ----------

_DEFAULT_LOCAL_PATH = os.environ.get("FEED_LOCAL_PATH", os.path.join("out", "last_good_feed.json"))
_DEFAULT_TIMEOUT = float(os.environ.get("FEED_TIMEOUT_S", "6"))
_DEFAULT_RETRIES = int(os.environ.get("FEED_MAX_RETRIES", "2"))  # tentativas extras (além da primeira)
_USER_AGENT = os.environ.get(
    "FEED_USER_AGENT",
    "finance_automation/1.0 (+local-first); Python requests"
)

LOG = logging.getLogger("feed")
if not LOG.handlers:
    h = logging.StreamHandler(sys.stderr)
    fmt = logging.Formatter("[%(asctime)s] %(levelname)s feed: %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ")
    h.setFormatter(fmt)
    LOG.addHandler(h)
LOG.setLevel(logging.INFO)


# ---------- Utils ----------

def _ensure_dir(path: str) -> None:
    d = os.path.dirname(os.path.abspath(path))
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def _load_config_yaml() -> Dict[str, Any]:
    """
    Lê config.yaml se existir na raiz do repo.
    Retorna {} se ausente ou se PyYAML não estiver disponível.
    """
    if yaml is None:
        return {}
    candidates = ["config.yaml", "./config.yaml"]
    for p in candidates:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    return yaml.safe_load(f) or {}
            except Exception as e:
                LOG.warning(f"Falha lendo {p}: {e}")
    return {}


def _get_local_path_from_config(default_path: str) -> str:
    cfg = _load_config_yaml()
    # Permite definir em config.yaml:
    # local_feed_path: ./out/last_good_feed.json
    local_cfg = (cfg.get("local_feed_path") if isinstance(cfg, dict) else None) or default_path
    return str(local_cfg)


def _save_json(path: str, data: Dict[str, Any]) -> None:
    _ensure_dir(path)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"), sort_keys=False)
    os.replace(tmp, path)


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _http_get_json(url: str, timeout: float, max_retries: int) -> Dict[str, Any]:
    """
    GET sem cache, com tratamento de 429 (Retry-After) e backoff leve.
    Retorna JSON ou levanta exceção se não conseguir.
    """
    sess = requests.Session()
    headers = {"User-Agent": _USER_AGENT, "Accept": "application/json"}
    attempt = 0
    last_exc: Optional[Exception] = None

    while attempt <= max_retries:
        try:
            resp = sess.get(url, headers=headers, timeout=timeout)
            # 429: respeitar Retry-After se vier
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_s = None
                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except ValueError:
                        sleep_s = None
                if sleep_s is None:
                    # backoff exponencial leve + jitter
                    sleep_s = min(1.5 * (2 ** attempt), 15) + random.uniform(0, 0.8)
                LOG.warning(f"HTTP 429 do GitHub; aguardando {sleep_s:.1f}s antes de tentar novamente…")
                time.sleep(sleep_s)
                attempt += 1
                continue

            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            last_exc = e
            if attempt >= max_retries:
                break
            sleep_s = min(1.2 * (2 ** attempt), 10) + random.uniform(0, 0.5)
            LOG.warning(f"Falha ao baixar feed ({e}); retry em {sleep_s:.1f}s…")
            time.sleep(sleep_s)
            attempt += 1

    # Se chegou aqui, não conseguiu
    assert last_exc is not None
    raise last_exc


# ---------- API pública ----------

def fetch_feed(url: str,
               *,
               local_path: Optional[str] = None,
               timeout_s: float = _DEFAULT_TIMEOUT,
               max_retries: int = _DEFAULT_RETRIES) -> Dict[str, Any]:
    """
    Tenta baixar o feed em tempo real de `url`.
    Se falhar (ex.: 429), tenta carregar do `local_path` (fallback).
    Em caso de sucesso remoto, salva/atualiza `local_path`.

    Parâmetros:
      - url: URL do feed (GitHub raw).
      - local_path: caminho da cópia local (se None, resolve por config/env/default).
      - timeout_s: timeout por requisição.
      - max_retries: tentativas extras em falha transitória (ex.: 429).

    Retorna:
      - dict com o conteúdo do feed.

    Levanta:
      - requests.exceptions.HTTPError (ou outra Exception) SE também não houver fallback local válido.
    """
    # Resolve caminho local
    local = local_path or _get_local_path_from_config(_DEFAULT_LOCAL_PATH)

    # 1) tenta online (sem cache)
    try:
        LOG.info(f"Tentando baixar feed em tempo real: {url}")
        data = _http_get_json(url, timeout=timeout_s, max_retries=max_retries)
        # salvamos cópia local last-good
        try:
            _save_json(local, data)
            LOG.info(f"Feed baixado com sucesso. Cópia local atualizada em: {local}")
        except Exception as e:
            LOG.warning(f"Não foi possível salvar cópia local em '{local}': {e}")
        return data
    except Exception as online_err:
        LOG.error(f"Falha no feed online ({type(online_err).__name__}: {online_err}) — tentando fallback local…")

    # 2) fallback local
    try:
        if os.path.exists(local):
            data = _load_json(local)
            LOG.info(f"Usando fallback local do feed: {local}")
            return data
        else:
            raise FileNotFoundError(f"Fallback local não encontrado: {local}")
    except Exception as local_err:
        # nenhuma das opções funcionou → propagar erro mais claro
        msg = (
            "Não foi possível obter o feed.\n"
            f"- URL: {url}\n"
            f"- Fallback local: {local}\n"
            f"Detalhe: online e fallback falharam. Verifique conectividade/limites do GitHub ou gere um snapshot local."
        )
        raise RuntimeError(msg) from local_err


def _uniq(seq: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for s in seq:
        if not s:
            continue
        s2 = str(s).strip()
        if s2 and s2 not in seen:
            seen.add(s2)
            out.append(s2)
    return out


def extract_watchlists(feed: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Extrai watchlists em formato **dicionário** (compatível com .get("eq")).

    Retorna um dict com:
      {
        "eq": [...],            # equities/ETFs
        "cr": [...],            # crypto
        "all": [...],           # eq + cr (únicos)
        "candidate_pool": [...] # se existir no feed
      }

    Aceita formatos:
    1) feed["watchlists"] = {"eq":[...], "cr":[...]}  (preferido)
    2) feed["watchlists"] = {"whitelist":[...]}       (mistos; tenta separar eq/cr por prefixo)
    3) feed["whitelist"] / feed["candidate_pool"] no topo (legado)
    4) Se nada casar, tudo cai em "all".
    """
    out: Dict[str, List[str]] = {"eq": [], "cr": [], "all": [], "candidate_pool": []}
    if not isinstance(feed, dict):
        return out

    wl_node = feed.get("watchlists", {})
    cp_node = []
    # candidate_pool pode estar dentro de watchlists ou no topo
    if isinstance(wl_node, dict):
        cp_node = wl_node.get("candidate_pool", []) or []
        # Caso 1: já vier eq/cr
        eq = wl_node.get("eq", []) or []
        cr = wl_node.get("cr", []) or []
        if eq or cr:
            out["eq"] = _uniq([str(x) for x in eq])
            out["cr"] = _uniq([str(x) for x in cr])
        else:
            # Caso 2: só "whitelist" misto
            mixed = wl_node.get("whitelist", []) or []
            if mixed:
                # Heurística simples: separa CR se contiver ":" com prefixo típico de cripto (ex.: BINANCE:)
                eq_guess, cr_guess = [], []
                for s in mixed:
                    s2 = str(s).strip()
                    if ":" in s2:
                        # heurística: se começar com "BINANCE:" ou for ALLCAPS sem mercado típico de equities, classifica como cr
                        prefix = s2.split(":", 1)[0].upper()
                        if prefix in {"BINANCE", "CRYPTO", "GATE", "KRAKEN", "COINBASE"}:
                            cr_guess.append(s2)
                        else:
                            eq_guess.append(s2)
                    else:
                        eq_guess.append(s2)
                out["eq"] = _uniq(eq_guess)
                out["cr"] = _uniq(cr_guess)

    # Caso 3: topo legado
    if not out["eq"] and isinstance(feed.get("whitelist"), list):
        out["eq"] = _uniq([str(x) for x in (feed.get("whitelist") or [])])
    if not cp_node and isinstance(feed.get("candidate_pool"), list):
        cp_node = feed.get("candidate_pool") or []

    out["candidate_pool"] = _uniq([str(x) for x in cp_node])
    out["all"] = _uniq(out["eq"] + out["cr"])

    return out


def extract_watchlists_tuple(feed: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """
    Versão alternativa (legada) que retorna (whitelist, candidate_pool).
    Útil apenas se algum código antigo precisar de tupla.
    """
    wl_dict = extract_watchlists(feed)
    wl = wl_dict.get("all", []) or (wl_dict.get("eq", []) + wl_dict.get("cr", []))
    cp = wl_dict.get("candidate_pool", [])
    return wl, cp

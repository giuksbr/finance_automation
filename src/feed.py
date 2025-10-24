#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
feed.py — leitura do feed JSON com fallback local + fallback de WATCHLISTS.

Fluxo:
1) fetch_feed(url): tenta baixar em tempo real; se falhar (ex.: 429), usa cópia local last-good.
   - Sucesso remoto → salva/atualiza cópia local.

2) extract_watchlists(feed): tenta extrair eq/cr do feed.
   - Se eq/cr vierem vazios → tenta carregar de um arquivo local (watchlists_local.json).
     • Ordem de resolução do caminho:
       a) env WATCHLISTS_LOCAL
       b) config.yaml: local_watchlists_path
       c) padrão: ./out/watchlists_local.json
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
    yaml = None  # yaml é opcional


# ---------- Config & Consts ----------

_DEFAULT_LOCAL_FEED = os.environ.get("FEED_LOCAL_PATH", os.path.join("out", "last_good_feed.json"))
_DEFAULT_TIMEOUT = float(os.environ.get("FEED_TIMEOUT_S", "6"))
_DEFAULT_RETRIES = int(os.environ.get("FEED_MAX_RETRIES", "2"))  # tentativas extras (além da primeira)
_USER_AGENT = os.environ.get(
    "FEED_USER_AGENT",
    "finance_automation/1.0 (+local-first); Python requests"
)

# watchlists locais (fallback)
_DEFAULT_LOCAL_WL = os.environ.get("WATCHLISTS_LOCAL", os.path.join("out", "watchlists_local.json"))

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
    """Lê config.yaml se existir na raiz do repo; {} se ausente."""
    if yaml is None:
        return {}
    for p in ("config.yaml", "./config.yaml"):
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    return yaml.safe_load(f) or {}
            except Exception as e:
                LOG.warning(f"Falha lendo {p}: {e}")
    return {}


def _get_local_feed_path(default_path: str) -> str:
    cfg = _load_config_yaml()
    return str((cfg.get("local_feed_path") if isinstance(cfg, dict) else None) or default_path)


def _get_local_wl_path(default_path: str) -> str:
    cfg = _load_config_yaml()
    return str((cfg.get("local_watchlists_path") if isinstance(cfg, dict) else None) or default_path)


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
    """GET sem cache, com tratamento de 429 (Retry-After) e backoff leve."""
    sess = requests.Session()
    headers = {"User-Agent": _USER_AGENT, "Accept": "application/json"}
    attempt = 0
    last_exc: Optional[Exception] = None

    while attempt <= max_retries:
        try:
            resp = sess.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except ValueError:
                        sleep_s = None
                else:
                    sleep_s = None
                if sleep_s is None:
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

    assert last_exc is not None
    raise last_exc


# ---------- API pública: feed ----------

def fetch_feed(url: str,
               *,
               local_path: Optional[str] = None,
               timeout_s: float = _DEFAULT_TIMEOUT,
               max_retries: int = _DEFAULT_RETRIES) -> Dict[str, Any]:
    """
    Tenta baixar o feed em tempo real de `url`.
    Se falhar (ex.: 429), tenta carregar do `local_path` (fallback).
    Sucesso remoto → salva/atualiza `local_path`.
    """
    local = local_path or _get_local_feed_path(_DEFAULT_LOCAL_FEED)

    try:
        LOG.info(f"Tentando baixar feed em tempo real: {url}")
        data = _http_get_json(url, timeout=timeout_s, max_retries=max_retries)
        try:
            _save_json(local, data)
            LOG.info(f"Feed baixado com sucesso. Cópia local atualizada em: {local}")
        except Exception as e:
            LOG.warning(f"Não foi possível salvar cópia local em '{local}': {e}")
        return data
    except Exception as online_err:
        LOG.error(f"Falha no feed online ({type(online_err).__name__}: {online_err}) — tentando fallback local…")

    if os.path.exists(local):
        data = _load_json(local)
        LOG.info(f"Usando fallback local do feed: {local}")
        return data

    msg = (
        "Não foi possível obter o feed.\n"
        f"- URL: {url}\n"
        f"- Fallback local: {local}\n"
        f"Detalhe: online e fallback falharam. Gere um snapshot local válido."
    )
    raise RuntimeError(msg)


# ---------- Helpers de watchlists ----------

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


def _extract_eq_cr_from_feed(feed: Dict[str, Any]) -> Dict[str, List[str]]:
    out = {"eq": [], "cr": []}
    if not isinstance(feed, dict):
        return out

    wl_node = feed.get("watchlists", {})
    if isinstance(wl_node, dict):
        # caso preferido
        eq = wl_node.get("eq", []) or []
        cr = wl_node.get("cr", []) or []
        if eq or cr:
            out["eq"] = _uniq([str(x) for x in eq])
            out["cr"] = _uniq([str(x) for x in cr])
            return out

        # whitelist misto (heurística simples)
        mixed = wl_node.get("whitelist", []) or []
        if mixed:
            eq_guess, cr_guess = [], []
            for s in mixed:
                s2 = str(s).strip()
                if ":" in s2:
                    prefix = s2.split(":", 1)[0].upper()
                    if prefix in {"BINANCE", "CRYPTO", "GATE", "KRAKEN", "COINBASE"}:
                        cr_guess.append(s2)
                    else:
                        eq_guess.append(s2)
                else:
                    eq_guess.append(s2)
            out["eq"] = _uniq(eq_guess)
            out["cr"] = _uniq(cr_guess)
            return out

    # formatos legados no topo
    if isinstance(feed.get("whitelist"), list):
        out["eq"] = _uniq([str(x) for x in (feed.get("whitelist") or [])])

    # candidate_pool pode estar em watchlists ou no topo — não usado aqui para eq/cr
    return out


def _load_local_watchlists(path: str) -> Dict[str, List[str]]:
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f) or {}
    eq = obj.get("eq") or []
    cr = obj.get("cr") or []
    return {"eq": _uniq([str(x) for x in eq]), "cr": _uniq([str(x) for x in cr])}


# ---------- API pública: watchlists ----------

def extract_watchlists(feed: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Retorna:
      {
        "eq": [...],
        "cr": [...],
        "all": [...],
        "candidate_pool": [...]
      }
    Estratégia:
      1) tenta extrair do feed;
      2) se eq/cr vazios → tenta arquivo local (watchlists_local.json).
    """
    # 1) do feed
    eq_cr = _extract_eq_cr_from_feed(feed)
    eq = eq_cr.get("eq", []) or []
    cr = eq_cr.get("cr", []) or []

    # candidato pool (se existir no feed)
    cp: List[str] = []
    wl_node = feed.get("watchlists", {}) if isinstance(feed, dict) else {}
    if isinstance(wl_node, dict) and isinstance(wl_node.get("candidate_pool"), list):
        cp = _uniq([str(x) for x in (wl_node.get("candidate_pool") or [])])
    elif isinstance(feed.get("candidate_pool"), list):
        cp = _uniq([str(x) for x in (feed.get("candidate_pool") or [])])

    # 2) fallback local se eq/cr vieram vazios
    if not eq and not cr:
        local_wl = _get_local_wl_path(_DEFAULT_LOCAL_WL)
        if os.path.exists(local_wl):
            try:
                wl_local = _load_local_watchlists(local_wl)
                eq = wl_local.get("eq", []) or []
                cr = wl_local.get("cr", []) or []
                LOG.info(f"Watchlists do feed ausentes/vazias — usando fallback local: {local_wl} (eq={len(eq)} cr={len(cr)})")
            except Exception as e:
                LOG.warning(f"Falha ao ler fallback local de watchlists '{local_wl}': {e}")

    all_syms = _uniq(eq + cr)
    return {"eq": eq, "cr": cr, "all": all_syms, "candidate_pool": cp}


def extract_watchlists_tuple(feed: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    wl = extract_watchlists(feed)
    return wl.get("all", []), wl.get("candidate_pool", [])

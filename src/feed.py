#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
feed.py — leitura do feed JSON com fallback local + fallback de WATCHLISTS e
extração robusta das listas (eq/cr) a partir de múltiplos formatos.

Novidades:
- Suporte a vários esquemas de watchlists:
    .watchlists.eq / .watchlists.cr
    .watchlists.whitelist (lista mista)
    .universe.eq / .universe.cr
    .symbols.eq / .symbols.cr
    .eq / .cr (top-level)
    .whitelist (lista mista)
- Suporte a caminho configurável em config.yaml:
    feed_watchlists_path:
      eq: "universe.eq"       # caminho para a lista de eq
      cr: "universe.cr"       # caminho para a lista de cr
  (ou por variável de ambiente FEED_WATCHLISTS_EQ_PATH / FEED_WATCHLISTS_CR_PATH)
- Logs claros dizendo de onde veio cada lista.
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


_DEFAULT_LOCAL_FEED = os.environ.get("FEED_LOCAL_PATH", os.path.join("out", "last_good_feed.json"))
_DEFAULT_TIMEOUT = float(os.environ.get("FEED_TIMEOUT_S", "6"))
_DEFAULT_RETRIES = int(os.environ.get("FEED_MAX_RETRIES", "2"))
_USER_AGENT = os.environ.get("FEED_USER_AGENT", "finance_automation/1.0 (+local-first); Python requests")

_DEFAULT_LOCAL_WL = os.environ.get("WATCHLISTS_LOCAL", os.path.join("out", "watchlists_local.json"))

LOG = logging.getLogger("feed")
if not LOG.handlers:
    h = logging.StreamHandler(sys.stderr)
    fmt = logging.Formatter("[%(asctime)s] %(levelname)s feed: %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ")
    h.setFormatter(fmt)
    LOG.addHandler(h)
LOG.setLevel(logging.INFO)


# ---------------- Utils ----------------

def _ensure_dir(path: str) -> None:
    d = os.path.dirname(os.path.abspath(path))
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def _load_config_yaml() -> Dict[str, Any]:
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


def _get_by_path(obj: Any, path: str) -> Any:
    """
    Caminho pontuado simples: "a.b.c".
    Retorna None se em algum passo não existir.
    """
    cur = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur


# ---------------- API: feed ----------------

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


# ---------------- Extração de watchlists ----------------

def _parse_mixed_list(mixed: List[str]) -> Tuple[List[str], List[str]]:
    """Heurística: separa provável cr vs eq pela presença de prefixos de exchange cripto."""
    if not mixed:
        return [], []
    cr_prefixes = {"BINANCE", "CRYPTO", "GATE", "KRAKEN", "COINBASE", "BYBIT", "OKX"}
    eq_guess, cr_guess = [], []
    for s in mixed:
        s2 = str(s).strip()
        if ":" in s2:
            prefix = s2.split(":", 1)[0].upper()
            if prefix in cr_prefixes:
                cr_guess.append(s2)
            else:
                eq_guess.append(s2)
        else:
            eq_guess.append(s2)
    return _uniq(eq_guess), _uniq(cr_guess)


def _try_via_config(feed: Dict[str, Any]) -> Tuple[List[str], List[str], str]:
    cfg = _load_config_yaml() or {}
    eq_path = os.environ.get("FEED_WATCHLISTS_EQ_PATH") or (cfg.get("feed_watchlists_path", {}) or {}).get("eq")
    cr_path = os.environ.get("FEED_WATCHLISTS_CR_PATH") or (cfg.get("feed_watchlists_path", {}) or {}).get("cr")
    if not eq_path and not cr_path:
        return [], [], ""

    eq = _get_by_path(feed, eq_path) if eq_path else None
    cr = _get_by_path(feed, cr_path) if cr_path else None

    eq_list = _uniq([str(x) for x in (eq or [])]) if isinstance(eq, list) else []
    cr_list = _uniq([str(x) for x in (cr or [])]) if isinstance(cr, list) else []

    if eq_list or cr_list:
        LOG.info(f"Watchlists extraídas via config.yaml (eq_path='{eq_path}' cr_path='{cr_path}')")
        return eq_list, cr_list, "config"

    return [], [], ""


def _extract_watchlists_from_known_shapes(feed: Dict[str, Any]) -> Tuple[List[str], List[str], str]:
    """
    Tenta diversos formatos conhecidos, retornando (eq, cr, origem).
    """
    # 1) .watchlists.eq/cr
    wl = feed.get("watchlists")
    if isinstance(wl, dict):
        eq = wl.get("eq") or []
        cr = wl.get("cr") or []
        if isinstance(eq, list) or isinstance(cr, list):
            eq_list = _uniq([str(x) for x in (eq or [])]) if isinstance(eq, list) else []
            cr_list = _uniq([str(x) for x in (cr or [])]) if isinstance(cr, list) else []
            if eq_list or cr_list:
                return eq_list, cr_list, "watchlists.eq/cr"

        # 1.1) .watchlists.whitelist (misto)
        mix = wl.get("whitelist") or []
        if isinstance(mix, list) and mix:
            eq_list, cr_list = _parse_mixed_list([str(x) for x in mix])
            if eq_list or cr_list:
                return eq_list, cr_list, "watchlists.whitelist"

    # 2) .universe.eq/cr
    u = feed.get("universe")
    if isinstance(u, dict):
        eq = u.get("eq") or []
        cr = u.get("cr") or []
        if isinstance(eq, list) or isinstance(cr, list):
            eq_list = _uniq([str(x) for x in (eq or [])]) if isinstance(eq, list) else []
            cr_list = _uniq([str(x) for x in (cr or [])]) if isinstance(cr, list) else []
            if eq_list or cr_list:
                return eq_list, cr_list, "universe.eq/cr"

    # 3) .symbols.eq/cr
    s = feed.get("symbols")
    if isinstance(s, dict):
        eq = s.get("eq") or []
        cr = s.get("cr") or []
        if isinstance(eq, list) or isinstance(cr, list):
            eq_list = _uniq([str(x) for x in (eq or [])]) if isinstance(eq, list) else []
            cr_list = _uniq([str(x) for x in (cr or [])]) if isinstance(cr, list) else []
            if eq_list or cr_list:
                return eq_list, cr_list, "symbols.eq/cr"

    # 4) .eq / .cr (top-level)
    eq = feed.get("eq")
    cr = feed.get("cr")
    if isinstance(eq, list) or isinstance(cr, list):
        eq_list = _uniq([str(x) for x in (eq or [])]) if isinstance(eq, list) else []
        cr_list = _uniq([str(x) for x in (cr or [])]) if isinstance(cr, list) else []
        if eq_list or cr_list:
            return eq_list, cr_list, "top.eq/cr"

    # 5) .whitelist (misto no topo)
    wl2 = feed.get("whitelist")
    if isinstance(wl2, list) and wl2:
        eq_list, cr_list = _parse_mixed_list([str(x) for x in wl2])
        if eq_list or cr_list:
            return eq_list, cr_list, "top.whitelist"

    return [], [], ""


def _load_local_watchlists(path: str) -> Dict[str, List[str]]:
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f) or {}
    eq = obj.get("eq") or []
    cr = obj.get("cr") or []
    return {"eq": _uniq([str(x) for x in eq]), "cr": _uniq([str(x) for x in cr])}


def extract_watchlists(feed: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Retorna:
      {"eq":[...], "cr":[...], "all":[...], "candidate_pool":[...]}
    Estratégia:
      1) tenta via config (caminhos explícitos);
      2) tenta formatos conhecidos;
      3) se ainda vazio → fallback local (out/watchlists_local.json).
    """
    # 1) via config
    eq, cr, origin = _try_via_config(feed)
    if not (eq or cr):
        # 2) formatos conhecidos
        eq, cr, origin = _extract_watchlists_from_known_shapes(feed)

    cp: List[str] = []
    # candidate_pool em caminhos usuais
    for path in ("watchlists.candidate_pool", "candidate_pool", "universe.candidate_pool"):
        node = _get_by_path(feed, path)
        if isinstance(node, list) and node:
            cp = _uniq([str(x) for x in node])
            break

    if eq or cr:
        LOG.info(f"Watchlists extraídas do feed ({origin}) — eq={len(eq)} cr={len(cr)}")
        return {"eq": eq, "cr": cr, "all": _uniq(eq + cr), "candidate_pool": cp}

    # 3) fallback local
    local_wl = _get_local_wl_path(_DEFAULT_LOCAL_WL)
    if os.path.exists(local_wl):
        try:
            wl_local = _load_local_watchlists(local_wl)
            eq = wl_local.get("eq", []) or []
            cr = wl_local.get("cr", []) or []
            LOG.info(f"Watchlists do feed ausentes/vazias — usando fallback local: {local_wl} (eq={len(eq)} cr={len(cr)})")
        except Exception as e:
            LOG.warning(f"Falha ao ler fallback local de watchlists '{local_wl}': {e}")

    return {"eq": eq, "cr": cr, "all": _uniq(eq + cr), "candidate_pool": cp}


def extract_watchlists_tuple(feed: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    wl = extract_watchlists(feed)
    return wl.get("all", []), wl.get("candidate_pool", [])

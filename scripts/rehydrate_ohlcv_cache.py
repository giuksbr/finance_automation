#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rehydrate OHLCV caches:
- Converte campos 'eq' e 'cr' de string JSON -> objeto (dict)
- Tolera arquivo inteiro como string JSON (dupla-serialização no root)
- Mantém demais campos inalterados
- Escreve de volta com separadores compactos
"""

import json
import sys
import glob
from pathlib import Path
from typing import Any, Dict

def _fromjson_maybe(v: Any) -> Any:
    """Se v for string JSON, tenta fazer json.loads(v); senão retorna v."""
    if isinstance(v, str):
        v = v.strip()
        # heurística: tem que começar com { ou [
        if (v.startswith("{") and v.endswith("}")) or (v.startswith("[") and v.endswith("]")):
            try:
                return json.loads(v)
            except Exception:
                return v
    return v

def _ensure_dict(v: Any) -> Dict[str, Any]:
    """Garante que o valor seja um dict; se None, vira {}; se lista/string, tenta normalizar."""
    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    # tentar desserializar se for string JSON
    v2 = _fromjson_maybe(v)
    if isinstance(v2, dict):
        return v2
    # fallback final
    return {}

def rehydrate_file(path: Path) -> bool:
    """Rehidrata um arquivo; retorna True se alterou algo."""
    raw = path.read_text(encoding="utf-8")
    changed = False

    # 1) Se o arquivo TODO for uma string JSON, desserializa
    obj: Any
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        # pode ser que o arquivo foi salvo como string com aspas
        raw_stripped = raw.strip()
        if raw_stripped.startswith('"') and raw_stripped.endswith('"'):
            try:
                obj = json.loads(json.loads(raw))
                changed = True
            except Exception:
                raise
        else:
            raise

    # 2) Se o root ainda for string, tentar desserializar
    if isinstance(obj, str):
        obj2 = _fromjson_maybe(obj)
        if obj2 is not obj:
            obj = obj2
            changed = True

    # 3) Esperamos um dict na raiz
    if not isinstance(obj, dict):
        # nada que possamos fazer com segurança
        return False

    # 4) Rehidratar eq/cr
    before_eq = obj.get("eq", None)
    before_cr = obj.get("cr", None)

    eq = _ensure_dict(before_eq)
    cr = _ensure_dict(before_cr)

    if eq is not before_eq:
        changed = True
    if cr is not before_cr:
        changed = True

    obj["eq"] = eq
    obj["cr"] = cr

    if changed:
        path.write_text(
            json.dumps(obj, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
    return changed

def main(args):
    # Se passar paths na linha de comando, usa-os; senão, pega últimos 20 do public/
    files = [Path(p) for p in args] if args else sorted(
        glob.glob("public/ohlcv_cache_*.json"), reverse=True
    )[:20]
    if not files:
        print("[rehydrate] nada para fazer (nenhum public/ohlcv_cache_*.json encontrado).")
        return

    touched = 0
    for p in files:
        try:
            changed = rehydrate_file(Path(p))
            status = "FIX" if changed else "OK "
            print(f"[{status}] {p}")
            touched += int(changed)
        except Exception as e:
            print(f"[ERR] {p}: {e}")

    print(f"[rehydrate] arquivos alterados: {touched}")

if __name__ == "__main__":
    main(sys.argv[1:])

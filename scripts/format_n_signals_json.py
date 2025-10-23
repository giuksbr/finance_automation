#!/usr/bin/env python3
"""
Pretty-print dos arquivos n_signals_v1*.json.

- Ordena chaves (estável para diffs)
- Usa indent=2, ensure_ascii=False
- Garante newline final

Uso:
  python scripts/format_n_signals_json.py
  python scripts/format_n_signals_json.py public/n_signals_v1_*.json public/n_signals_v1_latest.json
"""
from __future__ import annotations

import json
import sys
import glob
from pathlib import Path
from typing import Iterable, List


DEFAULT_PATTERNS: List[str] = [
    "public/n_signals_v1_*.json",
    "public/n_signals_v1_latest.json",
]


def iter_targets(args: Iterable[str]) -> Iterable[Path]:
    patterns = list(args) if args else DEFAULT_PATTERNS
    seen = set()
    for pat in patterns:
        for p in glob.glob(pat):
            path = Path(p)
            if path.is_file() and path not in seen:
                seen.add(path)
                yield path


def pretty_write(path: Path) -> bool:
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except Exception as e:
        print(f"[warn] ignorando {path} (não é JSON legível?): {e}", file=sys.stderr)
        return False

    try:
        # dump bonito + newline final
        path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(f"[ok] formatado: {path}")
        return True
    except Exception as e:
        print(f"[err] falha ao escrever {path}: {e}", file=sys.stderr)
        return False


def main() -> int:
    any_changed = False
    for path in iter_targets(sys.argv[1:]):
        changed = pretty_write(path)
        any_changed = any_changed or changed
    return 0 if any_changed else 0


if __name__ == "__main__":
    raise SystemExit(main())

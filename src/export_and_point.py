#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wrapper: exporta n_signals_v1 (com universe + latest) e atualiza o pointer.
"""

import subprocess
import sys

def sh(cmd: str):
    print(f"+ {cmd}")
    subprocess.check_call(cmd, shell=True)

def main():
    sh("python -m src.export_signals_v1 --with-universe --write-latest --update-pointer")

if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        print(f"[erro] comando falhou com código {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)

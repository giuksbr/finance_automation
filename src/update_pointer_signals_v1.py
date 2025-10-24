# src/update_pointer_signals_v1.py
import os
import json
from datetime import datetime, timedelta, timezone

RAW_BASE = os.environ.get(
    "RAW_BASE",
    "https://raw.githubusercontent.com/giuksbr/finance_automation/main",
)

# Prefer config.yaml storage.raw_base_url
def _load_raw_base_from_config() -> str | None:
    cfg_path = os.path.join(os.getcwd(), "config.yaml")
    try:
        import yaml  # already in requirements
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        raw = ((cfg or {}).get("storage") or {}).get("raw_base_url")
        if isinstance(raw, str) and raw.strip():
            return raw.rstrip("/")
    except Exception:
        return None
    return None

_cfg_raw = _load_raw_base_from_config()
if _cfg_raw:
    RAW_BASE = _cfg_raw

def write_json(obj, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def main():
    outdir = "public"
    latest_rel = "public/n_signals_v1_latest.json"
    pointer_path = os.path.join(outdir, "pointer_signals_v1.json")

    gen = datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(hours=-3)
    gen_iso = gen.astimezone(timezone(timedelta(hours=-3))).strftime("%Y-%m-%dT%H:%M:%S-03:00")
    exp = (datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")

    pointer = {
        "version": "1.0",
        "generated_at_brt": gen_iso,
        "signals_url": f"{RAW_BASE}/{latest_rel}",
        "expires_at_utc": exp,
    }
    write_json(pointer, pointer_path)
    print(f"[ok] pointer_signals_v1 atualizado -> {pointer['signals_url']}")

if __name__ == "__main__":
    main()

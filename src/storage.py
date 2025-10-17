import os, json, shutil

def repo_copy_to_public(public_dir: str, out_dir: str, files: dict[str,str]) -> dict[str, str]:
    os.makedirs(public_dir, exist_ok=True)
    out = {}
    for src_name, dst_name in files.items():
        src_path = os.path.join(out_dir, src_name)
        dst_path = os.path.join(public_dir, dst_name)
        shutil.copyfile(src_path, dst_path)
        out[dst_name] = dst_path
    return out

def repo_build_raw_urls(raw_base: str, public_dir: str, basenames: list[str]) -> dict[str, str]:
    base = raw_base.rstrip('/') + '/' + public_dir.strip('/') + '/'
    return {b: base + b for b in basenames}

def publish_pointer_local(pointer_path: str, ohlcv_url: str, indicators_url: str, signals_url: str, expires_at_utc: str):
    payload = {
        "ohlcv_url": ohlcv_url,
        "indicators_url": indicators_url,
        "signals_url": signals_url,
        "expires_at_utc": expires_at_utc,
    }
    os.makedirs(os.path.dirname(pointer_path), exist_ok=True)
    with open(pointer_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

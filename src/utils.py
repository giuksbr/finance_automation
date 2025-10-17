import os, json, datetime, pytz

BRT_TZ = pytz.timezone("America/Sao_Paulo")

def now_brt_iso():
    return datetime.datetime.now(BRT_TZ).strftime("%Y-%m-%dT%H:%M:%S%z")

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def write_json(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

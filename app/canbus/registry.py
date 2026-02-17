import json
from pathlib import Path

PROFILE_DIR = Path(__file__).parent / "profiles"

def load_profile(name):
    try:
        print(f"[DEBUG] Loading profile: {name}")
        with open(PROFILE_DIR / f"{name}.json") as f:
            return json.load(f)
    except:
        with open(PROFILE_DIR / "generic_unknown.json") as f:
            return json.load(f)

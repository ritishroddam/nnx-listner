from .utils import get_pgn
from .decoders import DECODERS

def match_frame(rule, can_id):
    if rule["id_type"] == "pgn":
        return get_pgn(can_id) == rule["id"]
    if rule["id_type"] == "can_id":
        return can_id.upper() == rule["id"].upper()
    return False

def decode_with_profile(frames, profile):
    results = {}
    print(f"[DEBUG] Decoding CAN frames with profile: {profile.get('name', 'unknown')}")
    for frame in frames:
        can_id = frame["id"]
        data = bytes.fromhex(frame["data"])

        for signal, rule in profile["signals"].items():
            if not match_frame(rule, can_id):
                continue

            decoder = DECODERS[rule["type"]]
            results[signal] = decoder(data, rule)
    print(f"[DEBUG] Decoded signals: {results}")
    return results

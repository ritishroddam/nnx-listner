from .utils import get_pgn
from .decoders import DECODERS
from .gear_interpreter import interpret_gear

def match_frame(rule, can_id):
    if rule["id_type"] == "pgn":
        return get_pgn(can_id) == rule["id"]
    if rule["id_type"] == "can_id":
        return can_id.upper() == rule["id"].upper()
    return False

def validate_range(value, rule):
    """Return None if value is outside allowed range"""
    if value is None:
        return None

    min_v = rule.get("min")
    max_v = rule.get("max")

    if min_v is not None and value < min_v:
        return None
    if max_v is not None and value > max_v:
        return None

    return value


def decode_with_profile(frames, profile):
    results = {}
    print(f"[DEBUG] Decoding CAN frames with profile: {profile.get('profile_name', 'unknown')}")
    for frame in frames:
        can_id = frame["id"]
        data = bytes.fromhex(frame["data"])

        for signal, rule in profile["signals"].items():
            if not match_frame(rule, can_id):
                continue

            decoder = DECODERS[rule["type"]]
            value = decoder(data, rule)
            value = validate_range(value, rule)
            if value is not None:
                results[signal] = value
    print(f"[DEBUG] Decoded signals: {results}")
    decoded_signals = interpret_gear(results)
    return decoded_signals

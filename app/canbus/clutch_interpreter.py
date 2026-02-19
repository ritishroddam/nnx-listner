def interpret_clutch(decoded_signals: dict) -> dict:
    val = decoded_signals.get("clutch_pedal_state")

    if val is None:
        return decoded_signals

    if val == 0:
        decoded_signals["clutch_label"] = "released"
    elif val == 1:
        decoded_signals["clutch_label"] = "pressed"
    else:
        # 2 and 3 = not available / error
        decoded_signals.pop("clutch_pedal_state", None)

    return decoded_signals

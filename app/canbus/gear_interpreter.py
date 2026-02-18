def interpret_gear(decoded_signals: dict) -> dict:
    """
    Convert numeric gear into real gear states
    """

    def map_value(val):
        if val is None:
            return None

        # Park (special case from FMS spec)
        if val == 126:
            return "P"

        # Neutral
        if val == 0:
            return "N"

        # Reverse gears
        if val < 0:
            return f"R{abs(val)}"

        # Forward gears
        return f"G{val}"

    if "current_gear" in decoded_signals:
        decoded_signals["current_gear_label"] = map_value(
            decoded_signals["current_gear"]
        )

    if "selected_gear" in decoded_signals:
        decoded_signals["selected_gear_label"] = map_value(
            decoded_signals["selected_gear"]
        )

    return decoded_signals

def interpret_gear(decoded_signals: dict) -> dict:
    """
    Convert numeric gear into real gear states
    """

    def map_value(val, speed):
        if val is None:
            return None
        
        if speed is None:
            return None

        # Park (special case from FMS spec)
        if val == 126 and speed == 0:
            return "P"

        # Neutral
        if val == 0:
            return "N"

        # Reverse gears
        if val < 0 and speed >= 5:
            return f"R{abs(val)}"
        
        if val > 0 and speed >= 5:
            return f"G{val}"

        # Forward gears
        return None
    
    if "wheel_based_speed_kmh" not in decoded_signals:
        decoded_signals.pop("current_gear", None)
        decoded_signals.pop("selected_gear", None)
        return decoded_signals

    if "current_gear" in decoded_signals:
        current_gear_label = map_value(
            decoded_signals["current_gear"], decoded_signals["wheel_based_speed_kmh"]
        )
        if current_gear_label is None:
            decoded_signals.pop("current_gear", None)
        else:
            decoded_signals["current_gear_label"] = current_gear_label

    if "selected_gear" in decoded_signals:
        selected_gear_label= map_value(
            decoded_signals["selected_gear"], decoded_signals["wheel_based_speed_kmh"]
        )
        if selected_gear_label is None:
            decoded_signals.pop("selected_gear", None)
        else:
            decoded_signals["selected_gear_label"] = selected_gear_label
            

    return decoded_signals

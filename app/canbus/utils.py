def get_pgn(can_id_hex: str) -> int:
    can_id = int(can_id_hex, 16)
    return (can_id >> 8) & 0x3FFFF

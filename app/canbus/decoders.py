def decode_uint(data, rule):
    start = rule["start_byte"] - 1
    end = start + rule["length"]
    raw = int.from_bytes(data[start:end], rule.get("byte_order", "little"))
    return raw * rule.get("scale", 1) + rule.get("offset", 0)

def decode_int(data, rule):
    start = rule["start_byte"] - 1
    end = start + rule["length"]
    raw = int.from_bytes(data[start:end], rule.get("byte_order", "little"), signed=True)
    return raw * rule.get("scale", 1)

def decode_bit(data, rule):
    return (data[rule["byte"] - 1] >> rule["bit"]) & 1

def decode_bool(data, rule):
    return bool(decode_bit(data, rule))

def decode_enum(data, rule):
    raw = data[rule["byte"] - 1]
    return rule["map"].get(str(raw))

def decode_bits(data: bytes, rule: dict):
    """
    Extract bit-field from a single byte
    """
    start_byte = rule["start_byte"] - 1
    start_bit  = rule["start_bit"]
    bit_length = rule["bit_length"]

    byte_val = data[start_byte]

    mask = (1 << bit_length) - 1
    value = (byte_val >> start_bit) & mask

    return value

DECODERS = {
    "uint": decode_uint,
    "int": decode_int,
    "bit": decode_bit,
    "bool": decode_bool,
    "enum": decode_enum,
    "bits": decode_bits
}

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

DECODERS = {
    "uint": decode_uint,
    "int": decode_int,
    "bit": decode_bit,
    "bool": decode_bool,
    "enum": decode_enum
}

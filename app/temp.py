# ais140_listener.py
import os
import asyncio
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, Any, List

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import InsertOne, ReplaceOne, ASCENDING, DESCENDING

# -----------------------
# Config
# -----------------------
HOST = "0.0.0.0"
PORT = 8001
READ_TIMEOUT = 300

# Tune bulk flush for your infra (throughput vs latency)
BULK_MAX_DOCS = 500
BULK_MAX_LATENCY_MS = 200

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI env var is required")

DB_NAME = "nnx"
COL_LOC = "atlantaAis140"            # structured history (location/events/health/emergency)
COL_RAW = "rawLogAtlantaAis140"      # raw packets (audit; TTL index applied)
COL_LATEST = "atlantaAis140_latest"  # one doc per IMEI (use IMEI as _id)

# -----------------------
# Mongo (Motor)
# -----------------------
mongo_client: AsyncIOMotorClient = AsyncIOMotorClient(MONGO_URI, tz_aware=True)
db = mongo_client[DB_NAME]
loc_coll = db[COL_LOC]
raw_coll = db[COL_RAW]
latest_coll = db[COL_LATEST]

# -----------------------
# Batching queues (backpressure)
# -----------------------
loc_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=50_000)
raw_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=50_000)
latest_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=50_000)

# -----------------------
# AIS 140 parsing helpers
# -----------------------

def _ddmmyyyy_hhmmss_to_utc(date_str: str, time_str: str) -> Optional[datetime]:
    # AIS 140 gives GPS time in UTC (ddmmyyyy, hhmmss)
    if not date_str or not time_str or len(date_str) != 8 or len(time_str) < 6:
        return None
    try:
        day = int(date_str[0:2]); month = int(date_str[2:4]); year = int(date_str[4:8])
        hour = int(time_str[0:2]); minute = int(time_str[2:4]); second = int(time_str[4:6])
        return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    except Exception:
        return None

def _to_float(val: Optional[str]) -> Optional[float]:
    try:
        return float(val) if val not in (None, "") else None
    except Exception:
        return None

def _to_int(val: Optional[str]) -> Optional[int]:
    try:
        return int(val) if val not in (None, "") else None
    except Exception:
        return None

def _ns_ew_to_signed(lat: Optional[float], ns: str, lon: Optional[float], ew: str) -> Tuple[Optional[float], Optional[float]]:
    if lat is not None and ns:
        lat = abs(lat) if ns.upper() == "N" else -abs(lat)
    if lon is not None and ew:
        lon = abs(lon) if ew.upper() == "E" else -abs(lon)
    return lat, lon

def parse_packet(raw: str) -> Dict[str, Any]:
    """
    Minimal, robust parsing for:
      - CP: Location/Event (Table-4/5 in AIS140)
      - HP: Health (Table-2)
      - EPB: Emergency (EMR/SEM)
    Keep a lean schema optimized for common queries.
    """
    pkt: Dict[str, Any] = {"raw": raw, "ingestedAt": datetime.now(timezone.utc)}
    raw = raw.strip()
    if not raw.startswith("$"):
        pkt["type"] = "UNKNOWN"
        return pkt

    # strip trailing '*' if present (examples usually end with '*')
    trimmed = raw[:-1] if raw.endswith("*") else raw
    parts = [p.strip() for p in trimmed.split(",")]
    if len(parts) < 2:
        pkt["type"] = "UNKNOWN"
        return pkt

    header = parts[1]
    pkt["header"] = header

    # ---------- CP (Location/Event) ----------
    if header == "CP":
        # $, CP, Vendor, FW, PacketType, PacketID, Status(L/H), IMEI, VRN, Fix,
        # Date, Time, Lat, LatDir, Lon, LonDir, Speed, Heading, Sats, Altitude, ...
        def g(i: int) -> Optional[str]:
            return parts[i] if i < len(parts) else None

        vendor = g(2)
        firmware = g(3)
        packet_type = g(4)            # NR/IN/IF/...
        packet_id = g(5)              # 01/02/...
        status = g(6)                 # L/H
        imei = g(7)
        vrn = g(8)
        fix = _to_int(g(9))
        ts = _ddmmyyyy_hhmmss_to_utc(g(10) or "", g(11) or "")
        lat = _to_float(g(12)); lat_dir = (g(13) or "")
        lon = _to_float(g(14)); lon_dir = (g(15) or "")
        speed = _to_float(g(16))
        heading = _to_float(g(17))
        sats = _to_int(g(18))
        alt = _to_float(g(19))

        lat, lon = _ns_ew_to_signed(lat, lat_dir, lon, lon_dir)

        pkt.update({
            "type": "LOCATION",
            "imei": imei,
            "vehicleRegNo": vrn,
            "timestamp": ts or datetime.now(timezone.utc),  # fallback to ingest time
            "packet": {"type": packet_type, "id": packet_id, "status": status},
            "gps": {"lat": lat, "lon": lon, "fix": fix, "satellites": sats, "altitude": alt},
            "telemetry": {"speed": speed, "heading": heading},
            "vendor": vendor,
            "firmware": firmware
        })
        return pkt

    # ---------- HP (Health) ----------
    if header == "HP":
        # $, HP, Vendor, FW, IMEI, Batt%, LowBatt%, Flash%, ignOnRate, ignOffRate, IO, A1, A2, *
        def g(i: int) -> Optional[str]:
            return parts[i] if i < len(parts) else None

        vendor = g(2)
        firmware = g(3)
        imei = g(4)
        batt_pct = _to_float(g(5))
        low_batt = _to_float(g(6))
        flash_used = _to_float(g(7))
        ign_on = _to_float(g(8))
        ign_off = _to_float(g(9))
        io = g(10)
        a1 = _to_float(g(11))
        a2 = _to_float(g(12))

        pkt.update({
            "type": "HEALTH",
            "imei": imei,
            "timestamp": datetime.now(timezone.utc),
            "health": {
                "batteryPct": batt_pct,
                "lowBatteryThresh": low_batt,
                "flashUsagePct": flash_used,
                "updateRate": {"ignOnSec": ign_on, "ignOffSec": ign_off},
                "io": io,
                "a1": a1,
                "a2": a2
            },
            "vendor": vendor,
            "firmware": firmware
        })
        return pkt

    # ---------- EPB (Emergency) ----------
    if header == "EPB":
        # $, EPB, EMR/SEM, IMEI, NM/SP, ddmmyyyyhhmmss, A/V, Lat, N/S, Lon, E/W, Heading, Speed, Dist, Provider, VRN, ReplyNum, *
        def g(i: int) -> Optional[str]:
            return parts[i] if i < len(parts) else None

        msg_type = g(2)
        imei = g(3)
        pkt_type = g(4)
        dtts = g(5) or ""  # ddmmyyyyhhmmss
        gps_valid = g(6)
        lat = _to_float(g(7)); lat_dir = (g(8) or "")
        lon = _to_float(g(9)); lon_dir = (g(10) or "")
        heading = _to_float(g(11))
        speed = _to_float(g(12))
        vrn = g(15)

        ts = None
        if len(dtts) >= 14:
            try:
                dd, mm, yyyy = int(dtts[0:2]), int(dtts[2:4]), int(dtts[4:8])
                hh, mn, ss = int(dtts[8:10]), int(dtts[10:12]), int(dtts[12:14])
                ts = datetime(yyyy, mm, dd, hh, mn, ss, tzinfo=timezone.utc)
            except Exception:
                ts = None

        lat, lon = _ns_ew_to_signed(lat, lat_dir, lon, lon_dir)

        pkt.update({
            "type": "EMERGENCY",
            "imei": imei,
            "vehicleRegNo": vrn,
            "timestamp": ts or datetime.now(timezone.utc),
            "emergency": {"messageType": msg_type, "packetType": pkt_type, "gpsValid": gps_valid},
            "gps": {"lat": lat, "lon": lon},
            "telemetry": {"speed": speed, "heading": heading}
        })
        return pkt

    # Unknown packet (e.g., vendor login or malformed)
    pkt["type"] = "UNKNOWN"
    return pkt

# -----------------------
# Bulk writer workers
# -----------------------

async def _bulk_insert_worker(coll, q: "asyncio.Queue[Dict[str, Any]]", name: str):
    """
    Generic bulk insert worker: consumes docs from q and bulk inserts into coll.
    Time- and size-based flush.
    """
    ops: List[InsertOne] = []
    loop = asyncio.get_event_loop()
    last = loop.time()

    async def flush():
        nonlocal ops, last
        if not ops:
            return
        try:
            await coll.bulk_write(ops, ordered=False)
        except Exception:
            # worst-case fallback: attempt single inserts
            for op in ops:
                try:
                    await coll.insert_one(op._doc)  # type: ignore
                except Exception:
                    pass
        ops = []
        last = loop.time()

    while True:
        try:
            timeout = BULK_MAX_LATENCY_MS / 1000.0
            try:
                doc = await asyncio.wait_for(q.get(), timeout=timeout)
                ops.append(InsertOne(doc))
                if len(ops) >= BULK_MAX_DOCS:
                    await flush()
            except asyncio.TimeoutError:
                await flush()
        except asyncio.CancelledError:
            if ops:
                try:
                    await coll.bulk_write(ops, ordered=False)
                except Exception:
                    for op in ops:
                        try:
                            await coll.insert_one(op._doc)  # type: ignore
                        except Exception:
                            pass
            break

async def _bulk_latest_upsert_worker(coll, q: "asyncio.Queue[Dict[str, Any]]"):
    """
    Bulk upsert into `atlantaAis140_latest` with IMEI as `_id`.
    Each doc stored is a skinny snapshot for fast 'current position' lookups.
    """
    ops: List[ReplaceOne] = []
    loop = asyncio.get_event_loop()
    last = loop.time()

    async def flush():
        nonlocal ops, last
        if not ops:
            return
        try:
            await coll.bulk_write(ops, ordered=False)
        except Exception:
            for op in ops:
                try:
                    await coll.replace_one(op._filter, op._replacement, upsert=True)  # type: ignore
                except Exception:
                    pass
        ops = []
        last = loop.time()

    while True:
        try:
            timeout = BULK_MAX_LATENCY_MS / 1000.0
            try:
                doc = await asyncio.wait_for(q.get(), timeout=timeout)
                imei = doc.get("imei")
                if not imei:
                    continue
                skinny = {
                    "_id": imei,  # IMEI as primary key
                    "vehicleRegNo": doc.get("vehicleRegNo"),
                    "timestamp": doc.get("timestamp"),
                    "gps": doc.get("gps"),
                    "telemetry": doc.get("telemetry"),
                    "packet": doc.get("packet"),
                }
                ops.append(ReplaceOne({"_id": imei}, skinny, upsert=True))
                if len(ops) >= BULK_MAX_DOCS:
                    await flush()
            except asyncio.TimeoutError:
                await flush()
        except asyncio.CancelledError:
            if ops:
                try:
                    await coll.bulk_write(ops, ordered=False)
                except Exception:
                    for op in ops:
                        try:
                            await coll.replace_one(op._filter, op._replacement, upsert=True)  # type: ignore
                        except Exception:
                            pass
            break

# -----------------------
# TCP client handler
# -----------------------

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    print(f"[{datetime.now()}] + Connect {addr}")
    buffer = bytearray()

    try:
        while True:
            chunk = await asyncio.wait_for(reader.read(4096), timeout=READ_TIMEOUT)
            if not chunk:
                print(f"[{datetime.now()}] - Disconnect {addr}")
                break
            buffer.extend(chunk)

            # Parse multiple packets in the buffer.
            # AIS140 examples: packets start with '$' and end with '*'
            while True:
                start = buffer.find(b"$")
                if start == -1:
                    # drop junk before next '$' if buffer grows too big
                    if len(buffer) > 1024 * 1024:
                        buffer.clear()
                    break
                end = buffer.find(b"*", start + 1)
                if end == -1:
                    # need more data
                    if start > 0:
                        del buffer[:start]  # discard leading junk
                    break

                pkt_bytes = buffer[start:end + 1]  # include '*'
                del buffer[:end + 1]

                # Decode robustly
                try:
                    raw = pkt_bytes.decode("utf-8", errors="replace")
                except Exception:
                    raw = pkt_bytes.decode("latin-1", errors="replace")

                # Enqueue raw (audit)
                await raw_queue.put({
                    "deviceAddr": repr(addr),
                    "raw": raw,
                    "receivedAt": datetime.now(timezone.utc)
                })

                # Parse + enqueue structured
                parsed = parse_packet(raw)
                ptype = parsed.get("type")
                if ptype in ("LOCATION", "HEALTH", "EMERGENCY"):
                    await loc_queue.put(parsed)
                    # Maintain latest for location/emergency
                    if ptype in ("LOCATION", "EMERGENCY"):
                        await latest_queue.put(parsed)

                # NOTE: Only send ACKs if the device strictly requires it.
                # EPB emergency ACK example (disabled by default):
                # if ptype == "EMERGENCY" and parsed.get("imei"):
                #     ack = f"${parsed['imei']},ACK_EMERGENCY&&"
                #     writer.write(ack.encode())
                #     await writer.drain()

    except asyncio.TimeoutError:
        print(f"[{datetime.now()}] ! Timeout {addr} after {READ_TIMEOUT}s")
    except Exception as e:
        print(f"[{datetime.now()}] ! Error {addr}: {e}")
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

# -----------------------
# Indexes & startup
# -----------------------

async def ensure_indexes():
    # History queries
    await loc_coll.create_index([("imei", ASCENDING), ("timestamp", DESCENDING)])
    await loc_coll.create_index([("vehicleRegNo", ASCENDING), ("timestamp", DESCENDING)])
    await loc_coll.create_index([("timestamp", DESCENDING)])

    # Latest: _id is IMEI (implicit unique + index)
    await latest_coll.create_index([("timestamp", DESCENDING)])

    # Raw logs TTL (30 days). Requires datetime field ('receivedAt') in docs.
    try:
        await raw_coll.create_index("receivedAt", expireAfterSeconds=30 * 24 * 3600)
    except Exception:
        pass

async def main():
    await ensure_indexes()

    # Start workers
    loc_task = asyncio.create_task(_bulk_insert_worker(loc_coll, loc_queue, "loc"))
    raw_task = asyncio.create_task(_bulk_insert_worker(raw_coll, raw_queue, "raw"))
    latest_task = asyncio.create_task(_bulk_latest_upsert_worker(latest_coll, latest_queue))

    server = await asyncio.start_server(handle_client, HOST, PORT)
    print(f"[{datetime.now()}] Listening on {HOST}:{PORT}")
    async with server:
        try:
            await server.serve_forever()
        finally:
            # Graceful shutdown: flush remaining ops
            for t in (loc_task, raw_task, latest_task):
                t.cancel()
            await asyncio.gather(loc_task, raw_task, latest_task, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
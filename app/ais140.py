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
COL_LATEST = "atlantaAis140_latest"  # one doc per IMEI (CP only, full doc)

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
# Helpers
# -----------------------

def _pad_left(s: Optional[str], length: int) -> Optional[str]:
    if s is None:
        return None
    s = s.strip()
    if len(s) >= length:
        return s
    return s.zfill(length)

def _ddmmyyyy_hhmmss_to_utc(date_str: Optional[str], time_str: Optional[str]) -> Optional[datetime]:
    """
    AIS 140 gives GPS time in UTC (ddmmyyyy, hhmmss). Some devices omit leading zeros.
    We left-pad to 8/6 chars when needed.
    """
    d = _pad_left(date_str or "", 8)
    t = _pad_left(time_str or "", 6)
    try:
        return datetime(
            int(d[4:8]), int(d[2:4]), int(d[0:2]),
            int(t[0:2]), int(t[2:4]), int(t[4:6]),
            tzinfo=timezone.utc
        )
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

def _ns_ew_to_signed(lat: Optional[float], ns: Optional[str],
                     lon: Optional[float], ew: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    if lat is not None and ns:
        lat = abs(lat) if ns.upper() == "N" else -abs(lat)
    if lon is not None and ew:
        lon = abs(lon) if ew.upper() == "E" else -abs(lon)
    return lat, lon

def _parse_neighbors(neigh_fields: List[str]) -> List[Dict[str, Any]]:
    """
    Neighboring cells appear as repeating triplets: [cellId, lac, ss]
    In the example, neighbors start at index 34 and run through index 45 (inclusive),
    i.e., slice fields[34:46].
    """
    res: List[Dict[str, Any]] = []
    for i in range(0, len(neigh_fields), 3):
        try:
            cell_id = neigh_fields[i]
            lac = neigh_fields[i + 1]
            ss = _to_int(neigh_fields[i + 2])
            if cell_id:
                res.append({"cellId": cell_id, "lac": lac, "ss": ss})
        except Exception:
            continue
    return res

# -----------------------
# Packet parsing
# -----------------------

def parse_packet(raw: str) -> Dict[str, Any]:
    """
    Robust parsing for:
      - CP: Location/Event (Table-4/5 in AIS140)  -> saved to history + latest (latest=CP only)
      - HP: Health (Table-2)                      -> saved to history
      - EPB: Emergency (EMR/SEM)                  -> saved to history
    """
    pkt: Dict[str, Any] = {"raw": raw, "ingestedAt": datetime.now(timezone.utc)}
    raw = raw.strip()
    if not raw.startswith("$"):
        pkt["type"] = "UNKNOWN"
        return pkt

    # Trim trailing '*' (checksum terminator visible in examples)
    trimmed = raw[:-1] if raw.endswith("*") else raw
    parts = [p.strip() for p in trimmed.split(",")]
    if len(parts) < 2:
        pkt["type"] = "UNKNOWN"
        return pkt

    header = parts[1]
    pkt["header"] = header

    # Utility for safe index access
    def g(i: int) -> Optional[str]:
        return parts[i] if i < len(parts) else None

    # ---------- CP (Location) ----------
    if header == "CP":
        # Index map from your sample:
        # 0:$ 1:CP 2:Vendor 3:FW 4:Type 5:ID 6:Status 7:IMEI 8:VRN 9:Fix
        # 10:Date 11:Time 12:Lat 13:N/S 14:Lon 15:E/W 16:Speed 17:Heading 18:Sats 19:Alt
        # 20:PDOP 21:HDOP 22:Operator 23:Ign 24:MainPower 25:MainBattV 26:IntBattV
        # 27:Emergency 28:Tamper 29:GsmSig 30:MCC 31:MNC 32:LAC 33:CellId(serving)
        # 34..45: Neighbors (cellId,lac,ss) x4
        # 46:Odometer 47:DigitalInputs 48:FrameNo 49:Analog1 50:Analog2 51:DigitalOutputs 52:Checksum
        imei = g(7)
        vrn = g(8)
        date_raw = g(10) or ""
        time_raw = g(11) or ""

        ts = _ddmmyyyy_hhmmss_to_utc(date_raw, time_raw)
        lat = _to_float(g(12)); lon = _to_float(g(14))
        lat_dir = g(13) or ""; lon_dir = g(15) or ""
        lat, lon = _ns_ew_to_signed(lat, lat_dir, lon, lon_dir)

        # neighbors slice inclusive of index 45 -> [34:46]
        neighbors = _parse_neighbors(parts[34:46])

        doc: Dict[str, Any] = {
            "type": "LOCATION",
            "imei": imei,
            "vehicleRegNo": vrn if vrn not in ("", None) else None,
            "timestamp": ts or datetime.now(timezone.utc),  # top-level ts for indexes
            "vendor": g(2),
            "firmware": g(3),
            "packet": {
                "type": g(4),
                "id": g(5),
                "status": g(6),
                "frameNumber": _to_int(g(48)),
            },
            "gps": {
                "date": _pad_left(date_raw, 8),       # raw ddmmyyyy (zero-padded)
                "time": _pad_left(time_raw, 6),       # raw hhmmss (zero-padded)
                "timestamp": ts,                      # ISO UTC (can be None if malformed)
                "fix": _to_int(g(9)),
                "lat": lat,
                "lon": lon,
                "latDir": lat_dir or None,
                "lonDir": lon_dir or None,
                "speed": _to_float(g(16)),
                "heading": _to_float(g(17)),
                "satellites": _to_int(g(18)),
                "altitude": _to_float(g(19)),
                "pdop": _to_float(g(20)),
                "hdop": _to_float(g(21)),
            },
            "telemetry": {
                "ignition": _to_int(g(23)),
                "mainPower": _to_int(g(24)),
                "mainBatteryVoltage": _to_float(g(25)),
                "internalBatteryVoltage": _to_float(g(26)),
                "emergency": _to_int(g(27)),
                "tamper": g(28),
                "odometer": _to_float(g(46)),
            },
            "network": {
                "operator": g(22),
                "gsmSignal": _to_int(g(29)),
                "mcc": _to_int(g(30)),
                "mnc": _to_int(g(31)),
                "lac": g(32),
                "cellId": g(33),
                "neighbors": neighbors,
            },
            "io": {
                "digitalInputs": g(47),
                "digitalOutputs": g(51),
                "analog1": _to_float(g(49)),
                "analog2": _to_float(g(50)),
            },
            "checksum": (g(52) or "").replace("*", "").strip(),
        }
        return doc

    # ---------- HP (Health) ----------
    if header == "HP":
        vendor = g(2); firmware = g(3)
        imei = g(4)
        doc = {
            "type": "HEALTH",
            "imei": imei,
            "timestamp": datetime.now(timezone.utc),
            "vendor": vendor,
            "firmware": firmware,
            "health": {
                "batteryPct": _to_float(g(5)),
                "lowBatteryThresh": _to_float(g(6)),
                "flashUsagePct": _to_float(g(7)),
                "updateRate": {"ignOnSec": _to_float(g(8)), "ignOffSec": _to_float(g(9))},
                "io": g(10),
                "a1": _to_float(g(11)),
                "a2": _to_float(g(12)),
            },
        }
        return doc

    # ---------- EPB (Emergency) ----------
    if header == "EPB":
        msg_type = g(2)     # EMR/SEM
        imei = g(3)
        pkt_type = g(4)     # NM/SP
        dtts = g(5) or ""   # ddmmyyyyhhmmss
        gps_valid = g(6)
        lat = _to_float(g(7)); lat_dir = g(8) or ""
        lon = _to_float(g(9)); lon_dir = g(10) or ""
        heading = _to_float(g(11))
        speed = _to_float(g(12))
        vrn = g(15)

        ts = None
        if len(dtts) >= 14:
            try:
                ts = datetime(
                    int(dtts[4:8]), int(dtts[2:4]), int(dtts[0:2]),
                    int(dtts[8:10]), int(dtts[10:12]), int(dtts[12:14]),
                    tzinfo=timezone.utc
                )
            except Exception:
                ts = None

        lat, lon = _ns_ew_to_signed(lat, lat_dir, lon, lon_dir)

        doc = {
            "type": "EMERGENCY",
            "imei": imei,
            "vehicleRegNo": vrn if vrn not in ("", None) else None,
            "timestamp": ts or datetime.now(timezone.utc),
            "emergency": {"messageType": msg_type, "packetType": pkt_type, "gpsValid": gps_valid},
            "gps": {"lat": lat, "lon": lon},
            "telemetry": {"speed": speed, "heading": heading},
        }
        return doc

    # Unknown packet (e.g. vendor login)
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
            # fallback: single inserts
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
    IMPORTANT: Only CP (type='LOCATION') is written to latest, and with FULL CP doc.
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
                if doc.get("type") != "LOCATION":
                    continue
                imei = doc.get("imei")
                if not imei:
                    continue
                # full CP doc in latest; add _id = IMEI
                latest_doc = dict(doc)
                latest_doc["_id"] = imei
                ops.append(ReplaceOne({"_id": imei}, latest_doc, upsert=True))
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
            # AIS140 packets start with '$' and end with '*'
            while True:
                start = buffer.find(b"$")
                if start == -1:
                    if len(buffer) > 1024 * 1024:
                        buffer.clear()
                    break
                end = buffer.find(b"*", start + 1)
                if end == -1:
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
                    
                raw = raw.encode('unicode_escape').decode('ascii')

                # Parse first (so raw log gets IMEI/VRN)
                parsed = parse_packet(raw)

                # Enqueue raw (audit) -> include IMEI + VRN, and NO device address
                await raw_queue.put({
                    "imei": parsed.get("imei"),
                    "vehicleRegNo": parsed.get("vehicleRegNo"),
                    "raw": raw,
                    "receivedAt": datetime.now(timezone.utc),
                })

                # Enqueue structured into history
                ptype = parsed.get("type")
                if ptype in ("LOCATION", "HEALTH", "EMERGENCY"):
                    # Mirror gps.timestamp into top-level timestamp if available
                    # (Already done for CP; keep as-is for others)
                    await loc_queue.put(parsed)
                    # Only CP -> latest
                    if ptype == "LOCATION":
                        await latest_queue.put(parsed)

                # ACKs (only if device requires; disabled by default)
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

    # Latest: _id is IMEI (implicit); add ts index for dashboards
    await latest_coll.create_index([("timestamp", DESCENDING)])

    # Raw logs TTL (30 days) on 'receivedAt'
    try:
        await raw_coll.create_index("receivedAt", expireAfterSeconds=30 * 24 * 3600)
    except Exception:
        # If index exists with different TTL, you'd need to drop/recreate manually.
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

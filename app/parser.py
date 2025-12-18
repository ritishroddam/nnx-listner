import os
import asyncio
import googlemaps
import socketio

from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Dict, Any, List
from math import atan2, degrees, radians, sin, cos
from geopy.distance import geodesic
from pymongo import InsertOne, ReplaceOne, ASCENDING, DESCENDING, MongoClient

mongo_client = MongoClient("mongodb://doadmin:4l239y815dQan0Vo@mongodb+srv://cordonnxDB-4f7df3c7.mongo.ondigitalocean.com/?tls=true&authSource=admin", tz_aware=True)
db = mongo_client["nnx"]

FLAT_TO_AIS140 = {
    "latitude": "gps.lat",
    "longitude": "gps.lon",
    "dir1": "gps.latDir",
    "dir2": "gps.lonDir",
    "course": "gps.heading",
    "date_time": "gps.timestamp",
    "speed": "telemetry.speed",
    "ignition": "telemetry.ignition",
    "odometer": "telemetry.odometer",
    "main_power": "telemetry.mainPower",
    "internal_bat": "telemetry.internalBatteryVoltage",
    "sos": "telemetry.emergencyStatus",
    "gsm_sig": "network.gsmSignal",
    "mobCountryCode": "network.mcc",
    "mobNetworkCode": "network.mnc",
    "localAreaCode": "network.lac",
    "cellid": "network.cellId",
    "harsh_speed": "packet.id",
    "harsh_break": "packet.id",
    "timestamp": "timestamp",
}

def _convert_date_time_emit(date):
    if not date:
        now = datetime.now(timezone(timedelta(hours=5, minutes=30)))
        return now.strftime("%d%m%y"), now.strftime("%H%M%S")
    # Convert UTC datetime to IST (UTC+5:30)
    ist = date.astimezone(timezone(timedelta(hours=5, minutes=30)))
    return ist.strftime("%d%m%y"), ist.strftime("%H%M%S")

def atlantaAis140ToFront(parsedData):
    date, time = _convert_date_time_emit(parsedData.get("timestamp"))

    gps = parsedData.get("gps", {}) or {}
    telemetry = parsedData.get("telemetry", {}) or {}
    network = parsedData.get("network", {}) or {}
    packet = parsedData.get("packet", {}) or {}
    packet_id = str(packet.get("id", ""))

    json_data = {
        "_id": parsedData.get("_id"),
        "imei": parsedData.get("imei"),
        "speed": telemetry.get("speed"),
        "latitude": gps.get("lat"),
        "dir1": gps.get("latDir"),
        "longitude": gps.get("lon"),
        "dir2": gps.get("lonDir"),
        "date": date,
        "time": time,
        "course": gps.get("heading"),
        "ignition": telemetry.get("ignition"),
        "gsm_sig": network.get("gsmSignal"),
        "sos": telemetry.get("emergencyStatus"),
        "odometer": telemetry.get("odometer"),
        "main_power": telemetry.get("mainPower"),
        "harsh_speed": "1" if packet_id == "14" else "0",
        "harsh_break": "1" if packet_id == "13" else "0",
        "adc_voltage": telemetry.get("mainBatteryVoltage"),
        "internal_bat": telemetry.get("internalBatteryVoltage"),
        "mobCountryCode": network.get("mcc"),
        "mobNetworkCode": network.get("mnc"),
        "localAreaCode": network.get("lac"),
        "cellid": network.get("cellId"),
        "date_time": gps.get("timestamp"),
        "timestamp": parsedData.get("timestamp"),
    }
    return json_data

def getData(imei, date_filter, projection):
    query = {"imei": imei, "gps": "A"}
    query.update(date_filter or {})
    data = list(db["atlanta"].find(query, projection).sort("date_time", DESCENDING))
    if data:
        return data

    wanted_fields = {k for k, v in projection.items() if v and k != "_id"}

    ais140_projection = {"_id": 0, "imei": 1}
    for flat in wanted_fields:
        path = FLAT_TO_AIS140.get(flat)
        if path:
            ais140_projection[path] = 1

    dt_filter = None
    if isinstance(date_filter, dict):
        dt_filter = date_filter.get("date_time")

    ais140_query = {"imei": imei, "gps.gpsStatus": 1}
    
    if dt_filter is not None:
        ais140_query["gps.timestamp"] = dt_filter

    ais140_cursor = db["atlantaAis140"].find(
            ais140_query,
            ais140_projection
        )

    ais140_data = list(ais140_cursor.sort("gps.timestamp", DESCENDING))

    if not ais140_data:
        return []

    converted = []
    for doc in ais140_data:
        flat_doc = atlantaAis140ToFront(doc)
        out_doc = {}
        for field in wanted_fields:
            if field in flat_doc:
                out_doc[field] = flat_doc[field]
        if "date_time" in flat_doc and ("date_time" in projection or "date_time" not in out_doc):
            out_doc.setdefault("date_time", flat_doc["date_time"])
        if projection.get("_id"):
            out_doc["_id"] = flat_doc.get("_id")
        converted.append(out_doc)

    return converted
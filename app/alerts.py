import os
import asyncio
import googlemaps
import socketio

from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Dict, Any, List
from math import atan2, degrees, radians, sin, cos
from geopy.distance import geodesic
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import InsertOne, ReplaceOne, ASCENDING, DESCENDING

from mail import buildAndSendEmail
from parser import getData

DIRECTIONS = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
BEARING_DEGREES = 360 / len(DIRECTIONS)
GMAPS = googlemaps.Client(key="AIzaSyCHlZGVWKK4ibhGfF__nv9B55VxCc-US84")

MONGO_URI = "mongodb+srv://doadmin:4T81NSqj572g3o9f@db-mongodb-blr1-27716-c2bd0cae.mongo.ondigitalocean.com/admin?tls=true&authSource=admin"
if not MONGO_URI:
    raise RuntimeError("MONGO_URI env var is required")

mongo_client: AsyncIOMotorClient = AsyncIOMotorClient(MONGO_URI, tz_aware=True)

db = mongo_client['nnx']

geocodeCollection = db['geocoded_address']
vehicleCOllection = db['vehicle_inventory']
companyCollection = db['customers_list']
userCollection = db['users']
userConfigCollection = db['userConfig']
recentAlertsCollection = db['alert_locks']

speedingCollection = db['speedingAlerts']
harshBrakeCollection = db['harshBrakes']
harshAccelerationCollection = db['harshAccelerations']
gsmSignalLowCollection = db['gsmSignalLows']
internalBatterLowCollection = db['internalBatteryLows']
mainPowerSupplyDissconnectCollection = db['powerSupplyDissconnects']
idleCollection = db['idles']
ignitionOnCollection = db['ignitionOns']
ignitionOffCollection = db['ignitionOffs']
geofenceInCollection = db['geodenceIns']
geofenceOutCollection = db['geofenceOuts']


ALERT_META = {
    "harsh_break_alerts": {"label": "Harsh Braking", "coll": harshBrakeCollection},
    "harsh_acceleration_alerts": {"label": "Harsh Acceleration", "coll": harshAccelerationCollection},
    "gsm_low_alerts": {"label": "GSM Signal Low", "coll": gsmSignalLowCollection},
    "internal_battery_low_alerts": {"label": "Internal Battery Low", "coll": internalBatterLowCollection},
    "idle_alerts": {"label": "Idle", "coll": idleCollection},
    "ignition_off_alerts": {"label": "Ignition Off", "coll": ignitionOffCollection},
    "ignition_on_alerts": {"label": "Ignition On", "coll": ignitionOnCollection},
    "main_power_supply": {"label": "Main Powersupply Disconnected", "coll": mainPowerSupplyDissconnectCollection}
}

def _ist_str_to_utc(dt_str: str) -> datetime:
    ist = timezone(timedelta(hours=5, minutes=30))
    parsed = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
    parsed = parsed.replace(tzinfo=ist)
    return parsed.astimezone(timezone.utc)

async def processDataForIdle(data, vehicleInfo, idleTime):
    existing_lock = await recentAlertsCollection.find_one({'imei': data.get('imei'), 'type': 'Idle'})
    if not existing_lock:
        if vehicleInfo:
            companyName = vehicleInfo.get('CompanyName')
            print(f"[DEBUG] Company Name: {companyName}")

            company = await companyCollection.find_one({'Company Name': companyName})
            print(f"[DEBUG] {company}")

            if company:
                companyId = str(company.get('_id'))


                cursor = userCollection.find({'company': companyId})
                users = [u async for u in cursor]

                userData = []
                if users:
                    for user in users:
                        disabled = int(user.get('disabled') or 0)
                        if disabled == 1:
                            continue
                        
                        userConfig = await userConfigCollection.find_one({'userID': user.get('_id')})

                        alerts_list = (userConfig.get('alerts') if userConfig else []) or []

                        if 'idle_alerts' in alerts_list:
                            userData.append({
                                "username": user.get('username'),
                                "email": user.get('email')
                            })

                print(f"[DEBUG] trying to send email with: {userData}")
                if userData:
                    data['alertType'] = 'Idle'
                    data['alertMessage'] = idleTime
                    await asyncio.to_thread(buildAndSendEmail, data, companyName, userData)
            else:
                print("[DEBUG] Company Not Found")

    utc_dt = _ist_str_to_utc(data.get('date_time'))

    await idleCollection.insert_one(
        {
            'imei': data.get('imei'),
            'LicensePlateNumber': vehicleInfo.get('LicensePlateNumber') if vehicleInfo else None,
            'alertMessage': idleTime,
            'date_time': utc_dt,
            'latitude': data.get('latitude'),
            'longitude': data.get('longitude'),
            'location': data.get('address'),
        }
    )

async def processDataForOverSpeed(data, vehicleInfo):
    existing_lock = await recentAlertsCollection.find_one({'imei': data.get('imei'), 'type': 'Idle'})
    if not existing_lock:
        if vehicleInfo:
            companyName = vehicleInfo.get('CompanyName')
            print(f"[DEBUG] Company Name: {companyName}")

            company = await companyCollection.find_one({'Company Name': companyName})
            print(f"[DEBUG] {company}")

            if company:
                companyId = str(company.get('_id'))

                print(f"[DEBUG] company ID: {companyId}")

                cursor = userCollection.find({'company': companyId})
                users = [u async for u in cursor]

                print(f"[DEBUG] user query done")

                if users:
                    print(f"[DEBUG] Users: {users}")
                else: 
                    print("No users found")

                userData = []
                if users:
                    for user in users:
                        disabled = int(user.get('disabled') or 0)
                        if disabled == 1:
                            continue
                        userConfig = await userConfigCollection.find_one({'userID': user.get('_id')})

                        alerts_list = (userConfig.get('alerts') if userConfig else []) or []

                        if userConfig:
                            if 'speeding_alerts' in alerts_list:
                                print(f"[DEBUG] {user.get('username')}, {user.get('email')}")
                                userData.append(
                                    {
                                        "username": user.get('username'),
                                        "email": user.get('email')
                                    }
                                )

                print(f"[DEBUG] trying to send email with: {userData}")
                if userData:
                    data['alertType'] = 'Speed'
                    await asyncio.to_thread(buildAndSendEmail, data, companyName, userData)
            else:
                print("[DEBUG] Company Not Found")

    utc_dt = _ist_str_to_utc(data.get('date_time'))

    await speedingCollection.insert_one(
        {
            'imei': data.get('imei'),
            'LicensePlateNumber': vehicleInfo.get('LicensePlateNumber') if vehicleInfo else None,
            'speed': data.get('speed'),
            'date_time': utc_dt,
            'latitude': data.get('latitude'),
            'longitude': data.get('longitude'),
            'location': data.get('address'),
        }
    )
    
async def process_generic_alert(data, vehicleInfo, alert_key):
    meta = ALERT_META.get(alert_key)
    if not meta:
        print(f"[DEBUG] Unknown alert key: {alert_key}")
        return

    existing_lock = await recentAlertsCollection.find_one({'imei': data.get('imei'), 'type': 'Idle'})
    if not existing_lock:
        companyName = (vehicleInfo or {}).get('CompanyName')
        if companyName:
            company = await companyCollection.find_one({'Company Name': companyName})
            if company:
                companyId = str(company.get('_id'))
                cursor = userCollection.find({'company': companyId})
                users = [u async for u in cursor]

                userData: List[Dict[str, str]] = []
                for user in users:
                    disabled = int(user.get('disabled') or 0)
                    if disabled == 1:
                        continue
                    userConfig = await userConfigCollection.find_one({'userID': user.get('_id')})
                    alerts_list = (userConfig.get('alerts') if userConfig else []) or []
                    if alert_key in alerts_list:
                        userData.append({
                            "username": user.get('username'),
                            "email": user.get('email')
                        })

                print(f"[DEBUG] trying to send email with: {userData}")
                if userData:
                    data['alertType'] = meta["label"]
                    await asyncio.to_thread(buildAndSendEmail, data, companyName, userData)
            else:
                print("[DEBUG] Company Not Found")

    utc_dt = _ist_str_to_utc(data.get('date_time'))

    doc = {
        'imei': data.get('imei'),
        'LicensePlateNumber': (vehicleInfo or {}).get('LicensePlateNumber'),
        'date_time': utc_dt,
        'latitude': data.get('latitude'),
        'longitude': data.get('longitude'),
        'location': data.get('address'),
    }

    try:
        await meta["coll"].insert_one(doc)
    except Exception as e:
        print(f"[DEBUG] Failed to persist {alert_key}: {e}")

def processDataForIgnition(data):
    pass

def processDataForGeofence(data):
    pass

ALERTS = ['Speeding', 'Harsh Braking', 'Harsh Acceleration', 'GSM Signal Low', 'Internal Battery Low', 
          'Main Power Supply Dissconnect', 'Idle', 'Ignition On', 'Ignition Off', 'Geofence In', 'Geofence Out']

async def dataToAlertParser(data):
    print("[DEBUG] Alerts Page")
    imei = data.get('imei')
    
    vehicleInfo = await vehicleCOllection.find_one({"IMEI": imei})
    
    speedThreshold = float(vehicleInfo.get("normalSpeed", '60')) if vehicleInfo else 60.00
    
    if float(data.get('speed', '')) > speedThreshold:
        await processDataForOverSpeed(data, vehicleInfo if vehicleInfo else None)
        
    if data.get('harsh_break', '') == '1':
        await process_generic_alert(data, vehicleInfo, "harsh_break_alerts")
        
    if data.get('harsh_speed', '') == '1':
        await process_generic_alert(data, vehicleInfo, "harsh_acceleration_alerts")
        
    if int(data.get('gsm_sig', '')) <= 8:
        await process_generic_alert(data, vehicleInfo, "gsm_low_alerts")
    
    if float(data.get('internal_bat', '')) <= 3.7:
        await process_generic_alert(data, vehicleInfo, "internal_battery_low_alerts")
        
    if data.get('main_power', '') == '0':
        await process_generic_alert(data, vehicleInfo, "main_power_supply")

    if data.get('ignition', '') ==  '1' and float(data.get('speed', '0.00')) < 1.00:
        now = datetime.now(timezone.utc)
        datetimeMax = now - timedelta(hours = 24)
        dateTimeFilter ={
            'date_time': {
                '$gte': datetimeMax,
                '$lte': now
            }
        }

        projection = {'ignition': 1, 'speed': 1, 'date_time': 1, '_id': 0}

        records = getData(imei, dateTimeFilter, projection)
            
        for record in records:
            if record.get('ignition', '') ==  '1' and float(record.get('speed', '0.00')) < 1.00:
                lastDateTime = record.get('date_time')
                continue
            
            break

        if lastDateTime:
            dataDateTime = _ist_str_to_utc(data.get('date_time'))
            idleTime = dataDateTime - lastDateTime
        else:
            idleTime = timedelta(0)
        

        if idleTime > timedelta(minutes = 10):
            idleTime = idleTime.total_seconds()
            
            if idleTime >= 86400:
                idleTime = f'for {((idleTime / 60) / 60) / 24} days'
            elif idleTime >= 3600:
                idleTime = f'for {((idleTime / 60) / 60)} hours'
            elif idleTime >= 60:
                idleTime = f'for {(idleTime / 60)} minutes'

            await processDataForIdle(data, vehicleInfo if vehicleInfo else None, idleTime)
        
    # processDataForIgnition(data, vehicleInfo if vehicleInfo else None)
    # processDataForGeofence(data, vehicleInfo if vehicleInfo else None)
    
    


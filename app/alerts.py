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


def _validate_coordinates(lat, lng):
    if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
        raise ValueError(f"Invalid coordinates {lat} and {lng}")
    
def _calculate_bearing(coord1, coord2):
    lat1, lon1 = radians(coord1[0]), radians(coord1[1])
    lat2, lon2 = radians(coord2[0]), radians(coord2[1])
    d_lon = lon2 - lon1
    x = sin(d_lon) * cos(lat2)
    y = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(d_lon)
    bearing = (degrees(atan2(x, y)) + 360) % 360
    return DIRECTIONS[int(((bearing + (BEARING_DEGREES/2)) % 360) // BEARING_DEGREES)]

async def processDataForOverSpeed(data, vehicleInfo):
    print("[DEBUG] In speed part of alerts")
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
                userConfig = await userConfigCollection.find_one({'userID': user.get('_id')})
                
                if userConfig:
                    if 'speeding_alerts' in userConfig.get('alerts'):
                        print(f"[DEBUG] {user.get('username')}, {user.get('email')}")
                        userData.append(
                            {
                                "username": user.get('username'),
                                "email": user.get('email')
                            }
                        )
        
        print(f"[DEBUG] trying to send email with: {userData}")
        if userData:
            data['alertType'] = 'speed'
            buildAndSendEmail(data, companyName, userData)
    else:
        print("[DEBUG] Company Not Found")
    
    
    # Convert IST string to UTC datetime before saving
    dt_str = data.get('date_time')
    ist = timezone(timedelta(hours=5, minutes=30))

    utc_dt = None
    parsed = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
    parsed = parsed.replace(tzinfo=ist)
    utc_dt = parsed.astimezone(timezone.utc)

    speedingCollection.insert_one(
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

def processDataForHarshBrake(data):
    pass

def processDataForHarshSpeed(data):
    pass

def processDataForGsmSig(data):
    pass

def processDataForInernalBat(data):
    pass

def processDataForMainPowerSupply(data):
    pass

def processDataForIdle(data):
    pass

def processDataForIgnition(data):
    pass

def processDataForGeofence(data):
    pass

ALERTS = ['Speeding', 'Harsh Braking', 'Harsh Acceleration', 'GSM Signal Low', 'Internal Battery Low', 
          'Main Power Supply Dissconnect', 'Idle', 'Ignition On', 'Ignition Off', 'Geofence In', 'Geofence Out']

async def dataToReportParser(data):
    print("[DEBUG] Alerts Page")
    imei = data.get('imei')
    
    vehicleInfo = await vehicleCOllection.find_one({"IMEI": imei})
    
    speedThreshold = float(vehicleInfo.get("normalSpeed", '60')) if vehicleInfo else 60.00
    
    if float(data.get('speed', '')) > speedThreshold:
        await processDataForOverSpeed(data, vehicleInfo if vehicleInfo else None)
        
    if data.get('harsh_break', '') == '1':
        processDataForHarshBrake(data, vehicleInfo if vehicleInfo else None)
        
    if data.get('harsh_speed', '') == '1':
        processDataForHarshSpeed(data, vehicleInfo if vehicleInfo else None)
        
    if int(data.get('gsm_sig', '')) <= 8:
        processDataForGsmSig(data, vehicleInfo if vehicleInfo else None)
    
    if float(data.get('internal_bat', '')) <= 3.7:
        processDataForInernalBat(data, vehicleInfo if vehicleInfo else None)
        
    if data.get('main_power', '') == '0':
        processDataForMainPowerSupply(data, vehicleInfo if vehicleInfo else None)
        
    processDataForIdle(data, vehicleInfo if vehicleInfo else None)
    processDataForIgnition(data, vehicleInfo if vehicleInfo else None)
    processDataForGeofence(data, vehicleInfo if vehicleInfo else None)
    
    


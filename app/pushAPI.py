from datetime import datetime
import requests
from pymongo import MongoClient

mongo_client = MongoClient("mongodb+srv://doadmin:4T81NSqj572g3o9f@db-mongodb-blr1-27716-c2bd0cae.mongo.ondigitalocean.com/admin?tls=true&authSource=admin", tz_aware=True)
db = mongo_client["nnx"]

moveInSyncSubscribed = db ['moveInSyncSubscriptions']

getMoveInSyncAlerts = {
    'speed': 'OVERSPEEDING',
    'sos': 'PANIC',
    'mainPowerSupplyDisconnect': 'EXTERNAL_POWER_SOURCE_DISCONNECTED_ALERT',
    'intternalBatteryLow': 'INTERNAL_BATTERY_LOW_ALERT',
    }

MOVE_IN_SYNC_ENDPOINT = 'http://tracking.moveinsync.com:8080/gps-tracking/devices/CORDON/packets/critical'

def sendDataToMoveInSync(data, alerts, date_time):
    imei = data.get('imei')
    if not imei or moveInSyncSubscribed.find_one({'imei': imei}) is None:
        return
    
    epochTime = date_time.timestamp()
    
    moveInSyncAlertsList = ['speed', 'sos', 'mainPowerSupplyDisconnect', 'intternalBatteryLow']
    
    sendAlertsDict = {}
    if alerts:
        for alert in alerts:
            if alert in moveInSyncAlertsList:
                sendAlertsDict[getMoveInSyncAlerts.get(alert)] = True
                
    payload =  [{ 
        "deviceImei": data.get('imei'),
        "deviceId": '',
        "timestamp": epochTime,
        "latitude": data.get('latitude'),
        "longitude": data.get('longitude'),
        "speed": str(data.get('speed')),
        "bearing": str(data.get('course')),
        "locationAccuracy": '10' if data.get('gps') == 'A' else '1000000000000',
        "deviceType": 'GPS DEVICE',
    }] 
    
    if sendAlertsDict:
        payload[0]['alertMap'] = sendAlertsDict

    try:
        response = requests.post(
            MOVE_IN_SYNC_ENDPOINT,
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=10,
        )
        response.raise_for_status()
        print(f"[INFO] MoveInSync response: {response}")
    except requests.RequestException as exc:
        print(f"[ERROR] MoveInSync push failed: {exc}")
    
    
def sendPushAPIs(data, alerts, date_time):
    try:
        sendDataToMoveInSync(data, alerts, date_time)
    
    except Exception as e:
        print(f"[ERROR] in sendPushAPIs: {e}")
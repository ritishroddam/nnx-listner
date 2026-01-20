from datetime import datetime, timezone
import httpx
from pymongo import MongoClient

mongo_client = MongoClient("mongodb+srv://doadmin:U6bOV204y9r75Iz3@private-db-mongodb-blr1-96186-4485159f.mongo.ondigitalocean.com/admin?tls=true&authSource=admin&replicaSet=db-mongodb-blr1-96186", tz_aware=True)
db = mongo_client["nnx"]

moveInSyncSubscribed = db['moveInSyncSubscriptions']
moveInSyncLogs = db['moveInSyncLogs']

getMoveInSyncAlerts = {
    'speed': 'OVERSPEEDING',
    'sos': 'PANIC',
    'mainPowerSupplyDisconnect': 'EXTERNAL_POWER_SOURCE_DISCONNECTED_ALERT',
    'intternalBatteryLow': 'INTERNAL_BATTERY_LOW_ALERT',
    }

MOVE_IN_SYNC_ENDPOINT = 'http://tracking.moveinsync.com:8080/gps-tracking/devices/CORDON/packets/critical'

async def sendDataToMoveInSync(data, alerts, date_time):
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
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(
                MOVE_IN_SYNC_ENDPOINT,
                json=payload,
                headers={'Content-Type': 'application/json'},
            )
        response.raise_for_status()
        print(f"[INFO] MoveInSync response: {response.text}")
        moveInSyncLogs.insert_one({
            'imei': imei,
            'LicensePlateNumber': data.get('LicensePlateNumber'),
            'payload': payload,
            'response': response.text,
            'timestamp': datetime.now(timezone.utc),
        })
    except httpx.HTTPError as exc:
        print(f"[ERROR] MoveInSync push failed: {exc}")
    
    
async def sendPushAPIs(data, alerts, date_time):
    try:
        await sendDataToMoveInSync(data, alerts, date_time)
    
    except Exception as e:
        print(f"[ERROR] in sendPushAPIs: {e}")
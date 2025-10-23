import http.client
import json
from datetime import datetime, timezone

conn = http.client.HTTPSConnection("control.msg91.com")

def buildVariable(company, data):
    variable = {
        "username": company,
        "alert": 
        {
            "vehicleNumber": data.get('LicensePlateNumber'),
            "type": data.get('alertType'),

            "latitude": data.get('latitude'),
            "longitude": data.get('longitude'),
            "location": data.get('address'),
            "timestamp": data.get('date_time')
        }
    }
    
    if data.get('alertType') == 'Speed':
        variable['alert']["speed"] = data.get('speed')
    elif data.get('alertType') == 'Geofence':
        variable['alert']['geofence']['entered'] = bool(data.get('EnteredGeofence'))
        variable['alert']['geofence']['name'] = data.get('geofenceName')

    return variable
        
def buildAndSendEmail(data, company, recepients):
    variables = buildVariable(company, data)
    
    payload = { "recipients": 
        [
            {
                "to": recepients,
                "variables": variables,
            },
        ],
        "from": 
            {
                "name": "Software",
                "email": "alert@email.cordontrack.com"
            },
        "domain": "email.cordontrack.com",
        "template_id": "cordonnx_alerts"
    }

    headers = {
        'accept': "application/json",
        'authkey': "408243Au2CYx9Kx66c0a4c0P1",
        'content-type': "application/json"
    }

    body = json.dumps(payload).encode('utf-8')

    conn.request("POST", "/api/v5/email/send", body, headers)
    res = conn.getresponse()
    data = res.read()
    print(res.status, res.reason)
    
    if res.status == 200:
        from parser import db
        
        db['alertsClock'].insert_one(
            {
                'LicensePlateNumber' : data.get('LicensePlateNumber'),
                'type': data.get('alertType'),
                'last_sent': datetime.now(timezone.utc)
            }
        )
    
    print(data.decode("utf-8"))

import eventlet
import threading
import socketserver
import json
from datetime import datetime, timedelta
from pytz import timezone
import os
from pymongo import MongoClient
from flask import Flask, render_template, jsonify, request
import signal
import sys
from datetime import datetime
from flask_cors import CORS
from math import radians, sin, cos, sqrt, atan2
import socketio
import eventlet.wsgi
import time
from pymongo import MongoClient
import ssl
from geopy.distance import geodesic
from math import atan2, degrees, radians, sin, cos
import googlemaps
from pymongo import ASCENDING
import asyncio
import re

client_activity = {}
last_emit_time = {}
comamandImeiList = []
rawLogList = []
rawLogImeiLiscenceMap = {}

mongo_client = MongoClient("mongodb+srv://doadmin:4T81NSqj572g3o9f@db-mongodb-blr1-27716-c2bd0cae.mongo.ondigitalocean.com/admin?tls=true&authSource=admin", tz_aware=True)
db = mongo_client["nnx"]

collection = db['atlanta']
sos_logs_collection = db['sos_logs']  
distance_travelled_collection = db['distanceTravelled']
vehicle_inventory_collection = db['vehicle_inventory']
geoCodeCollection = db['geocoded_address']
rawLogSubscriptions = db['raw_log_subscriptions']
rawLogDataCollection = db['raw_log_data']

app = Flask(__name__)
CORS(app)

gmaps = googlemaps.Client(key="AIzaSyCHlZGVWKK4ibhGfF__nv9B55VxCc-US84")

# Create compound index for fast queries
INACTIVITY_TIMEOUT = 600 
DIRECTIONS = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
BEARING_DEGREES = 360 / len(DIRECTIONS)

def storRawData(imei, raw_data):
    try:
        try:
            raw_data = raw_data.decode('utf-8')
        except UnicodeDecodeError:
            raw_data = raw_data.decode('latin-1')
        
        raw_data = raw_data.encode('unicode_escape').decode('ascii')
        
        rawLogDataCollection.insert_one({
            'LicensePlateNumber': rawLogImeiLiscenceMap.get(imei, 'Unknown'),
            'imei': imei,
            'raw_data': raw_data,
            'timestamp': datetime.now(timezone('UTC'))
        })
        print(f"[DEBUG] Stored raw data for IMEI: {imei}")
    except Exception as e:
        print(f"[DEBUG] Error storing raw data for IMEI {imei}: {e}")

async def update_raw_log_list():
    while True:
        try:
            results = rawLogSubscriptions.find()
            rawLogList.clear()
            rawLogImeiLiscenceMap.clear()
            for result in results:
                imei = result.get('IMEI', '').strip()
                if imei:
                    rawLogList.append(imei)
                    rawLogImeiLiscenceMap[imei] = result.get('LicensePlateNumber', 'Unknown')
            print("[DEBUG] Updated rawLogImeiLiscenceMap and rawLogList")
        except Exception as e:
            print(f"[DEBUG] Error updating raw log list: {e}")
        await asyncio.sleep(300)

def monitor_inactive_clients():
    while True:
        now = datetime.now()
        to_remove = []
        for addr, info in list(client_activity.items()):
            last_seen = info['last_seen']
            if (now - last_seen).total_seconds() > INACTIVITY_TIMEOUT:
                print(f"[DEBUG] Disconnecting inactive client {addr}")
                try:
                    # Close the connection
                    info['connection'].close()
                except Exception as e:
                    print(f"[DEBUG] Error closing connection for {addr}: {e}")
                to_remove.append(addr)
        # Remove inactive clients from the dictionary
        for addr in to_remove:
            client_activity.pop(addr, None)
        time.sleep(60)

def calculate_bearing(coord1, coord2):
    lat1, lon1 = radians(coord1[0]), radians(coord1[1])
    lat2, lon2 = radians(coord2[0]), radians(coord2[1])
    d_lon = lon2 - lon1
    x = sin(d_lon) * cos(lat2)
    y = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(d_lon)
    bearing = (degrees(atan2(x, y)) + 360) % 360
    return DIRECTIONS[int(((bearing + (BEARING_DEGREES/2)) % 360) // BEARING_DEGREES)]

def validate_coordinates(lat, lng):
    if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
        raise ValueError(f"Invalid coordinates {lat} and {lng}")
    
def nmea_to_decimal(nmea_value):
    nmea_value = str(nmea_value)
    if '.' in nmea_value:
        dot_index = nmea_value.index('.')
        degrees = float(nmea_value[:dot_index - 2])
        minutes = float(nmea_value[dot_index - 2:])
    else:
        raise ValueError("Invalid NMEA format")
    decimal_degrees = degrees + (minutes / 60.0)
    return decimal_degrees
    
def geocodeInternal(lat,lng):
    try:
        lat = float(lat)
        lng = float(lng)
        validate_coordinates(lat, lng)
    except(ValueError, TypeError) as e:
        print(f"Invalid input: {str(e)}")
        return "Invalid coordinates"

    try:
        nearby_entries = geoCodeCollection.find({
            "lat": {"$gte": lat - 0.0045, "$lte": lat + 0.0045},
            "lng": {"$gte": lng - 0.0045, "$lte": lng + 0.0045}
        })
        nearest_entry = None
        min_distance = 0.5
        for entry in nearby_entries:
            saved_coord = (entry['lat'], entry['lng'])
            current_coord = (lat, lng)
            distance = geodesic(saved_coord, current_coord).km
            if distance <= min_distance:
                nearest_entry = entry
                min_distance = distance
        if nearest_entry:
            saved_coord = (nearest_entry['lat'], nearest_entry['lng'])
            current_coord = (lat, lng)
            bearing = calculate_bearing(saved_coord, current_coord)
            return (f"{min_distance:.2f} km {bearing} from {nearest_entry['address']}"
                      if min_distance > 0 else nearest_entry['address'])
        reverse_geocode_result = gmaps.reverse_geocode((lat, lng))
        if not reverse_geocode_result:
            print("Geocoding API failed")
            return "Address unavailable"
        address = reverse_geocode_result[0]['formatted_address']
        geoCodeCollection.insert_one({
            'lat': lat,
            'lng': lng,
            'address': address
        })
        return address
    except Exception as e:
        print(f"Geocoding error: {str(e)}", exc_info=True)
        return "Error retrieving address"

def lastEmitInitial():
    all_documents = list(collection.aggregate([
        {"$match": {"gps": "A", "status": "01"}},
        {"$sort": {"date_time": -1}},
        {
            "$group": {
                "_id": "$imei",
                "latest_doc": {"$first": "$$ROOT"}
            }
        },
        {"$replaceRoot": {"newRoot": "$latest_doc"}}
    ]))

    for doc in all_documents:
        last_emit_time[doc['imei']] = doc['date_time']

def clean_imei(imei):
    return imei[-15:]

def clean_cellid(cellid):
    return cellid[:5]

def should_emit(imei, date_time):
    if imei not in last_emit_time or date_time - last_emit_time[imei] > timedelta(seconds=1):
        last_emit_time[imei] = date_time
        return True
    return False

def convert_to_datetime(date_str: str, time_str: str) -> datetime:
    dt_str = date_str + time_str
    dt_obj = datetime.strptime(dt_str, "%d%m%y%H%M%S")
    return dt_obj

def store_data_in_mongodb(json_data):
    try:
        result = collection.insert_one(json_data)
        ensure_socket_connection()
        if json_data['gps'] == 'A' and json_data['status'] == '01' and should_emit(json_data['imei'],json_data['date_time']):
            json_data['_id'] = str(json_data['_id'])
            json_data['date_time'] = str(json_data['date_time'])
            json_data['timestamp'] = str(json_data['timestamp'])
            inventory_data = vehicle_inventory_collection.find_one({'IMEI': json_data.get('imei')})
            if inventory_data:
                json_data['LicensePlateNumber'] = inventory_data.get('LicensePlateNumber', 'Unknown')
                json_data['VehicleType'] = inventory_data.get('vehicle_type', 'Unknown')
                json_data['slowSpeed'] = float(inventory_data.get('slowSpeed', "0") or 40.0)
                json_data['normalSpeed'] = float(inventory_data.get('normalSpeed', "0") or 60.0)
            else:
                json_data['LicensePlateNumber'] = 'Unknown'
                json_data['VehicleType'] = 'Unknown'
                json_data['slowSpeed'] = 40.0
                json_data['normalSpeed'] = 60.0
            json_data['address'] = geocodeInternal(json_data['latitude'],json_data['longitude'])
            if sio.connected:
                try:
                    sio.emit('vehicle_live_update', json_data)
                    sio.emit('vehicle_update', json_data)
                except Exception as e:
                    print("Error emitting data to WebSocket:", e)
    except Exception as e:
        print("Error storing data in MongoDB:", e)

def log_sos_to_mongodb(json_data):
    try:
        sos_log = {
            'imei': json_data['imei'],
            'date': json_data['date'],
            'time': json_data['time'],
            'latitude': json_data['latitude'],
            'longitude': json_data['longitude'],
            'date_time': json_data['date_time'],
            'timestamp': json_data['timestamp'],
        }
        sos_logs_collection.insert_one(sos_log)
    except Exception as e:
        print("Error logging SOS alert to MongoDB:", e)

def ensure_socket_connection():
    if not sio.connected:
        print("[DEBUG] Socket not connected, reconnecting...")
        try:
            sio.disconnect()
        except Exception:
            pass
        try:
            sio.connect(server_url, transports=['websocket'])
            print("Reconnected to WebSocket server successfully!")
        except Exception as e:
            print(f"Failed to reconnect to WebSocket server: {e}")

def split_atlanta_messages(data):
    # Use regex to split on the control character + ATL + 15 digits
    pattern = rb'([\x00-\x0F]ATL\d{15})'
    matches = list(re.finditer(pattern, data))
    messages = []
    for i, match in enumerate(matches):
        start = match.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(data)
        messages.append(data[start:end])
    return messages

def parse_and_process_data(data, status_prefix):
    json_data = parse_json_data(data, status_prefix)
    if json_data:
        sos_state = json_data.get('sos', '0')
        if sos_state == '1':
            log_sos_to_mongodb(json_data)
        store_data_in_mongodb(json_data)
    else:
        print("[DEBUG] Invalid JSON format")
    
    return json_data.get('imei')

def parse_json_data(data, status_prefix):
    try:
        parts = data.split(',')
        expected_fields_count = 26
        if len(parts) >= expected_fields_count:
            binary_string = parts[14].strip('#')
            ignition, door, sos = '0', '0', '0'
            
            imei = clean_imei(parts[0])
                
            if len(binary_string) == 14:
                ignition = binary_string[0]
                door = binary_string[1]
                sos = binary_string[2]
                reserve1 = binary_string[3]
                reserve2 = binary_string[4]
                ac = binary_string[5]
                reserve3 = binary_string[6]
                main_power = binary_string[7]
                harsh_speed = binary_string[8]
                harsh_break = binary_string[9]
                arm = binary_string[10]
                sleep = binary_string[11]
                reserve4 = binary_string[12]
                status_accelerometer = binary_string[13]
            else:
                print(f"Received data does not contain at least {expected_fields_count} fields.")
                return None
            if parts[3] != 'A':
                latitude = ''
                longitude = ''
            else:
                latitude = str(nmea_to_decimal(parts[4])) if parts[4] != '-' else ''
                longitude = str(nmea_to_decimal(parts[6])) if parts[6] != '-' else ''
            speed_mph = float(parts[8]) if parts[8].replace('.', '', 1).isdigit() else 0.0
            speed_kmph = round(speed_mph * 1.60934, 2)
            status = parts[0]
            ist = timezone('Asia/Kolkata')
            date_time_str = f"{parts[10]} {parts[2]}"
            date_time_tz = datetime.strptime(date_time_str, '%d%m%y %H%M%S')
            date_time_ist = ist.localize(date_time_tz.replace(tzinfo=None))
            date_time = date_time_ist.astimezone(timezone('UTC'))
            print(status_prefix)
            json_data = {
                'status': status_prefix,
                'imei': clean_imei(parts[0]),
                'header': parts[1],
                'time': parts[2],
                'gps': parts[3],
                'latitude': latitude,
                'dir1': parts[5],
                'longitude': longitude,
                'dir2': parts[7],
                'speed': str(speed_kmph),
                'course': parts[9],
                'date': parts[10],
                'checksum': parts[13] if len(parts) > 13 else '0',
                'ignition': ignition,
                'door': door,
                'sos': sos,
                'reserve1': reserve1,
                'reserve2': reserve2,
                'ac': ac,
                'reserve3': reserve3,
                'main_power': main_power,
                'harsh_speed': harsh_speed,
                'harsh_break': harsh_break,
                'arm': arm,
                'sleep': sleep,
                'reserve4': reserve4,
                'status_accelerometer': status_accelerometer,
                'adc_voltage': parts[15],
                'one_wire_temp': parts[16],
                'i_btn': parts[17],
                'odometer': parts[18],
                'onBoard_temp': parts[19],
                'internal_bat': parts[20],
                'gsm_sig': parts[21],
                'mobCountryCode': parts[22],
                'mobNetworkCode': parts[23],
                'localAreaCode': parts[24],
                'cellid':  clean_cellid(parts[25]),  
                'date_time': date_time,
                'timestamp': datetime.now(timezone('UTC'))     
            }
            return json_data
        else:
            print(f"Received data does not contain at least {expected_fields_count} fields.")
            return None
    except Exception as e:
        print("Error parsing JSON data:", e)
        return None

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True

    def __init__(self, server_address, handler_cls):
        super().__init__(server_address, handler_cls)
        self.shutdown_event = threading.Event()

    def server_close(self):
        super().server_close()

class MyTCPHandler(socketserver.BaseRequestHandler):

    lock = threading.Lock()
    sos_active = False
    sos_alert_triggered = False

    def handle(self):
        addr = self.client_address
        print(f"[DEBUG] Connection established with {addr}")
        client_activity[addr] = {'last_seen': datetime.now(), 'connection': self.request}
        
        try:
            while True:
                receive_data = self.request.recv(4096)
                
                if not receive_data:
                    print(f"[DEBUG] Client {addr} disconnected gracefully.")
                    break
                
                print(f"[DEBUG] Received data from {addr}: {receive_data!r}")
                
                for msg_bytes in split_atlanta_messages(receive_data):
                    try:
                        index_03 = msg_bytes.find(b'\x03')
                        index_01 = msg_bytes.find(b'\x01')

                        first_special_index = min(i for i in [index_03, index_01] if i != -1)
                        first_special_char = msg_bytes[first_special_index:first_special_index+1]

                        status_prefix = first_special_char.hex()
                    except Exception as e:
                        print(f"[DEBUG] Error finding special characters in data: {e}")
                        
                    try:
                        decoded_data = msg_bytes.decode('utf-8').strip()
                        print(f"[DEBUG] Decoded data (utf-8) from {addr}: {decoded_data!r}")
                    except UnicodeDecodeError:
                        decoded_data = msg_bytes.decode('latin-1').strip()
                        print(f"[DEBUG] Decoded data (latin-1) from {addr}: {decoded_data!r}")
                    imei = parse_and_process_data(decoded_data, status_prefix)
                    
                print(imei)    
                print(rawLogList)
                if imei in rawLogList:
                    storRawData(imei, receive_data)
                    print(f"[DEBUG] Stored raw data for IMEI: {imei}")
                    
                try:
                    ack_packet = '$MSG,GETGPS<6906>&'
                    self.request.sendall(ack_packet.encode('utf-8')) # Use self.request.sendall
                    print(f"[DEBUG] Sent ACK to {addr} {ack_packet!r}")
                except Exception as e:
                    print(f"[DEBUG] Failed to send ACK to {addr}: {e}")

        except Exception as e:
            print(f"[DEBUG] Socket error with {addr}: {e}")
        finally:
            print(f"[DEBUG] Client {addr} handler finished.")

def run_servers():
    global sio, server_url, ssl_context
    sio = socketio.Client(ssl_verify=False)  # Disable verification for self-signed certs

    server_url = "https://cordonnx.com" 
    cert_path = os.path.join(os.path.dirname(__file__), "cert", "fullchain.pem")  

    ssl_context = ssl.create_default_context(cafile=cert_path)

    try:
        sio.connect(server_url, transports=['websocket'])
        print("Connected to WebSocket server successfully!")
    except Exception as e:
        print(f"Failed to connect to WebSocket server: {e}")
        
    HOST = "0.0.0.0"
    PORT = 8000
    server = ThreadedTCPServer((HOST, PORT), MyTCPHandler)
    print(f"Starting TCP Server @ IP: {HOST}, port: {PORT}")
    
    monitor_thread = threading.Thread(target=monitor_inactive_clients, daemon=True)
    monitor_thread.start()

    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("Server running. Press Ctrl+C to stop.")
    while True:
        try:
            signal.pause()
        except KeyboardInterrupt:
            print("Server shutting down...")
            server.shutdown()
            server.server_close()
            sys.exit(0)

def signal_handler(signal, frame):
    print("Received signal:", signal)
    sys.exit(0)

async def main():
    # Schedule the update_raw_log_list function to run every 5 minutes
    asyncio.create_task(update_raw_log_list())

if __name__ == "__main__":
    try:
        asyncio.run(main())
        lastEmitInitial()
        run_servers()
    except Exception as e:
        import traceback
        with open("fatal_error.log", "w") as f:
            f.write(traceback.format_exc())
        print("Fatal error occurred, see fatal_error.log")
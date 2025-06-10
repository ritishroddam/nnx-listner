import socket
import json
import time
from datetime import datetime, timedelta
from pytz import timezone
import os
from pymongo import MongoClient
import ssl
import googlemaps
from geopy.distance import geodesic
import socketio
from math import radians, sin, cos, sqrt, atan2, degrees

last_emit_time = {}
comamandImeiList = []

mongo_client = MongoClient("mongodb+srv://doadmin:4T81NSqj572g3o9f@db-mongodb-blr1-27716-c2bd0cae.mongo.ondigitalocean.com/admin?tls=true&authSource=admin", tz_aware=True)
db = mongo_client["nnx"]

collection = db['atlanta']
sos_logs_collection = db['sos_logs']  
distance_travelled_collection = db['distanceTravelled']
vehicle_inventory_collection = db['vehicle_inventory']
geoCodeCollection = db['geocoded_address']

gmaps = googlemaps.Client(key="AIzaSyCHlZGVWKK4ibhGfF__nv9B55VxCc-US84")

DIRECTIONS = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
BEARING_DEGREES = 360 / len(DIRECTIONS)

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
    # Check if the string has a leading zero that should be removed
    nmea_value = str(nmea_value)

    # Extract degrees and minutes
    if '.' in nmea_value:
        dot_index = nmea_value.index('.')
        degrees = float(nmea_value[:dot_index - 2])  # All characters before the last two digits before the dot
        minutes = float(nmea_value[dot_index - 2:])  # Last two digits before the dot and everything after
    else:
        raise ValueError("Invalid NMEA format")
    
    # Convert to decimal degrees
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
        # Step 1: Fast bounding box filter (0.5km range)
        nearby_entries = geoCodeCollection.find({
            "lat": {"$gte": lat - 0.0045, "$lte": lat + 0.0045},
            "lng": {"$gte": lng - 0.0045, "$lte": lng + 0.0045}
        })

        # Step 2: Precise distance calculation
        nearest_entry = None
        min_distance = 0.5  # Max search radius in km
        
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

        # Step 3: Geocode new coordinates
        reverse_geocode_result = gmaps.reverse_geocode((lat, lng))
        if not reverse_geocode_result:
            print("Geocoding API failed")
            return "Address unavailable"

        address = reverse_geocode_result[0]['formatted_address']
        
        # Step 4: Insert new entry
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
# Extract the last 15 characters of the IMEI
    return imei[-15:]

def clean_cellid(cellid):
    return cellid[:5]

def should_emit(imei, date_time):
    if imei not in last_emit_time or date_time - last_emit_time[imei] > timedelta(seconds=1):
        last_emit_time[imei] = date_time
        return True
    return False

def convert_to_datetime(date_str: str, time_str: str) -> datetime:
# Parse the date and time components
    dt_str = date_str + time_str  # Combine both
    dt_obj = datetime.strptime(dt_str, "%d%m%y%H%M%S")  # Convert to datetime object
    return dt_obj

# ...existing code...

def store_data_in_mongodb(json_data):
    print("[DEBUG] store_data_in_mongodb called")
    try:
        result = collection.insert_one(json_data)
        print("[DEBUG] Data inserted into MongoDB:", json_data)
        ensure_socket_connection()
        if json_data['gps'] == 'A' and should_emit(json_data['imei'],json_data['date_time']):
            print("[DEBUG] Emitting data to WebSocket")
            json_data['_id'] = str(json_data['_id'])
            json_data['date_time'] = str(json_data['date_time'])
            json_data['timestamp'] = str(json_data['timestamp'])
            inventory_data = vehicle_inventory_collection.find_one({'IMEI': json_data.get('imei')})
            if inventory_data:
                print("[DEBUG] Inventory data found for IMEI:", json_data.get('imei'))
                json_data['LicensePlateNumber'] = inventory_data.get('LicensePlateNumber', 'Unknown')
                json_data['VehicleType'] = inventory_data.get('vehicle_type', 'Unknown')
                json_data['slowSpeed'] = float(inventory_data.get('slowSpeed', "0") or 40.0)
                json_data['normalSpeed'] = float(inventory_data.get('normalSpeed', "0") or 60.0)
            else:
                print("[DEBUG] No inventory data found for IMEI:", json_data.get('imei'))
                json_data['LicensePlateNumber'] = 'Unknown'
                json_data['VehicleType'] = 'Unknown'
                json_data['slowSpeed'] = 40.0
                json_data['normalSpeed'] = 60.0
            json_data['address'] = geocodeInternal(json_data['latitude'],json_data['longitude'])
            if sio.connected:
                try:
                    sio.emit('vehicle_live_update', json_data)
                    sio.emit('vehicle_update', json_data)
                    print("[DEBUG] Data emitted to WebSocket")
                except Exception as e:
                    print("Error emitting data to WebSocket:", e)
    except Exception as e:
        print("Error storing data in MongoDB:", e)

def log_sos_to_mongodb(json_data):
    print("[DEBUG] log_sos_to_mongodb called")
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
        print("[DEBUG] SOS log inserted:", sos_log)
    except Exception as e:
        print("Error logging SOS alert to MongoDB:", e)

def ensure_socket_connection():
    print("[DEBUG] ensure_socket_connection called")
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

def parse_json_data(data):
    print("[DEBUG] parse_json_data called with data:", data)
    try:
        parts = data.split(',')
        expected_fields_count = 35
        if len(parts) >= expected_fields_count:
            binary_string = parts[14].strip('#')
            ignition, door, sos = '0', '0', '0'
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
            latitude = str(nmea_to_decimal(parts[4])) if parts[4] != '-' else ''
            longitude = str(nmea_to_decimal(parts[6])) if parts[6] != '-' else ''
            speed_mph = float(parts[8]) if parts[8].replace('.', '', 1).isdigit() else 0.0
            speed_kmph = round(speed_mph * 1.60934, 2)
            status = parts[0]
            status_prefix = status[:-15] if len(status) > 15 else ''
            ist = timezone('Asia/Kolkata')
            date_time_str = f"{parts[10]} {parts[2]}"
            date_time_tz = datetime.strptime(date_time_str, '%d%m%y %H%M%S')
            date_time_ist = ist.localize(date_time_tz.replace(tzinfo=None))
            date_time = date_time_ist.astimezone(timezone('UTC'))
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
            print("[DEBUG] Parsed JSON data:", json_data)
            return json_data
        else:
            print(f"Received data does not contain at least {expected_fields_count} fields.")
            return None
    except Exception as e:
        print("Error parsing JSON data:", e)
        return None

def parse_and_process_data(data):
    print("[DEBUG] parse_and_process_data called")
    try:
        json_data = parse_json_data(data)
        if json_data:
            sos_state = json_data.get('sos', '0')
            print(f"[DEBUG] SOS state: {sos_state}")
            if sos_state == '1':
                log_sos_to_mongodb(json_data)
            store_data_in_mongodb(json_data)
        else:
            print("[DEBUG] Invalid JSON format")
    except Exception as e:
        print("Error handling request:", e)
        print("Error data:", data, e)

def start_server():
    print("[DEBUG] start_server called")
    host = '0.0.0.0'
    port = 8000

    global sio, server_url, ssl_context
    sio = socketio.Client(ssl_verify=False)
    server_url = "https://cordonnx.com"
    cert_path = os.path.join(os.path.dirname(__file__), "cert", "fullchain.pem")
    ssl_context = ssl.create_default_context(cafile=cert_path)

    try:
        sio.connect(server_url, transports=['websocket'])
        print("[DEBUG] Connected to WebSocket server successfully!")
    except Exception as e:
        print(f"[DEBUG] Failed to connect to WebSocket server: {e}")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((host, port))
        server_socket.listen(5)
        print(f"[DEBUG] Listening for connections on port {port}...")

        while True:
            client_socket, client_address = server_socket.accept()
            print(f"[DEBUG] Connection established with {client_address}")
            with client_socket:
                while True:
                    try:
                        data = client_socket.recv(4096)
                        if not data:
                            print(f"[DEBUG] Client {client_address} disconnected gracefully.")
                            break
                        try:
                            decoded_data = data.decode('utf-8').strip()
                        except UnicodeDecodeError:
                            decoded_data = data.decode('latin-1').strip()
                        print(f"[DEBUG] Received data from {client_address}: {decoded_data}")
                        parse_and_process_data(decoded_data)
                    except ConnectionResetError:
                        print(f"[DEBUG] Client {client_address} disconnected unexpectedly.")
                        break
                    except Exception as e:
                        print(f"[DEBUG] Socket error with {client_address}: {e}")
                        break

if __name__ == "__main__":
    print("[DEBUG] __main__ entrypoint")
    start_server()

# def store_data_in_mongodb(json_data):
#     try:
#         result = collection.insert_one(json_data)

#         ensure_socket_connection()

#         if json_data['gps'] == 'A' and should_emit(json_data['imei'],json_data['date_time']):
#             json_data['_id'] = str(json_data['_id'])
#             json_data['date_time'] = str(json_data['date_time'])
#             json_data['timestamp'] = str(json_data['timestamp'])
#             inventory_data = vehicle_inventory_collection.find_one({'IMEI': json_data.get('imei')})
#             if inventory_data:
#                 json_data['LicensePlateNumber'] = inventory_data.get('LicensePlateNumber', 'Unknown')
#                 json_data['VehicleType'] = inventory_data.get('vehicle_type', 'Unknown')
#                 json_data['slowSpeed'] = float(inventory_data.get('slowSpeed', "0") or 40.0)
#                 json_data['normalSpeed'] = float(inventory_data.get('normalSpeed', "0") or 60.0)
#             else:
#                 json_data['LicensePlateNumber'] = 'Unknown'
#                 json_data['VehicleType'] = 'Unknown'
#                 json_data['slowSpeed'] = 40.0
#                 json_data['normalSpeed'] = 60.0
#             json_data['address'] = geocodeInternal(json_data['latitude'],json_data['longitude'])

#             if sio.connected:
#                 try:
#                     sio.emit('vehicle_live_update', json_data)
#                     sio.emit('vehicle_update', json_data)
#                 except Exception as e:
#                     print("Error emitting data to WebSocket:", e)
#     except Exception as e:
#         print("Error storing data in MongoDB:", e)


# def log_sos_to_mongodb(json_data):
#     try:
#         sos_log = {
#             'imei': json_data['imei'],
#             'date': json_data['date'],
#             'time': json_data['time'],
#             'latitude': json_data['latitude'],
#             'longitude': json_data['longitude'],
#             'date_time': json_data['date_time'],
#             'timestamp': json_data['timestamp'],
#         }
#         sos_logs_collection.insert_one(sos_log)
#     except Exception as e:
#         print("Error logging SOS alert to MongoDB:", e)

# def ensure_socket_connection():
#     """Ensure the socket is connected, and reconnect if necessary."""
#     if not sio.connected:
#         try:
#             sio.disconnect()
#         except Exception:
#             pass
#         try:
#             sio.connect(server_url, transports=['websocket'])
#             print("Reconnected to WebSocket server successfully!")
#         except Exception as e:
#             print(f"Failed to reconnect to WebSocket server: {e}")

# def parse_json_data(data):
#     try:
#         parts = data.split(',')
#         # print(f"Parsed data parts: {parts}")
#         expected_fields_count = 35

#         if len(parts) >= expected_fields_count:

#             binary_string = parts[14].strip('#')
#             # print(f"Binary string: {binary_string}")

#             ignition, door, sos = '0', '0', '0'

#             if len(binary_string) == 14:
#                 ignition = binary_string[0]
#                 door = binary_string[1]
#                 sos = binary_string[2]
#                 reserve1 = binary_string[3]
#                 reserve2 = binary_string[4]
#                 ac = binary_string[5]
#                 reserve3 = binary_string[6]
#                 main_power = binary_string[7]
#                 harsh_speed = binary_string[8]
#                 harsh_break = binary_string[9]
#                 arm = binary_string[10]
#                 sleep = binary_string[11]
#                 reserve4 = binary_string[12]
#                 status_accelerometer = binary_string[13]
#             else:
#                 print(f"Received data does not contain at least {expected_fields_count} fields.")
#                 return None

#             latitude = str(nmea_to_decimal(parts[4])) if parts[4] != '-' else ''
#             longitude = str(nmea_to_decimal(parts[6])) if parts[6] != '-' else ''

#             speed_mph = float(parts[8]) if parts[8].replace('.', '', 1).isdigit() else 0.0
#             speed_kmph = round(speed_mph * 1.60934, 2)

#             status = parts[0]
#             status_prefix = status[:-15] if len(status) > 15 else ''

#             ist = timezone('Asia/Kolkata')

#             date_time_str = f"{parts[10]} {parts[2]}"
#             date_time_tz = datetime.strptime(date_time_str, '%d%m%y %H%M%S')
#             date_time_ist = ist.localize(date_time_tz.replace(tzinfo=None))
#             date_time = date_time_ist.astimezone(timezone('UTC'))
            
#             # imei = clean_imei(parts[0])
#             # conn = request
#             # command = "#SERVERCHANGE::gps.cordonnx.com::8000;<6906>"
#             # if imei not in comamandImeiList:
#             #     if conn:
#             #         try:
#             #             conn.sendall(command.encode('utf-8'))
#             #             print("Sent command to client:", imei)
#             #             comamandImeiList.append(imei)
#             #         except Exception as e:
#             #             print("Error sending command to client:", e)
            
#             json_data = {
#                 'status': status_prefix,
#                 'imei': clean_imei(parts[0]),
#                 'header': parts[1],
#                 'time': parts[2],
#                 'gps': parts[3],
#                 'latitude': latitude,
#                 'dir1': parts[5],
#                 'longitude': longitude,
#                 'dir2': parts[7],
#                 'speed': str(speed_kmph),
#                 'course': parts[9],
#                 'date': parts[10],
#                 'checksum': parts[13] if len(parts) > 13 else '0',
#                 'ignition': ignition,
#                 'door': door,
#                 'sos': sos,
#                 'reserve1': reserve1,
#                 'reserve2': reserve2,
#                 'ac': ac,
#                 'reserve3': reserve3,
#                 'main_power': main_power,
#                 'harsh_speed': harsh_speed,
#                 'harsh_break': harsh_break,
#                 'arm': arm,
#                 'sleep': sleep,
#                 'reserve4': reserve4,
#                 'status_accelerometer': status_accelerometer,
#                 'adc_voltage': parts[15],
#                 'one_wire_temp': parts[16],
#                 'i_btn': parts[17],
#                 'odometer': parts[18],
#                 'onBoard_temp': parts[19],
#                 'internal_bat': parts[20],
#                 'gsm_sig': parts[21],
#                 'mobCountryCode': parts[22],
#                 'mobNetworkCode': parts[23],
#                 'localAreaCode': parts[24],
#                 'cellid':  clean_cellid(parts[25]),  
#                 'date_time': date_time,
#                 'timestamp': datetime.now(timezone('UTC'))     
#             }
#             return json_data
#         else:
#             print(f"Received data does not contain at least {expected_fields_count} fields.")
#             return None

#     except Exception as e:
#         print("Error parsing JSON data:", e)
#         return None

# def parse_and_process_data(data):
#     try:
#         # ...copy the logic from MyTCPHandler.parse_json_data and store_data_in_mongodb here...
#         # For brevity, only the call structure is shown. Use your existing parsing logic.
#         json_data = parse_json_data(data)
#         if json_data:
#             sos_state = json_data.get('sos', '0')
#             if sos_state == '1':
#                 log_sos_to_mongodb(json_data)
#             store_data_in_mongodb(json_data)
#         else:
#             print("Invalid JSON format")
#     except Exception as e:
#         print("Error handling request:", e)
#         print("Error data:", data, e)

# def start_server():
#     host = '0.0.0.0'
#     port = 8000
    
#     global sio, server_url, ssl_context
#     sio = socketio.Client(ssl_verify=False)
#     server_url = "https://cordonnx.com"
#     cert_path = os.path.join(os.path.dirname(__file__), "cert", "fullchain.pem")
#     ssl_context = ssl.create_default_context(cafile=cert_path)
    
#     try:
#         sio.connect(server_url, transports=['websocket'])
#         print("Connected to WebSocket server successfully!")
#     except Exception as e:
#         print(f"Failed to connect to WebSocket server: {e}")
        
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
#         server_socket.bind((host, port))
#         server_socket.listen(5)
#         print(f"Listening for connections on port {port}...")

#         while True:
#             client_socket, client_address = server_socket.accept()
#             print(f"Connection established with {client_address}")
#             with client_socket:
#                 while True:
#                     try:
#                         data = client_socket.recv(4096)
#                         if not data:
#                             print(f"Client {client_address} disconnected gracefully.")
#                             break
#                         try:
#                             decoded_data = data.decode('utf-8').strip()
#                         except UnicodeDecodeError:
#                             decoded_data = data.decode('latin-1').strip()
#                         parse_and_process_data(decoded_data)
#                     except ConnectionResetError:
#                         print(f"Client {client_address} disconnected unexpectedly.")
#                         break
#                     except Exception as e:
#                         print(f"Socket error with {client_address}: {e}")
#                         break

# if __name__ == "__main__":
    # start_server()
from flask import Flask
import os
import subprocess
import time
import threading

app = Flask(__name__)

from app import routes

# Paths to the scripts
ais140_path = os.path.join(os.path.dirname(__file__), 'ais140.py')
map_server_path = os.path.join(os.path.dirname(__file__), 'map_server.py')
run_distinct_vehicle_data_store_path = os.path.join(os.path.dirname(__file__), 'distinctVehicleDataStore.py')

# Subprocess references
subprocesses = {
    "ais140": None,
    "map_server": None,
    "distinct_vehicle_data_store": None
}

# Function to start subprocesses
def start_subprocesses():
    global subprocesses
    subprocesses["ais140"] = subprocess.Popen(['python', ais140_path])
    subprocesses["map_server"] = subprocess.Popen(['python', map_server_path])
    subprocesses["distinct_vehicle_data_store"] = subprocess.Popen(['python', run_distinct_vehicle_data_store_path])

# Function to monitor subprocesses
def monitor_subprocesses():
    while True:
        for name, process in subprocesses.items():
            if process.poll() is not None:  # Check if the process has terminated
                print(f"{name} has stopped. Restarting...")
                if name == "map_server":
                    subprocesses[name] = subprocess.Popen(['python', map_server_path])
                elif name == "distinct_vehicle_data_store":
                    subprocesses[name] = subprocess.Popen(['python', run_distinct_vehicle_data_store_path])
                elif name == "ais140":
                    subprocesses[name] = subprocess.Popen(['python', ais140_path])
        time.sleep(60)  # Check every 60 seconds

# Start subprocesses initially
start_subprocesses()

# Start the monitoring function in a separate thread
monitor_thread = threading.Thread(target=monitor_subprocesses, daemon=True)
monitor_thread.start()
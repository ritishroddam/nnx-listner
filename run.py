import subprocess
import os

map_server_path = os.path.join(os.path.dirname(__file__), 'map_server.py')
subprocess.Popen(['python', map_server_path])

run_distinct_vehicle_data_store_path = os.path.join(os.path.dirname(__file__), 'distinctVehicleDataStore.py')
subprocess.Popen(['python', run_distinct_vehicle_data_store_path])

run_calculate_past_distances_path = os.path.join(os.path.dirname(__file__), 'calculate_past_distances.py')
subprocess.Popen(['python', run_calculate_past_distances_path])
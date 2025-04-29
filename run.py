import subprocess
import os

def start_map_server():
    map_server_path = os.path.join(os.path.dirname(__file__), 'map_server.py')
    subprocess.Popen(['python', map_server_path])
    print("Map server started!")

def start_distinct_vehicle_data_store():
    run_distinct_vehicle_data_store_path = os.path.join(os.path.dirname(__file__), 'distinctVehicleDataStore.py')
    subprocess.Popen(['python', run_distinct_vehicle_data_store_path])
    print("Distinct vehicle data store started!")

def start_calculate_past_distances():
    run_calculate_past_distances_path = os.path.join(os.path.dirname(__file__), 'calculate_past_distances.py')
    subprocess.Popen(['python', run_calculate_past_distances_path])
    print("Calculate past distances started!")

if __name__ == '__main__':
    start_map_server()
    start_distinct_vehicle_data_store()
    start_calculate_past_distances()

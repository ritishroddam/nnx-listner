import subprocess
import os
import time

def start_process(script_name):
    """Start a subprocess for the given script and return the process."""
    script_path = os.path.join(os.path.dirname(__file__), script_name)
    return subprocess.Popen(['python', script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def monitor_processes(processes):
    """Monitor the processes and restart them if they stop."""
    while True:
        for script_name, process in processes.items():
            if process.poll() is not None:  # Check if the process has stopped
                print(f"{script_name} stopped. Restarting...")
                processes[script_name] = start_process(script_name)
        time.sleep(3600)  # Check every 5 seconds

if __name__ == '__main__':
    # Start all scripts
    processes = {
        'map_server.py': start_process('map_server.py'),
        'distinctVehicleDataStore.py': start_process('distinctVehicleDataStore.py'),
        'calculate_past_distances.py': start_process('calculate_past_distances.py'),
    }

    print("All scripts started. Monitoring processes...")
    monitor_processes(processes)
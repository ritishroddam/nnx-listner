import subprocess
import os

# List of files to run as subprocesses
files_to_run = ["map_server.py", "distinctVehicleDataStore.py", "calculate_past_distances.py"]

# Get the current directory
current_directory = os.path.dirname(os.path.abspath(__file__))

# Run each file as a subprocess
for file in files_to_run:
    file_path = os.path.join(current_directory, file)
    if os.path.exists(file_path):
        print(f"Running {file}...")
        subprocess.run(["python", file_path])
    else:
        print(f"File {file} not found in the workspace.")
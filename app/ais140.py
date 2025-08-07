import socket
from datetime import datetime

def start_server():
    host = '0.0.0.0'  # Listen on all available interfaces
    port = 8001

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen(5)
        print(f"[Debug {datetime.now()}]  Listening for connections on port {port}...")

        while True:
            client_socket, client_address = server_socket.accept()
            print(f"[Debug {datetime.now()}] Connection established with {client_address}")

            command = 'GPRSSTATUS'
            
            client_socket.sendall(command.encode('utf-8'))
            
            with client_socket:
                while True:
                    try:
                        data = client_socket.recv(1024)
                        if not data:
                            print(f"[Debug {datetime.now()}] Client {client_address} disconnected gracefully.")
                            break
                        print(f"[Debug {datetime.now()}] Received AIS 140 data: {data}")
                    except ConnectionResetError:
                        print(f"[Debug {datetime.now()}] Client {client_address} disconnected unexpectedly.")
                        break
                    except Exception as e:
                        print(f"[Debug {datetime.now()}] Socket error with {client_address}: {e}")
                        break

if __name__ == "__main__":
    start_server()
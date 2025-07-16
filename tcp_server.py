"""
Thread TCP Socket Server
Reference: https://docs.python.org/3/library/socketserver.html
@author dijo.davis@transight.in
"""

import time
import threading
import socketserver
from app0011 import C_TS_VLT_APP_0011


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """
    Threaded TCP Server Class - asynchronously handling multiple requests
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self.shutdown_event = kwargs["server_shutdown_event"]
        ip, port = self.server_address

    def server_close(self):
        return super().server_close()


if __name__ == "__main__":
    host = "0.0.0.0"
    port = 8000
    server_shutdown = threading.Event()

    server = ThreadedTCPServer((host, port),
                               C_TS_VLT_APP_0011,
                               server_shutdown_event=server_shutdown)
    with server:
        ip, port = server.server_address
        """Start a thread with the server -- that thread will then start one
        more thread for each request"""
        server_thread = threading.Thread(target=server.serve_forever)
        # True => Exit the server thread when the main thread terminates
        server_thread.daemon = True
        server_thread.start()
        print(f"Starting TCP Server @ IP: {host} port: {port}")

        try:
            while True:
                """Keep this thread running to stop server thread on demand"""
                time.sleep(5)
        except KeyboardInterrupt:
            print(f"Shutting down TCP Server @ IP: {host} port: {port}")
            server_shutdown.set()  # trigger event to stop child threads
            server.shutdown()

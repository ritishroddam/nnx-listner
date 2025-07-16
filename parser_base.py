import socket
from abc import ABC, abstractmethod
import threading
import socketserver
import time


class ProtocolParserBase(ABC, socketserver.BaseRequestHandler):
    """
    Abstract Base Class for Custom Protocols
    """

    timeout = 15  # sock recv/sendall timeout in seconds
    max_timeout_count = 20  # no of successive timeouts to close connection
    last_command_time = 0
    imei = 0  # default value till it get overridden by child

    def handle(self):
        """
        Socket Server Handler Class
        handle() is called per connection to the server
        """
        self.shutdown_event = self.server.shutdown_event
        self.socket = self.request

        self.ip, self.port = self.socket.getpeername()
        self.thread_id = threading.get_ident()

        print(f"Connection from {self.ip}:{self.port} @ {self.thread_id}")
        self.read_write_loop()

    @abstractmethod
    def process(self, data):
        pass

    def read(self):
        binary = self.socket.recv(1024)
        return str(binary, "ascii")

    def write(self, data):
        binary = bytes(data, "ascii")
        self.socket.sendall(binary)

    def write_command(self, command):
        self.socket.sendall(bytes(command, 'utf-8'))

    def update_imei(self, imei):
        if imei != 0:
            self.imei = int(imei)
        elif self.imei == 0:
            self.imei = self.thread_id

    def send_command(self):
        time_now = int(time.time())
        if self.last_command_time == 0 or time_now - self.last_command_time >= 15:
            """
            Send commands in 15 seconds interval
            """
            self.last_command_time = time_now
            try:
                """
                Read commands from real database here
                """
                commands = ["GET TSIMEI"]
                if commands is not None:
                    for row in commands:
                        print(f"sending command to {self.imei}: {row}")
                        self.write_command(row)
                        """
                        Update command status in database here
                        """
            except Exception as e:
                print(f"Command sending failed! \n {e}")

    def client_disconn_command(self, data):
        return data == "exit"

    def close_socket(self, msg):
        print(f"Disconnecting Client {self.ip}:{self.port}: {msg}")
        self.socket.close()

    def read_write_loop(self):
        toggle_read_n_command = True
        self.socket.settimeout(self.timeout)
        timeout_count = 0
        while True:
            if self.shutdown_event.is_set():
                self.close_socket("shutdown event")
                break
            if toggle_read_n_command:
                try:
                    data = self.read()
                    print(f"Message from {self.ip}:{self.port} : {data}")
                    if len(data) > 0:
                        response = self.process(data)
                        if response:
                            self.write(response)

                        if self.client_disconn_command(data):
                            self.close_socket("disconnect command from client")
                            break
                    else:  # empty data mean disconnect
                        self.close_socket("Empty Frame")
                        break
                    timeout_count = 0
                except socket.timeout:  # socket recv/sendall timeout
                    timeout_count += 1
                    pass
                except socket.error as e:  # any other errors -> socket closed
                    self.close_socket(f"socket error : {repr(e)}")
                    break

                if timeout_count >= self.max_timeout_count:  # close connection if successive timeout limit reached
                    self.close_socket("max socket timeout count reached")
                    break

            else:
                self.send_command()
            toggle_read_n_command = not toggle_read_n_command

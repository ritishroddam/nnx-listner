from socketio import Client
import asyncio
import websockets
async def listen():
    uri = "ws://localhost:8001"  # WebSocket server URL
    async with websockets.connect(uri) as websocket:
        print("Connected to the server")
        try:
            while True:
                data = await websocket.recv()
                print("Received data:", data)
        except websockets.ConnectionClosed:
            print("Disconnected from the server")

# Start the WebSocket listener
asyncio.run(listen())

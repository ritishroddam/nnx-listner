import asyncio
from datetime import datetime

async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"[Debug {datetime.now()}] Connection from {addr}")

    command = b'$MSG,GPRSSTATUS<6906>&'
    print(f"[Debug {datetime.now()}] Sending command: {command}")
    writer.write(command)
    await writer.drain()

    try:
        while True:
            data = await reader.read(1024)
            if not data:
                print(f"[Debug {datetime.now()}] {addr} disconnected gracefully.")
                break
            print(f"[Debug {datetime.now()}] Received ({len(data)} bytes): {data}")
            # Optionally, parse and check if this data corresponds to GPRS status response
    except Exception as e:
        print(f"[Debug {datetime.now()}] Error: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

async def main():
    server = await asyncio.start_server(handle_client, '0.0.0.0', 8001)
    print(f"[Debug {datetime.now()}] Listening on port 8001")
    async with server:
        await server.serve_forever()

if __name__ == '__main__':
    asyncio.run(main())
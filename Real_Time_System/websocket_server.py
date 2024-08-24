import asyncio
import websockets

clients = set()

async def handler(websocket, path):
    # Register client
    clients.add(websocket)
    try:
        async for message in websocket:
            # Broadcast received message to all connected clients
            # Use asyncio.gather() to run multiple send tasks concurrently
            await asyncio.gather(*(client.send(message) for client in clients))
    finally:
        # Unregister client
        clients.remove(websocket)

start_server = websockets.serve(handler, "localhost", 6789)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()

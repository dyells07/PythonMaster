import asyncio
import websockets

async def send_message(uri, message):
    async with websockets.connect(uri) as websocket:
        await websocket.send(message)
        print(f"Sent: {message}")

async def receive_messages(uri):
    async with websockets.connect(uri) as websocket:
        async for message in websocket:
            print(f"Received: {message}")

async def main():
    uri = "ws://localhost:6789"
    # Create a task for receiving messages
    receive_task = asyncio.create_task(receive_messages(uri))
    
    # Simulate sending messages
    while True:
        message = input("Enter message to send: ")
        await send_message(uri, message)

if __name__ == "__main__":
    asyncio.run(main())

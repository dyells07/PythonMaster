import asyncio
import websockets

async def communicate(uri):
    async with websockets.connect(uri) as websocket:
        print("Connected to the WebSocket server.")

        # Create a task for receiving messages
        async def receive_messages():
            try:
                async for message in websocket:
                    print(f"Received: {message}")
            except websockets.ConnectionClosed:
                print("Connection closed by the server.")

        # Run the receiver in the background
        receive_task = asyncio.create_task(receive_messages())

        # Simulate sending messages
        try:
            while True:
                message = input("Enter message to send: ")
                if message.lower() == "exit":
                    print("Exiting...")
                    break
                await websocket.send(message)
                print(f"Sent: {message}")
        except websockets.ConnectionClosed:
            print("Connection closed. Unable to send message.")
        finally:
            # Cancel the receive task when done
            receive_task.cancel()
            print("Disconnected from the WebSocket server.")

if __name__ == "__main__":
    uri = "ws://localhost:8000/ws"
    asyncio.run(communicate(uri))

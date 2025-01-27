from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import List

app = FastAPI()

class ConnectionManager:
    def __init__(self):
        self.connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.connections.remove(websocket)

    async def broadcast(self, message: str, sender: WebSocket):
        # Broadcast message to all connections except the sender
        for connection in self.connections:
            if connection is not sender:
                await connection.send_text(message)

manager = ConnectionManager()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Wait to receive a message from the client
            data = await websocket.receive_text()
            # Broadcast the message to all other connections
            await manager.broadcast(data, websocket)
    except WebSocketDisconnect:
        print(f"Client disconnected: {websocket.client.host}")
        manager.disconnect(websocket)
    except Exception as e:
        print(f"Unexpected error: {e}")
        manager.disconnect(websocket)

import socketio
from aiohttp import web

sio = socketio.AsyncServer()
app = web.Application()
sio.attach(app)

@sio.event
async def connect(sid, environ):
    print(f'Client connected: {sid}')

@sio.event
async def disconnect(sid):
    print(f'Client disconnected: {sid}')

@sio.event
async def message(sid, data):
    print(f'Received message from {sid}: {data}')
    # Broadcast the message to all clients
    await sio.emit('message', data)

if __name__ == '__main__':
    web.run_app(app, port=5000)

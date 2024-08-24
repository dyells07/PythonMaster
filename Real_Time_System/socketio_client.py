import socketio

sio = socketio.Client()

@sio.event
def connect():
    print("Connected to server")

@sio.event
def disconnect():
    print("Disconnected from server")

@sio.event
def message(data):
    print(f"Received: {data}")

def send_message(sio, message):
    sio.emit('message', message)

if __name__ == "__main__":
    sio.connect('http://localhost:5000')
    while True:
        message = input("Enter message to send: ")
        send_message(sio, message)

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict
import json

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class RoomManager:
    def __init__(self):
        # room_id -> { client_id: WebSocket }
        self.rooms: Dict[str, Dict[str, WebSocket]] = {}
        # room_id -> { client_id: client_name }
        self.names: Dict[str, Dict[str, str]] = {}

    async def connect(self, room_id: str, client_id: str, client_name: str, websocket: WebSocket, is_host: bool = False):
        await websocket.accept()
        if room_id != "lobby" and not is_host and room_id not in self.rooms:
            await websocket.send_json({"type": "room-not-found"})
            await websocket.close()
            return False
        if room_id != "lobby" and room_id in self.rooms and len(self.rooms[room_id]) >= 4:
            await websocket.send_json({"type": "room-full"})
            await websocket.close()
            return False
            
        if room_id not in self.rooms:
            self.rooms[room_id] = {}
            self.names[room_id] = {}
        self.rooms[room_id][client_id] = websocket
        self.names[room_id][client_id] = client_name
        
        # Notify others in the room that a new player joined
        await self.broadcast_to_room(room_id, {
            "type": "player-joined",
            "clientId": client_id,
            "clientName": client_name
        }, exclude=client_id)
        
        # Send current players to the new client
        other_clients = [
            {"id": cid, "name": self.names[room_id].get(cid, "Unknown")} 
            for cid in self.rooms[room_id].keys() if cid != client_id
        ]
        host_id = list(self.rooms[room_id].keys())[0]
        await websocket.send_json({
            "type": "room-info",
            "players": other_clients,
            "hostId": host_id
        })

    def disconnect(self, room_id: str, client_id: str):
        if room_id in self.rooms and client_id in self.rooms[room_id]:
            del self.rooms[room_id][client_id]
            if client_id in self.names[room_id]:
                del self.names[room_id][client_id]
            if not self.rooms[room_id]:
                del self.rooms[room_id]
                del self.names[room_id]
            else:
                return True # Room still has players
        return False

    async def broadcast_to_room(self, room_id: str, message: dict, exclude: str = None):
        if room_id in self.rooms:
            for cid, ws in self.rooms[room_id].items():
                if cid != exclude:
                    await ws.send_json(message)

    async def send_to_client(self, room_id: str, client_id: str, message: dict):
        if room_id in self.rooms and client_id in self.rooms[room_id]:
            await self.rooms[room_id][client_id].send_json(message)

manager = RoomManager()

@app.websocket("/ws/{room_id}/{client_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str, client_id: str, name: str = "Unknown", isHost: str = "false"):
    is_host_bool = isHost.lower() == "true"
    success = await manager.connect(room_id, client_id, name, websocket, is_host_bool)
    if success is False:
        return
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            msg_type = message.get("type")
            
            if msg_type == "update-profile":
                new_name = message.get("name")
                if new_name:
                    manager.names[room_id][client_id] = new_name
                    await manager.broadcast_to_room(room_id, {
                        "type": "profile-updated",
                        "clientId": client_id,
                        "clientName": new_name
                    })
                continue
                
            if msg_type == "lobby-chat":
                await manager.broadcast_to_room(room_id, {
                    "type": "lobby-chat",
                    "senderId": client_id,
                    "senderName": manager.names[room_id].get(client_id, "Unknown"),
                    "text": message.get("text")
                })
                continue

            if msg_type == "player-ready":
                await manager.broadcast_to_room(room_id, {
                    "type": "player-ready",
                    "clientId": client_id,
                    "isReady": message.get("isReady", False)
                }, exclude=client_id)
                continue

            if msg_type == "room-chat":
                await manager.broadcast_to_room(room_id, {
                    "type": "room-chat",
                    "clientId": client_id,
                    "senderName": manager.names[room_id].get(client_id, "Unknown"),
                    "text": message.get("text")
                }, exclude=client_id)
                continue
            
            # Signaling for WebRTC
            target_id = message.get("target")
            if target_id:
                message["sender"] = client_id
                await manager.send_to_client(room_id, target_id, message)
            else:
                message["sender"] = client_id
                await manager.broadcast_to_room(room_id, message, exclude=client_id)
                
    except WebSocketDisconnect:
        has_players = manager.disconnect(room_id, client_id)
        if has_players:
            new_host_id = list(manager.rooms[room_id].keys())[0]
            await manager.broadcast_to_room(room_id, {
                "type": "player-left",
                "clientId": client_id,
                "newHostId": new_host_id
            })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

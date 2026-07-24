import os
import json
import asyncio
from typing import Dict, List, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse
from pydantic import BaseModel
from dotenv import load_dotenv
import libsql_client

load_dotenv()

from contextlib import asynccontextmanager

# --- Database & Leaderboard ---
TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL", "file:local.db")
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "")

db_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_client
    db_client = libsql_client.create_client(
        url=TURSO_DATABASE_URL,
        auth_token=TURSO_AUTH_TOKEN
    )
    
    # Initialize DB
    tables = ["apple_1p", "apple_2p", "apple_3p", "apple_4p"]
    for t in tables:
        try:
            await db_client.execute(f"""
                CREATE TABLE IF NOT EXISTS {t} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    player_names TEXT NOT NULL,
                    score INTEGER NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
        except Exception as e:
            print(f"Error initializing DB table {t}: {e}")
    print("DB initialized successfully (async)")
    yield
    await db_client.close()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class LeaderboardEntry(BaseModel):
    playerCount: int
    playerNames: List[str]
    score: int

# SSE Clients
leaderboard_clients: list[asyncio.Queue] = []

async def fetch_all_leaderboards() -> dict:
    """Asynchronously fetch all leaderboard data from DB."""
    data = {}
    tables = [("1p", "apple_1p"), ("2p", "apple_2p"), ("3p", "apple_3p"), ("4p", "apple_4p")]
    for key, table in tables:
        try:
            res = await db_client.execute(f"SELECT player_names, score, created_at FROM {table} ORDER BY score DESC, created_at ASC LIMIT 20")
            data[key] = [{"playerNames": row[0], "score": row[1], "date": str(row[2]) if row[2] else ""} for row in res.rows]
        except Exception as e:
            data[key] = []
            print(f"Error fetching {table}: {e}")
    return data

async def notify_leaderboard_update():
    """Push updated leaderboard data to all connected SSE clients."""
    data = await fetch_all_leaderboards()
    message = json.dumps(data)
    for client_queue in leaderboard_clients:
        await client_queue.put(message)

@app.post("/api/leaderboard")
async def post_leaderboard(entry: LeaderboardEntry):
    if entry.playerCount < 1 or entry.playerCount > 4:
        return {"error": "Invalid player count"}
        
    table_name = f"apple_{entry.playerCount}p"
    names_str = ", ".join(entry.playerNames)
    score = entry.score
    
    print(f"[Leaderboard] Received: table={table_name}, names={names_str}, score={score}")
    
    try:
        # Check if it makes it to top 20
        res = await db_client.execute(f"SELECT score FROM {table_name} ORDER BY score DESC LIMIT 20")
        rows = res.rows
        
        if len(rows) < 20 or score > rows[-1][0]:
            # Insert with list-style args (libsql_client requirement)
            await db_client.execute(
                f"INSERT INTO {table_name} (player_names, score) VALUES (?, ?)", 
                [names_str, score]
            )
            # Delete below top 20
            await db_client.execute(f"""
                DELETE FROM {table_name} 
                WHERE id NOT IN (
                    SELECT id FROM {table_name} ORDER BY score DESC, created_at ASC LIMIT 20
                )
            """)
            print(f"[Leaderboard] Inserted into {table_name}: {names_str} = {score}")
            await notify_leaderboard_update()
            return {"success": True, "message": "Leaderboard updated"}
        else:
            print(f"[Leaderboard] Score {score} not high enough for {table_name} (min top20: {rows[-1][0]})")
            return {"success": True, "message": "Score not high enough"}
    except Exception as e:
        print(f"[Leaderboard] Error: {e}")
        return {"error": str(e)}

@app.get("/api/leaderboard/stream")
async def leaderboard_stream(request: Request):
    queue = asyncio.Queue()
    leaderboard_clients.append(queue)
    
    # Pre-load initial data into queue before starting generator
    initial_data = await fetch_all_leaderboards()
    await queue.put(json.dumps(initial_data))
    
    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=30)
                    yield {"data": data}
                except asyncio.TimeoutError:
                    # Send keepalive comment to prevent connection timeout
                    yield {"comment": "keepalive"}
        except asyncio.CancelledError:
            pass
        finally:
            if queue in leaderboard_clients:
                leaderboard_clients.remove(queue)
            
    return EventSourceResponse(event_generator())


# --- Room & WebRTC Signaling ---
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

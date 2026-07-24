import os
import asyncio
import libsql_client
from dotenv import load_dotenv

load_dotenv()

TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL")
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN")

async def insert_dummy():
    print(f"Connecting to {TURSO_DATABASE_URL}...")
    try:
        client = libsql_client.create_client(
            url=TURSO_DATABASE_URL,
            auth_token=TURSO_AUTH_TOKEN
        )
        
        await client.execute(
            "INSERT INTO apple_1p (player_names, score) VALUES (?, ?)",
            ["DummyPlayer", 50]
        )
        print("Successfully inserted dummy data (DummyPlayer, 50) into apple_1p")
        
        # Verify by fetching
        res = await client.execute("SELECT * FROM apple_1p ORDER BY score DESC LIMIT 5")
        for row in res.rows:
            print(f"ID: {row[0]}, Name: {row[1]}, Score: {row[2]}")
            
        await client.close()
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    asyncio.run(insert_dummy())

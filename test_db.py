import os
import asyncio
import libsql_client
from dotenv import load_dotenv

load_dotenv()

TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL")
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN")

async def test_db():
    print(f"Connecting to {TURSO_DATABASE_URL}...")
    try:
        client = libsql_client.create_client(
            url=TURSO_DATABASE_URL,
            auth_token=TURSO_AUTH_TOKEN
        )
        res = await client.execute("SELECT 1")
        print("Success:", res)
        await client.close()
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    asyncio.run(test_db())

"""
Run this once to initialize the database.
Usage: python init_db.py
"""
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

async def init():
    raw_url = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/genesis")
    DATABASE_URL = raw_url.replace("postgres://", "postgresql://", 1)
    conn = await asyncpg.connect(DATABASE_URL)
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(base_dir, "schema.sql"), "r") as f:
        schema = f.read()
    
    # Split and run statements
    statements = [s.strip() for s in schema.split(';') if s.strip()]
    for stmt in statements:
        try:
            await conn.execute(stmt)
            print(f"✅ {stmt[:60]}...")
        except Exception as e:
            if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
                print(f"⏭️  Already exists: {stmt[:40]}...")
            else:
                print(f"❌ Error: {e}\n   Statement: {stmt[:60]}")
    
    await conn.close()
    print("\n✅ Database initialized successfully!")

asyncio.run(init())

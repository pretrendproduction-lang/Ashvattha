import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

raw_url = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/genesis")
DATABASE_URL = raw_url.replace("postgres://", "postgresql://", 1)

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
schema_path = os.path.join(base_dir, "schema.sql")

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

with open(schema_path, "r") as f:
    schema = f.read()

statements = [s.strip() for s in schema.split(';') if s.strip() and not s.strip().startswith('--')]
for stmt in statements:
    try:
        cur.execute(stmt)
        print(f"✅ {stmt[:70]}...")
    except Exception as e:
        msg = str(e).lower()
        if "already exists" in msg or "duplicate" in msg:
            print(f"⏭️  Already exists — skipping")
        else:
            print(f"❌ {e}")

cur.close()
conn.close()
print("\n✅ Database ready!")

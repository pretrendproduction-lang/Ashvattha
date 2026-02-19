import pg8000.native
import os
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv

load_dotenv()

raw_url = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/genesis")
url = raw_url.replace("postgres://", "postgresql://", 1)
p = urlparse(url)
qs = parse_qs(p.query)
ssl = True if qs.get("sslmode", [""])[0] == "require" else None

conn = pg8000.native.Connection(
    host=p.hostname, port=p.port or 5432,
    database=p.path.lstrip("/"), user=p.username,
    password=p.password, ssl_context=ssl
)

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(base_dir, "schema.sql"), "r") as f:
    schema = f.read()

statements = [s.strip() for s in schema.split(';') if s.strip() and not s.strip().startswith('--')]
for stmt in statements:
    try:
        conn.run(stmt)
        conn.commit()
        print(f"✅ {stmt[:70]}")
    except Exception as e:
        msg = str(e).lower()
        if "already exists" in msg or "duplicate" in msg:
            print(f"⏭️  Already exists")
        else:
            print(f"❌ {e}")

conn.close()
print("\n✅ Database ready!")

import os
import asyncio
import pg8000.native
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

_raw_url = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/genesis")
DATABASE_URL = _raw_url.replace("postgres://", "postgresql://", 1)

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
# index.html lives in the same directory as main.py (repo root on Render)
FRONTEND_DIR = THIS_DIR
print(f"Frontend dir: {FRONTEND_DIR} (exists: {os.path.exists(FRONTEND_DIR)})")

def _parse_url(url):
    from urllib.parse import urlparse, parse_qs
    p = urlparse(url)
    qs = parse_qs(p.query)
    return {
        "host": p.hostname, "port": p.port or 5432,
        "database": p.path.lstrip("/"), "user": p.username,
        "password": p.password,
        "ssl_context": True if qs.get("sslmode", [""])[0] == "require" else None,
    }

DB_PARAMS = _parse_url(DATABASE_URL)
executor = ThreadPoolExecutor(max_workers=4)

def run_query(sql, params=None, fetch="all"):
    cfg = dict(DB_PARAMS)
    ssl = cfg.pop("ssl_context", None)
    conn = pg8000.native.Connection(**cfg, ssl_context=ssl)
    try:
        result = conn.run(sql, **(params or {}))
        # pg8000 native Connection auto-commits — no conn.commit() needed/available
        columns = [c["name"] for c in conn.columns] if conn.columns else []
        if fetch == "none":
            return None
        rows = [dict(zip(columns, row)) for row in (result or [])]
        if fetch == "one":
            return rows[0] if rows else None
        elif fetch == "val":
            return list(rows[0].values())[0] if rows else None
        return rows
    finally:
        conn.close()

async def db(sql, params=None, fetch="all"):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, lambda: run_query(sql, params, fetch))

def auto_init_db():
    """Run schema.sql automatically if tables don't exist yet."""
    try:
        cfg = dict(DB_PARAMS)
        ssl = cfg.pop("ssl_context", None)

        def make_conn():
            return pg8000.native.Connection(**cfg, ssl_context=ssl)

        # Check if persons table exists
        conn = make_conn()
        result = conn.run("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema='public' AND table_name='persons'
        """)
        exists = result[0][0] if result else 0
        conn.close()

        if exists:
            print("✅ Database already initialized")
            return

        print("🔧 First run — initializing database...")
        base_dir = os.path.dirname(os.path.abspath(__file__))
        schema_path = os.path.join(base_dir, "schema.sql")
        with open(schema_path, "r") as f:
            schema = f.read()

        # Execute statement by statement, each in its own connection
        statements = [s.strip() for s in schema.split(';')
                      if s.strip() and not s.strip().startswith('--')]
        for stmt in statements:
            try:
                c = make_conn()
                c.run(stmt)
                c.close()
            except Exception as e:
                msg = str(e).lower()
                if "already exists" in msg or "duplicate" in msg:
                    pass
                else:
                    print(f"⚠️  Schema warning: {str(e)[:80]}")

        print("✅ Database initialized successfully!")

    except Exception as e:
        print(f"❌ DB init error: {e}")

app = FastAPI(title="Ashvattha")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_agent_started = False

@app.on_event("startup")
async def startup():
    global _agent_started
    # Auto-init DB on first deploy
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor, auto_init_db)
    # Start the agent
    from agent import GenesisAgent
    asyncio.create_task(GenesisAgent(db).run_forever())
    _agent_started = True
    print("✅ Ashvattha Agent started")

# ── MODELS ──
class PersonCreate(BaseModel):
    name: str
    gender: Optional[str] = "unknown"
    era: Optional[str] = None
    approx_birth_year: Optional[int] = None
    type: Optional[str] = "human"

class RelationshipCreate(BaseModel):
    child_id: int
    parent_id: int
    parent_type: str
    confidence: float = 80.0
    source_url: Optional[str] = None

# ── PERSONS ──
@app.post("/api/persons")
async def create_person(data: PersonCreate):
    existing = await db("SELECT id FROM persons WHERE LOWER(name)=LOWER(:name) LIMIT 1", {"name": data.name}, "one")
    if existing:
        return {"id": existing["id"], "message": "Already exists"}
    count = (await db("SELECT COUNT(*) as c FROM persons WHERE is_genesis=TRUE", fetch="one") or {}).get("c", 0)
    genesis_code = f"G{count + 1}"
    row = await db("""
        INSERT INTO persons (name, gender, era, approx_birth_year, type, is_genesis, genesis_code)
        VALUES (:name,:gender,:era,:birth,:type,TRUE,:gc) RETURNING id
    """, {"name":data.name,"gender":data.gender,"era":data.era,"birth":data.approx_birth_year,"type":data.type,"gc":genesis_code}, "one")
    pid = row["id"]
    await db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',100)", {"p":pid}, "none")
    await db("INSERT INTO agent_log (person_id,person_name,action,detail) VALUES (:p,:n,'created',:d)",
             {"p":pid,"n":data.name,"d":f"Genesis block {genesis_code}"}, "none")
    return {"id": pid, "genesis_code": genesis_code}

@app.get("/api/persons/search")
async def search_persons(q: str, limit: int = 20):
    rows = await db("SELECT id,name,type,genesis_code,is_genesis,era,gender,approx_birth_year FROM persons WHERE LOWER(name) LIKE :q ORDER BY id ASC LIMIT :lim",
                    {"q": f"%{q.lower()}%", "lim": limit})
    return [dict(r) for r in (rows or [])]

@app.get("/api/persons/{person_id}")
async def get_person(person_id: int):
    person = await db("SELECT * FROM persons WHERE id=:id", {"id": person_id}, "one")
    if not person:
        raise HTTPException(404, "Not found")
    cats = await db("SELECT c.name,c.icon FROM categories c JOIN person_categories pc ON pc.category_id=c.id WHERE pc.person_id=:id", {"id": person_id})
    return {**dict(person), "categories": [dict(c) for c in (cats or [])]}

@app.get("/api/persons/{person_id}/tree")
async def get_tree(person_id: int, depth: int = 4):
    return await build_tree(person_id, depth, set())

async def build_tree(person_id, depth, visited):
    if person_id in visited or depth == 0:
        return None
    visited.add(person_id)
    person = await db("SELECT * FROM persons WHERE id=:id", {"id": person_id}, "one")
    if not person:
        return None
    parents = await db("""
        SELECT r.id,r.child_id,r.parent_id,r.parent_type,r.confidence,r.is_primary,r.is_branch,
               p.name as parent_name, p.is_genesis, p.genesis_code, p.era
        FROM relationships r JOIN persons p ON p.id=r.parent_id
        WHERE r.child_id=:cid ORDER BY r.is_primary DESC, r.confidence DESC
    """, {"cid": person_id})
    children = await db("""
        SELECT r.id,r.child_id,r.parent_id,r.parent_type,r.confidence,r.is_primary,
               p.name as child_name FROM relationships r
        JOIN persons p ON p.id=r.child_id
        WHERE r.parent_id=:pid AND r.is_primary=TRUE LIMIT 20
    """, {"pid": person_id})
    result = dict(person)
    result["parents"] = []
    result["children"] = []
    for pr in (parents or []):
        pd = dict(pr)
        srcs = await db("SELECT url FROM sources WHERE relationship_id=:rid LIMIT 3", {"rid": pr["id"]})
        pd["sources"] = [s["url"] for s in (srcs or [])]
        pd["person"] = await build_tree(pr["parent_id"], depth - 1, visited)
        result["parents"].append(pd)
    for cr in (children or []):
        result["children"].append(dict(cr))
    return result

@app.post("/api/relationships")
async def add_relationship(data: RelationshipCreate):
    existing = await db("SELECT id FROM relationships WHERE child_id=:c AND parent_type=:t AND is_primary=TRUE",
                        {"c": data.child_id, "t": data.parent_type}, "one")
    is_primary = existing is None
    existing_rel = await db("SELECT id FROM relationships WHERE child_id=:c AND parent_id=:p AND parent_type=:t",
                            {"c":data.child_id,"p":data.parent_id,"t":data.parent_type}, "one")
    if existing_rel:
        await db("UPDATE relationships SET confidence=:conf WHERE id=:id", {"conf":data.confidence,"id":existing_rel["id"]}, "none")
        rel_id = existing_rel["id"]
    else:
        row = await db("""INSERT INTO relationships (child_id,parent_id,parent_type,confidence,is_primary,is_branch)
            VALUES (:c,:p,:t,:conf,:prim,:branch) RETURNING id""",
            {"c":data.child_id,"p":data.parent_id,"t":data.parent_type,"conf":data.confidence,"prim":is_primary,"branch":not is_primary}, "one")
        rel_id = row["id"]
    if data.source_url:
        await db("INSERT INTO sources (relationship_id,url,source_type) VALUES (:r,:u,'user')", {"r":rel_id,"u":data.source_url}, "none")
    return {"relationship_id": rel_id, "is_primary": is_primary}

@app.get("/api/categories")
async def get_categories():
    rows = await db("""
        SELECT c.id,c.name,c.icon,c.parent_category_id,c.display_order,
               COUNT(pc.person_id) as person_count, parent.name as parent_name
        FROM categories c
        LEFT JOIN person_categories pc ON pc.category_id=c.id
        LEFT JOIN categories parent ON parent.id=c.parent_category_id
        GROUP BY c.id,c.name,c.icon,c.parent_category_id,c.display_order,parent.name
        ORDER BY c.display_order, c.name
    """)
    return [dict(r) for r in (rows or [])]

@app.get("/api/categories/{category_id}/persons")
async def get_category_persons(category_id: int, limit: int = 50, offset: int = 0):
    rows = await db("""
        SELECT p.id,p.name,p.type,p.era,p.gender,p.is_genesis,p.genesis_code,p.approx_birth_year
        FROM persons p JOIN person_categories pc ON pc.person_id=p.id
        WHERE pc.category_id=:cid ORDER BY p.approx_birth_year ASC NULLS LAST, p.name ASC
        LIMIT :lim OFFSET :off
    """, {"cid":category_id,"lim":limit,"off":offset})
    return [dict(r) for r in (rows or [])]

@app.get("/api/stats")
async def get_stats():
    total = (await db("SELECT COUNT(*) as c FROM persons WHERE type!='genesis'", fetch="one") or {}).get("c", 0)
    active_genesis = (await db("SELECT COUNT(*) as c FROM persons WHERE is_genesis=TRUE AND type!='genesis'", fetch="one") or {}).get("c", 0)
    merges = (await db("SELECT COUNT(*) as c FROM merge_log", fetch="one") or {}).get("c", 0)
    relationships = (await db("SELECT COUNT(*) as c FROM relationships", fetch="one") or {}).get("c", 0)
    queue_pending = (await db("SELECT COUNT(*) as c FROM agent_queue WHERE status='pending'", fetch="one") or {}).get("c", 0)
    return {
        "total_persons": total, "unresolved_genesis_blocks": active_genesis,
        "merges_completed": merges, "total_relationships": relationships,
        "queue_pending": queue_pending,
        "coverage_pct": round((merges / max(1, merges + active_genesis)) * 100, 1)
    }

@app.get("/api/activity")
async def get_activity(limit: int = 50):
    rows = await db("SELECT * FROM agent_log ORDER BY logged_at DESC LIMIT :lim", {"lim": limit})
    return [dict(r) for r in (rows or [])]

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/")
@app.head("/")
async def serve_frontend():
    index = os.path.join(FRONTEND_DIR, "index.html")
    if os.path.exists(index):
        return FileResponse(index)
    return {"status": "Ashvattha API running — frontend not found", "frontend_dir": FRONTEND_DIR}

# ── AGENT CONTROL (no shell needed) ──

@app.post("/api/agent/trigger")
async def trigger_agent():
    """Manually trigger the agent — useful when shell is unavailable (e.g. Render free tier)."""
    global _agent_started
    if _agent_started:
        queue_count = (await db("SELECT COUNT(*) as c FROM agent_queue WHERE status='pending'", fetch="one") or {}).get("c", 0)
        return {"status": "Agent already running", "queue_pending": queue_count}
    from agent import GenesisAgent
    asyncio.create_task(GenesisAgent(db).run_forever())
    _agent_started = True
    return {"status": "Agent started successfully"}

@app.get("/api/agent/status")
async def agent_status():
    """Check agent status and queue depth."""
    queue_pending = (await db("SELECT COUNT(*) as c FROM agent_queue WHERE status='pending'", fetch="one") or {}).get("c", 0)
    queue_processing = (await db("SELECT COUNT(*) as c FROM agent_queue WHERE status='processing'", fetch="one") or {}).get("c", 0)
    queue_done = (await db("SELECT COUNT(*) as c FROM agent_queue WHERE status='done'", fetch="one") or {}).get("c", 0)
    last_activity = await db("SELECT action, person_name, detail, logged_at FROM agent_log ORDER BY logged_at DESC LIMIT 1", fetch="one")
    return {
        "agent_running": _agent_started,
        "queue_pending": queue_pending,
        "queue_processing": queue_processing,
        "queue_done": queue_done,
        "last_activity": dict(last_activity) if last_activity else None
    }

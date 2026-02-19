import os
import asyncio
import psycopg2
import psycopg2.extras
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

_raw_url = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/genesis")
DATABASE_URL = _raw_url.replace("postgres://", "postgresql://", 1)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FRONTEND_DIR = os.path.join(BASE_DIR, "frontend")

executor = ThreadPoolExecutor(max_workers=4)

def run_query(sql, params=None, fetch="all"):
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            conn.commit()
            if fetch == "one":
                return cur.fetchone()
            elif fetch == "val":
                row = cur.fetchone()
                return list(row.values())[0] if row else None
            elif fetch == "none":
                return None
            else:
                return cur.fetchall()
    finally:
        conn.close()

async def db(sql, params=None, fetch="all"):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, lambda: run_query(sql, params, fetch))

app = FastAPI(title="Ashvattha")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
async def startup():
    from agent import GenesisAgent
    asyncio.create_task(GenesisAgent(db).run_forever())
    print("✅ Ashvattha Agent started")

# ── MODELS (pydantic v1 style) ──
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
    existing = await db("SELECT id FROM persons WHERE LOWER(name)=LOWER(%s) LIMIT 1", (data.name,), "one")
    if existing:
        return {"id": existing["id"], "message": "Already exists"}
    count = await db("SELECT COUNT(*) FROM persons WHERE is_genesis=TRUE", fetch="val") or 0
    genesis_code = f"G{count + 1}"
    person_id = await db("""
        INSERT INTO persons (name, gender, era, approx_birth_year, type, is_genesis, genesis_code)
        VALUES (%s,%s,%s,%s,%s,TRUE,%s) RETURNING id
    """, (data.name, data.gender, data.era, data.approx_birth_year, data.type, genesis_code), "val")
    await db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (%s,'both',100)", (person_id,), "none")
    await db("INSERT INTO agent_log (person_id,person_name,action,detail) VALUES (%s,%s,'created',%s)",
             (person_id, data.name, f"New person — genesis block {genesis_code}"), "none")
    return {"id": person_id, "genesis_code": genesis_code}

@app.get("/api/persons/search")
async def search_persons(q: str, limit: int = 20):
    rows = await db("SELECT id,name,type,genesis_code,is_genesis,era,gender,approx_birth_year FROM persons WHERE LOWER(name) LIKE LOWER(%s) ORDER BY agent_researched DESC, id ASC LIMIT %s",
                    (f"%{q}%", limit))
    return [dict(r) for r in (rows or [])]

@app.get("/api/persons/{person_id}")
async def get_person(person_id: int):
    person = await db("SELECT * FROM persons WHERE id=%s", (person_id,), "one")
    if not person:
        raise HTTPException(404, "Not found")
    cats = await db("SELECT c.name,c.icon FROM categories c JOIN person_categories pc ON pc.category_id=c.id WHERE pc.person_id=%s", (person_id,))
    return {**dict(person), "categories": [dict(c) for c in (cats or [])]}

@app.get("/api/persons/{person_id}/tree")
async def get_tree(person_id: int, depth: int = 4):
    return await build_tree(person_id, depth, set())

async def build_tree(person_id, depth, visited):
    if person_id in visited or depth == 0:
        return None
    visited.add(person_id)
    person = await db("SELECT * FROM persons WHERE id=%s", (person_id,), "one")
    if not person:
        return None
    parents = await db("""
        SELECT r.*, p.name as parent_name, p.is_genesis, p.genesis_code, p.era,
               ARRAY(SELECT url FROM sources WHERE relationship_id=r.id LIMIT 3) as sources
        FROM relationships r JOIN persons p ON p.id=r.parent_id
        WHERE r.child_id=%s ORDER BY r.is_primary DESC, r.confidence DESC
    """, (person_id,))
    children = await db("""
        SELECT r.*, p.name as child_name FROM relationships r
        JOIN persons p ON p.id=r.child_id
        WHERE r.parent_id=%s AND r.is_primary=TRUE LIMIT 20
    """, (person_id,))
    result = dict(person)
    result["parents"] = []
    result["children"] = []
    for pr in (parents or []):
        pd = dict(pr)
        pd["person"] = await build_tree(pr["parent_id"], depth - 1, visited)
        result["parents"].append(pd)
    for cr in (children or []):
        result["children"].append(dict(cr))
    return result

@app.post("/api/relationships")
async def add_relationship(data: RelationshipCreate):
    existing = await db("SELECT id FROM relationships WHERE child_id=%s AND parent_type=%s AND is_primary=TRUE",
                        (data.child_id, data.parent_type), "one")
    is_primary = existing is None
    rel_id = await db("""
        INSERT INTO relationships (child_id,parent_id,parent_type,confidence,is_primary,is_branch)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (child_id,parent_id,parent_type) DO UPDATE SET confidence=EXCLUDED.confidence
        RETURNING id
    """, (data.child_id, data.parent_id, data.parent_type, data.confidence, is_primary, not is_primary), "val")
    if data.source_url:
        await db("INSERT INTO sources (relationship_id,url,source_type) VALUES (%s,%s,'user')", (rel_id, data.source_url), "none")
    return {"relationship_id": rel_id, "is_primary": is_primary}

@app.get("/api/categories")
async def get_categories():
    rows = await db("""
        SELECT c.*, COUNT(pc.person_id) as person_count, parent.name as parent_name
        FROM categories c
        LEFT JOIN person_categories pc ON pc.category_id=c.id
        LEFT JOIN categories parent ON parent.id=c.parent_category_id
        GROUP BY c.id, parent.name ORDER BY c.display_order, c.name
    """)
    return [dict(r) for r in (rows or [])]

@app.get("/api/categories/{category_id}/persons")
async def get_category_persons(category_id: int, limit: int = 50, offset: int = 0):
    rows = await db("""
        SELECT p.id,p.name,p.type,p.era,p.gender,p.is_genesis,p.genesis_code,p.approx_birth_year
        FROM persons p JOIN person_categories pc ON pc.person_id=p.id
        WHERE pc.category_id=%s ORDER BY p.approx_birth_year ASC NULLS LAST, p.name ASC
        LIMIT %s OFFSET %s
    """, (category_id, limit, offset))
    return [dict(r) for r in (rows or [])]

@app.get("/api/stats")
async def get_stats():
    total = await db("SELECT COUNT(*) FROM persons WHERE type!='genesis'", fetch="val") or 0
    active_genesis = await db("SELECT COUNT(*) FROM persons WHERE is_genesis=TRUE AND type!='genesis'", fetch="val") or 0
    merges = await db("SELECT COUNT(*) FROM merge_log", fetch="val") or 0
    relationships = await db("SELECT COUNT(*) FROM relationships", fetch="val") or 0
    queue_pending = await db("SELECT COUNT(*) FROM agent_queue WHERE status='pending'", fetch="val") or 0
    return {
        "total_persons": total, "unresolved_genesis_blocks": active_genesis,
        "merges_completed": merges, "total_relationships": relationships,
        "queue_pending": queue_pending,
        "coverage_pct": round((merges / max(1, merges + active_genesis)) * 100, 1)
    }

@app.get("/api/activity")
async def get_activity(limit: int = 50):
    rows = await db("SELECT * FROM agent_log ORDER BY logged_at DESC LIMIT %s", (limit,))
    return [dict(r) for r in (rows or [])]

app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

@app.get("/")
async def serve_frontend():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))

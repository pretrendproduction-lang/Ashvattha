import os
import asyncio
import asyncpg
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional, List
from dotenv import load_dotenv
from agent import GenesisAgent

load_dotenv()

app = FastAPI(title="Genesis Tree", description="Universal Human Lineage Graph")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/genesis")
_pool = None

async def get_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    return _pool

@app.on_event("startup")
async def startup():
    pool = await get_pool()
    # Start the autonomous agent loop
    agent = GenesisAgent(pool)
    asyncio.create_task(agent.run_forever())
    print("✅ Genesis Agent started — growing forever")

# ─────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────
class PersonCreate(BaseModel):
    name: str
    gender: Optional[str] = "unknown"
    era: Optional[str] = None
    approx_birth_year: Optional[int] = None
    type: Optional[str] = "human"

class RelationshipCreate(BaseModel):
    child_id: int
    parent_id: int
    parent_type: str  # father | mother
    confidence: float = 80.0
    source_url: Optional[str] = None

# ─────────────────────────────────────────────
# PERSON ENDPOINTS
# ─────────────────────────────────────────────
@app.post("/api/persons")
async def create_person(data: PersonCreate):
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Check if exists
        existing = await conn.fetchrow(
            "SELECT id FROM persons WHERE LOWER(name) = LOWER($1) LIMIT 1", data.name
        )
        if existing:
            return {"id": existing["id"], "message": "Person already exists"}

        # Assign genesis block
        genesis_count = await conn.fetchval("SELECT COUNT(*) FROM persons WHERE is_genesis=TRUE")
        genesis_code = f"G{genesis_count + 1}"

        person_id = await conn.fetchval("""
            INSERT INTO persons (name, gender, era, approx_birth_year, type, is_genesis, genesis_code)
            VALUES ($1, $2, $3, $4, $5, TRUE, $6)
            RETURNING id
        """, data.name, data.gender, data.era, data.approx_birth_year, data.type, genesis_code)

        # Add to agent queue
        await conn.execute("""
            INSERT INTO agent_queue (person_id, direction, priority)
            VALUES ($1, 'both', 100)
        """, person_id)

        await conn.execute("""
            INSERT INTO agent_log (person_id, person_name, action, detail)
            VALUES ($1, $2, 'created', 'New person added — assigned genesis block ' || $3)
        """, person_id, data.name, genesis_code)

        return {"id": person_id, "genesis_code": genesis_code, "message": f"Created with genesis block {genesis_code}"}

@app.get("/api/persons/search")
async def search_persons(q: str, limit: int = 20):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, name, type, genesis_code, is_genesis, era, gender, approx_birth_year
            FROM persons
            WHERE to_tsvector('english', name) @@ plainto_tsquery('english', $1)
               OR LOWER(name) LIKE LOWER($2)
            ORDER BY agent_researched DESC, id ASC
            LIMIT $3
        """, q, f"%{q}%", limit)
        return [dict(r) for r in rows]

@app.get("/api/persons/{person_id}")
async def get_person(person_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        person = await conn.fetchrow("SELECT * FROM persons WHERE id=$1", person_id)
        if not person:
            raise HTTPException(404, "Person not found")

        categories = await conn.fetch("""
            SELECT c.name, c.icon FROM categories c
            JOIN person_categories pc ON pc.category_id = c.id
            WHERE pc.person_id = $1
        """, person_id)

        return {**dict(person), "categories": [dict(c) for c in categories]}

@app.get("/api/persons/{person_id}/tree")
async def get_tree(person_id: int, depth: int = 5):
    pool = await get_pool()
    async with pool.acquire() as conn:
        tree = await build_tree(conn, person_id, depth)
        return tree

async def build_tree(conn, person_id: int, depth: int, visited=None):
    if visited is None:
        visited = set()
    if person_id in visited or depth == 0:
        return None
    visited.add(person_id)

    person = await conn.fetchrow("SELECT * FROM persons WHERE id=$1", person_id)
    if not person:
        return None

    # Get parents (primary first)
    parents = await conn.fetch("""
        SELECT r.*, p.name as parent_name, p.type as parent_type_str,
               p.is_genesis, p.genesis_code, p.era,
               array_agg(s.url) FILTER (WHERE s.url IS NOT NULL) as sources
        FROM relationships r
        JOIN persons p ON p.id = r.parent_id
        LEFT JOIN sources s ON s.relationship_id = r.id
        WHERE r.child_id = $1
        GROUP BY r.id, p.name, p.type, p.is_genesis, p.genesis_code, p.era
        ORDER BY r.is_primary DESC, r.confidence DESC
    """, person_id)

    # Get children (primary only to avoid explosion)
    children = await conn.fetch("""
        SELECT r.*, p.name as child_name, p.type as child_type_str
        FROM relationships r
        JOIN persons p ON p.id = r.child_id
        WHERE r.parent_id = $1 AND r.is_primary = TRUE
        LIMIT 20
    """, person_id)

    result = dict(person)
    result["parents"] = []
    result["children"] = []

    for parent_rel in parents:
        parent_data = dict(parent_rel)
        parent_data["person"] = await build_tree(conn, parent_rel["parent_id"], depth - 1, visited)
        result["parents"].append(parent_data)

    for child_rel in children:
        child_data = dict(child_rel)
        result["children"].append(child_data)

    return result

# ─────────────────────────────────────────────
# RELATIONSHIP ENDPOINTS
# ─────────────────────────────────────────────
@app.post("/api/relationships")
async def add_relationship(data: RelationshipCreate):
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Check if a primary already exists
        existing_primary = await conn.fetchrow("""
            SELECT id FROM relationships
            WHERE child_id=$1 AND parent_type=$2 AND is_primary=TRUE
        """, data.child_id, data.parent_type)

        is_primary = existing_primary is None
        is_branch = not is_primary

        rel_id = await conn.fetchval("""
            INSERT INTO relationships (child_id, parent_id, parent_type, confidence, is_primary, is_branch)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (child_id, parent_id, parent_type) DO UPDATE
              SET confidence = EXCLUDED.confidence
            RETURNING id
        """, data.child_id, data.parent_id, data.parent_type, data.confidence, is_primary, is_branch)

        if data.source_url:
            await conn.execute("""
                INSERT INTO sources (relationship_id, url, source_type)
                VALUES ($1, $2, 'user')
            """, rel_id, data.source_url)

        # Check merge threshold (95%)
        if data.confidence >= 95.0:
            await check_genesis_merge(conn, data.child_id)

        return {"relationship_id": rel_id, "is_primary": is_primary}

async def check_genesis_merge(conn, person_id: int):
    """If confidence >= 95% and person was a genesis root, merge it"""
    person = await conn.fetchrow("SELECT * FROM persons WHERE id=$1", person_id)
    if person and person["is_genesis"]:
        # Check if now connected with high confidence
        high_conf = await conn.fetchrow("""
            SELECT parent_id FROM relationships
            WHERE child_id=$1 AND confidence >= 95
            LIMIT 1
        """, person_id)
        if high_conf:
            await conn.execute("""
                UPDATE persons SET is_genesis=FALSE, genesis_code=NULL WHERE id=$1
            """, person_id)
            await conn.execute("""
                INSERT INTO merge_log (genesis_person_id, genesis_code, merged_into_person_id, confidence_at_merge)
                VALUES ($1, $2, $3, 95)
            """, person_id, person["genesis_code"], high_conf["parent_id"])

# ─────────────────────────────────────────────
# CATEGORIES
# ─────────────────────────────────────────────
@app.get("/api/categories")
async def get_categories():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT c.*, 
                   COUNT(pc.person_id) as person_count,
                   parent.name as parent_name
            FROM categories c
            LEFT JOIN person_categories pc ON pc.category_id = c.id
            LEFT JOIN categories parent ON parent.id = c.parent_category_id
            GROUP BY c.id, parent.name
            ORDER BY c.display_order, c.name
        """)
        return [dict(r) for r in rows]

@app.get("/api/categories/{category_id}/persons")
async def get_category_persons(category_id: int, limit: int = 50, offset: int = 0):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT p.id, p.name, p.type, p.era, p.gender, p.is_genesis, p.genesis_code,
                   p.approx_birth_year
            FROM persons p
            JOIN person_categories pc ON pc.person_id = p.id
            WHERE pc.category_id = $1
            ORDER BY p.approx_birth_year ASC NULLS LAST, p.name ASC
            LIMIT $2 OFFSET $3
        """, category_id, limit, offset)
        return [dict(r) for r in rows]

# ─────────────────────────────────────────────
# STATS / PROGRESS
# ─────────────────────────────────────────────
@app.get("/api/stats")
async def get_stats():
    pool = await get_pool()
    async with pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM persons WHERE type != 'genesis'")
        genesis_count = await conn.fetchval("SELECT COUNT(*) FROM persons WHERE is_genesis=TRUE AND type='genesis'")
        active_genesis = await conn.fetchval("SELECT COUNT(*) FROM persons WHERE is_genesis=TRUE AND type!='genesis'")
        merges = await conn.fetchval("SELECT COUNT(*) FROM merge_log")
        relationships = await conn.fetchval("SELECT COUNT(*) FROM relationships")
        queue_pending = await conn.fetchval("SELECT COUNT(*) FROM agent_queue WHERE status='pending'")

        return {
            "total_persons": total,
            "unresolved_genesis_blocks": active_genesis,
            "total_genesis_blocks_ever": genesis_count + merges,
            "merges_completed": merges,
            "total_relationships": relationships,
            "queue_pending": queue_pending,
            "coverage_pct": round((merges / max(1, merges + active_genesis)) * 100, 1)
        }

@app.get("/api/activity")
async def get_activity(limit: int = 50):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM agent_log
            ORDER BY logged_at DESC
            LIMIT $1
        """, limit)
        return [dict(r) for r in rows]

# ─────────────────────────────────────────────
# SERVE FRONTEND
# ─────────────────────────────────────────────
app.mount("/static", StaticFiles(directory="../frontend"), name="static")

@app.get("/")
async def serve_frontend():
    return FileResponse("../frontend/index.html")

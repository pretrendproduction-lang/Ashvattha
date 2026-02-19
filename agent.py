"""
Genesis Agent — The autonomous tree builder
Runs forever, pulling from the queue, discovering ancestors/descendants,
writing to DB with confidence scores and source links.
Uses only free public sources: Wikipedia API + Wikidata SPARQL
"""

import asyncio
import httpx
import json
import re
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

logger = logging.getLogger("genesis_agent")
logging.basicConfig(level=logging.INFO)

WIKIPEDIA_API = "https://en.wikipedia.org/api/rest_v1"
WIKIPEDIA_SEARCH = "https://en.wikipedia.org/w/api.php"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"

# Category keywords for auto-tagging
CATEGORY_KEYWORDS = {
    "Greek Gods": ["greek god", "olympian", "greek deity", "ancient greek"],
    "Norse Gods": ["norse god", "viking god", "norse deity", "asgard"],
    "Hindu Deities": ["hindu god", "hindu deity", "vedic god", "avatar of"],
    "Egyptian Gods": ["egyptian god", "egyptian deity", "ancient egyptian"],
    "Roman Gods": ["roman god", "roman deity"],
    "Mesopotamian Gods": ["sumerian god", "babylonian god", "akkadian god", "mesopotamian"],
    "Egyptian Pharaohs": ["pharaoh", "king of egypt"],
    "Roman Emperors": ["roman emperor", "emperor of rome"],
    "Greek Kings": ["king of macedon", "king of sparta", "king of athens"],
    "Sumerian Kings": ["king of sumer", "sumerian king", "king of ur"],
    "Biblical Figures": ["biblical", "old testament", "new testament", "patriarch"],
    "British Royals": ["king of england", "queen of england", "british royal"],
    "Americans": ["american politician", "american president", "united states"],
    "South Asians": ["indian", "pakistani", "bangladeshi", "nepali"],
}


class GenesisAgent:
    def __init__(self, pool):
        self.pool = pool
        self.client = None
        self.running = True

    async def run_forever(self):
        """Main loop — never stops"""
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers={"User-Agent": "GenesisTree/1.0 (genealogy research; contact@genesistree.app)"}
        )
        logger.info("🌱 Genesis Agent awakened — growing the tree forever")

        # Seed initial famous people to bootstrap the graph
        await self.seed_initial_persons()

        while self.running:
            try:
                await self.process_next()
                await asyncio.sleep(2)  # polite delay between requests
            except Exception as e:
                logger.error(f"Agent error: {e}")
                await asyncio.sleep(10)

    async def seed_initial_persons(self):
        """Bootstrap with well-connected historical figures"""
        seeds = [
            ("Zeus", "mythological", "Mythological"),
            ("Adam", "human", "Ancient"),
            ("Abraham", "human", "Ancient"),
            ("Ramesses II", "human", "Ancient"),
            ("Alexander the Great", "human", "Ancient"),
            ("Julius Caesar", "human", "Ancient"),
            ("Genghis Khan", "human", "Medieval"),
            ("Charlemagne", "human", "Medieval"),
            ("Muhammad", "human", "Ancient"),
            ("Odin", "mythological", "Mythological"),
            ("Brahma", "mythological", "Mythological"),
            ("Osiris", "mythological", "Mythological"),
        ]
        async with self.pool.acquire() as conn:
            for name, ptype, era in seeds:
                existing = await conn.fetchval(
                    "SELECT id FROM persons WHERE LOWER(name)=LOWER($1)", name
                )
                if not existing:
                    genesis_count = await conn.fetchval(
                        "SELECT COUNT(*) FROM persons WHERE is_genesis=TRUE"
                    )
                    genesis_code = f"G{genesis_count + 1}"
                    pid = await conn.fetchval("""
                        INSERT INTO persons (name, type, era, is_genesis, genesis_code)
                        VALUES ($1, $2, $3, TRUE, $4) RETURNING id
                    """, name, ptype, era, genesis_code)

                    await conn.execute("""
                        INSERT INTO agent_queue (person_id, direction, priority)
                        VALUES ($1, 'both', 90)
                    """, pid)
                    logger.info(f"🌱 Seeded: {name}")

    async def process_next(self):
        """Pull highest priority item from queue and research it"""
        async with self.pool.acquire() as conn:
            job = await conn.fetchrow("""
                UPDATE agent_queue
                SET status='processing', last_attempt=NOW(), attempts=attempts+1
                WHERE id = (
                    SELECT id FROM agent_queue
                    WHERE status='pending'
                    ORDER BY priority DESC, created_at ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING *
            """)

            if not job:
                # Queue empty — re-seed with unresearched persons
                unresearched = await conn.fetch("""
                    SELECT id FROM persons
                    WHERE agent_researched=FALSE AND type != 'genesis'
                    LIMIT 10
                """)
                for p in unresearched:
                    await conn.execute("""
                        INSERT INTO agent_queue (person_id, direction, priority)
                        VALUES ($1, 'both', 50)
                        ON CONFLICT DO NOTHING
                    """, p["id"])
                return

            person = await conn.fetchrow("SELECT * FROM persons WHERE id=$1", job["person_id"])
            if not person:
                await conn.execute("UPDATE agent_queue SET status='done' WHERE id=$1", job["id"])
                return

        # Do the research
        logger.info(f"🔍 Researching: {person['name']}")
        discoveries = await self.research_person(person["name"], person.get("wikidata_id"))

        async with self.pool.acquire() as conn:
            if discoveries:
                await self.save_discoveries(conn, person, discoveries)
                await conn.execute(
                    "UPDATE persons SET agent_researched=TRUE WHERE id=$1", person["id"]
                )
                await conn.execute(
                    "UPDATE agent_queue SET status='done' WHERE id=$1", job["id"]
                )
            else:
                status = "failed" if job["attempts"] >= 3 else "pending"
                await conn.execute(
                    "UPDATE agent_queue SET status=$1 WHERE id=$2", status, job["id"]
                )

    async def research_person(self, name: str, wikidata_id: Optional[str] = None) -> Optional[Dict]:
        """Research a person using Wikipedia + Wikidata"""
        try:
            # Try Wikidata first (structured data)
            if not wikidata_id:
                wikidata_id = await self.find_wikidata_id(name)

            if wikidata_id:
                data = await self.fetch_wikidata_family(wikidata_id, name)
                if data:
                    return data

            # Fallback to Wikipedia text parsing
            return await self.fetch_wikipedia_family(name)

        except Exception as e:
            logger.error(f"Research failed for {name}: {e}")
            return None

    async def find_wikidata_id(self, name: str) -> Optional[str]:
        """Find Wikidata Q-ID for a person by name"""
        try:
            resp = await self.client.get(WIKIDATA_API, params={
                "action": "wbsearchentities",
                "search": name,
                "language": "en",
                "type": "item",
                "format": "json",
                "limit": 5
            })
            data = resp.json()
            for result in data.get("search", []):
                desc = result.get("description", "").lower()
                # Prefer human/deity results
                if any(kw in desc for kw in ["human", "person", "deity", "god", "king", "emperor", "pharaoh", "politician"]):
                    return result["id"]
            if data.get("search"):
                return data["search"][0]["id"]
        except Exception as e:
            logger.warning(f"Wikidata ID lookup failed for {name}: {e}")
        return None

    async def fetch_wikidata_family(self, qid: str, name: str) -> Optional[Dict]:
        """Fetch father, mother, children from Wikidata SPARQL"""
        query = f"""
        SELECT ?father ?fatherLabel ?mother ?motherLabel ?child ?childLabel ?childGender
               ?birthYear ?article WHERE {{
          OPTIONAL {{ wd:{qid} wdt:P22 ?father. }}
          OPTIONAL {{ wd:{qid} wdt:P25 ?mother. }}
          OPTIONAL {{ ?child wdt:P22 wd:{qid}. }}
          OPTIONAL {{ ?child wdt:P25 wd:{qid}. }}
          OPTIONAL {{ wd:{qid} wdt:P569 ?dob. BIND(YEAR(?dob) AS ?birthYear) }}
          OPTIONAL {{ ?article schema:about wd:{qid}; schema:isPartOf <https://en.wikipedia.org/> . }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
        LIMIT 50
        """
        try:
            resp = await self.client.get(
                WIKIDATA_SPARQL,
                params={"query": query, "format": "json"},
                headers={"Accept": "application/json"}
            )
            data = resp.json()
            results = data.get("results", {}).get("bindings", [])
            if not results:
                return None

            discoveries = {
                "wikidata_id": qid,
                "father": None,
                "mother": None,
                "children": [],
                "birth_year": None,
                "wikipedia_url": None,
                "description": None,
                "categories": []
            }

            seen_children = set()
            for row in results:
                if row.get("fatherLabel") and not discoveries["father"]:
                    discoveries["father"] = {
                        "name": row["fatherLabel"]["value"],
                        "wikidata_id": row["father"]["value"].split("/")[-1] if row.get("father") else None,
                        "confidence": 90.0
                    }
                if row.get("motherLabel") and not discoveries["mother"]:
                    discoveries["mother"] = {
                        "name": row["motherLabel"]["value"],
                        "wikidata_id": row["mother"]["value"].split("/")[-1] if row.get("mother") else None,
                        "confidence": 90.0
                    }
                if row.get("childLabel"):
                    child_name = row["childLabel"]["value"]
                    child_qid = row["child"]["value"].split("/")[-1] if row.get("child") else None
                    if child_name not in seen_children:
                        seen_children.add(child_name)
                        discoveries["children"].append({
                            "name": child_name,
                            "wikidata_id": child_qid,
                            "confidence": 88.0
                        })
                if row.get("birthYear") and not discoveries["birth_year"]:
                    discoveries["birth_year"] = int(row["birthYear"]["value"])
                if row.get("article") and not discoveries["wikipedia_url"]:
                    discoveries["wikipedia_url"] = row["article"]["value"]

            source_url = f"https://www.wikidata.org/wiki/{qid}"
            discoveries["source_url"] = source_url
            return discoveries

        except Exception as e:
            logger.error(f"Wikidata SPARQL failed for {qid}: {e}")
            return None

    async def fetch_wikipedia_family(self, name: str) -> Optional[Dict]:
        """Fallback: parse Wikipedia infobox for family data"""
        try:
            resp = await self.client.get(WIKIPEDIA_SEARCH, params={
                "action": "query",
                "titles": name,
                "prop": "revisions|categories",
                "rvprop": "content",
                "rvslots": "main",
                "format": "json",
                "formatversion": "2",
                "cllimit": "50"
            })
            data = resp.json()
            pages = data.get("query", {}).get("pages", [])
            if not pages or pages[0].get("missing"):
                return None

            page = pages[0]
            content = page.get("revisions", [{}])[0].get("slots", {}).get("main", {}).get("content", "")
            categories = [c["title"] for c in page.get("categories", [])]

            discoveries = {
                "father": self.extract_infobox_field(content, ["father", "Father"]),
                "mother": self.extract_infobox_field(content, ["mother", "Mother"]),
                "children": self.extract_children_from_infobox(content),
                "wikipedia_url": f"https://en.wikipedia.org/wiki/{page.get('title','').replace(' ','_')}",
                "source_url": f"https://en.wikipedia.org/wiki/{page.get('title','').replace(' ','_')}",
                "categories": categories,
                "description": None,
                "wikidata_id": None,
                "birth_year": None
            }

            # Auto-detect category tags
            auto_cats = self.detect_categories(content + " ".join(categories))
            discoveries["auto_categories"] = auto_cats

            return discoveries if (discoveries["father"] or discoveries["mother"] or discoveries["children"]) else None

        except Exception as e:
            logger.error(f"Wikipedia parse failed for {name}: {e}")
            return None

    def extract_infobox_field(self, content: str, fields: List[str]) -> Optional[Dict]:
        for field in fields:
            pattern = rf"\|\s*{field}\s*=\s*([^\|\n\}}]+)"
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                raw = match.group(1).strip()
                # Clean wiki markup
                name = re.sub(r'\[\[([^\|\]]+)(?:\|[^\]]+)?\]\]', r'\1', raw)
                name = re.sub(r'\{\{[^\}]+\}\}', '', name).strip()
                name = re.sub(r'<[^>]+>', '', name).strip()
                if name and len(name) > 1 and len(name) < 200:
                    return {"name": name, "confidence": 75.0}
        return None

    def extract_children_from_infobox(self, content: str) -> List[Dict]:
        children = []
        pattern = r"\|\s*(?:children|Children|issue|Issue)\s*=\s*((?:[^\|]|\n(?!\|))+)"
        match = re.search(pattern, content, re.IGNORECASE)
        if match:
            raw = match.group(1)
            # Extract linked names
            names = re.findall(r'\[\[([^\|\]]+)(?:\|[^\]]+)?\]\]', raw)
            for name in names[:10]:  # max 10 children
                name = name.strip()
                if name and len(name) > 1:
                    children.append({"name": name, "confidence": 70.0})
        return children

    def detect_categories(self, text: str) -> List[str]:
        text_lower = text.lower()
        matched = []
        for category, keywords in CATEGORY_KEYWORDS.items():
            if any(kw in text_lower for kw in keywords):
                matched.append(category)
        return matched

    async def save_discoveries(self, conn, person: Dict, discoveries: Dict):
        """Write all discovered relationships to DB"""
        person_id = person["id"]
        source_url = discoveries.get("source_url") or discoveries.get("wikipedia_url")

        # Update wikidata_id if found
        if discoveries.get("wikidata_id"):
            await conn.execute(
                "UPDATE persons SET wikidata_id=$1 WHERE id=$2",
                discoveries["wikidata_id"], person_id
            )
        if discoveries.get("birth_year"):
            await conn.execute(
                "UPDATE persons SET approx_birth_year=$1 WHERE id=$2",
                discoveries["birth_year"], person_id
            )
        if discoveries.get("wikipedia_url"):
            slug = discoveries["wikipedia_url"].split("/wiki/")[-1]
            await conn.execute(
                "UPDATE persons SET wikipedia_slug=$1 WHERE id=$2",
                slug, person_id
            )

        # Process father
        if discoveries.get("father"):
            await self.link_parent(conn, person_id, discoveries["father"], "father", source_url)

        # Process mother
        if discoveries.get("mother"):
            await self.link_parent(conn, person_id, discoveries["mother"], "mother", source_url)

        # Process children
        for child_data in discoveries.get("children", []):
            await self.link_child(conn, person_id, child_data, source_url)

        # Auto-tag categories
        auto_cats = discoveries.get("auto_categories", [])
        for cat_name in auto_cats:
            cat_id = await conn.fetchval(
                "SELECT id FROM categories WHERE name=$1", cat_name
            )
            if cat_id:
                await conn.execute("""
                    INSERT INTO person_categories (person_id, category_id)
                    VALUES ($1, $2) ON CONFLICT DO NOTHING
                """, person_id, cat_id)

        await conn.execute("""
            INSERT INTO agent_log (person_id, person_name, action, detail)
            VALUES ($1, $2, 'discovered', $3)
        """, person_id, person["name"],
            f"Found: father={discoveries.get('father',{}).get('name') if discoveries.get('father') else 'none'}, "
            f"mother={discoveries.get('mother',{}).get('name') if discoveries.get('mother') else 'none'}, "
            f"children={len(discoveries.get('children',[]))}"
        )

    async def link_parent(self, conn, child_id: int, parent_data: Dict, parent_type: str, source_url: str):
        """Find or create parent, then link"""
        parent_name = parent_data["name"]
        confidence = parent_data["confidence"]

        # Find or create parent person
        parent_id = await self.find_or_create_person(conn, parent_name, parent_data.get("wikidata_id"))
        if not parent_id or parent_id == child_id:
            return

        # Check for existing primary
        existing_primary = await conn.fetchrow("""
            SELECT id, confidence FROM relationships
            WHERE child_id=$1 AND parent_type=$2 AND is_primary=TRUE
        """, child_id, parent_type)

        if existing_primary:
            if confidence > existing_primary["confidence"]:
                # Better confidence — make this primary, demote old
                await conn.execute("""
                    UPDATE relationships SET is_primary=FALSE, is_branch=TRUE
                    WHERE id=$1
                """, existing_primary["id"])
                is_primary = True
                is_branch = False
            else:
                is_primary = False
                is_branch = True
        else:
            is_primary = True
            is_branch = False

        try:
            rel_id = await conn.fetchval("""
                INSERT INTO relationships (child_id, parent_id, parent_type, confidence, is_primary, is_branch)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (child_id, parent_id, parent_type) DO UPDATE
                  SET confidence=EXCLUDED.confidence, is_primary=EXCLUDED.is_primary
                RETURNING id
            """, child_id, parent_id, parent_type, confidence, is_primary, is_branch)

            if source_url and rel_id:
                await conn.execute("""
                    INSERT INTO sources (relationship_id, url, source_type)
                    VALUES ($1, $2, 'wikidata')
                    ON CONFLICT DO NOTHING
                """, rel_id, source_url)

            # Check merge threshold
            if confidence >= 95.0:
                child = await conn.fetchrow("SELECT * FROM persons WHERE id=$1", child_id)
                if child and child["is_genesis"]:
                    await conn.execute(
                        "UPDATE persons SET is_genesis=FALSE, genesis_code=NULL WHERE id=$1", child_id
                    )
                    await conn.execute("""
                        INSERT INTO merge_log (genesis_person_id, genesis_code, merged_into_person_id, confidence_at_merge)
                        VALUES ($1, $2, $3, $4)
                    """, child_id, child["genesis_code"], parent_id, confidence)
                    logger.info(f"🔗 MERGE: {child['name']} connected, genesis dissolved")

            # Queue parent for research
            await conn.execute("""
                INSERT INTO agent_queue (person_id, direction, priority)
                VALUES ($1, 'both', 80)
                ON CONFLICT DO NOTHING
            """, parent_id)

        except Exception as e:
            logger.warning(f"Failed to link {parent_type}: {e}")

    async def link_child(self, conn, parent_id: int, child_data: Dict, source_url: str):
        """Find or create child, then link to parent"""
        child_name = child_data["name"]
        child_id = await self.find_or_create_person(conn, child_name, child_data.get("wikidata_id"))
        if not child_id or child_id == parent_id:
            return

        try:
            rel_id = await conn.fetchval("""
                INSERT INTO relationships (child_id, parent_id, parent_type, confidence, is_primary)
                VALUES ($1, $2, 'father', $3, TRUE)
                ON CONFLICT (child_id, parent_id, parent_type) DO UPDATE
                  SET confidence=EXCLUDED.confidence
                RETURNING id
            """, child_id, parent_id, child_data["confidence"])

            if source_url and rel_id:
                await conn.execute("""
                    INSERT INTO sources (relationship_id, url, source_type)
                    VALUES ($1, $2, 'wikidata') ON CONFLICT DO NOTHING
                """, rel_id, source_url)

            # Queue child for research
            await conn.execute("""
                INSERT INTO agent_queue (person_id, direction, priority)
                VALUES ($1, 'both', 60)
                ON CONFLICT DO NOTHING
            """, child_id)

        except Exception as e:
            logger.warning(f"Failed to link child {child_name}: {e}")

    async def find_or_create_person(self, conn, name: str, wikidata_id: Optional[str] = None) -> Optional[int]:
        """Find existing person or create new one with genesis block"""
        # Try by wikidata_id first
        if wikidata_id:
            pid = await conn.fetchval("SELECT id FROM persons WHERE wikidata_id=$1", wikidata_id)
            if pid:
                return pid

        # Try by name
        pid = await conn.fetchval(
            "SELECT id FROM persons WHERE LOWER(name)=LOWER($1) LIMIT 1", name
        )
        if pid:
            if wikidata_id:
                await conn.execute("UPDATE persons SET wikidata_id=$1 WHERE id=$2", wikidata_id, pid)
            return pid

        # Create new with genesis block
        genesis_count = await conn.fetchval("SELECT COUNT(*) FROM persons WHERE is_genesis=TRUE")
        genesis_code = f"G{genesis_count + 1}"

        try:
            pid = await conn.fetchval("""
                INSERT INTO persons (name, wikidata_id, is_genesis, genesis_code, type)
                VALUES ($1, $2, TRUE, $3, 'human')
                RETURNING id
            """, name, wikidata_id, genesis_code)

            await conn.execute("""
                INSERT INTO agent_log (person_id, person_name, action, detail)
                VALUES ($1, $2, 'discovered', 'New person discovered by agent')
            """, pid, name)

            return pid
        except Exception as e:
            logger.warning(f"Could not create person {name}: {e}")
            return None

"""
Ashvattha Agent — autonomous tree builder
Uses psycopg2 via the shared async db() wrapper from main.py
"""
import asyncio
import httpx
import re
import logging
from typing import Optional, List, Dict

logger = logging.getLogger("ashvattha_agent")
logging.basicConfig(level=logging.INFO)

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIPEDIA_SEARCH = "https://en.wikipedia.org/w/api.php"

CATEGORY_KEYWORDS = {
    "Greek Gods": ["greek god", "olympian", "greek deity"],
    "Norse Gods": ["norse god", "asgard", "viking god"],
    "Hindu Deities": ["hindu god", "hindu deity", "vedic god"],
    "Egyptian Gods": ["egyptian god", "egyptian deity"],
    "Roman Gods": ["roman god", "roman deity"],
    "Mesopotamian Gods": ["sumerian god", "babylonian god", "mesopotamian"],
    "Egyptian Pharaohs": ["pharaoh", "king of egypt"],
    "Roman Emperors": ["roman emperor", "emperor of rome"],
    "Biblical Figures": ["biblical", "old testament", "new testament", "patriarch"],
    "British Royals": ["king of england", "queen of england", "british royal"],
    "Americans": ["american president", "united states president"],
    "South Asians": ["indian politician", "prime minister of india"],
}

SEEDS = [
    ("Zeus", "mythological"), ("Adam", "human"), ("Abraham", "human"),
    ("Ramesses II", "human"), ("Alexander the Great", "human"),
    ("Julius Caesar", "human"), ("Genghis Khan", "human"),
    ("Charlemagne", "human"), ("Odin", "mythological"),
    ("Brahma", "mythological"), ("Osiris", "mythological"),
    ("Muhammad", "human"),
]

class GenesisAgent:
    def __init__(self, db):
        self.db = db
        self.client = None

    async def run_forever(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers={"User-Agent": "Ashvattha/1.0 (genealogy research)"}
        )
        logger.info("🌱 Ashvattha Agent awakened")
        await self.seed_initial_persons()
        while True:
            try:
                await self.process_next()
                await asyncio.sleep(3)
            except Exception as e:
                logger.error(f"Agent loop error: {e}")
                await asyncio.sleep(15)

    async def seed_initial_persons(self):
        for name, ptype in SEEDS:
            existing = await self.db("SELECT id FROM persons WHERE LOWER(name)=LOWER(%s)", (name,), "val")
            if not existing:
                count = await self.db("SELECT COUNT(*) FROM persons WHERE is_genesis=TRUE", fetch="val") or 0
                gc = f"G{count + 1}"
                pid = await self.db("""
                    INSERT INTO persons (name, type, is_genesis, genesis_code)
                    VALUES (%s,%s,TRUE,%s) RETURNING id
                """, (name, ptype, gc), "val")
                await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (%s,'both',90)",
                              (pid,), "none")
                logger.info(f"🌱 Seeded: {name}")

    async def process_next(self):
        # Get next pending job
        job = await self.db("""
            SELECT id, person_id FROM agent_queue
            WHERE status='pending' ORDER BY priority DESC, created_at ASC LIMIT 1
        """, fetch="one")
        if not job:
            # Re-queue unresearched persons
            unresearched = await self.db("""
                SELECT id FROM persons WHERE agent_researched=FALSE AND type!='genesis' LIMIT 5
            """, fetch="all")
            for p in (unresearched or []):
                await self.db("""
                    INSERT INTO agent_queue (person_id,direction,priority)
                    VALUES (%s,'both',40) ON CONFLICT DO NOTHING
                """, (p["id"],), "none")
            return

        await self.db("UPDATE agent_queue SET status='processing' WHERE id=%s", (job["id"],), "none")
        person = await self.db("SELECT * FROM persons WHERE id=%s", (job["person_id"],), "one")
        if not person:
            await self.db("UPDATE agent_queue SET status='done' WHERE id=%s", (job["id"],), "none")
            return

        logger.info(f"🔍 Researching: {person['name']}")
        discoveries = await self.research_person(person["name"], person.get("wikidata_id"))

        if discoveries:
            await self.save_discoveries(person, discoveries)
            await self.db("UPDATE persons SET agent_researched=TRUE WHERE id=%s", (person["id"],), "none")
            await self.db("UPDATE agent_queue SET status='done' WHERE id=%s", (job["id"],), "none")
        else:
            attempts = await self.db("SELECT attempts FROM agent_queue WHERE id=%s", (job["id"],), "val") or 0
            status = "failed" if attempts >= 3 else "pending"
            await self.db("UPDATE agent_queue SET status=%s, attempts=attempts+1 WHERE id=%s",
                          (status, job["id"]), "none")

    async def research_person(self, name: str, wikidata_id: Optional[str] = None) -> Optional[Dict]:
        try:
            if not wikidata_id:
                wikidata_id = await self.find_wikidata_id(name)
            if wikidata_id:
                data = await self.fetch_wikidata_family(wikidata_id)
                if data:
                    return data
            return await self.fetch_wikipedia_family(name)
        except Exception as e:
            logger.error(f"Research failed for {name}: {e}")
            return None

    async def find_wikidata_id(self, name: str) -> Optional[str]:
        try:
            r = await self.client.get(WIKIDATA_API, params={
                "action": "wbsearchentities", "search": name,
                "language": "en", "type": "item", "format": "json", "limit": 5
            })
            data = r.json()
            for result in data.get("search", []):
                desc = result.get("description", "").lower()
                if any(k in desc for k in ["human", "person", "deity", "god", "king", "emperor", "pharaoh"]):
                    return result["id"]
            if data.get("search"):
                return data["search"][0]["id"]
        except Exception as e:
            logger.warning(f"Wikidata ID lookup failed: {e}")
        return None

    async def fetch_wikidata_family(self, qid: str) -> Optional[Dict]:
        query = f"""
        SELECT ?father ?fatherLabel ?mother ?motherLabel ?child ?childLabel ?birthYear ?article WHERE {{
          OPTIONAL {{ wd:{qid} wdt:P22 ?father. }}
          OPTIONAL {{ wd:{qid} wdt:P25 ?mother. }}
          OPTIONAL {{ ?child wdt:P22 wd:{qid}. }}
          OPTIONAL {{ wd:{qid} wdt:P569 ?dob. BIND(YEAR(?dob) AS ?birthYear) }}
          OPTIONAL {{ ?article schema:about wd:{qid}; schema:isPartOf <https://en.wikipedia.org/> . }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }} LIMIT 40
        """
        try:
            r = await self.client.get(WIKIDATA_SPARQL,
                params={"query": query, "format": "json"},
                headers={"Accept": "application/json"})
            bindings = r.json().get("results", {}).get("bindings", [])
            if not bindings:
                return None
            d = {"wikidata_id": qid, "father": None, "mother": None, "children": [],
                 "birth_year": None, "wikipedia_url": None, "source_url": f"https://www.wikidata.org/wiki/{qid}",
                 "auto_categories": []}
            seen = set()
            for row in bindings:
                if row.get("fatherLabel") and not d["father"]:
                    d["father"] = {"name": row["fatherLabel"]["value"],
                                   "wikidata_id": row["father"]["value"].split("/")[-1] if row.get("father") else None,
                                   "confidence": 90.0}
                if row.get("motherLabel") and not d["mother"]:
                    d["mother"] = {"name": row["motherLabel"]["value"],
                                   "wikidata_id": row["mother"]["value"].split("/")[-1] if row.get("mother") else None,
                                   "confidence": 88.0}
                if row.get("childLabel"):
                    cn = row["childLabel"]["value"]
                    if cn not in seen:
                        seen.add(cn)
                        d["children"].append({"name": cn,
                            "wikidata_id": row["child"]["value"].split("/")[-1] if row.get("child") else None,
                            "confidence": 85.0})
                if row.get("birthYear") and not d["birth_year"]:
                    d["birth_year"] = int(row["birthYear"]["value"])
                if row.get("article") and not d["wikipedia_url"]:
                    d["wikipedia_url"] = row["article"]["value"]
            return d
        except Exception as e:
            logger.error(f"Wikidata SPARQL failed for {qid}: {e}")
            return None

    async def fetch_wikipedia_family(self, name: str) -> Optional[Dict]:
        try:
            r = await self.client.get(WIKIPEDIA_SEARCH, params={
                "action": "query", "titles": name, "prop": "revisions|categories",
                "rvprop": "content", "rvslots": "main", "format": "json",
                "formatversion": "2", "cllimit": "30"
            })
            data = r.json()
            pages = data.get("query", {}).get("pages", [])
            if not pages or pages[0].get("missing"):
                return None
            page = pages[0]
            content = page.get("revisions", [{}])[0].get("slots", {}).get("main", {}).get("content", "")
            cats = [c["title"] for c in page.get("categories", [])]
            father = self.extract_field(content, ["father", "Father"])
            mother = self.extract_field(content, ["mother", "Mother"])
            children = self.extract_children(content)
            if not father and not mother and not children:
                return None
            return {
                "father": father, "mother": mother, "children": children,
                "source_url": f"https://en.wikipedia.org/wiki/{page.get('title','').replace(' ','_')}",
                "wikipedia_url": f"https://en.wikipedia.org/wiki/{page.get('title','').replace(' ','_')}",
                "wikidata_id": None, "birth_year": None,
                "auto_categories": self.detect_categories(content + " ".join(cats))
            }
        except Exception as e:
            logger.error(f"Wikipedia parse failed for {name}: {e}")
            return None

    def extract_field(self, content, fields):
        for f in fields:
            m = re.search(rf"\|\s*{f}\s*=\s*([^\|\n\}}]+)", content, re.IGNORECASE)
            if m:
                raw = re.sub(r'\[\[([^\|\]]+)(?:\|[^\]]+)?\]\]', r'\1', m.group(1))
                raw = re.sub(r'\{\{[^\}]+\}\}|<[^>]+>', '', raw).strip()
                if raw and 1 < len(raw) < 200:
                    return {"name": raw, "confidence": 72.0}
        return None

    def extract_children(self, content):
        children = []
        m = re.search(r"\|\s*(?:children|issue)\s*=\s*((?:[^\|]|\n(?!\|))+)", content, re.IGNORECASE)
        if m:
            for name in re.findall(r'\[\[([^\|\]]+)(?:\|[^\]]+)?\]\]', m.group(1))[:8]:
                name = name.strip()
                if name and len(name) > 1:
                    children.append({"name": name, "confidence": 68.0})
        return children

    def detect_categories(self, text):
        tl = text.lower()
        return [cat for cat, kws in CATEGORY_KEYWORDS.items() if any(k in tl for k in kws)]

    async def save_discoveries(self, person: Dict, d: Dict):
        pid = person["id"]
        if d.get("wikidata_id"):
            await self.db("UPDATE persons SET wikidata_id=%s WHERE id=%s", (d["wikidata_id"], pid), "none")
        if d.get("birth_year"):
            await self.db("UPDATE persons SET approx_birth_year=%s WHERE id=%s", (d["birth_year"], pid), "none")
        if d.get("father"):
            await self.link_parent(pid, d["father"], "father", d.get("source_url"))
        if d.get("mother"):
            await self.link_parent(pid, d["mother"], "mother", d.get("source_url"))
        for child in d.get("children", []):
            await self.link_child(pid, child, d.get("source_url"))
        for cat_name in d.get("auto_categories", []):
            cat_id = await self.db("SELECT id FROM categories WHERE name=%s", (cat_name,), "val")
            if cat_id:
                await self.db("INSERT INTO person_categories (person_id,category_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                              (pid, cat_id), "none")
        await self.db("INSERT INTO agent_log (person_id, person_name, action, detail) VALUES (%s,%s,'discovered',%s)",
                      (pid, person["name"],
                       f"father={d.get('father',{}).get('name') if d.get('father') else '-'}, "
                       f"mother={d.get('mother',{}).get('name') if d.get('mother') else '-'}, "
                       f"children={len(d.get('children',[]))}"), "none")

    async def link_parent(self, child_id, parent_data, parent_type, source_url):
        parent_id = await self.find_or_create(parent_data["name"], parent_data.get("wikidata_id"))
        if not parent_id or parent_id == child_id:
            return
        existing = await self.db("SELECT id FROM relationships WHERE child_id=%s AND parent_type=%s AND is_primary=TRUE",
                                 (child_id, parent_type), "one")
        is_primary = existing is None
        try:
            rel_id = await self.db("""
                INSERT INTO relationships (child_id, parent_id, parent_type, confidence, is_primary, is_branch)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (child_id, parent_id, parent_type) DO UPDATE SET confidence=EXCLUDED.confidence
                RETURNING id
            """, (child_id, parent_id, parent_type, parent_data["confidence"], is_primary, not is_primary), "val")
            if source_url and rel_id:
                await self.db("INSERT INTO sources (relationship_id, url, source_type) VALUES (%s,%s,'wikidata') ON CONFLICT DO NOTHING",
                              (rel_id, source_url), "none")
            if parent_data["confidence"] >= 95.0:
                child = await self.db("SELECT * FROM persons WHERE id=%s", (child_id,), "one")
                if child and child["is_genesis"]:
                    await self.db("UPDATE persons SET is_genesis=FALSE, genesis_code=NULL WHERE id=%s", (child_id,), "none")
                    await self.db("INSERT INTO merge_log (genesis_person_id, genesis_code, merged_into_person_id, confidence_at_merge) VALUES (%s,%s,%s,%s)",
                                  (child_id, child["genesis_code"], parent_id, parent_data["confidence"]), "none")
                    logger.info(f"🔗 MERGE: {child['name']} genesis dissolved")
            await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (%s,'both',75) ON CONFLICT DO NOTHING",
                          (parent_id,), "none")
        except Exception as e:
            logger.warning(f"link_parent error: {e}")

    async def link_child(self, parent_id, child_data, source_url):
        child_id = await self.find_or_create(child_data["name"], child_data.get("wikidata_id"))
        if not child_id or child_id == parent_id:
            return
        try:
            rel_id = await self.db("""
                INSERT INTO relationships (child_id, parent_id, parent_type, confidence, is_primary)
                VALUES (%s,%s,'father',%s,TRUE)
                ON CONFLICT (child_id, parent_id, parent_type) DO UPDATE SET confidence=EXCLUDED.confidence
                RETURNING id
            """, (child_id, parent_id, child_data["confidence"]), "val")
            if source_url and rel_id:
                await self.db("INSERT INTO sources (relationship_id, url, source_type) VALUES (%s,%s,'wikidata') ON CONFLICT DO NOTHING",
                              (rel_id, source_url), "none")
            await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (%s,'both',55) ON CONFLICT DO NOTHING",
                          (child_id,), "none")
        except Exception as e:
            logger.warning(f"link_child error: {e}")

    async def find_or_create(self, name: str, wikidata_id: Optional[str] = None) -> Optional[int]:
        if wikidata_id:
            pid = await self.db("SELECT id FROM persons WHERE wikidata_id=%s", (wikidata_id,), "val")
            if pid:
                return pid
        pid = await self.db("SELECT id FROM persons WHERE LOWER(name)=LOWER(%s) LIMIT 1", (name,), "val")
        if pid:
            if wikidata_id:
                await self.db("UPDATE persons SET wikidata_id=%s WHERE id=%s", (wikidata_id, pid), "none")
            return pid
        try:
            count = await self.db("SELECT COUNT(*) FROM persons WHERE is_genesis=TRUE", fetch="val") or 0
            gc = f"G{count + 1}"
            pid = await self.db("""
                INSERT INTO persons (name, wikidata_id, is_genesis, genesis_code, type)
                VALUES (%s,%s,TRUE,%s,'human') RETURNING id
            """, (name, wikidata_id, gc), "val")
            await self.db("INSERT INTO agent_log (person_id, person_name, action, detail) VALUES (%s,%s,'discovered','Discovered by agent')",
                          (pid, name), "none")
            return pid
        except Exception as e:
            logger.warning(f"Could not create {name}: {e}")
            return None

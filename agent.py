import asyncio
import httpx
import re
import logging
from typing import Optional, Dict, List

logger = logging.getLogger("ashvattha_agent")
logging.basicConfig(level=logging.INFO)

WIKIDATA_API    = "https://www.wikidata.org/w/api.php"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIPEDIA_API   = "https://en.wikipedia.org/w/api.php"
DBPEDIA_SPARQL  = "https://dbpedia.org/sparql"

REQUEST_DELAY = 5
RETRY_DELAY   = 90
MAX_ATTEMPTS  = 5

CATEGORY_KEYWORDS = {
    "Greek Gods":        ["greek god", "olympian", "greek deity", "greek mytholog"],
    "Norse Gods":        ["norse god", "asgard", "viking god", "norse mytholog"],
    "Hindu Deities":     ["hindu god", "hindu deity", "vedic god", "hinduism"],
    "Egyptian Gods":     ["egyptian god", "egyptian deity", "ancient egyptian religion"],
    "Roman Gods":        ["roman god", "roman deity", "roman mytholog"],
    "Mesopotamian Gods": ["sumerian god", "babylonian god", "akkadian", "mesopotamian"],
    "Celtic Gods":       ["celtic god", "celtic deity", "irish mytholog"],
    "Aztec Gods":        ["aztec god", "aztec deity", "mesoamerican"],
    "Egyptian Pharaohs": ["pharaoh", "king of egypt", "ancient egypt"],
    "Roman Emperors":    ["roman emperor", "emperor of rome"],
    "Greek Kings":       ["king of macedon", "king of sparta", "king of athens", "king of greece"],
    "Sumerian Kings":    ["king of sumer", "sumerian king", "king of ur"],
    "Persian Kings":     ["king of persia", "achaemenid", "sassanid"],
    "Biblical Figures":  ["biblical", "old testament", "new testament", "patriarch", "prophet of"],
    "Quranic Figures":   ["quran", "islamic prophet", "companion of the prophet"],
    "Vedic Figures":     ["vedic", "rigveda", "upanishad"],
    "British Royals":    ["king of england", "queen of england", "british royal", "house of windsor"],
    "Mughal Dynasty":    ["mughal emperor", "mughal dynasty"],
    "Mongol Khans":      ["khan of mongolia", "mongol khan", "golden horde"],
    "Ottoman Dynasty":   ["ottoman sultan", "sultan of the ottoman"],
    "Americans":         ["american president", "united states president", "founding father"],
    "South Asians":      ["prime minister of india", "indian ruler", "maharaja"],
}

# Explicit type→category mapping for seeded persons
# so they get categorized immediately at seed time
TYPE_CATEGORY_MAP = {
    "Zeus":                 ["Greek Gods", "Mythological"],
    "Odin":                 ["Norse Gods", "Mythological"],
    "Brahma":               ["Hindu Deities", "Mythological"],
    "Osiris":               ["Egyptian Gods", "Mythological"],
    "Adam":                 ["Biblical Figures", "Religion & Scripture"],
    "Abraham":              ["Biblical Figures", "Religion & Scripture"],
    "Muhammad":             ["Quranic Figures", "Religion & Scripture"],
    "Ramesses II":          ["Egyptian Pharaohs", "Ancient"],
    "Alexander the Great":  ["Greek Kings", "Ancient"],
    "Julius Caesar":        ["Roman Emperors", "Ancient"],
    "Genghis Khan":         ["Mongol Khans", "Royalty & Dynasties"],
    "Charlemagne":          ["Medieval", "Royalty & Dynasties"],
}

SEEDS = [
    ("Zeus","mythological"), ("Adam","human"), ("Abraham","human"),
    ("Ramesses II","human"), ("Alexander the Great","human"),
    ("Julius Caesar","human"), ("Genghis Khan","human"),
    ("Charlemagne","human"), ("Odin","mythological"),
    ("Brahma","mythological"), ("Osiris","mythological"),
    ("Muhammad","human"),
]

EXPANSION_SEEDS = [
    ("Noah","human"), ("Isaac","human"), ("Jacob","human"),
    ("Moses","human"), ("King David","human"), ("Solomon","human"),
    ("Cronus","mythological"), ("Uranus","mythological"),
    ("Heracles","mythological"), ("Apollo","mythological"),
    ("Achilles","human"), ("Odysseus","human"),
    ("Thor","mythological"), ("Loki","mythological"),
    ("Ra","mythological"), ("Horus","mythological"),
    ("Cyrus the Great","human"), ("Darius I","human"),
    ("Tutankhamun","human"), ("Cleopatra","human"),
    ("Ashoka","human"), ("Chandragupta Maurya","human"),
    ("Attila the Hun","human"), ("Saladin","human"),
    ("Suleiman the Magnificent","human"), ("Akbar","human"),
    ("William the Conqueror","human"), ("Henry VIII","human"),
    ("Louis XIV","human"), ("Napoleon Bonaparte","human"),
    ("Peter the Great","human"), ("Qin Shi Huang","human"),
    ("Kublai Khan","human"), ("Krishna","mythological"),
    ("Rama","mythological"), ("Vishnu","mythological"),
]


class GenesisAgent:
    def __init__(self, db):
        self.db = db
        self.client = None
        self.consecutive_failures = 0
        self.expansion_index = 0

    async def run_forever(self):
        self.client = httpx.AsyncClient(
            timeout=45.0,
            headers={"User-Agent": "Ashvattha/1.0 (universal genealogy; https://ashvattha.onrender.com)"},
            follow_redirects=True
        )
        logger.info("🌱 Ashvattha Agent awakened")
        await self.seed_initial_persons()
        while True:
            try:
                if self.consecutive_failures >= 5:
                    logger.warning(f"⏸️  Backing off {RETRY_DELAY}s")
                    await asyncio.sleep(RETRY_DELAY)
                    self.consecutive_failures = 0
                processed = await self.process_next()
                if processed:
                    self.consecutive_failures = 0
                    await asyncio.sleep(REQUEST_DELAY)
                else:
                    await self.refill_queue()
                    await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"Agent loop error: {e}")
                self.consecutive_failures += 1
                await asyncio.sleep(15)

    # ── QUEUE ────────────────────────────────────────────────────────────────

    async def refill_queue(self):
        failed_count = (await self.db(
            "SELECT COUNT(*) as c FROM agent_queue WHERE status='failed'", fetch="one") or {}).get("c", 0)
        if failed_count > 0:
            await self.db("UPDATE agent_queue SET status='pending', attempts=0 WHERE status='failed'", fetch="none")
            logger.info(f"♻️  Reset {failed_count} failed jobs")
            return
        if self.expansion_index < len(EXPANSION_SEEDS):
            name, ptype = EXPANSION_SEEDS[self.expansion_index]
            self.expansion_index += 1
            existing = await self.db("SELECT id FROM persons WHERE LOWER(name)=LOWER(:n)", {"n": name}, "one")
            if not existing:
                # BUG FIX: new persons are NOT genesis — they just don't have a known father yet
                # is_genesis=FALSE, genesis_code=NULL — will only become genesis if father is unknown after research
                row = await self.db(
                    "INSERT INTO persons (name,type,is_genesis,genesis_code) VALUES (:n,:t,FALSE,NULL) RETURNING id",
                    {"n": name, "t": ptype}, "one")
                if row:
                    await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',85)",
                                  {"p": row["id"]}, "none")
                    logger.info(f"🌱 Expansion: {name}")
            else:
                # Already exists — just re-queue for more research
                existing_q = await self.db(
                    "SELECT id FROM agent_queue WHERE person_id=:p AND status='pending'",
                    {"p": existing["id"]}, "one")
                if not existing_q:
                    await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',85)",
                                  {"p": existing["id"]}, "none")
        else:
            unresearched = await self.db(
                "SELECT id FROM persons WHERE agent_researched=FALSE AND type!='genesis' LIMIT 20", fetch="all")
            if unresearched:
                for p in unresearched:
                    existing_q = await self.db(
                        "SELECT id FROM agent_queue WHERE person_id=:p AND status='pending'",
                        {"p": p["id"]}, "one")
                    if not existing_q:
                        await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',30)",
                                      {"p": p["id"]}, "none")
            else:
                self.expansion_index = 0
                logger.info("🔄 Full cycle — restarting expansion")

    async def seed_initial_persons(self):
        for name, ptype in SEEDS:
            existing = await self.db("SELECT id FROM persons WHERE LOWER(name)=LOWER(:n)", {"n": name}, "one")
            if not existing:
                # Seeds get genesis_code only if they truly have no known parent.
                # Seeds like Zeus DO have a known parent (Cronus) — agent will discover and remove genesis flag.
                # For now mark is_genesis=TRUE so they appear as open roots until agent connects them.
                row = await self.db(
                    "INSERT INTO persons (name,type,is_genesis,genesis_code) VALUES (:n,:t,TRUE,:g) RETURNING id",
                    {"n": name, "t": ptype, "g": f"G-{name[:4].upper()}"}, "one")
                if row:
                    pid = row["id"]
                    await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',90)",
                                  {"p": pid}, "none")
                    # Assign known categories immediately at seed time
                    await self.assign_seed_categories(pid, name, ptype)
                    logger.info(f"🌱 Seeded: {name}")
            else:
                # Already exists — ensure categories are assigned
                await self.assign_seed_categories(existing["id"], name, ptype)

    async def assign_seed_categories(self, person_id: int, name: str, ptype: str):
        """Assign categories to seeded persons immediately, don't wait for research."""
        cats_to_assign = list(TYPE_CATEGORY_MAP.get(name, []))

        # Also assign based on type
        if ptype == "mythological" and not cats_to_assign:
            cats_to_assign.append("Mythological")
        elif ptype == "human" and not cats_to_assign:
            cats_to_assign.append("Human")

        for cat_name in cats_to_assign:
            await self.assign_category(person_id, cat_name)

    # ── PROCESS ──────────────────────────────────────────────────────────────

    async def process_next(self) -> bool:
        job = await self.db(
            "SELECT id,person_id,attempts FROM agent_queue WHERE status='pending' ORDER BY priority DESC, created_at ASC LIMIT 1",
            fetch="one")
        if not job:
            return False

        await self.db("UPDATE agent_queue SET status='processing' WHERE id=:id", {"id": job["id"]}, "none")
        person = await self.db("SELECT * FROM persons WHERE id=:id", {"id": job["person_id"]}, "one")
        if not person:
            await self.db("UPDATE agent_queue SET status='done' WHERE id=:id", {"id": job["id"]}, "none")
            return True

        logger.info(f"🔍 [{person['type']}] {person['name']}")
        discoveries = await self.research_person(person["name"], person.get("wikidata_id"))

        if discoveries and (discoveries.get("father") or discoveries.get("mother") or discoveries.get("children")):
            await self.save_discoveries(person, discoveries)
            await self.db("UPDATE persons SET agent_researched=TRUE WHERE id=:id", {"id": person["id"]}, "none")
            await self.db("UPDATE agent_queue SET status='done' WHERE id=:id", {"id": job["id"]}, "none")
        else:
            attempts = (job.get("attempts") or 0) + 1
            status = "failed" if attempts >= MAX_ATTEMPTS else "pending"
            await self.db("UPDATE agent_queue SET status=:s, attempts=:a WHERE id=:id",
                          {"s": status, "a": attempts, "id": job["id"]}, "none")
            if status == "failed":
                self.consecutive_failures += 1
                # BUG FIX: if agent couldn't find a father after MAX_ATTEMPTS,
                # THEN mark as genesis (unknown father) — not before
                has_father = await self.db(
                    "SELECT id FROM relationships WHERE child_id=:id AND parent_type='father'",
                    {"id": job["person_id"]}, "one")
                if not has_father:
                    current = await self.db("SELECT is_genesis FROM persons WHERE id=:id",
                                           {"id": job["person_id"]}, "one")
                    if current and not current["is_genesis"]:
                        count = (await self.db("SELECT COUNT(*) as c FROM persons WHERE is_genesis=TRUE",
                                               fetch="one") or {}).get("c", 0)
                        gc = f"G{count + 1}"
                        await self.db("UPDATE persons SET is_genesis=TRUE, genesis_code=:gc WHERE id=:id",
                                      {"gc": gc, "id": job["person_id"]}, "none")
                        logger.info(f"⬡ {person['name']} → genesis {gc} (no father found)")
            else:
                await asyncio.sleep(min(attempts * 8, 60))
        return True

    # ── RESEARCH ─────────────────────────────────────────────────────────────

    async def research_person(self, name: str, wikidata_id=None) -> Optional[Dict]:
        merged = {"father": None, "mother": None, "children": [],
                  "birth_year": None, "source_url": None, "auto_categories": []}

        # Source 1: Wikidata
        try:
            if not wikidata_id:
                wikidata_id = await self.find_wikidata_id(name)
            if wikidata_id:
                wd = await self.fetch_wikidata(wikidata_id)
                if wd:
                    self._merge(merged, wd)
        except Exception as e:
            logger.warning(f"Wikidata error for {name}: {e}")

        # Source 2: DBpedia (cross-validates, finds gaps)
        try:
            dbp = await self.fetch_dbpedia(name)
            if dbp:
                self._merge(merged, dbp, confidence_offset=-5)
        except Exception as e:
            logger.warning(f"DBpedia error for {name}: {e}")

        # Source 3: Wikipedia infobox fallback
        if not merged["father"] and not merged["mother"] and not merged["children"]:
            try:
                wiki = await self.fetch_wikipedia(name)
                if wiki:
                    self._merge(merged, wiki, confidence_offset=-10)
            except Exception as e:
                logger.warning(f"Wikipedia error for {name}: {e}")

        return merged if (merged["father"] or merged["mother"] or merged["children"]) else None

    def _merge(self, target: Dict, source: Dict, confidence_offset: float = 0):
        """Merge source into target — best confidence wins."""
        if source.get("father"):
            f = dict(source["father"])
            f["confidence"] = min(99.0, f.get("confidence", 70) + confidence_offset)
            if not target["father"] or f["confidence"] > target["father"]["confidence"]:
                target["father"] = f
        if source.get("mother"):
            m = dict(source["mother"])
            m["confidence"] = min(99.0, m.get("confidence", 70) + confidence_offset)
            if not target["mother"] or m["confidence"] > target["mother"]["confidence"]:
                target["mother"] = m
        existing_names = {c["name"].lower() for c in target["children"]}
        for child in source.get("children", []):
            c = dict(child)
            c["confidence"] = min(99.0, c.get("confidence", 65) + confidence_offset)
            if c["name"].lower() not in existing_names:
                target["children"].append(c)
                existing_names.add(c["name"].lower())
        if source.get("birth_year") and not target["birth_year"]:
            target["birth_year"] = source["birth_year"]
        if source.get("source_url") and not target["source_url"]:
            target["source_url"] = source["source_url"]
        if source.get("wikidata_id") and not target.get("wikidata_id"):
            target["wikidata_id"] = source["wikidata_id"]
        for cat in source.get("auto_categories", []):
            if cat not in target["auto_categories"]:
                target["auto_categories"].append(cat)

    # ── SOURCE 1: WIKIDATA ───────────────────────────────────────────────────

    async def find_wikidata_id(self, name: str) -> Optional[str]:
        try:
            r = await self.client.get(WIKIDATA_API, params={
                "action": "wbsearchentities", "search": name,
                "language": "en", "type": "item", "format": "json", "limit": 5
            })
            if r.status_code == 429:
                await asyncio.sleep(90)
                return None
            for result in r.json().get("search", []):
                desc = result.get("description", "").lower()
                if any(k in desc for k in ["human", "person", "deity", "god", "king",
                                            "emperor", "pharaoh", "ruler", "queen",
                                            "prince", "prophet", "mythological", "figure"]):
                    return result["id"]
            results = r.json().get("search", [])
            return results[0]["id"] if results else None
        except Exception as e:
            logger.warning(f"Wikidata ID lookup failed: {e}")
            return None

    async def fetch_wikidata(self, qid: str) -> Optional[Dict]:
        query = f"""
        SELECT ?father ?fatherLabel ?mother ?motherLabel ?child ?childLabel
               ?birthYear ?article ?occupation ?occupationLabel WHERE {{
          OPTIONAL {{ wd:{qid} wdt:P22 ?father. }}
          OPTIONAL {{ wd:{qid} wdt:P25 ?mother. }}
          OPTIONAL {{ ?child wdt:P22 wd:{qid}. }}
          OPTIONAL {{ ?child wdt:P25 wd:{qid}. }}
          OPTIONAL {{ wd:{qid} wdt:P569 ?dob. BIND(YEAR(?dob) AS ?birthYear) }}
          OPTIONAL {{ ?article schema:about wd:{qid}; schema:isPartOf <https://en.wikipedia.org/> . }}
          OPTIONAL {{ wd:{qid} wdt:P106 ?occupation. }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }} LIMIT 60"""
        try:
            r = await self.client.get(WIKIDATA_SPARQL,
                params={"query": query, "format": "json"},
                headers={"Accept": "application/json"})
            if r.status_code == 429:
                await asyncio.sleep(90)
                return None
            if r.status_code != 200:
                return None
            bindings = r.json().get("results", {}).get("bindings", [])
            if not bindings:
                return None
            d = {"wikidata_id": qid, "father": None, "mother": None,
                 "children": [], "birth_year": None,
                 "source_url": f"https://www.wikidata.org/wiki/{qid}",
                 "auto_categories": []}
            seen_children = set()
            occupation_text = ""
            for row in bindings:
                if row.get("fatherLabel") and not d["father"]:
                    fn = row["fatherLabel"]["value"]
                    if not fn.startswith("Q") and len(fn) > 1:
                        d["father"] = {"name": fn,
                            "wikidata_id": row["father"]["value"].split("/")[-1] if row.get("father") else None,
                            "confidence": 92.0}
                if row.get("motherLabel") and not d["mother"]:
                    mn = row["motherLabel"]["value"]
                    if not mn.startswith("Q") and len(mn) > 1:
                        d["mother"] = {"name": mn,
                            "wikidata_id": row["mother"]["value"].split("/")[-1] if row.get("mother") else None,
                            "confidence": 90.0}
                if row.get("childLabel"):
                    cn = row["childLabel"]["value"]
                    if cn not in seen_children and not cn.startswith("Q") and len(cn) > 1:
                        seen_children.add(cn)
                        d["children"].append({"name": cn,
                            "wikidata_id": row["child"]["value"].split("/")[-1] if row.get("child") else None,
                            "confidence": 88.0})
                if row.get("birthYear") and not d["birth_year"]:
                    try: d["birth_year"] = int(row["birthYear"]["value"])
                    except: pass
                if row.get("occupationLabel"):
                    occupation_text += " " + row["occupationLabel"]["value"].lower()
            # Detect categories from occupations too
            d["auto_categories"] = self._detect_cats(occupation_text)
            return d
        except Exception as e:
            logger.error(f"Wikidata SPARQL failed {qid}: {e}")
            return None

    # ── SOURCE 2: DBPEDIA ────────────────────────────────────────────────────

    async def fetch_dbpedia(self, name: str) -> Optional[Dict]:
        resource = name.replace(" ", "_")
        query = f"""
        PREFIX dbo: <http://dbpedia.org/ontology/>
        PREFIX dbr: <http://dbpedia.org/resource/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?fatherName ?motherName ?childName ?birthYear WHERE {{
          OPTIONAL {{
            dbr:{resource} dbo:father ?father .
            ?father rdfs:label ?fatherName .
            FILTER(LANG(?fatherName) = 'en')
          }}
          OPTIONAL {{
            dbr:{resource} dbo:mother ?mother .
            ?mother rdfs:label ?motherName .
            FILTER(LANG(?motherName) = 'en')
          }}
          OPTIONAL {{
            ?child dbo:father dbr:{resource} .
            ?child rdfs:label ?childName .
            FILTER(LANG(?childName) = 'en')
          }}
          OPTIONAL {{ dbr:{resource} dbo:birthYear ?birthYear . }}
        }} LIMIT 40"""
        try:
            r = await self.client.get(DBPEDIA_SPARQL,
                params={"query": query, "format": "application/sparql-results+json"},
                headers={"Accept": "application/sparql-results+json"})
            if r.status_code == 429:
                await asyncio.sleep(60)
                return None
            if r.status_code != 200:
                return None
            bindings = r.json().get("results", {}).get("bindings", [])
            if not bindings:
                return None
            d = {"father": None, "mother": None, "children": [],
                 "birth_year": None,
                 "source_url": f"https://dbpedia.org/resource/{resource}",
                 "auto_categories": []}
            seen = set()
            for row in bindings:
                if row.get("fatherName") and not d["father"]:
                    fn = row["fatherName"]["value"]
                    if len(fn) > 1:
                        d["father"] = {"name": fn, "confidence": 85.0}
                if row.get("motherName") and not d["mother"]:
                    mn = row["motherName"]["value"]
                    if len(mn) > 1:
                        d["mother"] = {"name": mn, "confidence": 83.0}
                if row.get("childName"):
                    cn = row["childName"]["value"]
                    if cn not in seen and len(cn) > 1:
                        seen.add(cn)
                        d["children"].append({"name": cn, "confidence": 80.0})
                if row.get("birthYear") and not d["birth_year"]:
                    try: d["birth_year"] = int(row["birthYear"]["value"][:4])
                    except: pass
            return d if (d["father"] or d["mother"] or d["children"]) else None
        except Exception as e:
            logger.error(f"DBpedia failed for {name}: {e}")
            return None

    # ── SOURCE 3: WIKIPEDIA ──────────────────────────────────────────────────

    async def fetch_wikipedia(self, name: str) -> Optional[Dict]:
        try:
            r = await self.client.get(WIKIPEDIA_API, params={
                "action": "query", "titles": name,
                "prop": "revisions|categories",
                "rvprop": "content", "rvslots": "main",
                "format": "json", "formatversion": "2", "cllimit": "30"
            })
            if r.status_code == 429:
                await asyncio.sleep(60)
                return None
            pages = r.json().get("query", {}).get("pages", [])
            if not pages or pages[0].get("missing"):
                return None
            page = pages[0]
            content = page.get("revisions", [{}])[0].get("slots", {}).get("main", {}).get("content", "")
            cats = [c["title"] for c in page.get("categories", [])]
            father = self._extract_infobox(content, ["father", "Father"])
            mother = self._extract_infobox(content, ["mother", "Mother"])
            children = self._extract_children_infobox(content)
            if not father and not mother and not children:
                return None
            slug = page.get("title", "").replace(" ", "_")
            return {
                "father": father, "mother": mother, "children": children,
                "source_url": f"https://en.wikipedia.org/wiki/{slug}",
                "wikidata_id": None, "birth_year": None,
                "auto_categories": self._detect_cats(content + " ".join(cats))
            }
        except Exception as e:
            logger.error(f"Wikipedia failed {name}: {e}")
            return None

    # ── HELPERS ──────────────────────────────────────────────────────────────

    def _extract_infobox(self, content, fields):
        for f in fields:
            m = re.search(rf"\|\s*{f}\s*=\s*([^\|\n\}}]+)", content, re.IGNORECASE)
            if m:
                raw = re.sub(r'\[\[([^\|\]]+)(?:\|[^\]]+)?\]\]', r'\1', m.group(1))
                raw = re.sub(r'\{\{[^\}]+\}\}|<[^>]+>', '', raw).strip()
                if raw and 1 < len(raw) < 200:
                    return {"name": raw, "confidence": 72.0}
        return None

    def _extract_children_infobox(self, content):
        children = []
        m = re.search(r"\|\s*(?:children|issue)\s*=\s*((?:[^\|]|\n(?!\|))+)", content, re.IGNORECASE)
        if m:
            for n in re.findall(r'\[\[([^\|\]]+)(?:\|[^\]]+)?\]\]', m.group(1))[:10]:
                n = n.strip()
                if n and len(n) > 1:
                    children.append({"name": n, "confidence": 68.0})
        return children

    def _detect_cats(self, text):
        tl = text.lower()
        return [cat for cat, kws in CATEGORY_KEYWORDS.items() if any(k in tl for k in kws)]

    async def assign_category(self, person_id: int, cat_name: str):
        """BUG FIX: use COUNT(*) as c instead of SELECT 1 — pg8000 needs named columns."""
        cat = await self.db("SELECT id FROM categories WHERE name=:n", {"n": cat_name}, "one")
        if not cat:
            return
        # FIX: was "SELECT 1 FROM..." which pg8000 can't handle — use COUNT with alias
        existing = (await self.db(
            "SELECT COUNT(*) as c FROM person_categories WHERE person_id=:p AND category_id=:c",
            {"p": person_id, "c": cat["id"]}, "one") or {}).get("c", 0)
        if existing == 0:
            try:
                await self.db("INSERT INTO person_categories (person_id,category_id) VALUES (:p,:c)",
                              {"p": person_id, "c": cat["id"]}, "none")
                logger.info(f"🏷️  Assigned category '{cat_name}' to person {person_id}")
            except Exception as e:
                logger.warning(f"Category assign error: {e}")

    # ── SAVE ─────────────────────────────────────────────────────────────────

    async def save_discoveries(self, person, d):
        pid = person["id"]
        if d.get("wikidata_id"):
            await self.db("UPDATE persons SET wikidata_id=:w WHERE id=:id",
                          {"w": d["wikidata_id"], "id": pid}, "none")
        if d.get("birth_year"):
            await self.db("UPDATE persons SET approx_birth_year=:y WHERE id=:id",
                          {"y": d["birth_year"], "id": pid}, "none")
        if d.get("father"):
            await self.link_parent(pid, d["father"], "father", d.get("source_url"))
        if d.get("mother"):
            await self.link_parent(pid, d["mother"], "mother", d.get("source_url"))
        for child in d.get("children", []):
            await self.link_child(pid, child, d.get("source_url"))
        # Assign detected categories using the fixed assign_category method
        for cat_name in d.get("auto_categories", []):
            await self.assign_category(pid, cat_name)
        await self.db(
            "INSERT INTO agent_log (person_id,person_name,action,detail) VALUES (:p,:n,'discovered',:d)",
            {"p": pid, "n": person["name"],
             "d": f"f={d.get('father',{}).get('name','-') if d.get('father') else '-'} "
                  f"m={d.get('mother',{}).get('name','-') if d.get('mother') else '-'} "
                  f"c={len(d.get('children',[]))}"}, "none")

    async def link_parent(self, child_id, parent_data, parent_type, source_url):
        parent_id = await self.find_or_create(parent_data["name"], parent_data.get("wikidata_id"))
        if not parent_id or parent_id == child_id:
            return
        existing_primary = await self.db(
            "SELECT id FROM relationships WHERE child_id=:c AND parent_type=:t AND is_primary=TRUE",
            {"c": child_id, "t": parent_type}, "one")
        is_primary = existing_primary is None
        try:
            existing_rel = await self.db(
                "SELECT id FROM relationships WHERE child_id=:c AND parent_id=:p AND parent_type=:t",
                {"c": child_id, "p": parent_id, "t": parent_type}, "one")
            if existing_rel:
                await self.db("UPDATE relationships SET confidence=:conf WHERE id=:id",
                              {"conf": parent_data["confidence"], "id": existing_rel["id"]}, "none")
                rel_id = existing_rel["id"]
            else:
                row = await self.db("""
                    INSERT INTO relationships (child_id,parent_id,parent_type,confidence,is_primary,is_branch)
                    VALUES (:c,:p,:t,:conf,:prim,:branch) RETURNING id""",
                    {"c": child_id, "p": parent_id, "t": parent_type,
                     "conf": parent_data["confidence"],
                     "prim": is_primary, "branch": not is_primary}, "one")
                rel_id = row["id"] if row else None
            if source_url and rel_id:
                existing_src = await self.db(
                    "SELECT COUNT(*) as c FROM sources WHERE relationship_id=:r AND url=:u",
                    {"r": rel_id, "u": source_url}, "one")
                if (existing_src or {}).get("c", 0) == 0:
                    src_type = ("wikidata" if "wikidata" in source_url else
                                "dbpedia" if "dbpedia" in source_url else "wikipedia")
                    await self.db("INSERT INTO sources (relationship_id,url,source_type) VALUES (:r,:u,:t)",
                                  {"r": rel_id, "u": source_url, "t": src_type}, "none")
            # BUG FIX: only merge genesis if the person was a genesis node
            # AND now has a confirmed parent — this correctly removes genesis status
            if parent_data["confidence"] >= 95.0:
                child = await self.db("SELECT * FROM persons WHERE id=:id", {"id": child_id}, "one")
                if child and child["is_genesis"]:
                    await self.db("UPDATE persons SET is_genesis=FALSE, genesis_code=NULL WHERE id=:id",
                                  {"id": child_id}, "none")
                    await self.db("""INSERT INTO merge_log
                        (genesis_person_id,genesis_code,merged_into_person_id,confidence_at_merge)
                        VALUES (:gp,:gc,:mi,:conf)""",
                        {"gp": child_id, "gc": child["genesis_code"],
                         "mi": parent_id, "conf": parent_data["confidence"]}, "none")
                    logger.info(f"🔗 MERGE: {child['name']} — father found, genesis dissolved")
            # Queue parent for research too
            existing_q = await self.db(
                "SELECT COUNT(*) as c FROM agent_queue WHERE person_id=:p AND status='pending'",
                {"p": parent_id}, "one")
            if (existing_q or {}).get("c", 0) == 0:
                await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',75)",
                              {"p": parent_id}, "none")
        except Exception as e:
            logger.warning(f"link_parent error: {e}")

    async def link_child(self, parent_id, child_data, source_url):
        child_id = await self.find_or_create(child_data["name"], child_data.get("wikidata_id"))
        if not child_id or child_id == parent_id:
            return
        try:
            existing_rel = await self.db(
                "SELECT id FROM relationships WHERE child_id=:c AND parent_id=:p AND parent_type='father'",
                {"c": child_id, "p": parent_id}, "one")
            if existing_rel:
                await self.db("UPDATE relationships SET confidence=:conf WHERE id=:id",
                              {"conf": child_data["confidence"], "id": existing_rel["id"]}, "none")
                rel_id = existing_rel["id"]
            else:
                row = await self.db("""
                    INSERT INTO relationships (child_id,parent_id,parent_type,confidence,is_primary)
                    VALUES (:c,:p,'father',:conf,TRUE) RETURNING id""",
                    {"c": child_id, "p": parent_id, "conf": child_data["confidence"]}, "one")
                rel_id = row["id"] if row else None
            if source_url and rel_id:
                existing_src = await self.db(
                    "SELECT COUNT(*) as c FROM sources WHERE relationship_id=:r AND url=:u",
                    {"r": rel_id, "u": source_url}, "one")
                if (existing_src or {}).get("c", 0) == 0:
                    await self.db("INSERT INTO sources (relationship_id,url,source_type) VALUES (:r,:u,'wikipedia')",
                                  {"r": rel_id, "u": source_url}, "none")
            existing_q = await self.db(
                "SELECT COUNT(*) as c FROM agent_queue WHERE person_id=:p AND status='pending'",
                {"p": child_id}, "one")
            if (existing_q or {}).get("c", 0) == 0:
                await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',55)",
                              {"p": child_id}, "none")
        except Exception as e:
            logger.warning(f"link_child error: {e}")

    async def find_or_create(self, name: str, wikidata_id=None) -> Optional[int]:
        if wikidata_id:
            row = await self.db("SELECT id FROM persons WHERE wikidata_id=:w", {"w": wikidata_id}, "one")
            if row:
                return row["id"]
        row = await self.db("SELECT id FROM persons WHERE LOWER(name)=LOWER(:n) LIMIT 1", {"n": name}, "one")
        if row:
            if wikidata_id:
                await self.db("UPDATE persons SET wikidata_id=:w WHERE id=:id",
                              {"w": wikidata_id, "id": row["id"]}, "none")
            return row["id"]
        try:
            # BUG FIX: newly discovered persons start with is_genesis=FALSE
            # They only become genesis if agent exhausts all sources and finds no father
            new_row = await self.db(
                "INSERT INTO persons (name,wikidata_id,is_genesis,genesis_code,type) VALUES (:n,:w,FALSE,NULL,'human') RETURNING id",
                {"n": name, "w": wikidata_id}, "one")
            if new_row:
                await self.db(
                    "INSERT INTO agent_log (person_id,person_name,action,detail) VALUES (:p,:n,'discovered','Discovered by agent')",
                    {"p": new_row["id"], "n": name}, "none")
            return new_row["id"] if new_row else None
        except Exception as e:
            logger.warning(f"Could not create {name}: {e}")
            return None

import asyncio
import httpx
import re
import logging
from typing import Optional, Dict, List
from bs4 import BeautifulSoup

logger = logging.getLogger("ashvattha_agent")
logging.basicConfig(level=logging.INFO)

# ── DATA SOURCES ──────────────────────────────────────────────────────────────
WIKIDATA_API    = "https://www.wikidata.org/w/api.php"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIPEDIA_API   = "https://en.wikipedia.org/w/api.php"
DBPEDIA_SPARQL  = "https://dbpedia.org/sparql"
WIKISOURCE_API  = "https://en.wikisource.org/w/api.php"
THEOI_BASE      = "https://www.theoi.com"
SACRED_TEXTS    = "https://www.sacred-texts.com"

REQUEST_DELAY  = 5    # seconds between persons
RETRY_DELAY    = 90   # seconds when rate limited
MAX_ATTEMPTS   = 5

CATEGORY_KEYWORDS = {
    "Greek Gods":        ["greek god", "olympian", "greek deity", "theoi"],
    "Norse Gods":        ["norse god", "asgard", "viking god", "eddic"],
    "Hindu Deities":     ["hindu god", "hindu deity", "vedic god", "devanagari"],
    "Egyptian Gods":     ["egyptian god", "egyptian deity", "ancient egyptian god"],
    "Roman Gods":        ["roman god", "roman deity"],
    "Mesopotamian Gods": ["sumerian god", "babylonian god", "akkadian"],
    "Celtic Gods":       ["celtic god", "celtic deity", "druid"],
    "Aztec Gods":        ["aztec god", "aztec deity", "mesoamerican god"],
    "Egyptian Pharaohs": ["pharaoh", "king of egypt", "ancient egypt"],
    "Roman Emperors":    ["roman emperor", "emperor of rome", "caesar"],
    "Greek Kings":       ["king of macedon", "king of sparta", "king of athens"],
    "Sumerian Kings":    ["king of sumer", "sumerian king", "king of ur"],
    "Persian Kings":     ["king of persia", "achaemenid", "sassanid"],
    "Biblical Figures":  ["biblical", "old testament", "new testament", "patriarch", "prophet"],
    "Quranic Figures":   ["quran", "islamic prophet", "companion of the prophet"],
    "Vedic Figures":     ["vedic", "rigveda", "upanishad"],
    "British Royals":    ["king of england", "queen of england", "british royal", "house of windsor"],
    "Mughal Dynasty":    ["mughal emperor", "mughal dynasty"],
    "Mongol Khans":      ["khan of mongolia", "mongol khan", "golden horde"],
    "Ottoman Dynasty":   ["ottoman sultan", "sultan of the ottoman"],
    "Americans":         ["american president", "united states president", "founding father"],
    "South Asians":      ["prime minister of india", "indian ruler", "maharaja"],
    "Notable Families":  ["dynasty", "noble family", "aristocratic family"],
}

# ── SEED PERSONS ──────────────────────────────────────────────────────────────
SEEDS = [
    ("Zeus","mythological"), ("Adam","human"), ("Abraham","human"),
    ("Ramesses II","human"), ("Alexander the Great","human"),
    ("Julius Caesar","human"), ("Genghis Khan","human"),
    ("Charlemagne","human"), ("Odin","mythological"),
    ("Brahma","mythological"), ("Osiris","mythological"),
    ("Muhammad","human"),
]

EXPANSION_SEEDS = [
    # Biblical lineage — extremely well connected
    ("Noah","human"), ("Isaac","human"), ("Jacob","human"),
    ("Joseph son of Jacob","human"), ("Moses","human"),
    ("King David","human"), ("Solomon","human"),
    # Greek mythology — massive family trees
    ("Cronus","mythological"), ("Uranus","mythological"),
    ("Heracles","mythological"), ("Apollo","mythological"),
    ("Achilles","human"), ("Odysseus","human"), ("Aeneas","human"),
    # Hindu — deep lineages
    ("Brahma","mythological"), ("Vishnu","mythological"),
    ("Krishna","mythological"), ("Rama","mythological"),
    ("Manu","mythological"),
    # Norse
    ("Thor","mythological"), ("Loki","mythological"),
    ("Freyr","mythological"), ("Baldr","mythological"),
    # Egyptian
    ("Ra","mythological"), ("Horus","mythological"),
    ("Set","mythological"), ("Thoth","mythological"),
    # Ancient rulers — well documented
    ("Cyrus the Great","human"), ("Darius I","human"),
    ("Tutankhamun","human"), ("Cleopatra","human"),
    ("Ashoka","human"), ("Chandragupta Maurya","human"),
    ("Attila the Hun","human"), ("Saladin","human"),
    ("Suleiman the Magnificent","human"), ("Akbar","human"),
    ("Timur","human"), ("Babur","human"),
    # Medieval Europe
    ("William the Conqueror","human"), ("Henry II of England","human"),
    ("Henry VIII","human"), ("Queen Elizabeth I","human"),
    ("Louis XIV","human"), ("Napoleon Bonaparte","human"),
    ("Frederick the Great","human"), ("Peter the Great","human"),
    ("Ivan the Terrible","human"), ("Vlad the Impaler","human"),
    # East Asia
    ("Qin Shi Huang","human"), ("Kublai Khan","human"),
    ("Emperor Meiji","human"),
    # Americas
    ("Moctezuma II","human"), ("Pachacuti","human"),
    # Romulus — connects to Roman mythology
    ("Romulus","human"), ("Remus","human"),
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
            headers={"User-Agent": "Ashvattha/1.0 (universal genealogy project; https://ashvattha.onrender.com)"},
            follow_redirects=True
        )
        logger.info("🌱 Ashvattha Agent awakened — growing forever")
        await self.seed_initial_persons()

        while True:
            try:
                if self.consecutive_failures >= 5:
                    logger.warning(f"⏸️  Backing off {RETRY_DELAY}s after {self.consecutive_failures} failures")
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

    # ── QUEUE MANAGEMENT ────────────────────────────────────────────────────

    async def refill_queue(self):
        """Keep the queue alive — reset failed, add expansion seeds, cycle."""
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
                count = (await self.db("SELECT COUNT(*) as c FROM persons WHERE is_genesis=TRUE", fetch="one") or {}).get("c", 0)
                gc = f"G{count + 1}"
                row = await self.db(
                    "INSERT INTO persons (name,type,is_genesis,genesis_code) VALUES (:n,:t,TRUE,:g) RETURNING id",
                    {"n": name, "t": ptype, "g": gc}, "one")
                if row:
                    await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',85)",
                                  {"p": row["id"]}, "none")
                    logger.info(f"🌱 Expansion: {name}")
            else:
                await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',85)",
                              {"p": existing["id"]}, "none")
        else:
            unresearched = await self.db(
                "SELECT id FROM persons WHERE agent_researched=FALSE AND type!='genesis' LIMIT 20", fetch="all")
            if unresearched:
                for p in unresearched:
                    await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',30)",
                                  {"p": p["id"]}, "none")
            else:
                self.expansion_index = 0
                logger.info("🔄 Full cycle complete — restarting")

    async def seed_initial_persons(self):
        for name, ptype in SEEDS:
            existing = await self.db("SELECT id FROM persons WHERE LOWER(name)=LOWER(:n)", {"n": name}, "one")
            if not existing:
                count = (await self.db("SELECT COUNT(*) as c FROM persons WHERE is_genesis=TRUE", fetch="one") or {}).get("c", 0)
                gc = f"G{count + 1}"
                row = await self.db(
                    "INSERT INTO persons (name,type,is_genesis,genesis_code) VALUES (:n,:t,TRUE,:g) RETURNING id",
                    {"n": name, "t": ptype, "g": gc}, "one")
                if row:
                    await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',90)",
                                  {"p": row["id"]}, "none")
                    logger.info(f"🌱 Seeded: {name}")

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
        discoveries = await self.research_person(person)

        if discoveries and (discoveries.get("father") or discoveries.get("mother") or discoveries.get("children")):
            await self.save_discoveries(person, discoveries)
            await self.db("UPDATE persons SET agent_researched=TRUE WHERE id=:id", {"id": person["id"]}, "none")
            await self.db("UPDATE agent_queue SET status='done' WHERE id=:id", {"id": job["id"]}, "none")
            logger.info(f"✅ {person['name']} — f:{bool(discoveries.get('father'))} m:{bool(discoveries.get('mother'))} c:{len(discoveries.get('children',[]))}")
        else:
            attempts = (job.get("attempts") or 0) + 1
            status = "failed" if attempts >= MAX_ATTEMPTS else "pending"
            await self.db("UPDATE agent_queue SET status=:s, attempts=:a WHERE id=:id",
                          {"s": status, "a": attempts, "id": job["id"]}, "none")
            if status == "failed":
                self.consecutive_failures += 1
            else:
                await asyncio.sleep(min(attempts * 8, 60))
        return True

    # ── MULTI-SOURCE RESEARCH ORCHESTRATOR ──────────────────────────────────

    async def research_person(self, person: Dict) -> Optional[Dict]:
        """
        Try multiple sources in priority order, merge results.
        Sources: Wikidata → DBpedia → Wikipedia → Theoi (if mythological) → Sacred-Texts
        """
        name = person["name"]
        ptype = person.get("type", "human")
        wikidata_id = person.get("wikidata_id")
        merged = {"father": None, "mother": None, "children": [],
                  "birth_year": None, "source_url": None, "auto_categories": []}

        # 1. Wikidata (most structured, highest confidence)
        try:
            if not wikidata_id:
                wikidata_id = await self.find_wikidata_id(name)
            if wikidata_id:
                wd = await self.fetch_wikidata(wikidata_id)
                if wd:
                    self._merge_into(merged, wd, boost=0)
        except Exception as e:
            logger.warning(f"Wikidata failed for {name}: {e}")

        # 2. DBpedia (structured, cross-validates Wikidata)
        try:
            dbp = await self.fetch_dbpedia(name)
            if dbp:
                self._merge_into(merged, dbp, boost=-5)  # slightly lower confidence
        except Exception as e:
            logger.warning(f"DBpedia failed for {name}: {e}")

        # 3. Wikipedia infobox (text fallback)
        if not merged["father"] and not merged["mother"] and not merged["children"]:
            try:
                wiki = await self.fetch_wikipedia(name)
                if wiki:
                    self._merge_into(merged, wiki, boost=-10)
            except Exception as e:
                logger.warning(f"Wikipedia failed for {name}: {e}")

        # 4. Theoi.com — Greek mythology specialist (much better than Wikipedia for gods)
        if ptype == "mythological" and ("greek" in name.lower() or
                any("Greek" in c for c in merged.get("auto_categories", []))):
            try:
                theoi = await self.fetch_theoi(name)
                if theoi:
                    self._merge_into(merged, theoi, boost=5)  # higher confidence for mythology
            except Exception as e:
                logger.warning(f"Theoi failed for {name}: {e}")

        # 5. Wikisource — biblical/ancient genealogies
        if any(kw in name.lower() for kw in ["adam","noah","abraham","isaac","jacob","moses","david","solomon"]) or \
           any(c in merged.get("auto_categories",[]) for c in ["Biblical Figures","Vedic Figures","Quranic Figures"]):
            try:
                ws = await self.fetch_wikisource_genealogy(name)
                if ws:
                    self._merge_into(merged, ws, boost=8)  # ancient texts = high confidence
            except Exception as e:
                logger.warning(f"Wikisource failed for {name}: {e}")

        return merged if (merged["father"] or merged["mother"] or merged["children"]) else None

    def _merge_into(self, target: Dict, source: Dict, boost: float = 0):
        """Merge source discoveries into target, adjusting confidence by boost."""
        # Father: take highest confidence
        if source.get("father"):
            f = dict(source["father"])
            f["confidence"] = min(99.0, f.get("confidence", 70) + boost)
            if not target["father"] or f["confidence"] > target["father"]["confidence"]:
                target["father"] = f

        # Mother: same
        if source.get("mother"):
            m = dict(source["mother"])
            m["confidence"] = min(99.0, m.get("confidence", 70) + boost)
            if not target["mother"] or m["confidence"] > target["mother"]["confidence"]:
                target["mother"] = m

        # Children: add new ones, update confidence on existing
        existing_names = {c["name"].lower() for c in target["children"]}
        for child in source.get("children", []):
            c = dict(child)
            c["confidence"] = min(99.0, c.get("confidence", 65) + boost)
            if c["name"].lower() not in existing_names:
                target["children"].append(c)
                existing_names.add(c["name"].lower())

        # Birth year — take any
        if source.get("birth_year") and not target["birth_year"]:
            target["birth_year"] = source["birth_year"]

        # Source URL — keep first
        if source.get("source_url") and not target["source_url"]:
            target["source_url"] = source["source_url"]

        # Categories — union
        for cat in source.get("auto_categories", []):
            if cat not in target["auto_categories"]:
                target["auto_categories"].append(cat)

        # Wikidata ID
        if source.get("wikidata_id") and not target.get("wikidata_id"):
            target["wikidata_id"] = source["wikidata_id"]

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
                if any(k in desc for k in ["human","person","deity","god","king","emperor",
                                            "pharaoh","ruler","queen","prince","prophet","mythological"]):
                    return result["id"]
            results = r.json().get("search", [])
            return results[0]["id"] if results else None
        except Exception as e:
            logger.warning(f"Wikidata ID lookup failed: {e}")
            return None

    async def fetch_wikidata(self, qid: str) -> Optional[Dict]:
        query = f"""
        SELECT ?father ?fatherLabel ?mother ?motherLabel ?child ?childLabel
               ?birthYear ?article WHERE {{
          OPTIONAL {{ wd:{qid} wdt:P22 ?father. }}
          OPTIONAL {{ wd:{qid} wdt:P25 ?mother. }}
          OPTIONAL {{ ?child wdt:P22 wd:{qid}. }}
          OPTIONAL {{ ?child wdt:P25 wd:{qid}. }}
          OPTIONAL {{ wd:{qid} wdt:P569 ?dob. BIND(YEAR(?dob) AS ?birthYear) }}
          OPTIONAL {{ ?article schema:about wd:{qid};
                       schema:isPartOf <https://en.wikipedia.org/> . }}
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
            seen = set()
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
                    if cn not in seen and not cn.startswith("Q") and len(cn) > 1:
                        seen.add(cn)
                        d["children"].append({"name": cn,
                            "wikidata_id": row["child"]["value"].split("/")[-1] if row.get("child") else None,
                            "confidence": 88.0})
                if row.get("birthYear") and not d["birth_year"]:
                    try: d["birth_year"] = int(row["birthYear"]["value"])
                    except: pass
            return d
        except Exception as e:
            logger.error(f"Wikidata SPARQL failed {qid}: {e}")
            return None

    # ── SOURCE 2: DBPEDIA ────────────────────────────────────────────────────

    async def fetch_dbpedia(self, name: str) -> Optional[Dict]:
        """
        DBpedia mirrors Wikipedia as structured RDF.
        Uses different properties than Wikidata — good for cross-validation.
        """
        # DBpedia uses underscored resource names
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
          OPTIONAL {{
            dbr:{resource} dbo:birthYear ?birthYear .
          }}
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

    # ── SOURCE 4: THEOI.COM (Greek mythology specialist) ────────────────────

    async def fetch_theoi(self, name: str) -> Optional[Dict]:
        """
        Theoi.com is the most accurate Greek mythology genealogy source.
        Has structured family trees for all major deities and heroes.
        """
        try:
            # Search theoi for the deity
            search_url = f"{THEOI_BASE}/greek-mythology/{name.replace(' ', '').lower()}.html"
            r = await self.client.get(search_url)
            if r.status_code != 200:
                # Try alternate URL format
                search_url = f"{THEOI_BASE}/Olympios/{name.replace(' ', '')}.html"
                r = await self.client.get(search_url)
            if r.status_code != 200:
                return None

            soup = BeautifulSoup(r.text, "html.parser")
            d = {"father": None, "mother": None, "children": [],
                 "source_url": search_url, "auto_categories": ["Greek Gods"]}

            # Theoi lists parents and children in structured sections
            text = soup.get_text()
            # Look for "PARENTS" section
            parent_match = re.search(r'PARENTS?\s*\n([^\n]+)\n', text, re.IGNORECASE)
            if parent_match:
                parent_text = parent_match.group(1)
                # Usually "Father X and Mother Y" or "X & Y"
                father_m = re.search(r'(?:father|sire)[:\s]+([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)', parent_text, re.IGNORECASE)
                mother_m = re.search(r'(?:mother|dam)[:\s]+([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)', parent_text, re.IGNORECASE)
                if father_m:
                    d["father"] = {"name": father_m.group(1), "confidence": 88.0}
                if mother_m:
                    d["mother"] = {"name": mother_m.group(1), "confidence": 86.0}

            return d if (d["father"] or d["mother"] or d["children"]) else None
        except Exception as e:
            logger.warning(f"Theoi failed for {name}: {e}")
            return None

    # ── SOURCE 5: WIKISOURCE (biblical/ancient genealogies) ─────────────────

    async def fetch_wikisource_genealogy(self, name: str) -> Optional[Dict]:
        """
        Wikisource has complete biblical genealogies, royal chronicles,
        and ancient genealogical texts. Very high confidence for biblical figures.
        """
        try:
            # Search Wikisource for genealogy pages
            r = await self.client.get(WIKISOURCE_API, params={
                "action": "query",
                "list": "search",
                "srsearch": f"{name} genealogy lineage",
                "format": "json",
                "srlimit": 3
            })
            if r.status_code != 200:
                return None
            results = r.json().get("query", {}).get("search", [])
            if not results:
                return None

            # Fetch the most relevant page
            page_title = results[0]["title"]
            r2 = await self.client.get(WIKISOURCE_API, params={
                "action": "query", "titles": page_title,
                "prop": "revisions", "rvprop": "content",
                "rvslots": "main", "format": "json", "formatversion": "2"
            })
            pages = r2.json().get("query", {}).get("pages", [])
            if not pages or pages[0].get("missing"):
                return None

            content = pages[0].get("revisions", [{}])[0].get("slots", {}).get("main", {}).get("content", "")

            # Look for "begat" patterns (biblical genealogy)
            begat_pattern = rf'{re.escape(name)}\s+begat\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)'
            begat_matches = re.findall(begat_pattern, content, re.IGNORECASE)

            father_pattern = rf'son of\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)'
            father_match = re.search(father_pattern, content, re.IGNORECASE)

            d = {"father": None, "mother": None, "children": [],
                 "source_url": f"https://en.wikisource.org/wiki/{page_title.replace(' ','_')}",
                 "auto_categories": ["Biblical Figures"]}

            if father_match:
                d["father"] = {"name": father_match.group(1), "confidence": 91.0}
            for child_name in begat_matches[:10]:
                d["children"].append({"name": child_name, "confidence": 89.0})

            return d if (d["father"] or d["children"]) else None
        except Exception as e:
            logger.warning(f"Wikisource failed for {name}: {e}")
            return None

    # ── HELPERS ─────────────────────────────────────────────────────────────

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

    # ── SAVE TO DB ──────────────────────────────────────────────────────────

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
        for cat_name in d.get("auto_categories", []):
            cat = await self.db("SELECT id FROM categories WHERE name=:n", {"n": cat_name}, "one")
            if cat:
                existing_pc = await self.db(
                    "SELECT 1 as x FROM person_categories WHERE person_id=:p AND category_id=:c",
                    {"p": pid, "c": cat["id"]}, "one")
                if not existing_pc:
                    await self.db("INSERT INTO person_categories (person_id,category_id) VALUES (:p,:c)",
                                  {"p": pid, "c": cat["id"]}, "none")
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
            "SELECT id,confidence FROM relationships WHERE child_id=:c AND parent_type=:t AND is_primary=TRUE",
            {"c": child_id, "t": parent_type}, "one")
        is_primary = existing_primary is None
        # If new data has higher confidence, promote it
        if existing_primary and parent_data["confidence"] > (existing_primary.get("confidence") or 0):
            await self.db("UPDATE relationships SET is_primary=FALSE, is_branch=TRUE WHERE id=:id",
                          {"id": existing_primary["id"]}, "none")
            is_primary = True
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
                     "conf": parent_data["confidence"], "prim": is_primary, "branch": not is_primary}, "one")
                rel_id = row["id"] if row else None

            if source_url and rel_id:
                existing_src = await self.db(
                    "SELECT id FROM sources WHERE relationship_id=:r AND url=:u",
                    {"r": rel_id, "u": source_url}, "one")
                if not existing_src:
                    await self.db("INSERT INTO sources (relationship_id,url,source_type) VALUES (:r,:u,:t)",
                                  {"r": rel_id, "u": source_url,
                                   "t": "wikidata" if "wikidata" in source_url else
                                        "dbpedia" if "dbpedia" in source_url else
                                        "wikisource" if "wikisource" in source_url else
                                        "wikipedia"}, "none")

            # Genesis merge check
            if parent_data["confidence"] >= 95.0:
                child = await self.db("SELECT * FROM persons WHERE id=:id", {"id": child_id}, "one")
                if child and child["is_genesis"]:
                    await self.db("UPDATE persons SET is_genesis=FALSE,genesis_code=NULL WHERE id=:id",
                                  {"id": child_id}, "none")
                    await self.db("""INSERT INTO merge_log
                        (genesis_person_id,genesis_code,merged_into_person_id,confidence_at_merge)
                        VALUES (:gp,:gc,:mi,:conf)""",
                        {"gp": child_id, "gc": child["genesis_code"],
                         "mi": parent_id, "conf": parent_data["confidence"]}, "none")
                    logger.info(f"🔗 MERGE: {child['name']} dissolved")

            # Queue parent for research
            existing_q = await self.db(
                "SELECT id FROM agent_queue WHERE person_id=:p AND status='pending'",
                {"p": parent_id}, "one")
            if not existing_q:
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
                    "SELECT id FROM sources WHERE relationship_id=:r AND url=:u",
                    {"r": rel_id, "u": source_url}, "one")
                if not existing_src:
                    await self.db("INSERT INTO sources (relationship_id,url,source_type) VALUES (:r,:u,'wikipedia')",
                                  {"r": rel_id, "u": source_url}, "none")
            existing_q = await self.db(
                "SELECT id FROM agent_queue WHERE person_id=:p AND status='pending'",
                {"p": child_id}, "one")
            if not existing_q:
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
            count = (await self.db("SELECT COUNT(*) as c FROM persons WHERE is_genesis=TRUE",
                                   fetch="one") or {}).get("c", 0)
            gc = f"G{count + 1}"
            new_row = await self.db(
                "INSERT INTO persons (name,wikidata_id,is_genesis,genesis_code,type) VALUES (:n,:w,TRUE,:g,'human') RETURNING id",
                {"n": name, "w": wikidata_id, "g": gc}, "one")
            if new_row:
                await self.db(
                    "INSERT INTO agent_log (person_id,person_name,action,detail) VALUES (:p,:n,'discovered','Discovered by agent')",
                    {"p": new_row["id"], "n": name}, "none")
            return new_row["id"] if new_row else None
        except Exception as e:
            logger.warning(f"Could not create {name}: {e}")
            return None

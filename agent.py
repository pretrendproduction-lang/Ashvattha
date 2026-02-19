import asyncio
import httpx
import re
import logging
from typing import Optional, Dict, List

logger = logging.getLogger("ashvattha_agent")
logging.basicConfig(level=logging.INFO)

WIKIDATA_API  = "https://www.wikidata.org/w/api.php"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"

CATEGORY_KEYWORDS = {
    "Greek Gods": ["greek god", "olympian", "greek deity"],
    "Norse Gods":  ["norse god", "asgard", "viking god"],
    "Hindu Deities": ["hindu god", "hindu deity", "vedic god"],
    "Egyptian Gods": ["egyptian god", "egyptian deity"],
    "Roman Gods": ["roman god", "roman deity"],
    "Mesopotamian Gods": ["sumerian god", "babylonian god"],
    "Egyptian Pharaohs": ["pharaoh", "king of egypt"],
    "Roman Emperors": ["roman emperor"],
    "Biblical Figures": ["biblical", "old testament", "patriarch"],
    "British Royals": ["king of england", "queen of england"],
    "Americans": ["american president", "united states president"],
    "South Asians": ["prime minister of india"],
}

SEEDS = [
    ("Zeus","mythological"),("Adam","human"),("Abraham","human"),
    ("Ramesses II","human"),("Alexander the Great","human"),
    ("Julius Caesar","human"),("Genghis Khan","human"),
    ("Charlemagne","human"),("Odin","mythological"),
    ("Brahma","mythological"),("Osiris","mythological"),
    ("Muhammad","human"),
]

class GenesisAgent:
    def __init__(self, db):
        self.db = db
        self.client = None

    async def run_forever(self):
        self.client = httpx.AsyncClient(timeout=30.0,
            headers={"User-Agent":"Ashvattha/1.0 (genealogy research)"})
        logger.info("🌱 Ashvattha Agent awakened")
        await self.seed_initial_persons()
        while True:
            try:
                await self.process_next()
                await asyncio.sleep(3)
            except Exception as e:
                logger.error(f"Agent error: {e}")
                await asyncio.sleep(15)

    async def seed_initial_persons(self):
        for name, ptype in SEEDS:
            existing = await self.db("SELECT id FROM persons WHERE LOWER(name)=LOWER(:n)", {"n":name}, "one")
            if not existing:
                count = (await self.db("SELECT COUNT(*) as c FROM persons WHERE is_genesis=TRUE", fetch="one") or {}).get("c",0)
                gc = f"G{count+1}"
                row = await self.db("INSERT INTO persons (name,type,is_genesis,genesis_code) VALUES (:n,:t,TRUE,:g) RETURNING id",
                                    {"n":name,"t":ptype,"g":gc}, "one")
                await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',90)", {"p":row["id"]}, "none")
                logger.info(f"🌱 Seeded: {name}")

    async def process_next(self):
        job = await self.db("SELECT id,person_id FROM agent_queue WHERE status='pending' ORDER BY priority DESC, created_at ASC LIMIT 1", fetch="one")
        if not job:
            unresearched = await self.db("SELECT id FROM persons WHERE agent_researched=FALSE AND type!='genesis' LIMIT 5", fetch="all")
            for p in (unresearched or []):
                existing_q = await self.db("SELECT id FROM agent_queue WHERE person_id=:p AND status='pending'", {"p":p["id"]}, "one")
                if not existing_q:
                    await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',40)", {"p":p["id"]}, "none")
            return

        await self.db("UPDATE agent_queue SET status='processing' WHERE id=:id", {"id":job["id"]}, "none")
        person = await self.db("SELECT * FROM persons WHERE id=:id", {"id":job["person_id"]}, "one")
        if not person:
            await self.db("UPDATE agent_queue SET status='done' WHERE id=:id", {"id":job["id"]}, "none")
            return

        logger.info(f"🔍 Researching: {person['name']}")
        discoveries = await self.research_person(person["name"], person.get("wikidata_id"))

        if discoveries:
            await self.save_discoveries(person, discoveries)
            await self.db("UPDATE persons SET agent_researched=TRUE WHERE id=:id", {"id":person["id"]}, "none")
            await self.db("UPDATE agent_queue SET status='done' WHERE id=:id", {"id":job["id"]}, "none")
        else:
            attempts_row = await self.db("SELECT attempts FROM agent_queue WHERE id=:id", {"id":job["id"]}, "one")
            attempts = (attempts_row or {}).get("attempts", 0)
            status = "failed" if attempts >= 3 else "pending"
            await self.db("UPDATE agent_queue SET status=:s, attempts=attempts+1 WHERE id=:id", {"s":status,"id":job["id"]}, "none")

    async def research_person(self, name, wikidata_id=None):
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

    async def find_wikidata_id(self, name):
        try:
            r = await self.client.get(WIKIDATA_API, params={
                "action":"wbsearchentities","search":name,
                "language":"en","type":"item","format":"json","limit":5})
            for result in r.json().get("search",[]):
                desc = result.get("description","").lower()
                if any(k in desc for k in ["human","person","deity","god","king","emperor","pharaoh"]):
                    return result["id"]
            results = r.json().get("search",[])
            return results[0]["id"] if results else None
        except:
            return None

    async def fetch_wikidata_family(self, qid):
        query = f"""
        SELECT ?father ?fatherLabel ?mother ?motherLabel ?child ?childLabel ?birthYear ?article WHERE {{
          OPTIONAL {{ wd:{qid} wdt:P22 ?father. }}
          OPTIONAL {{ wd:{qid} wdt:P25 ?mother. }}
          OPTIONAL {{ ?child wdt:P22 wd:{qid}. }}
          OPTIONAL {{ wd:{qid} wdt:P569 ?dob. BIND(YEAR(?dob) AS ?birthYear) }}
          OPTIONAL {{ ?article schema:about wd:{qid}; schema:isPartOf <https://en.wikipedia.org/> . }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }} LIMIT 40"""
        try:
            r = await self.client.get(WIKIDATA_SPARQL, params={"query":query,"format":"json"},
                                      headers={"Accept":"application/json"})
            bindings = r.json().get("results",{}).get("bindings",[])
            if not bindings:
                return None
            d = {"wikidata_id":qid,"father":None,"mother":None,"children":[],
                 "birth_year":None,"source_url":f"https://www.wikidata.org/wiki/{qid}","auto_categories":[]}
            seen = set()
            for row in bindings:
                if row.get("fatherLabel") and not d["father"]:
                    d["father"] = {"name":row["fatherLabel"]["value"],
                                   "wikidata_id":row["father"]["value"].split("/")[-1] if row.get("father") else None,
                                   "confidence":90.0}
                if row.get("motherLabel") and not d["mother"]:
                    d["mother"] = {"name":row["motherLabel"]["value"],
                                   "wikidata_id":row["mother"]["value"].split("/")[-1] if row.get("mother") else None,
                                   "confidence":88.0}
                if row.get("childLabel"):
                    cn = row["childLabel"]["value"]
                    if cn not in seen:
                        seen.add(cn)
                        d["children"].append({"name":cn,
                            "wikidata_id":row["child"]["value"].split("/")[-1] if row.get("child") else None,
                            "confidence":85.0})
                if row.get("birthYear") and not d["birth_year"]:
                    try: d["birth_year"] = int(row["birthYear"]["value"])
                    except: pass
            return d
        except Exception as e:
            logger.error(f"SPARQL failed {qid}: {e}")
            return None

    async def fetch_wikipedia_family(self, name):
        try:
            r = await self.client.get(WIKIPEDIA_API, params={
                "action":"query","titles":name,"prop":"revisions|categories",
                "rvprop":"content","rvslots":"main","format":"json","formatversion":"2","cllimit":"20"})
            pages = r.json().get("query",{}).get("pages",[])
            if not pages or pages[0].get("missing"):
                return None
            page = pages[0]
            content = page.get("revisions",[{}])[0].get("slots",{}).get("main",{}).get("content","")
            cats = [c["title"] for c in page.get("categories",[])]
            father = self._extract(content, ["father","Father"])
            mother = self._extract(content, ["mother","Mother"])
            children = self._extract_children(content)
            if not father and not mother and not children:
                return None
            slug = page.get("title","").replace(" ","_")
            return {"father":father,"mother":mother,"children":children,
                    "source_url":f"https://en.wikipedia.org/wiki/{slug}",
                    "wikidata_id":None,"birth_year":None,
                    "auto_categories":self._detect_cats(content + " ".join(cats))}
        except Exception as e:
            logger.error(f"Wikipedia failed {name}: {e}")
            return None

    def _extract(self, content, fields):
        for f in fields:
            m = re.search(rf"\|\s*{f}\s*=\s*([^\|\n\}}]+)", content, re.IGNORECASE)
            if m:
                raw = re.sub(r'\[\[([^\|\]]+)(?:\|[^\]]+)?\]\]', r'\1', m.group(1))
                raw = re.sub(r'\{\{[^\}]+\}\}|<[^>]+>', '', raw).strip()
                if raw and 1 < len(raw) < 200:
                    return {"name":raw,"confidence":72.0}
        return None

    def _extract_children(self, content):
        children = []
        m = re.search(r"\|\s*(?:children|issue)\s*=\s*((?:[^\|]|\n(?!\|))+)", content, re.IGNORECASE)
        if m:
            for n in re.findall(r'\[\[([^\|\]]+)(?:\|[^\]]+)?\]\]', m.group(1))[:8]:
                n = n.strip()
                if n and len(n) > 1:
                    children.append({"name":n,"confidence":68.0})
        return children

    def _detect_cats(self, text):
        tl = text.lower()
        return [cat for cat, kws in CATEGORY_KEYWORDS.items() if any(k in tl for k in kws)]

    async def save_discoveries(self, person, d):
        pid = person["id"]
        if d.get("wikidata_id"):
            await self.db("UPDATE persons SET wikidata_id=:w WHERE id=:id", {"w":d["wikidata_id"],"id":pid}, "none")
        if d.get("birth_year"):
            await self.db("UPDATE persons SET approx_birth_year=:y WHERE id=:id", {"y":d["birth_year"],"id":pid}, "none")
        if d.get("father"):
            await self.link_parent(pid, d["father"], "father", d.get("source_url"))
        if d.get("mother"):
            await self.link_parent(pid, d["mother"], "mother", d.get("source_url"))
        for child in d.get("children",[]):
            await self.link_child(pid, child, d.get("source_url"))
        for cat_name in d.get("auto_categories",[]):
            cat = await self.db("SELECT id FROM categories WHERE name=:n", {"n":cat_name}, "one")
            if cat:
                existing_pc = await self.db("SELECT 1 FROM person_categories WHERE person_id=:p AND category_id=:c",
                                            {"p":pid,"c":cat["id"]}, "one")
                if not existing_pc:
                    await self.db("INSERT INTO person_categories (person_id,category_id) VALUES (:p,:c)",
                                  {"p":pid,"c":cat["id"]}, "none")
        await self.db("INSERT INTO agent_log (person_id,person_name,action,detail) VALUES (:p,:n,'discovered',:d)",
                      {"p":pid,"n":person["name"],
                       "d":f"father={d.get('father',{}).get('name','-') if d.get('father') else '-'}, children={len(d.get('children',[]))}"}, "none")

    async def link_parent(self, child_id, parent_data, parent_type, source_url):
        parent_id = await self.find_or_create(parent_data["name"], parent_data.get("wikidata_id"))
        if not parent_id or parent_id == child_id:
            return
        existing_primary = await self.db("SELECT id FROM relationships WHERE child_id=:c AND parent_type=:t AND is_primary=TRUE",
                                         {"c":child_id,"t":parent_type}, "one")
        is_primary = existing_primary is None
        try:
            existing_rel = await self.db("SELECT id FROM relationships WHERE child_id=:c AND parent_id=:p AND parent_type=:t",
                                         {"c":child_id,"p":parent_id,"t":parent_type}, "one")
            if existing_rel:
                await self.db("UPDATE relationships SET confidence=:conf WHERE id=:id",
                              {"conf":parent_data["confidence"],"id":existing_rel["id"]}, "none")
                rel_id = existing_rel["id"]
            else:
                row = await self.db("""INSERT INTO relationships (child_id,parent_id,parent_type,confidence,is_primary,is_branch)
                    VALUES (:c,:p,:t,:conf,:prim,:branch) RETURNING id""",
                    {"c":child_id,"p":parent_id,"t":parent_type,"conf":parent_data["confidence"],"prim":is_primary,"branch":not is_primary}, "one")
                rel_id = row["id"] if row else None
            if source_url and rel_id:
                existing_src = await self.db("SELECT id FROM sources WHERE relationship_id=:r AND url=:u",
                                             {"r":rel_id,"u":source_url}, "one")
                if not existing_src:
                    await self.db("INSERT INTO sources (relationship_id,url,source_type) VALUES (:r,:u,'wikidata')",
                                  {"r":rel_id,"u":source_url}, "none")
            if parent_data["confidence"] >= 95.0:
                child = await self.db("SELECT * FROM persons WHERE id=:id", {"id":child_id}, "one")
                if child and child["is_genesis"]:
                    await self.db("UPDATE persons SET is_genesis=FALSE,genesis_code=NULL WHERE id=:id", {"id":child_id}, "none")
                    await self.db("INSERT INTO merge_log (genesis_person_id,genesis_code,merged_into_person_id,confidence_at_merge) VALUES (:gp,:gc,:mi,:conf)",
                                  {"gp":child_id,"gc":child["genesis_code"],"mi":parent_id,"conf":parent_data["confidence"]}, "none")
                    logger.info(f"🔗 MERGE: {child['name']} dissolved")
            existing_q = await self.db("SELECT id FROM agent_queue WHERE person_id=:p AND status='pending'", {"p":parent_id}, "one")
            if not existing_q:
                await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',75)", {"p":parent_id}, "none")
        except Exception as e:
            logger.warning(f"link_parent error: {e}")

    async def link_child(self, parent_id, child_data, source_url):
        child_id = await self.find_or_create(child_data["name"], child_data.get("wikidata_id"))
        if not child_id or child_id == parent_id:
            return
        try:
            existing_rel = await self.db("SELECT id FROM relationships WHERE child_id=:c AND parent_id=:p AND parent_type='father'",
                                         {"c":child_id,"p":parent_id}, "one")
            if existing_rel:
                await self.db("UPDATE relationships SET confidence=:conf WHERE id=:id",
                              {"conf":child_data["confidence"],"id":existing_rel["id"]}, "none")
                rel_id = existing_rel["id"]
            else:
                row = await self.db("INSERT INTO relationships (child_id,parent_id,parent_type,confidence,is_primary) VALUES (:c,:p,'father',:conf,TRUE) RETURNING id",
                                    {"c":child_id,"p":parent_id,"conf":child_data["confidence"]}, "one")
                rel_id = row["id"] if row else None
            if source_url and rel_id:
                existing_src = await self.db("SELECT id FROM sources WHERE relationship_id=:r AND url=:u",
                                             {"r":rel_id,"u":source_url}, "one")
                if not existing_src:
                    await self.db("INSERT INTO sources (relationship_id,url,source_type) VALUES (:r,:u,'wikidata')",
                                  {"r":rel_id,"u":source_url}, "none")
            existing_q = await self.db("SELECT id FROM agent_queue WHERE person_id=:p AND status='pending'", {"p":child_id}, "one")
            if not existing_q:
                await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',55)", {"p":child_id}, "none")
        except Exception as e:
            logger.warning(f"link_child error: {e}")

    async def find_or_create(self, name, wikidata_id=None):
        if wikidata_id:
            row = await self.db("SELECT id FROM persons WHERE wikidata_id=:w", {"w":wikidata_id}, "one")
            if row:
                return row["id"]
        row = await self.db("SELECT id FROM persons WHERE LOWER(name)=LOWER(:n) LIMIT 1", {"n":name}, "one")
        if row:
            if wikidata_id:
                await self.db("UPDATE persons SET wikidata_id=:w WHERE id=:id", {"w":wikidata_id,"id":row["id"]}, "none")
            return row["id"]
        try:
            count = (await self.db("SELECT COUNT(*) as c FROM persons WHERE is_genesis=TRUE", fetch="one") or {}).get("c",0)
            gc = f"G{count+1}"
            new_row = await self.db("INSERT INTO persons (name,wikidata_id,is_genesis,genesis_code,type) VALUES (:n,:w,TRUE,:g,'human') RETURNING id",
                                    {"n":name,"w":wikidata_id,"g":gc}, "one")
            if new_row:
                await self.db("INSERT INTO agent_log (person_id,person_name,action,detail) VALUES (:p,:n,'discovered','Discovered by agent')",
                              {"p":new_row["id"],"n":name}, "none")
            return new_row["id"] if new_row else None
        except Exception as e:
            logger.warning(f"Could not create {name}: {e}")
            return None

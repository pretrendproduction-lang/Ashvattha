"""
AGENT 1: Research Agent
Finds parents and children for persons in the queue.
Sources: Wikidata → DBpedia → Wikipedia
Runs every 5s per person.
"""
import asyncio
import re
import logging
from typing import Optional, Dict
from .base import BaseAgent

logger = logging.getLogger("ashvattha.research")

WIKIDATA_API    = "https://www.wikidata.org/w/api.php"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIPEDIA_API   = "https://en.wikipedia.org/w/api.php"
DBPEDIA_SPARQL  = "https://dbpedia.org/sparql"

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
    ("Romulus","human"), ("Aeneas","human"),
    ("Henry II of England","human"), ("Edward I of England","human"),
    ("Vlad the Impaler","human"), ("Ivan the Terrible","human"),
    ("Moctezuma II","human"), ("Pachacuti","human"),
    ("Emperor Meiji","human"), ("Babur","human"),
    ("Timur","human"), ("Hammurabi","human"),
]

MAX_ATTEMPTS = 5
TICK_DELAY   = 5   # seconds between persons

class ResearchAgent(BaseAgent):
    name = "research"

    def __init__(self, db):
        super().__init__(db)
        self.expansion_index = 0
        self.consecutive_failures = 0

    async def tick(self):
        if self.consecutive_failures >= 5:
            logger.warning("⏸️  Too many failures — backing off 90s")
            await asyncio.sleep(90)
            self.consecutive_failures = 0

        job = await self.db(
            "SELECT id,person_id,attempts FROM agent_queue WHERE status='pending' "
            "ORDER BY priority DESC, created_at ASC LIMIT 1", fetch="one")

        if not job:
            await self._refill_queue()
            await asyncio.sleep(10)
            return

        await self.db("UPDATE agent_queue SET status='processing' WHERE id=:id",
                      {"id": job["id"]}, "none")
        person = await self.db("SELECT * FROM persons WHERE id=:id",
                               {"id": job["person_id"]}, "one")
        if not person:
            await self.db("UPDATE agent_queue SET status='done' WHERE id=:id",
                          {"id": job["id"]}, "none")
            return

        logger.info(f"🔍 [{person['type']}] {person['name']}")
        result = await self._research(person)

        if result and (result.get("father") or result.get("mother") or result.get("children")):
            await self._save(person, result)
            await self.db("UPDATE persons SET agent_researched=TRUE WHERE id=:id",
                          {"id": person["id"]}, "none")
            await self.db("UPDATE agent_queue SET status='done' WHERE id=:id",
                          {"id": job["id"]}, "none")
            self.consecutive_failures = 0
            logger.info(f"✅ {person['name']}")
        else:
            attempts = (job.get("attempts") or 0) + 1
            if attempts >= MAX_ATTEMPTS:
                await self.db("UPDATE agent_queue SET status='failed', attempts=:a WHERE id=:id",
                              {"a": attempts, "id": job["id"]}, "none")
                # No father found after all attempts → mark as genesis
                has_father = await self.db(
                    "SELECT COUNT(*) as c FROM relationships WHERE child_id=:id AND parent_type='father'",
                    {"id": job["person_id"]}, "one")
                if (has_father or {}).get("c", 0) == 0:
                    existing = await self.db("SELECT is_genesis,genesis_code FROM persons WHERE id=:id",
                                            {"id": job["person_id"]}, "one")
                    if existing and not existing["is_genesis"]:
                        count = (await self.db("SELECT COUNT(*) as c FROM persons WHERE is_genesis=TRUE",
                                              fetch="one") or {}).get("c", 0)
                        gc = f"G{count + 1}"
                        await self.db("UPDATE persons SET is_genesis=TRUE, genesis_code=:gc WHERE id=:id",
                                      {"gc": gc, "id": job["person_id"]}, "none")
                        logger.info(f"⬡ {person['name']} → {gc} (no father found)")
                self.consecutive_failures += 1
            else:
                await self.db("UPDATE agent_queue SET status='pending', attempts=:a WHERE id=:id",
                              {"a": attempts, "id": job["id"]}, "none")
                await asyncio.sleep(min(attempts * 8, 60))

        await asyncio.sleep(TICK_DELAY)

    async def _refill_queue(self):
        """Reset failed jobs, then add expansion seeds."""
        failed = (await self.db("SELECT COUNT(*) as c FROM agent_queue WHERE status='failed'",
                                fetch="one") or {}).get("c", 0)
        if failed > 0:
            await self.db("UPDATE agent_queue SET status='pending', attempts=0 WHERE status='failed'",
                          fetch="none")
            logger.info(f"♻️  Reset {failed} failed jobs")
            return

        if self.expansion_index < len(EXPANSION_SEEDS):
            name, ptype = EXPANSION_SEEDS[self.expansion_index]
            self.expansion_index += 1
            existing = await self.db("SELECT id FROM persons WHERE LOWER(name)=LOWER(:n)",
                                     {"n": name}, "one")
            if not existing:
                row = await self.db(
                    "INSERT INTO persons (name,type,is_genesis,genesis_code) VALUES (:n,:t,FALSE,NULL) RETURNING id",
                    {"n": name, "t": ptype}, "one")
                if row:
                    await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',85)",
                                  {"p": row["id"]}, "none")
                    logger.info(f"🌱 Expansion seed: {name}")
            else:
                q = await self.db(
                    "SELECT COUNT(*) as c FROM agent_queue WHERE person_id=:p AND status='pending'",
                    {"p": existing["id"]}, "one")
                if (q or {}).get("c", 0) == 0:
                    await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',85)",
                                  {"p": existing["id"]}, "none")
        else:
            unresearched = await self.db(
                "SELECT id FROM persons WHERE agent_researched=FALSE AND type!='genesis' LIMIT 20",
                fetch="all")
            if unresearched:
                for p in unresearched:
                    q = await self.db(
                        "SELECT COUNT(*) as c FROM agent_queue WHERE person_id=:p AND status='pending'",
                        {"p": p["id"]}, "one")
                    if (q or {}).get("c", 0) == 0:
                        await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',30)",
                                      {"p": p["id"]}, "none")
            else:
                self.expansion_index = 0
                logger.info("🔄 Full cycle complete — restarting")

    # ── MULTI-SOURCE RESEARCH ─────────────────────────────────────────────

    async def _research(self, person) -> Optional[Dict]:
        name = person["name"]
        wikidata_id = person.get("wikidata_id")
        merged = {"father": None, "mother": None, "children": [],
                  "birth_year": None, "source_url": None,
                  "auto_categories": [], "wikidata_id": wikidata_id}

        # 1. Wikidata
        try:
            if not wikidata_id:
                wikidata_id = await self._wikidata_id(name)
            if wikidata_id:
                wd = await self._fetch_wikidata(wikidata_id)
                if wd:
                    _merge(merged, wd)
        except Exception as e:
            logger.warning(f"Wikidata error: {e}")

        # 2. DBpedia
        try:
            dbp = await self._fetch_dbpedia(name)
            if dbp:
                _merge(merged, dbp, offset=-5)
        except Exception as e:
            logger.warning(f"DBpedia error: {e}")

        # 3. Wikipedia fallback
        if not merged["father"] and not merged["mother"] and not merged["children"]:
            try:
                wiki = await self._fetch_wikipedia(name)
                if wiki:
                    _merge(merged, wiki, offset=-10)
            except Exception as e:
                logger.warning(f"Wikipedia error: {e}")

        return merged

    async def _wikidata_id(self, name) -> Optional[str]:
        try:
            r = await self.client.get(WIKIDATA_API, params={
                "action": "wbsearchentities", "search": name,
                "language": "en", "type": "item", "format": "json", "limit": 5})
            if not await self.rate_check(r):
                return None
            for res in r.json().get("search", []):
                desc = res.get("description", "").lower()
                if any(k in desc for k in ["human","person","deity","god","king",
                                           "emperor","pharaoh","ruler","queen","prophet"]):
                    return res["id"]
            results = r.json().get("search", [])
            return results[0]["id"] if results else None
        except Exception as e:
            logger.warning(f"Wikidata ID error: {e}")
            return None

    async def _fetch_wikidata(self, qid) -> Optional[Dict]:
        query = f"""
        SELECT ?father ?fatherLabel ?mother ?motherLabel ?child ?childLabel
               ?birthYear ?occupationLabel WHERE {{
          OPTIONAL {{ wd:{qid} wdt:P22 ?father. }}
          OPTIONAL {{ wd:{qid} wdt:P25 ?mother. }}
          OPTIONAL {{ ?child wdt:P22 wd:{qid}. }}
          OPTIONAL {{ ?child wdt:P25 wd:{qid}. }}
          OPTIONAL {{ wd:{qid} wdt:P569 ?dob. BIND(YEAR(?dob) AS ?birthYear) }}
          OPTIONAL {{ wd:{qid} wdt:P106 ?occupation. }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }} LIMIT 60"""
        try:
            r = await self.client.get(WIKIDATA_SPARQL,
                params={"query": query, "format": "json"},
                headers={"Accept": "application/json"})
            if not await self.rate_check(r):
                return None
            if r.status_code != 200:
                return None
            bindings = r.json().get("results", {}).get("bindings", [])
            d = {"wikidata_id": qid, "father": None, "mother": None, "children": [],
                 "birth_year": None, "source_url": f"https://www.wikidata.org/wiki/{qid}",
                 "auto_categories": []}
            seen = set()
            occ_text = ""
            for row in bindings:
                if row.get("fatherLabel") and not d["father"]:
                    fn = row["fatherLabel"]["value"]
                    if not fn.startswith("Q") and len(fn) > 1:
                        d["father"] = {"name": fn, "confidence": 92.0,
                            "wikidata_id": row["father"]["value"].split("/")[-1] if row.get("father") else None}
                if row.get("motherLabel") and not d["mother"]:
                    mn = row["motherLabel"]["value"]
                    if not mn.startswith("Q") and len(mn) > 1:
                        d["mother"] = {"name": mn, "confidence": 90.0,
                            "wikidata_id": row["mother"]["value"].split("/")[-1] if row.get("mother") else None}
                if row.get("childLabel"):
                    cn = row["childLabel"]["value"]
                    if cn not in seen and not cn.startswith("Q") and len(cn) > 1:
                        seen.add(cn)
                        d["children"].append({"name": cn, "confidence": 88.0,
                            "wikidata_id": row["child"]["value"].split("/")[-1] if row.get("child") else None})
                if row.get("birthYear") and not d["birth_year"]:
                    try: d["birth_year"] = int(row["birthYear"]["value"])
                    except: pass
                if row.get("occupationLabel"):
                    occ_text += " " + row["occupationLabel"]["value"]
            d["auto_categories"] = _detect_cats(occ_text)
            return d
        except Exception as e:
            logger.error(f"Wikidata SPARQL error {qid}: {e}")
            return None

    async def _fetch_dbpedia(self, name) -> Optional[Dict]:
        resource = name.replace(" ", "_")
        query = f"""
        PREFIX dbo: <http://dbpedia.org/ontology/>
        PREFIX dbr: <http://dbpedia.org/resource/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?fatherName ?motherName ?childName ?birthYear WHERE {{
          OPTIONAL {{ dbr:{resource} dbo:father ?f. ?f rdfs:label ?fatherName. FILTER(LANG(?fatherName)='en') }}
          OPTIONAL {{ dbr:{resource} dbo:mother ?m. ?m rdfs:label ?motherName. FILTER(LANG(?motherName)='en') }}
          OPTIONAL {{ ?c dbo:father dbr:{resource}. ?c rdfs:label ?childName. FILTER(LANG(?childName)='en') }}
          OPTIONAL {{ dbr:{resource} dbo:birthYear ?birthYear. }}
        }} LIMIT 40"""
        try:
            r = await self.client.get(DBPEDIA_SPARQL,
                params={"query": query, "format": "application/sparql-results+json"},
                headers={"Accept": "application/sparql-results+json"})
            if not await self.rate_check(r):
                return None
            if r.status_code != 200:
                return None
            bindings = r.json().get("results", {}).get("bindings", [])
            d = {"father": None, "mother": None, "children": [], "birth_year": None,
                 "source_url": f"https://dbpedia.org/resource/{resource}", "auto_categories": []}
            seen = set()
            for row in bindings:
                if row.get("fatherName") and not d["father"]:
                    fn = row["fatherName"]["value"]
                    if len(fn) > 1: d["father"] = {"name": fn, "confidence": 85.0}
                if row.get("motherName") and not d["mother"]:
                    mn = row["motherName"]["value"]
                    if len(mn) > 1: d["mother"] = {"name": mn, "confidence": 83.0}
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
            logger.error(f"DBpedia error {name}: {e}")
            return None

    async def _fetch_wikipedia(self, name) -> Optional[Dict]:
        try:
            r = await self.client.get(WIKIPEDIA_API, params={
                "action": "query", "titles": name,
                "prop": "revisions|categories",
                "rvprop": "content", "rvslots": "main",
                "format": "json", "formatversion": "2", "cllimit": "30"})
            if not await self.rate_check(r):
                return None
            pages = r.json().get("query", {}).get("pages", [])
            if not pages or pages[0].get("missing"):
                return None
            page = pages[0]
            content = page.get("revisions",[{}])[0].get("slots",{}).get("main",{}).get("content","")
            cats = [c["title"] for c in page.get("categories", [])]
            father = _extract_infobox(content, ["father","Father"])
            mother = _extract_infobox(content, ["mother","Mother"])
            children = _extract_children_infobox(content)
            if not father and not mother and not children:
                return None
            slug = page.get("title","").replace(" ","_")
            return {"father": father, "mother": mother, "children": children,
                    "source_url": f"https://en.wikipedia.org/wiki/{slug}",
                    "wikidata_id": None, "birth_year": None,
                    "auto_categories": _detect_cats(content + " ".join(cats))}
        except Exception as e:
            logger.error(f"Wikipedia error {name}: {e}")
            return None

    # ── SAVE RESULTS ─────────────────────────────────────────────────────

    async def _save(self, person, d):
        pid = person["id"]
        if d.get("wikidata_id"):
            await self.db("UPDATE persons SET wikidata_id=:w WHERE id=:id",
                          {"w": d["wikidata_id"], "id": pid}, "none")
        if d.get("birth_year"):
            await self.db("UPDATE persons SET approx_birth_year=:y WHERE id=:id",
                          {"y": d["birth_year"], "id": pid}, "none")
        if d.get("father"):
            await self._link_parent(pid, d["father"], "father", d.get("source_url"))
        if d.get("mother"):
            await self._link_parent(pid, d["mother"], "mother", d.get("source_url"))
        for child in d.get("children", []):
            await self._link_child(pid, child, d.get("source_url"))
        await self.log(pid, person["name"], "researched",
                       f"f={d.get('father',{}).get('name','-') if d.get('father') else '-'} "
                       f"m={d.get('mother',{}).get('name','-') if d.get('mother') else '-'} "
                       f"c={len(d.get('children',[]))}")

    async def _link_parent(self, child_id, parent_data, parent_type, source_url):
        parent_id = await self._find_or_create(parent_data["name"], parent_data.get("wikidata_id"))
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
                row = await self.db("""INSERT INTO relationships
                    (child_id,parent_id,parent_type,confidence,is_primary,is_branch)
                    VALUES (:c,:p,:t,:conf,:prim,:branch) RETURNING id""",
                    {"c": child_id, "p": parent_id, "t": parent_type,
                     "conf": parent_data["confidence"],
                     "prim": is_primary, "branch": not is_primary}, "one")
                rel_id = row["id"] if row else None
            if source_url and rel_id:
                src_exists = (await self.db(
                    "SELECT COUNT(*) as c FROM sources WHERE relationship_id=:r AND url=:u",
                    {"r": rel_id, "u": source_url}, "one") or {}).get("c", 0)
                if src_exists == 0:
                    src_type = ("wikidata" if "wikidata" in source_url else
                                "dbpedia" if "dbpedia" in source_url else "wikipedia")
                    await self.db("INSERT INTO sources (relationship_id,url,source_type) VALUES (:r,:u,:t)",
                                  {"r": rel_id, "u": source_url, "t": src_type}, "none")
            # Dissolve genesis if parent found with high confidence
            if parent_data["confidence"] >= 95.0:
                child = await self.db("SELECT is_genesis,genesis_code FROM persons WHERE id=:id",
                                      {"id": child_id}, "one")
                if child and child["is_genesis"]:
                    await self.db("UPDATE persons SET is_genesis=FALSE,genesis_code=NULL WHERE id=:id",
                                  {"id": child_id}, "none")
                    await self.db("""INSERT INTO merge_log
                        (genesis_person_id,genesis_code,merged_into_person_id,confidence_at_merge)
                        VALUES (:gp,:gc,:mi,:conf)""",
                        {"gp": child_id, "gc": child["genesis_code"],
                         "mi": parent_id, "conf": parent_data["confidence"]}, "none")
                    logger.info(f"🔗 MERGE: person {child_id} genesis dissolved")
            # Queue parent for research
            q = await self.db(
                "SELECT COUNT(*) as c FROM agent_queue WHERE person_id=:p AND status='pending'",
                {"p": parent_id}, "one")
            if (q or {}).get("c", 0) == 0:
                await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',75)",
                              {"p": parent_id}, "none")
        except Exception as e:
            logger.warning(f"link_parent error: {e}")

    async def _link_child(self, parent_id, child_data, source_url):
        child_id = await self._find_or_create(child_data["name"], child_data.get("wikidata_id"))
        if not child_id or child_id == parent_id:
            return
        try:
            existing = await self.db(
                "SELECT id FROM relationships WHERE child_id=:c AND parent_id=:p AND parent_type='father'",
                {"c": child_id, "p": parent_id}, "one")
            if not existing:
                row = await self.db("""INSERT INTO relationships
                    (child_id,parent_id,parent_type,confidence,is_primary)
                    VALUES (:c,:p,'father',:conf,TRUE) RETURNING id""",
                    {"c": child_id, "p": parent_id, "conf": child_data["confidence"]}, "one")
                rel_id = row["id"] if row else None
                if source_url and rel_id:
                    await self.db("INSERT INTO sources (relationship_id,url,source_type) VALUES (:r,:u,'wikipedia')",
                                  {"r": rel_id, "u": source_url}, "none")
            q = await self.db(
                "SELECT COUNT(*) as c FROM agent_queue WHERE person_id=:p AND status='pending'",
                {"p": child_id}, "one")
            if (q or {}).get("c", 0) == 0:
                await self.db("INSERT INTO agent_queue (person_id,direction,priority) VALUES (:p,'both',55)",
                              {"p": child_id}, "none")
        except Exception as e:
            logger.warning(f"link_child error: {e}")

    async def _find_or_create(self, name, wikidata_id=None):
        if wikidata_id:
            row = await self.db("SELECT id FROM persons WHERE wikidata_id=:w", {"w": wikidata_id}, "one")
            if row: return row["id"]
        row = await self.db("SELECT id FROM persons WHERE LOWER(name)=LOWER(:n) LIMIT 1", {"n": name}, "one")
        if row:
            if wikidata_id:
                await self.db("UPDATE persons SET wikidata_id=:w WHERE id=:id",
                              {"w": wikidata_id, "id": row["id"]}, "none")
            return row["id"]
        try:
            new = await self.db(
                "INSERT INTO persons (name,wikidata_id,is_genesis,genesis_code,type) VALUES (:n,:w,FALSE,NULL,'human') RETURNING id",
                {"n": name, "w": wikidata_id}, "one")
            return new["id"] if new else None
        except Exception as e:
            logger.warning(f"find_or_create error: {e}")
            return None


# ── SHARED HELPERS (module-level, used by both agents) ───────────────────────

CATEGORY_KEYWORDS = {
    "Greek Gods":        ["greek god", "olympian", "greek deity", "greek mytholog"],
    "Norse Gods":        ["norse god", "asgard", "norse mytholog"],
    "Hindu Deities":     ["hindu god", "hinduism", "vedic god"],
    "Egyptian Gods":     ["egyptian god", "egyptian deity"],
    "Roman Gods":        ["roman god", "roman mytholog"],
    "Mesopotamian Gods": ["sumerian god", "babylonian god", "akkadian"],
    "Celtic Gods":       ["celtic god", "celtic deity"],
    "Aztec Gods":        ["aztec god", "aztec deity", "mesoamerican"],
    "Egyptian Pharaohs": ["pharaoh", "king of egypt"],
    "Roman Emperors":    ["roman emperor", "emperor of rome"],
    "Greek Kings":       ["king of macedon", "king of sparta", "king of greece"],
    "Sumerian Kings":    ["king of sumer", "king of ur"],
    "Persian Kings":     ["king of persia", "achaemenid", "sassanid"],
    "Biblical Figures":  ["biblical", "old testament", "new testament", "patriarch"],
    "Quranic Figures":   ["quran", "islamic prophet"],
    "Vedic Figures":     ["vedic", "rigveda"],
    "British Royals":    ["king of england", "queen of england", "house of windsor"],
    "Mughal Dynasty":    ["mughal emperor", "mughal dynasty"],
    "Mongol Khans":      ["mongol khan", "golden horde"],
    "Ottoman Dynasty":   ["ottoman sultan"],
    "Americans":         ["american president", "united states president"],
    "South Asians":      ["prime minister of india", "maharaja"],
}

def _detect_cats(text):
    tl = text.lower()
    return [cat for cat, kws in CATEGORY_KEYWORDS.items() if any(k in tl for k in kws)]

def _merge(target, source, offset=0):
    if source.get("father"):
        f = dict(source["father"])
        f["confidence"] = min(99.0, f.get("confidence", 70) + offset)
        if not target["father"] or f["confidence"] > target["father"]["confidence"]:
            target["father"] = f
    if source.get("mother"):
        m = dict(source["mother"])
        m["confidence"] = min(99.0, m.get("confidence", 70) + offset)
        if not target["mother"] or m["confidence"] > target["mother"]["confidence"]:
            target["mother"] = m
    existing = {c["name"].lower() for c in target["children"]}
    for child in source.get("children", []):
        c = dict(child)
        c["confidence"] = min(99.0, c.get("confidence", 65) + offset)
        if c["name"].lower() not in existing:
            target["children"].append(c)
            existing.add(c["name"].lower())
    if source.get("birth_year") and not target["birth_year"]:
        target["birth_year"] = source["birth_year"]
    if source.get("source_url") and not target["source_url"]:
        target["source_url"] = source["source_url"]
    if source.get("wikidata_id") and not target.get("wikidata_id"):
        target["wikidata_id"] = source["wikidata_id"]
    for cat in source.get("auto_categories", []):
        if cat not in target["auto_categories"]:
            target["auto_categories"].append(cat)

def _extract_infobox(content, fields):
    for f in fields:
        m = re.search(rf"\|\s*{f}\s*=\s*([^\|\n\}}]+)", content, re.IGNORECASE)
        if m:
            raw = re.sub(r'\[\[([^\|\]]+)(?:\|[^\]]+)?\]\]', r'\1', m.group(1))
            raw = re.sub(r'\{\{[^\}]+\}\}|<[^>]+>', '', raw).strip()
            if raw and 1 < len(raw) < 200:
                return {"name": raw, "confidence": 72.0}
    return None

def _extract_children_infobox(content):
    children = []
    m = re.search(r"\|\s*(?:children|issue)\s*=\s*((?:[^\|]|\n(?!\|))+)", content, re.IGNORECASE)
    if m:
        for n in re.findall(r'\[\[([^\|\]]+)(?:\|[^\]]+)?\]\]', m.group(1))[:10]:
            n = n.strip()
            if n and len(n) > 1:
                children.append({"name": n, "confidence": 68.0})
    return children

"""
AGENT 2: Category & Enrichment Agent
Fixes existing nodes that have wrong/missing categories and genesis flags.
Also corrects the 958 already-researched persons.
Runs slower (15s per person) so it doesn't compete with research agent.
"""
import asyncio
import logging
from .base import BaseAgent
from .research_agent import CATEGORY_KEYWORDS, _detect_cats

logger = logging.getLogger("ashvattha.category")

WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"

# Hard-coded correct categories for well-known mythological figures
# that Wikipedia/Wikidata descriptions may not explicitly state
KNOWN_CATEGORIES = {
    # Greek Gods
    "zeus":       ["Greek Gods", "Mythological"],
    "hera":       ["Greek Gods", "Mythological"],
    "poseidon":   ["Greek Gods", "Mythological"],
    "athena":     ["Greek Gods", "Mythological"],
    "apollo":     ["Greek Gods", "Mythological"],
    "artemis":    ["Greek Gods", "Mythological"],
    "ares":       ["Greek Gods", "Mythological"],
    "aphrodite":  ["Greek Gods", "Mythological"],
    "hermes":     ["Greek Gods", "Mythological"],
    "hephaestus": ["Greek Gods", "Mythological"],
    "demeter":    ["Greek Gods", "Mythological"],
    "dionysus":   ["Greek Gods", "Mythological"],
    "heracles":   ["Greek Gods", "Mythological"],
    "achilles":   ["Greek Kings", "Mythological"],
    "odysseus":   ["Greek Kings", "Mythological"],
    "cronus":     ["Greek Gods", "Mythological"],
    "uranus":     ["Greek Gods", "Mythological"],
    "gaia":       ["Greek Gods", "Mythological"],
    "prometheus": ["Greek Gods", "Mythological"],
    "perseus":    ["Greek Gods", "Mythological"],
    "theseus":    ["Greek Gods", "Mythological"],
    "aeneas":     ["Greek Gods", "Mythological"],
    # Norse Gods
    "odin":       ["Norse Gods", "Mythological"],
    "thor":       ["Norse Gods", "Mythological"],
    "loki":       ["Norse Gods", "Mythological"],
    "freyr":      ["Norse Gods", "Mythological"],
    "freyja":     ["Norse Gods", "Mythological"],
    "baldr":      ["Norse Gods", "Mythological"],
    "tyr":        ["Norse Gods", "Mythological"],
    "heimdall":   ["Norse Gods", "Mythological"],
    # Hindu
    "brahma":     ["Hindu Deities", "Mythological"],
    "vishnu":     ["Hindu Deities", "Mythological"],
    "shiva":      ["Hindu Deities", "Mythological"],
    "krishna":    ["Hindu Deities", "Mythological"],
    "rama":       ["Hindu Deities", "Mythological"],
    "ganesha":    ["Hindu Deities", "Mythological"],
    "hanuman":    ["Hindu Deities", "Mythological"],
    "lakshmi":    ["Hindu Deities", "Mythological"],
    "saraswati":  ["Hindu Deities", "Mythological"],
    # Egyptian
    "osiris":     ["Egyptian Gods", "Mythological"],
    "isis":       ["Egyptian Gods", "Mythological"],
    "ra":         ["Egyptian Gods", "Mythological"],
    "horus":      ["Egyptian Gods", "Mythological"],
    "set":        ["Egyptian Gods", "Mythological"],
    "anubis":     ["Egyptian Gods", "Mythological"],
    "thoth":      ["Egyptian Gods", "Mythological"],
    "hathor":     ["Egyptian Gods", "Mythological"],
    # Biblical
    "adam":       ["Biblical Figures", "Religion & Scripture"],
    "eve":        ["Biblical Figures", "Religion & Scripture"],
    "noah":       ["Biblical Figures", "Religion & Scripture"],
    "abraham":    ["Biblical Figures", "Religion & Scripture"],
    "isaac":      ["Biblical Figures", "Religion & Scripture"],
    "jacob":      ["Biblical Figures", "Religion & Scripture"],
    "moses":      ["Biblical Figures", "Religion & Scripture"],
    "david":      ["Biblical Figures", "Religion & Scripture"],
    "solomon":    ["Biblical Figures", "Religion & Scripture"],
    "joseph":     ["Biblical Figures", "Religion & Scripture"],
    "jesus":      ["Biblical Figures", "Religion & Scripture"],
    # Islamic
    "muhammad":   ["Quranic Figures", "Religion & Scripture"],
    "ali":        ["Quranic Figures", "Religion & Scripture"],
    # Pharaohs
    "ramesses":   ["Egyptian Pharaohs", "Ancient"],
    "tutankhamun":["Egyptian Pharaohs", "Ancient"],
    "cleopatra":  ["Egyptian Pharaohs", "Ancient"],
    "akhenaten":  ["Egyptian Pharaohs", "Ancient"],
    # Romans
    "julius caesar":  ["Roman Emperors", "Ancient"],
    "augustus":       ["Roman Emperors", "Ancient"],
    "nero":           ["Roman Emperors", "Ancient"],
    "marcus aurelius":["Roman Emperors", "Ancient"],
    "charlemagne":    ["Medieval", "Royalty & Dynasties"],
    # Mongols
    "genghis khan":   ["Mongol Khans", "Royalty & Dynasties"],
    "kublai khan":    ["Mongol Khans", "Royalty & Dynasties"],
    "timur":          ["Royalty & Dynasties", "Medieval"],
    # Mughals
    "akbar":   ["Mughal Dynasty", "Royalty & Dynasties"],
    "babur":   ["Mughal Dynasty", "Royalty & Dynasties"],
    # Others
    "alexander the great": ["Greek Kings", "Ancient"],
    "ashoka":              ["South Asians", "Ancient"],
    "napoleon":            ["Europeans", "Modern"],
}

class CategoryAgent(BaseAgent):
    name = "category"
    BATCH_SIZE = 50   # persons to fix per cycle
    TICK_DELAY = 15   # seconds — slower than research agent

    async def tick(self):
        """
        Two jobs per tick:
        1. Fix persons with no categories assigned
        2. Fix persons wrongly marked as genesis who actually have a father
        """
        fixed = 0
        fixed += await self._fix_missing_categories()
        fixed += await self._fix_wrong_genesis()
        if fixed == 0:
            # Nothing to fix — sleep longer
            await asyncio.sleep(60)
        else:
            await asyncio.sleep(self.TICK_DELAY)

    # ── JOB 1: FIX MISSING CATEGORIES ────────────────────────────────────

    async def _fix_missing_categories(self) -> int:
        """Find persons with no categories and assign them."""
        # Get persons that have NO category assignments yet
        persons = await self.db("""
            SELECT p.id, p.name, p.type, p.wikidata_id
            FROM persons p
            WHERE p.type != 'genesis'
            AND NOT EXISTS (
                SELECT 1 FROM person_categories pc WHERE pc.person_id = p.id
            )
            ORDER BY p.id ASC
            LIMIT :batch
        """, {"batch": self.BATCH_SIZE}, fetch="all")

        if not persons:
            return 0

        fixed = 0
        for person in persons:
            cats = await self._determine_categories(person)
            for cat_name in cats:
                await self._assign_category(person["id"], cat_name)
                fixed += 1

        if fixed > 0:
            logger.info(f"🏷️  Fixed categories for {len(persons)} persons ({fixed} assignments)")
        return fixed

    async def _determine_categories(self, person) -> list:
        """Figure out what categories a person belongs to."""
        name = person["name"]
        name_lower = name.lower()
        cats = []

        # 1. Check hard-coded known categories first
        for key, known_cats in KNOWN_CATEGORIES.items():
            if key in name_lower:
                cats.extend(known_cats)
                break

        if cats:
            return list(set(cats))

        # 2. Check by type
        if person["type"] == "mythological":
            cats.append("Mythological")

        # 3. Fetch Wikipedia categories for this person
        try:
            r = await self.client.get(WIKIPEDIA_API, params={
                "action": "query", "titles": name,
                "prop": "categories|revisions",
                "rvprop": "content", "rvslots": "main",
                "format": "json", "formatversion": "2",
                "cllimit": "30", "clshow": "!hidden"
            })
            if r.status_code == 200:
                pages = r.json().get("query", {}).get("pages", [])
                if pages and not pages[0].get("missing"):
                    page = pages[0]
                    wiki_cats = [c["title"] for c in page.get("categories", [])]
                    content = page.get("revisions", [{}])[0].get("slots", {}).get("main", {}).get("content", "")
                    detected = _detect_cats(content + " ".join(wiki_cats))
                    cats.extend(detected)
        except Exception as e:
            logger.warning(f"Wikipedia category fetch failed for {name}: {e}")

        return list(set(cats))

    # ── JOB 2: FIX WRONG GENESIS FLAGS ───────────────────────────────────

    async def _fix_wrong_genesis(self) -> int:
        """
        Find persons marked is_genesis=TRUE who actually DO have a parent.
        These were wrongly flagged — remove their genesis status.
        """
        wrong_genesis = await self.db("""
            SELECT p.id, p.name, p.genesis_code
            FROM persons p
            WHERE p.is_genesis = TRUE
            AND p.type != 'genesis'
            AND EXISTS (
                SELECT 1 FROM relationships r
                WHERE r.child_id = p.id AND r.parent_type = 'father'
            )
            LIMIT :batch
        """, {"batch": 20}, fetch="all")

        if not wrong_genesis:
            return 0

        for person in wrong_genesis:
            await self.db(
                "UPDATE persons SET is_genesis=FALSE, genesis_code=NULL WHERE id=:id",
                {"id": person["id"]}, "none")
            logger.info(f"🔧 Fixed genesis flag: {person['name']} (had father, was wrongly G)")
            await self.log(person["id"], person["name"], "fixed",
                          f"Removed incorrect genesis flag {person['genesis_code']}")

        return len(wrong_genesis)

    # ── HELPER ────────────────────────────────────────────────────────────

    async def _assign_category(self, person_id: int, cat_name: str):
        cat = await self.db("SELECT id FROM categories WHERE name=:n", {"n": cat_name}, "one")
        if not cat:
            return
        existing = (await self.db(
            "SELECT COUNT(*) as c FROM person_categories WHERE person_id=:p AND category_id=:c",
            {"p": person_id, "c": cat["id"]}, "one") or {}).get("c", 0)
        if existing == 0:
            try:
                await self.db("INSERT INTO person_categories (person_id,category_id) VALUES (:p,:c)",
                              {"p": person_id, "c": cat["id"]}, "none")
            except Exception as e:
                logger.warning(f"Category insert error: {e}")

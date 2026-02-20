"""
AGENT 3: Enrichment Agent
Adds detail to existing nodes — birth/death years, descriptions,
Wikipedia slugs, era classification, gender correction.
Runs slowest (30s per person) — purely additive, never changes relationships.
"""
import asyncio
import logging
from .base import BaseAgent

logger = logging.getLogger("ashvattha.enrichment")

WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIDATA_API  = "https://www.wikidata.org/w/api.php"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"

class EnrichmentAgent(BaseAgent):
    name = "enrichment"
    TICK_DELAY = 30

    async def tick(self):
        """Pick a person missing details and fill them in."""
        # Find persons missing birth year OR description OR era
        person = await self.db("""
            SELECT id, name, type, wikidata_id, approx_birth_year, era, gender
            FROM persons
            WHERE type != 'genesis'
            AND (
                approx_birth_year IS NULL
                OR era IS NULL
                OR gender IS NULL
                OR gender = 'unknown'
                OR wikipedia_slug IS NULL
            )
            ORDER BY id ASC
            LIMIT 1
        """, fetch="one")

        if not person:
            await asyncio.sleep(60)
            return

        await self._enrich(person)
        await asyncio.sleep(self.TICK_DELAY)

    async def _enrich(self, person):
        pid = person["id"]
        name = person["name"]
        updates = {}

        try:
            # Get full Wikidata entity data
            qid = person.get("wikidata_id")
            if not qid:
                qid = await self._get_wikidata_id(name)
                if qid:
                    updates["wikidata_id"] = qid

            if qid:
                data = await self._fetch_wikidata_details(qid)
                if data:
                    if data.get("birth_year") and not person["approx_birth_year"]:
                        updates["approx_birth_year"] = data["birth_year"]
                    if data.get("death_year"):
                        updates["approx_death_year"] = data["death_year"]
                    if data.get("gender") and (not person["gender"] or person["gender"] == "unknown"):
                        updates["gender"] = data["gender"]
                    if data.get("wikipedia_slug"):
                        updates["wikipedia_slug"] = data["wikipedia_slug"]

            # Determine era from birth year
            if "approx_birth_year" in updates or person["approx_birth_year"]:
                year = updates.get("approx_birth_year") or person["approx_birth_year"]
                updates["era"] = self._classify_era(year, person["type"])

            # Fallback: classify era from type
            if not updates.get("era") and not person["era"]:
                if person["type"] == "mythological":
                    updates["era"] = "Mythological"

            if updates:
                set_parts = ", ".join(f"{k}=:{k}" for k in updates)
                updates["id"] = pid
                await self.db(f"UPDATE persons SET {set_parts} WHERE id=:id", updates, "none")
                logger.info(f"📝 Enriched {name}: {list(k for k in updates if k != 'id')}")

        except Exception as e:
            logger.warning(f"Enrichment failed for {name}: {e}")

    async def _get_wikidata_id(self, name) -> str | None:
        try:
            r = await self.client.get(WIKIDATA_API, params={
                "action": "wbsearchentities", "search": name,
                "language": "en", "type": "item", "format": "json", "limit": 3})
            if not await self.rate_check(r):
                return None
            results = r.json().get("search", [])
            for res in results:
                desc = res.get("description", "").lower()
                if any(k in desc for k in ["human","person","deity","god","king","emperor","queen","prophet"]):
                    return res["id"]
            return results[0]["id"] if results else None
        except:
            return None

    async def _fetch_wikidata_details(self, qid) -> dict | None:
        query = f"""
        SELECT ?birthYear ?deathYear ?genderLabel ?article WHERE {{
          OPTIONAL {{ wd:{qid} wdt:P569 ?dob. BIND(YEAR(?dob) AS ?birthYear) }}
          OPTIONAL {{ wd:{qid} wdt:P570 ?dod. BIND(YEAR(?dod) AS ?deathYear) }}
          OPTIONAL {{ wd:{qid} wdt:P21 ?gender. }}
          OPTIONAL {{ ?article schema:about wd:{qid};
                       schema:isPartOf <https://en.wikipedia.org/> . }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }} LIMIT 5"""
        try:
            r = await self.client.get(WIKIDATA_SPARQL,
                params={"query": query, "format": "json"},
                headers={"Accept": "application/json"})
            if not await self.rate_check(r):
                return None
            bindings = r.json().get("results", {}).get("bindings", [])
            d = {}
            for row in bindings:
                if row.get("birthYear") and "birth_year" not in d:
                    try: d["birth_year"] = int(row["birthYear"]["value"])
                    except: pass
                if row.get("deathYear") and "death_year" not in d:
                    try: d["death_year"] = int(row["deathYear"]["value"])
                    except: pass
                if row.get("genderLabel") and "gender" not in d:
                    gl = row["genderLabel"]["value"].lower()
                    if "female" in gl or "woman" in gl: d["gender"] = "female"
                    elif "male" in gl or "man" in gl: d["gender"] = "male"
                    else: d["gender"] = "deity"
                if row.get("article") and "wikipedia_slug" not in d:
                    url = row["article"]["value"]
                    d["wikipedia_slug"] = url.split("/wiki/")[-1] if "/wiki/" in url else None
            return d
        except Exception as e:
            logger.warning(f"Wikidata detail fetch failed {qid}: {e}")
            return None

    def _classify_era(self, year: int, ptype: str) -> str:
        if ptype == "mythological":
            return "Mythological"
        if year is None:
            return "Unknown"
        if year < -3000:
            return "Prehistoric"
        if year < -500:
            return "Ancient"
        if year < 500:
            return "Classical"
        if year < 1400:
            return "Medieval"
        if year < 1800:
            return "Early Modern"
        return "Modern"

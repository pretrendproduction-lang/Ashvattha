# Genesis Tree 🌳
### Universal Human Lineage Graph

> *Connecting gods, ancients, and modern humans through a single unbroken lineage graph — growing forever.*

---

## What This Is

An autonomous, self-growing family tree that connects every recorded human, historical figure, and mythological being into one continuous graph. The AI agent runs 24/7, discovering ancestors and descendants from Wikipedia and Wikidata, scoring confidence on every relationship, and resolving "genesis blocks" as more connections are found.

**Key concepts:**
- **Genesis Blocks** — Every new unconnected person gets a temporary root (G1, G2...). As the agent finds their ancestry, genesis blocks dissolve into the main tree
- **Confidence Scoring** — Every parent-child link has a 0–100% confidence score. Conflicting sources create branches, all shown with their %
- **Auto-merge** — When confidence ≥ 95%, a genesis block is automatically merged into the connected tree
- **Source Links** — Every relationship links back to its Wikipedia/Wikidata source
- **Infinite Growth** — The agent never stops. Every person discovered queues their parents and children for research

---

## Project Structure

```
genesis-tree/
├── schema.sql              ← PostgreSQL schema + seed data
├── render.yaml             ← Render deployment config
├── .env.example            ← Environment template
├── backend/
│   ├── main.py             ← FastAPI app + all API endpoints
│   ├── agent.py            ← Autonomous agent (runs forever)
│   ├── init_db.py          ← One-time DB setup script
│   └── requirements.txt
└── frontend/
    └── index.html          ← Full UI (single file, no build step)
```

---

## Deploy to Render (Free)

### 1. Push to GitHub

```bash
git init
git add .
git commit -m "Initial Genesis Tree"
git remote add origin https://github.com/YOUR_USERNAME/genesis-tree.git
git push -u origin main
```

### 2. Deploy on Render

1. Go to [render.com](https://render.com) and sign up/login
2. Click **New** → **Blueprint**
3. Connect your GitHub repo
4. Render will read `render.yaml` and automatically create:
   - A **web service** (FastAPI app + agent)
   - A **PostgreSQL database** (free tier)
5. Click **Apply**

### 3. Initialize the Database

Once deployed, open the Render shell for your web service and run:

```bash
cd backend && python init_db.py
```

This creates all tables and seeds categories + the G0 genesis root.

### 4. Done

Your app is live. The agent starts automatically and begins growing the tree from 12 seed persons (Zeus, Abraham, Alexander the Great, Genghis Khan, etc.)

---

## Run Locally

```bash
# 1. Create a PostgreSQL database
createdb genesis

# 2. Set up environment
cp .env.example .env
# Edit .env with your database URL

# 3. Install dependencies
cd backend
pip install -r requirements.txt

# 4. Initialize DB
python init_db.py

# 5. Start the server
uvicorn main:app --reload --port 8000
```

Open http://localhost:8000

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/stats` | Graph-wide progress stats |
| GET | `/api/persons/search?q=` | Full-text person search |
| POST | `/api/persons` | Add a new person manually |
| GET | `/api/persons/{id}` | Get person details |
| GET | `/api/persons/{id}/tree?depth=` | Get full family tree |
| POST | `/api/relationships` | Manually add a relationship |
| GET | `/api/categories` | All browsable categories |
| GET | `/api/categories/{id}/persons` | Persons in a category |
| GET | `/api/activity` | Agent activity log |

---

## How the Agent Works

1. **Seeds** 12 highly-connected historical figures on first run
2. **Pulls** the highest-priority job from the queue
3. **Looks up** the person on Wikidata (structured) → fallback to Wikipedia (text parsing)
4. **Extracts** father, mother, children with confidence scores
5. **Writes** relationships + source URLs to DB
6. **Queues** every discovered person for their own research
7. **Checks** if any genesis block can be merged (confidence ≥ 95%)
8. **Repeats** — forever, with a 2-second polite delay between requests

The graph grows exponentially: 12 seeds → ~60 parents found → ~300 grandparents → ...

---

## Data Sources (all free, no API key needed)

| Source | What it provides |
|--------|-----------------|
| [Wikidata SPARQL](https://query.wikidata.org) | Structured father/mother/children relationships |
| [Wikidata API](https://www.wikidata.org/w/api.php) | Person lookup by name |
| [Wikipedia API](https://en.wikipedia.org/w/api.php) | Infobox parsing for family data |

---

## The Ultimate Goal

> Every genesis block in the system eventually connects to a single root — or is identified as its own named branch of humanity.

**Progress metric:** The number of open genesis blocks, going toward zero over time.

The system never fully reaches zero — but every merge is a step toward the most complete picture of human lineage ever assembled.

---

*Built with FastAPI · PostgreSQL · Wikipedia/Wikidata APIs · No paid services required*

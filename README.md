# ExileSage

A Path of Exile 2 AI advisor. Ask it anything — crafting, items, currencies, builds — and it answers from real game data, not guesswork.

---

## What Stage 1 built

Stage 1 is a working local advisor: a structured game data pipeline feeding a tool-use AI that answers PoE2 questions grounded in actual mod, item, and currency data.

### The pipeline

```
RePoE (community game data)
        ↓
scraper/repoe.py        — fetches raw JSON from repoe-fork.github.io/poe2
scraper/processor.py    — cleans and normalises the raw data
        ↓
data/processed/         — 4 clean JSON files (mods, base_items, currencies, augments)
        ↓
pipeline/ingest.py      — validates with Pydantic, inserts into SQLite with FTS5 indexes
        ↓
data/exilesage.db       — 18,645 rows across 4 tables, fully indexed and searchable
```

### The advisor

```
exilesage ask "question"
        ↓
classify_query()        — Haiku classifies: factual | crafting | analysis | guide | innovation
        ↓
MODEL_MAP               — selects cheapest model that fits (Haiku → Sonnet → Opus)
        ↓
agentic loop            — Claude calls tools as needed, up to 8 iterations
    ↓ search_mods()         — FTS5 + stat_id search over 14,840 mods
    ↓ search_base_items()   — FTS5 search over 2,902 item bases
    ↓ search_currencies()   — FTS5 search over 780 currencies
    ↓ search_augments()     — FTS5 search over 123 augments (Soul Cores, Runes, Talismans)
        ↓
answer grounded in real game data
```

---

## Data

| Table | Rows | What it contains |
|---|---|---|
| `mods` | 14,840 | All explicit/implicit modifiers — value ranges, spawn weights, domains |
| `base_items` | 2,902 | All item bases — defence values, requirements, item class |
| `currencies` | 780 | All stackable currencies — descriptions, stack sizes |
| `augments` | 123 | Soul Cores, Runes, Talismans — slot bonuses per equipment type |

All text columns are indexed with SQLite FTS5 for fast natural language search.

---

## Setup

### Prerequisites

- Python 3.11+
- An Anthropic API key with credits — get one at [console.anthropic.com](https://console.anthropic.com)

### Install

```bash
pip install -e ".[dev]"
```

### Configure

Create a `.env` file in the project root:

```
ANTHROPIC_API_KEY=sk-ant-...
```

### Load the database

```bash
# Fetch latest game data from RePoE and load into SQLite
exilesage update

# Or if you already have data/processed/ files:
exilesage ingest
```

---

## Usage

### Ask a question

```bash
exilesage ask "What does a Chaos Orb do?"
exilesage ask "What mods can spawn on a wand?"
exilesage ask "How do I craft a +1 to all spell skills wand?"
exilesage ask "What is the best base for an armour evasion hybrid chest?"
exilesage ask "How do essences work in crafting?"
```

### Force a query type

```bash
exilesage ask --type crafting "How do I brick-proof a wand craft?"
exilesage ask --type innovation "Design a novel cold damage build"
```

Query types and their models:

| Type | Model | When used |
|---|---|---|
| `factual` | Haiku (fast, cheap) | "What does X do", "How much does Y cost" |
| `crafting` | Sonnet | "How do I craft X", step-by-step crafting |
| `analysis` | Sonnet | "What's best for X", comparisons |
| `guide` | Sonnet | "How do I play X build" |
| `innovation` | Opus | "Design a novel build", synergy exploration |

### Verbose mode (shows tool calls)

```bash
exilesage ask --verbose "What mods can spawn on a wand?"
```

### Refresh data

```bash
exilesage update    # fetch fresh RePoE data + re-ingest
exilesage ingest    # re-ingest existing processed files only
```

---

## Architecture

```
exilesage/
├── config.py           MODEL_MAP, QueryType, DB_PATH, constants
├── db.py               SQLite connection (WAL mode, FTS5)
├── tools/
│   ├── mods.py         search_mods(query, domain, generation_type, tag, stat_id)
│   ├── items.py        search_base_items(query, item_class, domain, level_range)
│   ├── currencies.py   search_currencies(query)
│   └── augments.py     search_augments(query, slot)
├── advisor/
│   ├── system_prompt.py   PoE2 domain knowledge baked into Claude's context
│   ├── tool_defs.py       Anthropic API tool schemas
│   └── core.py            classify_query() + agentic loop ask()
└── cli/
    └── app.py             typer CLI: ask, ingest, update

pipeline/
├── update.py           fetch raw data from RePoE
├── ingest.py           orchestrate all 4 importers
└── importers/
    ├── mods_importer.py
    ├── base_items_importer.py
    ├── currencies_importer.py
    └── augments_importer.py

data/
├── raw/                raw RePoE JSON (cached)
├── processed/          cleaned JSON (source for ingest)
├── db/schema.sql       SQLite schema definition
└── exilesage.db        live database

.claude/
├── settings.json       session model (Opus), tool permissions, hooks
├── rules/              agent model assignments, data layer + advisor conventions
└── hooks/              safe-bash guard, auto-lint, test gate on stop
```

---

## Key design decisions

**LLM-first, not rule-based.** PoE2 has tens of thousands of mods with implicit synergies that can't be precomputed. Claude queries the data dynamically and reasons about combinations rather than looking up a static graph.

**SQLite + FTS5, not embeddings.** Full-text search over game data is fast, free, and requires no external services. Embedding-based RAG is reserved for Stage 2 when unstructured community content (guides, wiki) is added.

**Model routing.** Not every query needs the most capable model. A `factual` question runs on Haiku in ~1 second. An `innovation` request for a novel build synthesis uses Opus with full context. This keeps costs low without sacrificing quality where it matters.

**Pydantic as single source of truth.** All data validation at import time. If RePoE changes a field shape, Pydantic logs a warning and skips the row — the rest of the database remains intact.

---

## What Stage 2 will add

- PoE2 wiki scraper — passive tree, ascendancies, skills, keystones
- Community guide ingestion — maxroll, poe2db indexed with embeddings
- RAG layer — semantic search over unstructured guide content
- Freshness tracking — patch version detection, stale content flagging
- Innovative build synthesis — gap analysis between synergy graph and known builds

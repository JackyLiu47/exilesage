# ExileSage

PoE2 AI advisor. LLM-first: Claude reasons over structured game data via tool calls.
No static synergy graphs. No precomputed rules. Data lives in SQLite.

## Architecture

```
User query → [Haiku router] → [Haiku/Sonnet/Opus advisor] → tool calls → answer
                                                              ↓
                                                        exilesage.db (SQLite)
                                                        + community RAG (S2+)
```

## Directory map

```
exilesage/          main package
  db.py             SQLite connection + schema creation
  config.py         MODEL_MAP, QueryType, constants
  tools/            search_mods, search_base_items, search_currencies, search_augments
  advisor/          system_prompt, tool_defs, core agentic loop
  cli/app.py        typer CLI: ask, ingest, update
pipeline/
  update.py         fetch raw repoe data (existing)
  ingest.py         JSON → SQLite (S1.3)
scraper/            repoe fetcher + processor (existing)
tests/
  benchmark_queries.py   10 fixed acceptance queries
data/
  processed/        source JSON (never query directly after S1.3)
  exilesage.db      SQLite knowledge base
```

## Key decisions

- **SQLite not JSON**: 11MB mods.json too slow per query; FTS5 gives free text search
- **LLM-first**: PoE2 synergies are implicit — Claude reasons, doesn't look up a graph
- **Model routing**: query complexity determines model; see `exilesage/config.py`
- **FTS5 over embeddings**: avoids embedding pipeline until S2+ when community guides added
- **Pydantic validation at import**: each importer defines its own row model; validation failure = skip row, never crash

## Stage tracker

### Stage 1 — complete (2026-04-09)
- [x] S1.1 Rename + CLAUDE.md + config
- [x] S1.2 SQLite schema
- [x] S1.3 JSON → SQLite importers ×4 — 18,645 rows, 0 skipped
- [x] S1.4 Tool function layer — 4 tools, FTS5 + stat_id search
- [x] S1.5 Claude API advisor core — model routing + agentic loop
- [x] S1.6 CLI — exilesage ask / ingest / update
- [x] S1.7 Smoke tests — 5/5 benchmark queries passed

### Stage 2 — pending
- [ ] Wiki + skills scraper (passive tree, ascendancies, gems)
- [ ] Community guide ingestion (maxroll, poe2db)
- [ ] Embedding pipeline + RAG layer
- [ ] Freshness tracking (patch version detection)
- [ ] Innovative build synthesis

## Agent team

5-agent team per session: pipeline-agent (Haiku), tools-agent (Sonnet), advisor-agent (Opus),
tech-lead (Sonnet), devil-advocate (Opus). After each step: tech-lead reviews → devil-advocate
challenges → orchestrator synthesizes. See @.claude/rules/agents.md

## Data layer conventions

See @.claude/rules/data-layer.md

## Advisor conventions

See @.claude/rules/advisor.md

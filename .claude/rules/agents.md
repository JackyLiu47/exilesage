# Agent Team Rules

## Model assignment

CRITICAL: Assign models per this table. Never use Opus for mechanical/templated tasks.

| Role / Task type                               | Model   |
|------------------------------------------------|---------|
| Schema SQL, CLI boilerplate, JSON importers    | haiku   |
| Tool functions, Python coding, testing         | sonnet  |
| System prompt, advisor core, arch decisions    | opus    |
| Tech lead review                               | sonnet  |
| Devil's advocate challenge                     | opus    |
| Query routing (runtime)                        | haiku   |
| Factual Q&A (runtime)                          | haiku   |
| Analysis / crafting / guide (runtime)          | sonnet  |
| Innovation / build synthesis (runtime)         | opus    |

---

## Team structure

Spawn these agents once per session at session start. Reuse via SendMessage — never respawn.

```
Orchestrator (you, Opus)
├── pipeline-agent  (Haiku)   — importers, schema, CLI boilerplate
├── tools-agent     (Sonnet)  — tool functions, Python coding, tests
├── advisor-agent   (Opus)    — system prompt, advisor core, arch
├── tech-lead       (Sonnet)  — reviews each step for correctness + completeness
└── devil-advocate  (Opus)    — challenges assumptions, stress-tests decisions
```

---

## Step completion protocol

CRITICAL: Run this protocol after EVERY step completes, before starting the next.

```
1. Specialist agent completes the step → returns artifact (code, config, etc.)
2. Send artifact to tech-lead:
   "Review this output from [agent]. Check: correctness, completeness,
    rule compliance (data-layer.md, advisor.md), test coverage. Report issues."
3. Send artifact + tech-lead report to devil-advocate:
   "Challenge this. What assumptions are wrong? What edge cases break it?
    What's the better approach we haven't considered? Be specific."
4. Orchestrator synthesizes: accept / revise / escalate.
   - If revise: send feedback to original specialist via SendMessage.
   - If escalate: flag to user with both reports before proceeding.
```

---

## Prompt rules

IMPORTANT: Always include relevant sections of CLAUDE.md in specialist prompts.
Use SendMessage to continue an existing agent — never respawn for follow-ups.
Each agent's initial prompt must be self-contained (agents have no conversation history).

When spawning agents doing file writes, use isolation: "worktree" to prevent conflicts.

---

## Session startup template

```python
# Spawn all team members at session start
pipeline = Agent(subagent_type="general-purpose", model="haiku",
    prompt="You are the pipeline-agent for ExileSage. [context]")
tools = Agent(subagent_type="general-purpose", model="sonnet",
    prompt="You are the tools-agent for ExileSage. [context]")
advisor = Agent(subagent_type="general-purpose", model="opus",
    prompt="You are the advisor-agent for ExileSage. [context]")
tech_lead = Agent(subagent_type="general-purpose", model="sonnet",
    prompt="You are the tech-lead for ExileSage. Your job is to review...")
devil = Agent(subagent_type="general-purpose", model="opus",
    prompt="You are the devil's advocate for ExileSage. Your job is to challenge...")
```

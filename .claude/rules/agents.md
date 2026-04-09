# Subagent Rules

CRITICAL: Assign models per this table. Never use Opus for mechanical/templated tasks.

| Task type                                      | Model   |
|------------------------------------------------|---------|
| Schema SQL, CLI boilerplate, JSON importers    | haiku   |
| Tool functions, Python coding, testing         | sonnet  |
| System prompt, advisor core, arch decisions    | opus    |
| Query routing (runtime)                        | haiku   |
| Factual Q&A (runtime)                          | haiku   |
| Analysis / crafting / guide (runtime)          | sonnet  |
| Innovation / build synthesis (runtime)         | opus    |

IMPORTANT: Always include relevant sections of CLAUDE.md in subagent prompts.
Use SendMessage to continue an existing agent — never respawn for follow-ups.
Each agent prompt must be self-contained (agents have no conversation history).

When spawning import agents (S1.3), use isolation: "worktree" to prevent file conflicts.

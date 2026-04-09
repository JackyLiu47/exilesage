# Advisor Rules

## Model routing

Model routing logic lives in exilesage/config.py MODEL_MAP.
CRITICAL: Never hardcode model name strings outside config.py.
Router model (Haiku) classifies query into QueryType enum, then core.py selects model.

## Agentic loop rules

- Max tool call iterations: MAX_TOOL_ITER = 8 (safety cap, in config.py)
- tool_choice="auto" — let the model decide when to stop calling tools
- Always append tool_result blocks before next API call (Anthropic SDK requirement)
- On iteration cap hit: return partial answer with note that more data may exist

## System prompt rules

IMPORTANT: System prompt lives only in advisor/system_prompt.py — never duplicated elsewhere.
The system prompt must include:
- PoE2 domain context (prefix/suffix/implicit/explicit, crafting currency hierarchy, domains)
- Instruction to always ground answers in tool results
- Instruction to explicitly say "I don't have data on X" rather than hallucinate
- Instruction to cite specific mod/item names from tool results in answers

## Output quality rules

YOU MUST: Advisor answers cite real names from tool results (mod IDs, item names).
NEVER: Invent stat ranges not present in tool results.
IMPORTANT: If a query returns no tool results, say so — do not fill gap with general knowledge alone.

## Query classification (router)

Router prompt must be brief — classify only, no explanation:
Input: user question string
Output: one of: factual | analysis | crafting | guide | innovation

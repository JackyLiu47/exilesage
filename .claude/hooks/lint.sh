#!/usr/bin/env bash
# PostToolUse hook — auto-lint Python files after Write/Edit
INPUT=$(cat)
FILE=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

if [ -z "$FILE" ]; then
  exit 0
fi

if [[ "$FILE" == *.py ]]; then
  ruff check --fix "$FILE" --silent 2>/dev/null || true
fi

exit 0

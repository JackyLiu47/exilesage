#!/usr/bin/env bash
# PreToolUse hook — blocks destructive operations on project data
INPUT=$(cat)
CMD=$(echo "$INPUT" | jq -r '.tool_input.command // empty')

if [ -z "$CMD" ]; then
  exit 0
fi

BLOCKED_PATTERNS=(
  "rm -rf data/"
  "rm -rf exilesage"
  "rm .*\.db"
  "DROP TABLE"
  "DELETE FROM"
  "git push --force"
  "git reset --hard"
)

for pattern in "${BLOCKED_PATTERNS[@]}"; do
  if echo "$CMD" | grep -qiE "$pattern"; then
    echo "Blocked: '$pattern' matched in command. Confirm manually if intentional." >&2
    exit 2
  fi
done

exit 0

#!/usr/bin/env bash
# Stop hook — run tool tests before session ends (only after S1.4 complete)
INPUT=$(cat)

# Prevent infinite loop
if [ "$(echo "$INPUT" | jq -r '.stop_hook_active // false')" = "true" ]; then
  exit 0
fi

# Only enforce once test file and DB exist
if [ ! -f "data/exilesage.db" ] || [ ! -f "tests/test_tools.py" ]; then
  exit 0
fi

python -m pytest tests/test_tools.py -q --tb=short 2>/dev/null
if [ $? -ne 0 ]; then
  echo "Tool tests failing — fix before stopping or check tests/test_tools.py" >&2
  exit 1
fi

exit 0

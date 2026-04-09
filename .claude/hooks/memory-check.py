#!/usr/bin/env python3
"""
Memory health check — runs after every Write/Edit tool call.
Exits silently if the file is not in a memory directory.
Prints structured warnings if memory has grown stale, bloated, or diluted.
"""

import json
import sys
from pathlib import Path


def load_tool_input() -> dict:
    try:
        return json.load(sys.stdin)
    except Exception:
        return {}


def is_memory_file(file_path: str) -> bool:
    return "memory" in file_path.replace("\\", "/").lower()


def check_index(memory_dir: Path) -> list[str]:
    issues = []
    index = memory_dir / "MEMORY.md"
    if not index.exists():
        return issues

    lines = index.read_text(encoding="utf-8").splitlines()
    count = len([l for l in lines if l.strip()])  # non-blank lines

    if count > 160:
        issues.append(
            f"CRITICAL: MEMORY.md has {count} non-blank lines "
            f"(hard truncation at 200). Prune index entries now or future sessions will lose context."
        )
    elif count > 120:
        issues.append(
            f"WARNING: MEMORY.md has {count} lines (approaching 200-line truncation limit). "
            f"Consider consolidating index entries."
        )
    return issues


def check_files(memory_dir: Path) -> list[str]:
    issues = []
    total_words = 0
    large_files = []
    file_count = 0

    for md_file in sorted(memory_dir.glob("*.md")):
        if md_file.name == "MEMORY.md":
            continue
        text = md_file.read_text(encoding="utf-8")
        words = len(text.split())
        lines = len(text.splitlines())
        total_words += words
        file_count += 1

        if lines > 50:
            large_files.append(f"{md_file.name} ({lines} lines, {words} words)")

    if large_files:
        issues.append(
            f"BLOAT: These memory files are large and may bury critical info:\n"
            + "\n".join(f"    - {f}" for f in large_files)
            + "\n  Summarize the body — keep rule, Why, and How to apply tight."
        )

    if total_words > 2000:
        issues.append(
            f"DILUTION: Total memory content is {total_words} words across {file_count} files. "
            f"High-signal lessons risk being drowned. Prune entries that are obvious, "
            f"already in code, or no longer apply."
        )
    elif total_words > 1200:
        issues.append(
            f"NOTICE: Memory is {total_words} words. "
            f"Watch for entries that duplicate what CLAUDE.md already says."
        )

    return issues


def main():
    tool_input = load_tool_input()
    file_path = tool_input.get("file_path", "")

    if not is_memory_file(file_path):
        sys.exit(0)

    memory_dir = Path(file_path).parent

    issues = check_index(memory_dir) + check_files(memory_dir)

    if not issues:
        print("[memory-check] OK — memory index and files within healthy bounds.")
    else:
        print("\n[memory-check] ACTION REQUIRED:")
        for issue in issues:
            print(f"  {issue}")
        print(
            "\n  Next step: Review memory files. Remove entries that are low-value, "
            "obvious, or already captured in CLAUDE.md. Tighten large files to their "
            "core rule + Why + How to apply."
        )
        print()


if __name__ == "__main__":
    main()

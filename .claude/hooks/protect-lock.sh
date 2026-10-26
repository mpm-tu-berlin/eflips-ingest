#!/bin/bash
# PreToolUse(Edit|Write): poetry.lock is only ever changed by Poetry.
f=$(jq -r '.tool_input.file_path // empty')
if [ "$(basename "$f")" = "poetry.lock" ]; then
  echo "Do not edit poetry.lock by hand. Change pyproject.toml and ask the user to run 'poetry lock'." >&2
  exit 2
fi
exit 0

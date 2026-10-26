#!/bin/bash
# PostToolUse(Edit|Write): format the edited Python file with black.
source "$CLAUDE_PROJECT_DIR/.claude/hooks/lib.sh"
f=$(jq -r '.tool_input.file_path // empty')
case "$f" in
  *.py) run_tool black -q "$f" ;;
esac
exit 0

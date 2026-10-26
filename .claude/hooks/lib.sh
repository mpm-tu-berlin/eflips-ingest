# Sourced by the other hooks. Runs a dev tool from the project venv, Poetry or PATH, in that order.
# Returns 127 if the tool is unavailable so callers can skip rather than block.
run_tool() {
  local tool=$1
  shift
  if [ -x "$CLAUDE_PROJECT_DIR/.venv/bin/$tool" ]; then
    "$CLAUDE_PROJECT_DIR/.venv/bin/$tool" "$@"
  elif [ "$tool" != poetry ] && command -v poetry >/dev/null 2>&1; then
    poetry run "$tool" "$@"
  elif command -v "$tool" >/dev/null 2>&1; then
    "$tool" "$@"
  else
    return 127
  fi
}

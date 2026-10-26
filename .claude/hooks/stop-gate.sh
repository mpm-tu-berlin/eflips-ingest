#!/bin/bash
# Stop: block finishing until the CI lint checks pass on what this working tree changed.
# Tests are not run here: the fixture wipes whatever DATABASE_URL points at.
source "$CLAUDE_PROJECT_DIR/.claude/hooks/lib.sh"
input=$(cat)
[ "$(jq -r '.stop_hook_active // false' <<<"$input")" = "true" ] && exit 0
cd "$CLAUDE_PROJECT_DIR" || exit 0

changed=$({ git diff --name-only HEAD; git ls-files --others --exclude-standard; } 2>/dev/null | sort -u)
py=$(grep '\.py$' <<<"$changed" | while read -r f; do [ -f "$f" ] && echo "$f"; done)

failures=""
skipped=""
check() {
  local name=$1
  shift
  local out
  out=$(run_tool "$@" 2>&1)
  case $? in
    0) ;;
    127) skipped+=" $name" ;;
    *) failures+=$'\n'"## $name"$'\n'"$(tail -40 <<<"$out")" ;;
  esac
}

if [ -n "$py" ]; then
  # shellcheck disable=SC2086
  check black black --check $py
fi
if grep -q '^eflips/.*\.py$' <<<"$changed"; then
  check mypy mypy eflips --explicit-package-bases --strict
fi
if grep -qE '^(pyproject\.toml|poetry\.lock)$' <<<"$changed"; then
  check "poetry check" poetry check
fi

[ -n "$skipped" ] && echo "stop-gate: not installed, skipped:$skipped" >&2
if [ -n "$failures" ]; then
  jq -n --arg r "Checks failed. Fix the code, then finish:$failures" '{decision: "block", reason: $r}'
fi
exit 0

#!/usr/bin/env bash
# Checks that commit subjects follow Conventional Commits 1.0:
#   type(scope)!: description      scope and ! are optional
#
#   scripts/check-commit-msg.sh FILE              # one message (commit-msg hook)
#   scripts/check-commit-msg.sh --range A..B      # every commit in a range (CI)
#   scripts/check-commit-msg.sh --subject "TEXT"  # a single subject (PR title)
set -euo pipefail

TYPES='feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert'
RE="^($TYPES)(\([a-z0-9._/-]+\))?!?: [^ ].{0,98}$"

check() { # subject, label
  if [[ "$1" =~ ^(Merge|Revert\ \") ]] || [[ "$1" =~ ^(fixup|squash|amend)!\  ]]; then return 0; fi
  if [[ ! "$1" =~ $RE ]]; then
    echo "not a Conventional Commit: $2: \"$1\"" >&2
    return 1
  fi
}

usage() {
  echo "expected: <type>(<scope>)!: <description>  (types: ${TYPES//|/, }; max 100 chars)" >&2
  echo "e.g. feat(queue): support delayed send | fix(php)!: drop PHP 8.3" >&2
}

case "${1:-}" in
  --range)
    fail=0
    while IFS=$'\t' read -r sha subject; do
      check "$subject" "${sha:0:7}" || fail=1
    done < <(git log --no-merges --format='%H%x09%s' "$2")
    [ "$fail" = 0 ] || { usage; exit 1; }
    ;;
  --subject)
    check "$2" "subject" || { usage; exit 1; }
    ;;
  ?*)
    subject=$(grep -v '^#' "$1" | sed -n '/[^[:space:]]/{p;q;}')
    check "$subject" "commit message" || { usage; exit 1; }
    ;;
  *)
    echo "usage: $0 FILE | --range A..B | --subject TEXT" >&2; exit 2
    ;;
esac

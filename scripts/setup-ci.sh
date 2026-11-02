#!/usr/bin/env bash
# One-time setup of what CI needs beyond GITHUB_TOKEN, with the gh CLI (run
# it with an account that administers the repo). Idempotent: run it again and
# it only does what is missing.
#
#   scripts/setup-ci.sh               # set up
#   scripts/setup-ci.sh --rotate-key  # replace the deploy key
#
# What it does: a deploy key with write access on romycode/qsrv, its private
# half stored as the Actions secret QSRV_TIDY_KEY. The Tidy workflow
# (.github/workflows/tidy.yml) pushes go mod tidy commits to pull request
# branches with it: a push with GITHUB_TOKEN wouldn't trigger the checks on
# the new commit. The key never touches disk outside a temp dir that is
# removed on exit.
set -euo pipefail

REPO=${QSRV_REPO:-romycode/qsrv}
SECRET=QSRV_TIDY_KEY
KEY_TITLE="qsrv CI go mod tidy (Actions secret $SECRET)"

rotate=false
case "${1:-}" in
  "") ;;
  --rotate-key) rotate=true ;;
  *) echo "usage: $0 [--rotate-key]" >&2; exit 2 ;;
esac

say()  { printf '\033[1m==> %s\033[0m\n' "$*"; }
ok()   { printf '    ok: %s\n' "$*"; }
die()  { printf 'setup-ci: %s\n' "$*" >&2; exit 1; }

command -v gh >/dev/null || die "the gh CLI is required: https://cli.github.com"
command -v ssh-keygen >/dev/null || die "ssh-keygen is required"
gh auth status >/dev/null 2>&1 || die "not logged in: run gh auth login"
perm=$(gh api "repos/$REPO" --jq '.permissions.admin' 2>/dev/null) || die "can't read $REPO (does it exist? do you have access?)"
[ "$perm" = true ] || die "you need admin rights on $REPO"

say "Deploy key and secret $SECRET on $REPO"
key_ids=$(gh repo deploy-key list -R "$REPO" --json id,title --jq ".[] | select(.title == \"$KEY_TITLE\") | .id")
has_secret=$(gh secret list -R "$REPO" --json name --jq ".[] | select(.name == \"$SECRET\") | .name")

if [ -n "$key_ids" ] && [ -n "$has_secret" ] && [ "$rotate" = false ]; then
  ok "already set up (use --rotate-key to replace the key)"
else
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT
  ssh-keygen -q -t ed25519 -N "" -C "$KEY_TITLE" -f "$tmp/key"
  # New key first, then the secret, then remove the old keys: a run in
  # progress keeps working with one key or the other.
  gh repo deploy-key add "$tmp/key.pub" -R "$REPO" --allow-write --title "$KEY_TITLE" >/dev/null
  ok "deploy key added (write access)"
  gh secret set "$SECRET" -R "$REPO" < "$tmp/key"
  ok "secret $SECRET set"
  for id in $key_ids; do
    gh repo deploy-key delete "$id" -R "$REPO" >/dev/null
    ok "old deploy key $id removed"
  done
fi

say "Done"
cat <<EOF
    Pull requests that change imports get a "build(deps): go mod tidy" commit
    from the Tidy workflow; the checks run again on it.
EOF

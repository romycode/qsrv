#!/usr/bin/env bash
# One-time setup of the repositories for releases, with the gh CLI (run it
# with an account that administers both repos). Idempotent: run it again and
# it only does what is missing.
#
#   scripts/setup-release.sh               # set up
#   scripts/setup-release.sh --rotate-key  # replace the mirror's deploy key
#
# What it does:
#   1. romycode/qsrv: squash-only merges, delete branches after merge.
#   2. Deploy key with write access on the PHP mirror (romycode/qsrv-php),
#      its private half stored as the Actions secret QSRV_PHP_MIRROR_KEY in
#      romycode/qsrv. The key never touches disk outside a temp dir that is
#      removed on exit. Used by .github/workflows/tag.yml (component php).
#   3. Optional, with PACKAGIST_USERNAME and PACKAGIST_TOKEN (packagist.org →
#      Profile → API Token): registers romycode/qsrv-php on Packagist and adds
#      the GitHub webhook that publishes new tags automatically.
#
# Afterwards, releases are Actions → Tag release (see docs/development.md).
set -euo pipefail

REPO=${QSRV_REPO:-romycode/qsrv}
MIRROR=${QSRV_PHP_MIRROR_REPO:-romycode/qsrv-php}
SECRET=QSRV_PHP_MIRROR_KEY
KEY_TITLE="qsrv release (Actions secret $SECRET in $REPO)"

rotate=false
case "${1:-}" in
  "") ;;
  --rotate-key) rotate=true ;;
  *) echo "usage: $0 [--rotate-key]" >&2; exit 2 ;;
esac

say()  { printf '\033[1m==> %s\033[0m\n' "$*"; }
ok()   { printf '    ok: %s\n' "$*"; }
die()  { printf 'setup-release: %s\n' "$*" >&2; exit 1; }

command -v gh >/dev/null || die "the gh CLI is required: https://cli.github.com"
command -v ssh-keygen >/dev/null || die "ssh-keygen is required"
gh auth status >/dev/null 2>&1 || die "not logged in: run gh auth login"
for r in "$REPO" "$MIRROR"; do
  perm=$(gh api "repos/$r" --jq '.permissions.admin' 2>/dev/null) || die "can't read $r (does it exist? do you have access?)"
  [ "$perm" = true ] || die "you need admin rights on $r"
done

# 1. Merge settings ------------------------------------------------------------
say "Merge settings on $REPO"
gh repo edit "$REPO" \
  --enable-squash-merge --enable-merge-commit=false --enable-rebase-merge=false \
  --delete-branch-on-merge >/dev/null
ok "squash merges only, head branches deleted after merge"

# 2. Mirror deploy key + Actions secret -----------------------------------------
say "Deploy key on $MIRROR and secret $SECRET on $REPO"
key_ids=$(gh repo deploy-key list -R "$MIRROR" --json id,title --jq ".[] | select(.title == \"$KEY_TITLE\") | .id")
has_secret=$(gh secret list -R "$REPO" --json name --jq ".[] | select(.name == \"$SECRET\") | .name")

if [ -n "$key_ids" ] && [ -n "$has_secret" ] && [ "$rotate" = false ]; then
  ok "already set up (use --rotate-key to replace the key)"
else
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT
  ssh-keygen -q -t ed25519 -N "" -C "$KEY_TITLE" -f "$tmp/key"
  # New key first, then the secret, then remove the old keys: a release
  # running meanwhile keeps working with one key or the other.
  gh repo deploy-key add "$tmp/key.pub" -R "$MIRROR" --allow-write --title "$KEY_TITLE" >/dev/null
  ok "deploy key added to $MIRROR (write access)"
  gh secret set "$SECRET" -R "$REPO" < "$tmp/key"
  ok "secret $SECRET set on $REPO"
  for id in $key_ids; do
    gh repo deploy-key delete "$id" -R "$MIRROR" >/dev/null
    ok "old deploy key $id removed"
  done
  if ssh -i "$tmp/key" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -T git@github.com 2>&1 | grep -q "$MIRROR"; then
    ok "key authenticates as $MIRROR"
  else
    echo "    warning: could not verify the key over SSH (port 22 blocked?); the Tag release action will tell" >&2
  fi
fi

# 3. Packagist --------------------------------------------------------------------
say "Packagist"
pkg=$(gh api "repos/$MIRROR/contents/composer.json" --jq '.content' | base64 -d | sed -n 's/.*"name": *"\([^"]*\)".*/\1/p' | head -1)
[ -n "$pkg" ] || die "no package name in $MIRROR/composer.json: sync the mirror first (scripts/release-php.sh --sync)"

if [ -z "${PACKAGIST_USERNAME:-}" ] || [ -z "${PACKAGIST_TOKEN:-}" ]; then
  if curl -fsS "https://repo.packagist.org/p2/$pkg.json" >/dev/null 2>&1; then
    ok "$pkg is on Packagist"
  else
    echo "    skipped: set PACKAGIST_USERNAME and PACKAGIST_TOKEN to register $pkg,"
    echo "    or submit https://github.com/$MIRROR at https://packagist.org/packages/submit"
  fi
else
  if curl -fsS "https://repo.packagist.org/p2/$pkg.json" >/dev/null 2>&1; then
    ok "$pkg already on Packagist"
  else
    curl -fsS -X POST "https://packagist.org/api/create-package" \
      -H "Authorization: Bearer $PACKAGIST_USERNAME:$PACKAGIST_TOKEN" \
      -H "Content-Type: application/json" \
      -d "{\"repository\":{\"url\":\"https://github.com/$MIRROR\"}}" >/dev/null \
      || die "Packagist refused to create $pkg (check the token)"
    ok "$pkg registered on Packagist"
  fi
  hook_url="https://packagist.org/api/github?username=$PACKAGIST_USERNAME"
  if gh api "repos/$MIRROR/hooks" --jq '.[].config.url' | grep -qF "packagist.org/api/github"; then
    ok "Packagist webhook already on $MIRROR"
  else
    gh api "repos/$MIRROR/hooks" --method POST --silent \
      -f name=web -F active=true -f 'events[]=push' \
      -f "config[url]=$hook_url" -f "config[content_type]=json" \
      -f "config[secret]=$PACKAGIST_TOKEN" -f "config[insecure_ssl]=0"
    ok "webhook added: Packagist updates on every push and tag to $MIRROR"
  fi
fi

say "Done"
cat <<EOF
    Release the server:  Actions → Tag release → server, vX.Y.Z  (needs a CHANGELOG section)
    Release PHP:         Actions → Tag release → php, vX.Y.Z
    Still by hand (no API for it): make the image public once it exists:
      https://github.com/users/${REPO%%/*}/packages/container/${REPO##*/}/settings → Change visibility
EOF

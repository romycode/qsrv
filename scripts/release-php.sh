#!/usr/bin/env bash
# Publishes the PHP package (php/) to the read-only mirror romycode/qsrv-php,
# which Packagist reads. Uses your own git credentials: no CI secrets.
#
#   scripts/release-php.sh v0.2.0        # tag php/v0.2.0 here, v0.2.0 on the mirror
#   scripts/release-php.sh --sync        # only update the mirror's main (no release)
#
# PHP versions are independent of the Go server's plain vX.Y.Z tags.
set -euo pipefail

MIRROR=${QSRV_PHP_MIRROR:-git@github.com:romycode/qsrv-php.git}
ORIGIN=${QSRV_ORIGIN:-origin}

die() { echo "release-php: $*" >&2; exit 1; }

[ $# -eq 1 ] || die "usage: $0 vX.Y.Z | --sync"
if [ "$1" != "--sync" ]; then
  [[ "$1" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]] || die "version must look like v1.2.3"
fi
cd "$(git rev-parse --show-toplevel)"

# Only publish what is on origin/main: nothing local, nothing unmerged.
git fetch --quiet "$ORIGIN" main --tags
[ -z "$(git status --porcelain -- php)" ] || die "php/ has uncommitted changes"
head=$(git rev-parse HEAD)
[ "$head" = "$(git rev-parse "$ORIGIN/main")" ] || die "HEAD is not $ORIGIN/main: checkout main and pull first"
git cat-file -e HEAD:php/composer.json 2>/dev/null || die "php/composer.json not found on main"

echo "splitting php/ at $(git rev-parse --short HEAD)..."
split=$(git subtree split --prefix=php HEAD)

# The split is deterministic, so this is a fast-forward unless someone pushed
# to the mirror directly (then it fails instead of overwriting).
git push "$MIRROR" "$split:refs/heads/main"

if [ "$1" = "--sync" ]; then
  echo "mirror main -> $split"
  exit 0
fi

version=$1
tag="php/$version"
if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
  [ "$(git rev-parse "$tag^{commit}")" = "$head" ] || die "$tag already exists on another commit"
else
  git tag -a "$tag" -m "PHP package $version" HEAD
  git push "$ORIGIN" "refs/tags/$tag"
fi
git ls-remote --exit-code --tags "$MIRROR" "refs/tags/$version" >/dev/null 2>&1 && die "$version already exists on the mirror"
git push "$MIRROR" "$split:refs/tags/$version"
echo "published romycode/qsrv-php $version ($split)"

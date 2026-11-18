#!/bin/sh
# Called by the repository's post-publication hook, never during implementation.
# The build recipe owns branch publication; this only releases the verified module.
set -eu
merged=${1:?published merged commit required}
branch=${2:?published target branch required}
# Native publication supplies the complete branch reference. Reject other ref
# namespaces and malformed refs instead of guessing or prefixing it again.
case "$branch" in refs/heads/*) ;; *) echo 'fully qualified branch ref required' >&2; exit 1;; esac
git check-ref-format "$branch" || { echo 'invalid target branch ref' >&2; exit 1; }
[ "$(git rev-parse HEAD)" = "$merged" ] || { echo 'release workspace differs from published candidate' >&2; exit 1; }
version=$(cat plugin/VERSION)
case "$version" in v[0-9]*.[0-9]*.[0-9]*) ;; *) echo 'invalid plugin/VERSION' >&2; exit 1;; esac
tag="plugin/$version"
remote_branch=$(git ls-remote --heads origin "$branch" | cut -f1)
[ "$remote_branch" = "$merged" ] || { echo 'published branch must match the release candidate' >&2; exit 1; }
existing=$(git ls-remote --tags origin "refs/tags/$tag" | cut -f1)
if [ -n "$existing" ]; then
 git fetch --no-tags origin "refs/tags/$tag"
 [ "$(git rev-parse FETCH_HEAD:plugin)" = "$(git rev-parse HEAD:plugin)" ] || { echo 'plugin version already identifies different contents; bump plugin/VERSION' >&2; exit 1; }
else
 # A lightweight nested-module tag references the actual published merge result.
 # No force, branch update, or retagging is permitted.
 git push origin "$merged:refs/tags/$tag"
fi
plugin/scripts/check-release.sh "$version"
printf 'Published %s; independent no-replacement author/host verification passed.\n' "$tag"

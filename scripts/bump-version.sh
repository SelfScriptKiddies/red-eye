#!/usr/bin/env bash
# Bump the canonical project version: the repo-root VERSION file, then sync
# it into the three crate Cargo.tomls and frontend/package.json.
#
# The release workflow (.github/workflows/release.yml) rejects a merge to
# main whose VERSION equals an already-released tag — every release PR must
# carry a bump (`make bump V=X.Y.Z`).
set -euo pipefail

cd "$(dirname "$0")/.."

if [ $# -ne 1 ]; then
    echo "usage: $0 X.Y.Z" >&2
    exit 1
fi

NEW=$1
if ! [[ $NEW =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
    echo "error: '$NEW' is not a semver (X.Y.Z)" >&2
    exit 1
fi

OLD=$(cat VERSION)
if [ "$OLD" = "$NEW" ]; then
    echo "error: VERSION is already $NEW" >&2
    exit 1
fi

echo "$NEW" > VERSION
for manifest in backend/Cargo.toml agent/Cargo.toml traffic-analyzer/Cargo.toml; do
    sed -i 's/^version = ".*"/version = "'"$NEW"'"/' "$manifest"
done
sed -i 's/"version": ".*"/"version": "'"$NEW"'"/' frontend/package.json

echo "bumped $OLD -> $NEW (VERSION, Cargo.toml x3, package.json)"

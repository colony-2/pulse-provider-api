#!/bin/sh
# Check both unpublished modules together without committing a local replacement.
set -eu
cd "$(dirname "$0")/.."
candidate_dir=$(mktemp -d)
trap 'rm -rf "$candidate_dir"' EXIT
cp go.mod provider.go launch_logs.go "$candidate_dir/"
cp ./*_test.go "$candidate_dir/"
cp Makefile "$candidate_dir/"
cp -R plugin "$candidate_dir/plugin"
cd "$candidate_dir/plugin"
# First check the committed dependency metadata without a replacement. The
# subsequent candidate-only replacement legitimately removes this module's two
# published checksum entries; normalize only those entries in the scratch copy.
GOWORK=off go mod tidy -diff
GOWORK=off go mod edit -replace="github.com/colony-2/pulse-provider-api=$candidate_dir"
awk '$1 != "github.com/colony-2/pulse-provider-api"' go.sum > go.sum.candidate
mv go.sum.candidate go.sum
GOWORK=off go list -m -json github.com/colony-2/pulse-provider-api
cd "$candidate_dir"
if [ "$#" -eq 0 ]; then
  set -- check-modules
fi
make "$@"

#!/bin/sh
# Verify a published nested-module tag from an independent module and fresh cache.
# This script does not create, merge, tag, push, or release repository changes.
set -eu
version=${1:?published version such as v0.1.0 required}
case "$version" in v[0-9]*.[0-9]*.[0-9]*) ;; *) echo 'invalid module version' >&2; exit 1;; esac
sdk_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
consumer_dir=$(mktemp -d)
trap 'rm -rf "$consumer_dir"' EXIT
mkdir "$consumer_dir/author" "$consumer_dir/host" "$consumer_dir/cache"
cp "$sdk_root/examples/provider/main.go" "$consumer_dir/author/main.go"
cp "$sdk_root/examples/host/main.go" "$consumer_dir/host/main.go"
cd "$consumer_dir"
export GOWORK=off
export GOMODCACHE="$consumer_dir/cache"
printf 'module example.org/published-consumer\n\ngo 1.22\n' > go.mod
go get "github.com/colony-2/pulse-provider-api/plugin@$version"
go mod tidy
# Reject replacements, including any injected by module metadata.
[ -z "$(go list -m -f '{{if .Replace}}{{.Path}}{{end}}' all)" ] || { echo 'replacement found' >&2; exit 1; }
go list -m -json github.com/colony-2/pulse-provider-api/plugin github.com/colony-2/pulse-provider-api
go build -o author-bin ./author
go build -o host-bin ./host
./host-bin "$consumer_dir/author-bin"

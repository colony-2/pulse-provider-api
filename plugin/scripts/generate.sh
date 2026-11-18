#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
# protoc 36.2; generators are pinned independently of the installed PATH.
[ "$(protoc --version)" = 'libprotoc 36.2' ] || { echo 'protoc 36.2 required' >&2; exit 1; }
tools_dir=$(mktemp -d)
trap 'rm -rf "$tools_dir"' EXIT
GOWORK=off GOBIN="$tools_dir" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.2
GOWORK=off GOBIN="$tools_dir" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
PATH="$tools_dir:$PATH" protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative internal/wire/plugin.proto

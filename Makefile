.PHONY: check check-modules generate release-check
check:
	scripts/check-candidate.sh

check-modules:
	GOWORK=off go test ./...
	GOWORK=off go vet ./...
	GOWORK=off go test -race ./...
	GOWORK=off go mod tidy -diff
	cd plugin && GOWORK=off go test ./...
	cd plugin && GOWORK=off go vet ./...
	cd plugin && GOWORK=off go test -race ./...
	cd plugin && GOWORK=off go mod tidy -diff
	$(MAKE) generate

generate:
	@set -eu; before=$$(mktemp); after=$$(mktemp); trap 'rm -f "$$before" "$$after"' EXIT; \
	sha256sum plugin/internal/wire/*.pb.go > "$$before"; \
	plugin/scripts/generate.sh; \
	sha256sum plugin/internal/wire/*.pb.go > "$$after"; \
	diff -u "$$before" "$$after"

# VERSION=v0.1.0 corresponds to the repository tag plugin/v0.1.0.
release-check:
	@test -n "$(VERSION)" || { echo 'VERSION is required' >&2; exit 1; }
	plugin/scripts/check-release.sh "$(VERSION)"

MODULE        := github.com/rlsvr/hirnok
GOLANGCI_LINT := golangci-lint

.PHONY: all build test test-verbose test-integration test-integration-verbose \
	fmt lint lint-fix check tidy clean

## all: build + test + lint
all: build test lint

## build: compile all packages
build:
	go build ./...

## test: run all tests with race detector
test:
	go test -race ./...

## test-verbose: run all tests in verbose mode
test-verbose:
	go test -race -v ./...

## test-integration: run integration tests
test-integration:
	go test -race -tags=integration ./...

## test-integration-verbose: run integration tests in verbose mode
test-integration-verbose:
	go test -race -tags=integration -v ./...

## fmt: apply gofmt + goimports via golangci-lint
fmt:
	$(GOLANGCI_LINT) fmt

## lint: report lint issues (no fix)
lint:
	$(GOLANGCI_LINT) run ./...

## lint-fix: apply auto-fixable lint issues
lint-fix:
	$(GOLANGCI_LINT) run --fix ./...

## tidy: tidy go.mod / go.sum
tidy:
	go mod tidy

## check: fmt + lint + tidy
check: fmt lint tidy

## clean: remove build artifacts
clean:
	rm -rf bin/

MODULE        := github.com/rlsvr/hirnok
GOLANGCI_LINT := golangci-lint

.PHONY: all build test test-verbose test-integration test-integration-verbose \
	fmt lint lint-fix check tidy clean \
	nats nats-stop smoke stress stress-core stress-jetstream stress-backpressure stress-burst

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

## nats: start a local nats-server -js (Ctrl-C to stop)
nats:
	./test/setup.sh

## nats-stop: stop the dockerized NATS started by `make nats`
nats-stop:
	docker rm -f hirnok-nats || true

## smoke: connect to local NATS and run a core + JS round-trip smoke test
smoke:
	go run ./test/smoke

## stress: run all stress benches sequentially
stress: stress-core stress-jetstream stress-backpressure stress-burst

## stress-core: core NATS throughput bench
stress-core:
	go run ./test/stress/core

## stress-jetstream: JetStream throughput bench
stress-jetstream:
	go run ./test/stress/jetstream

## stress-backpressure: sustained slow-handler / fast-producer bench
stress-backpressure:
	go run ./test/stress/backpressure

## stress-burst: cold-idle then sudden burst bench
stress-burst:
	go run ./test/stress/burst

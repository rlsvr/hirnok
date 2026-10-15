# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- GitHub Actions CI workflow with Go 1.25, race detection, format check, and `golangci-lint` v2.13.
- `SECURITY.md` vulnerability reporting guidelines.
- `LICENSE` (MIT License).
- Clean badges for Go Reference, CI status, `golangci-lint`, and MIT License.

### Changed
- Upgraded `github.com/rlsvr/inas` dependency to latest.
- Resolved unused parameter revive lint warning in `pkg/nats/json_test.go`.
- Increased wait timeout in `TestConsumerStopNaksQueuedMessages` to prevent race detector timing flakes under heavy concurrent load.

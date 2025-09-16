MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))

ifndef GOBIN
export GOBIN := $(MAKEFILE_PATH)/../bin
endif

export PATH := $(GOBIN):$(PATH)

MODULE = sdkv1

.PHONY: clean
clean:
	@echo "======== [${MODULE}] Cleaning"
	@go clean -r ./...

.PHONY: build
build:
	@echo "======== [${MODULE}] Building"
	@go build ./...

.PHONY: clean-caches
clean-caches:
	@echo "======== [${MODULE}] Cleaning caches"
	@go clean -r -cache -testcache -modcache ./...

.PHONY: check
check: check-golangci

.PHONY: fix
fix: fix-golangci

.PHONY: check-golangci
check-golangci: .prepare-golangci
	@echo "======== [${MODULE}] Linting with golangci"
	@golangci-lint run --config .golangci.yml ./...

.PHONY: fix-golangci
fix-golangci: .prepare-golangci
	@echo "======== [${MODULE}] Fixing code with golangci"
	@golangci-lint run --config .golangci.yml --fix ./...

.PHONY: test
test: build check test-unit test-integration

.PHONY: test-unit
test-unit:
	@echo "======== [${MODULE}] Running unit tests"
	@go test -v -cover -race ./...

.PHONY: test-integration
test-integration: scylla-start
	@echo "======== [${MODULE}] Running integration tests"
	@go test -v -cover -race -tags integration ./...

.PHONY: scylla-start
scylla-start:
	$(MAKE) -C .. scylla-start

.PHONY: scylla-stop
scylla-stop:
	$(MAKE) -C .. scylla-stop

.PHONY: scylla-kill
scylla-kill:
	$(MAKE) -C .. scylla-kill

.PHONY: scylla-rm
scylla-rm:
	$(MAKE) -C .. scylla-rm

.prepare-golangci:
	$(MAKE) -C .. .prepare-golangci

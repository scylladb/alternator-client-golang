define dl_tgz
	@if ! $(1) 2>/dev/null 1>&2; then \
		[ -d "$(GOBIN)" ] || mkdir "$(GOBIN)"; \
		if [ ! -f "$(GOBIN)/$(1)" ]; then \
			echo "Downloading $(GOBIN)/$(1)"; \
			curl --progress-bar -L $(2) | tar zxf - --wildcards --strip 1 -C $(GOBIN) '*/$(1)'; \
			chmod +x "$(GOBIN)/$(1)"; \
		fi; \
	fi
endef

define dl_bin
	@if ! $(1) 2>/dev/null 1>&2; then \
		[ -d "$(GOBIN)" ] || mkdir "$(GOBIN)"; \
		if [ ! -f "$(GOBIN)/$(1)" ]; then \
			echo "Downloading $(GOBIN)/$(1)"; \
			curl --progress-bar -L $(2) --output "$(GOBIN)/$(1)"; \
			chmod +x "$(GOBIN)/$(1)"; \
		fi; \
	fi
endef

MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
GOOS := $(shell uname | tr '[:upper:]' '[:lower:]')
GOARCH := $(shell go env GOARCH)
DOCKER_COMPOSE_VERSION := 2.34.0
GOLANGCI_VERSION := v2.1.6

ifeq ($(GOARCH),arm64)
	DOCKER_COMPOSE_DOWNLOAD_URL := "https://github.com/docker/compose/releases/download/v$(DOCKER_COMPOSE_VERSION)/docker-compose-$(GOOS)-aarch64"
	GOLANGCI_DOWNLOAD_URL := "https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_VERSION)/golangci-lint-$(GOLANGCI_VERSION)-$(GOOS)-arm64.tar.gz"
else ifeq ($(GOARCH),amd64)
	DOCKER_COMPOSE_DOWNLOAD_URL := "https://github.com/docker/compose/releases/download/v$(DOCKER_COMPOSE_VERSION)/docker-compose-$(GOOS)-x86_64"
	GOLANGCI_DOWNLOAD_URL := "https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_VERSION)/golangci-lint-$(GOLANGCI_VERSION)-$(GOOS)-amd64.tar.gz"
else
	@printf 'Unknown architecture "%s"\n', "$(GOARCH)"
	@exit 69
endif


ifndef GOBIN
export GOBIN := $(MAKEFILE_PATH)/bin
endif

export PATH := $(GOBIN):$(PATH)

COMPOSE := docker-compose -f $(MAKEFILE_PATH)/test/docker-compose.yml

.PHONY: clean
clean:
	$(MAKE) -C ./shared clean
	$(MAKE) -C ./sdkv1 clean
	$(MAKE) -C ./sdkv2 clean

.PHONY: build
build:
	$(MAKE) -C ./shared build
	$(MAKE) -C ./sdkv1 build
	$(MAKE) -C ./sdkv2 build

.PHONY: clean-caches
clean-caches:
	$(MAKE) -C ./shared clean-caches
	$(MAKE) -C ./sdkv1 clean-caches
	$(MAKE) -C ./sdkv2 clean-caches

.PHONY: check
check: check-golangci

.PHONY: fix
fix: fix-golangci

.PHONY: check-golangci
check-golangci: .prepare-golangci
	$(MAKE) -C ./shared check-golangci
	$(MAKE) -C ./sdkv1 check-golangci
	$(MAKE) -C ./sdkv2 check-golangci

.PHONY: fix-golangci
fix-golangci: .prepare-golangci
	$(MAKE) -C ./shared fix-golangci
	$(MAKE) -C ./sdkv1 fix-golangci
	$(MAKE) -C ./sdkv2 fix-golangci

.PHONY: test
test: build check test-unit test-integration

.PHONY: test-unit
test-unit:
	$(MAKE) -C ./shared test-unit
	$(MAKE) -C ./sdkv1 test-unit
	$(MAKE) -C ./sdkv2 test-unit

.PHONY: test-integration
test-integration: scylla-start
	$(MAKE) -C ./shared test-integration
	$(MAKE) -C ./sdkv1 test-integration
	$(MAKE) -C ./sdkv2 test-integration

.PHONY: .prepare-cert
.prepare-cert:
	@[ -f "${MAKEFILE_PATH}/test/scylla/db.key" ] || (echo "Prepare certificate" && cd ${MAKEFILE_PATH}/test/scylla/ && openssl req -subj "/C=US/ST=Denial/L=Springfield/O=Dis/CN=www.example.com" -x509 -newkey rsa:4096 -keyout db.key -out db.crt -days 3650 -nodes && chmod 644 db.key)

.PHONY: scylla-start
scylla-start: .prepare-cert $(GOBIN)/docker-compose
	@sudo sysctl -w fs.aio-max-nr=10485760
	$(COMPOSE) up -d

.PHONY: scylla-stop
scylla-stop: $(GOBIN)/docker-compose
	$(COMPOSE) down

.PHONY: scylla-kill
scylla-kill: $(GOBIN)/docker-compose
	$(COMPOSE) kill

.PHONY: scylla-rm
scylla-rm: $(GOBIN)/docker-compose
	$(COMPOSE) rm -f

.prepare-golangci:
	@if ! golangci-lint --version 2>/dev/null | grep ${GOLANGCI_VERSION} >/dev/null; then \
  		echo "Installing golangci-ling ${GOLANGCI_VERSION}"; \
		go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_VERSION}; \
  	fi

$(GOBIN)/docker-compose: Makefile
	$(call dl_bin,docker-compose,$(DOCKER_COMPOSE_DOWNLOAD_URL))

.PHONY: update-go-mod
update-go-mod:
	$(MAKE) -C ./shared update-go-mod
	$(MAKE) -C ./sdkv1 update-go-mod
	$(MAKE) -C ./sdkv2 update-go-mod

.PHONY: upgrade-all-deps
upgrade-all-deps:
	$(MAKE) -C ./shared upgrade-all-deps
	$(MAKE) -C ./sdkv1 upgrade-all-deps
	$(MAKE) -C ./sdkv2 upgrade-all-deps

.PHONY: create-release-commits
create-release-commits:
	@if [ -z "$(version)" ]; then echo "Usage: make create-release-commits version=vX.Y.Z"; exit 1; fi
	@$(MAKE) update-go-mod
	VERSION="$(version)"; \
	for module in sdkv1 sdkv2; do \
		( cd $$module && \
			go mod edit -require=github.com/scylladb/alternator-client-golang/shared@$$VERSION && \
			(go mod edit -dropreplace github.com/scylladb/alternator-client-golang/shared || true) ); \
	done; \
	git add sdkv1/go.mod sdkv2/go.mod; \
	git commit -m "Release $$VERSION"; \
	git tag "$$VERSION"; \
	git tag "shared/$$VERSION"; \
	git tag "sdkv1/$$VERSION"; \
	git tag "sdkv2/$$VERSION"; \
	for module in sdkv1 sdkv2; do \
		( cd $$module && go mod edit -replace=github.com/scylladb/alternator-client-golang/shared=../shared ); \
	done; \
	git add sdkv1/go.mod sdkv2/go.mod; \
	git commit -m "New iteration"

.PHONY: push-release-commits
push-release-commits:
	@set -eu; \
	LAST1=$$(git log -1 --pretty=%s); \
	LAST2=$$(git log -2 --pretty=%s | tail -n 1); \
	if [ "$$LAST1" != "New iteration" ]; then echo "Head commit must be 'New iteration'"; exit 1; fi; \
	case "$$LAST2" in \
	"Release "*) ;; \
	*) echo "Second commit must be 'Release <version>'"; exit 1;; \
	esac; \
	VERSION=$${LAST2#"Release "}; \
	if [ -z "$$VERSION" ]; then echo "Unable to parse version from release commit: $$LAST2"; exit 1; fi; \
	for tag in "$$VERSION" "shared/$$VERSION" "sdkv1/$$VERSION" "sdkv2/$$VERSION"; do \
		if ! git rev-parse --verify "$$tag" >/dev/null 2>&1; then echo "Missing tag $$tag"; exit 1; fi; \
	done; \
	git push origin HEAD; \
	git push origin "$$VERSION" "shared/$$VERSION" "sdkv1/$$VERSION" "sdkv2/$$VERSION"

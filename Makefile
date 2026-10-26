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

# Keep private CLI inputs out of recursive flags before entering the helper.
release_make_scope := $(filter create-release-commits create-release-tags push-release-commits,$(MAKECMDGOALS)) \
  $(filter-out undefined,$(origin RELEASE_GPG_GIT_CONFIG_COUNT)) \
  $(foreach name,RELEASE_GPG_PRIVATE_KEY RELEASE_GPG_PRIVATE_KEY_FILE RELEASE_GPG_PASSPHRASE RELEASE_GPG_PASSPHRASE_FILE,$(filter command line,$(origin $(name))))
ifneq ($(strip $(release_make_scope)),)
release_make_names := $(filter-out RELEASE_GPG_PRIVATE_KEY RELEASE_GPG_PRIVATE_KEY_FILE MAKEFLAGS MAKEOVERRIDES MFLAGS GNUMAKEFLAGS,$(foreach name,$(.VARIABLES),$(if $(filter command line,$(origin $(name))),$(name))))
release_make_simple := $(foreach name,$(release_make_names),$(if $(filter simple,$(flavor $(name))),$(name)))
release_make_indices :=
$(foreach name,$(release_make_names),$(eval release_make_indices += x)\
  $(eval export RELEASE_GPG_MAKE_$(MAKELEVEL)_$(words $(release_make_indices)) := $$(value $(name))))
# Preserve recursive expressions and simple values, without serializing their data.
override MAKEOVERRIDES = $(eval release_make_indices :=)$(foreach name,$(release_make_names),\
  $(eval release_make_indices += x)$(name)$(if $(filter $(name),$(release_make_simple)),:)=$(if $(filter $(name),$(release_make_simple)),$$$$(value\ RELEASE_GPG_MAKE_$(MAKELEVEL)_$(words $(release_make_indices))),$$$$(RELEASE_GPG_MAKE_$(MAKELEVEL)_$(words $(release_make_indices)))))
endif

# Scoped Git values travel through the environment, never recipe arguments.
ifdef RELEASE_GPG_GIT_CONFIG_COUNT
unexport RELEASE_GPG_PRIVATE_KEY RELEASE_GPG_PRIVATE_KEY_FILE
release_signing_config = $(filter RELEASE_GPG_GIT_CONFIG_%,$(.VARIABLES))
$(foreach name,$(release_signing_config),\
  $(eval override $(patsubst RELEASE_GPG_%,%,$(name)) := $$(value $(name)))\
  $(eval export $(patsubst RELEASE_GPG_%,%,$(name))))
# Four dollars preserve a literal value reference through export and child parsing.
override MAKEOVERRIDES += $(foreach name,$(release_signing_config),$(patsubst RELEASE_GPG_%,%,$(name))=$$$$(value\ $(name)))
endif

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
	@[ -f "${MAKEFILE_PATH}/test/scylla/db.key" ] || (echo "Prepare certificate" && cd ${MAKEFILE_PATH}/test/scylla/ && openssl req -subj "/C=US/ST=Denial/L=Springfield/O=Dis/CN=www.example.com" -addext "subjectAltName=IP:172.41.0.2,IP:172.41.0.3,IP:172.41.0.4" -x509 -newkey rsa:4096 -keyout db.key -out db.crt -days 3650 -nodes && chmod 644 db.key)

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

export RELEASE_VERSION = $(version)
# Make normalizes dry-run options into the first short-option word. Inspect
# that word at recipe expansion time, ignoring long options and overrides.
RELEASE_DRY_RUN = $(findstring n,$(filter-out -%,$(firstword $(MAKEFLAGS))))

.PHONY: release-check-user
# Release mutations run only in the protected, manually dispatched main pipeline.
release-check-user:
	+@set -eu; \
	if [ "$${GITHUB_ACTIONS:-}" != true ] || \
	   [ "$${GITHUB_EVENT_NAME:-}" != workflow_dispatch ] || \
	   [ "$${GITHUB_REF:-}" != refs/heads/main ] || \
	   [ "$${GITHUB_WORKFLOW_REF:-}" != "$${GITHUB_REPOSITORY:-}/.github/workflows/release.yml@refs/heads/main" ]; then \
		echo "Release preparation and publication require the Release pipeline dispatched from main." >&2; \
		exit 1; \
	fi

.PHONY: release-check-version
release-check-version: release-check-user
	@set -eu; VERSION=$${RELEASE_VERSION:-}; \
	if [ -z "$$VERSION" ]; then \
		LATEST=$$(git tag --list 'v[0-9]*' --sort=-v:refname | head -n1); \
		echo "Usage: make create-release-commits version=vX.Y.Z"; \
		if [ -n "$$LATEST" ]; then echo "Latest release: $$LATEST"; fi; \
		exit 1; \
	fi; \
	number='(0|[1-9][0-9]*)'; \
	identifier='(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)'; \
	case "$$VERSION" in *[!0-9A-Za-z.+-]*) echo 'Invalid release version' >&2; exit 1;; esac; \
	if ! printf '%s\n' "$$VERSION" | grep -Eq "^v$$number\.$$number\.$$number(-$$identifier(\.$$identifier)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$$"; then \
		echo 'Expected a Go module version such as vX.Y.Z or vX.Y.Z-rc.1' >&2; exit 1; \
	fi

.PHONY: create-release-commits _create-release-commits
create-release-commits: release-check-version
	python3 scripts/release/signing.py check-baseline
	@set -eu; \
	status=$$(git status --porcelain); \
	if [ -n "$$status" ]; then \
		echo "Error: working tree has uncommitted changes"; \
		exit 1; \
	fi; \
	for tag in "$$RELEASE_VERSION" "shared/$$RELEASE_VERSION" "sdkv1/$$RELEASE_VERSION" "sdkv2/$$RELEASE_VERSION"; do \
		if git show-ref --verify --quiet "refs/tags/$$tag"; then \
			echo "Release tag already exists: $$tag" >&2; exit 1; \
		fi; \
	done
	+$(if $(RELEASE_DRY_RUN),,python3 scripts/release/signing.py exec-build )$(MAKE) update-go-mod
	+$(if $(RELEASE_DRY_RUN),,python3 scripts/release/signing.py exec )$(MAKE) _create-release-commits

_create-release-commits: release-check-version
	python3 scripts/release/signing.py check-baseline
	@set -eu; VERSION=$$RELEASE_VERSION; \
	for module in sdkv1 sdkv2; do \
		( cd $$module && \
			go mod edit -require=github.com/scylladb/alternator-client-golang/shared@$$VERSION && \
			go mod edit -dropreplace github.com/scylladb/alternator-client-golang/shared ); \
	done; \
	for module in shared sdkv1 sdkv2; do \
		git add "$$module/go.mod"; \
		if [ -f "$$module/go.sum" ]; then git add "$$module/go.sum"; fi; \
	done; \
	if [ -f go.work.sum ]; then git add go.work.sum; fi; \
	git commit -S -m "Release $$VERSION"; \
	python3 scripts/release/signing.py verify-commit HEAD
	+$(MAKE) _create-release-tags
	@set -eu; \
	for module in sdkv1 sdkv2; do \
		( cd $$module && go mod edit -replace=github.com/scylladb/alternator-client-golang/shared=../shared ); \
	done; \
	git add sdkv1/go.mod sdkv2/go.mod; \
	git commit -S -m "New iteration"; \
	python3 scripts/release/signing.py verify-release "$$RELEASE_VERSION"

.PHONY: create-release-tags _create-release-tags
create-release-tags: release-check-version
	+$(if $(RELEASE_DRY_RUN),,python3 scripts/release/signing.py exec )$(MAKE) _create-release-tags

_create-release-tags: release-check-version
	@set -eu; VERSION=$$RELEASE_VERSION; \
	commit=$$(git rev-parse HEAD); \
	if [ "$$(git show -s --format=%s HEAD)" != "Release $$VERSION" ]; then \
		echo "HEAD must be the signed release commit for $$VERSION" >&2; exit 1; \
	fi; \
	python3 scripts/release/signing.py verify-commit "$$commit"; \
	for tag in "$$VERSION" "shared/$$VERSION" "sdkv1/$$VERSION" "sdkv2/$$VERSION"; do \
		if ! git show-ref --verify --quiet "refs/tags/$$tag"; then \
			git tag -s "$$tag" -m "Release $$tag"; \
		fi; \
		python3 scripts/release/signing.py verify-tag "$$tag" "$$commit"; \
	done

.PHONY: push-release-commits
push-release-commits: release-check-user
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
	python3 scripts/release/signing.py verify-release "$$VERSION"; \
	git -c credential.helper= -c 'credential.helper=!gh auth git-credential' \
		push --atomic origin HEAD:refs/heads/main \
		"refs/tags/shared/$$VERSION" "refs/tags/sdkv1/$$VERSION" \
		"refs/tags/sdkv2/$$VERSION" "refs/tags/$$VERSION"

.PHONY: test-release-notes
test-release-notes:
	npm ci --ignore-scripts --prefix .github
	node --test .github/tests/*.test.cjs

.PHONY: test-release-signing
test-release-signing: test-release-notes
	python3 -B -m unittest discover -s scripts/release -p 'test_*.py' -v

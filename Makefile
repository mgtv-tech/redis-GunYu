
### definition
BINARY_NAME := "redisGunYu"
ARTIFACT_ROOT ?= $(CURDIR)/.artifacts/tests
COVERAGE_MIN ?= 30.5


GOPATHVAR=${GOPATH}
SHELL=/usr/bin/env bash

### version,branch,commit,date,changes,
VERSION := ""
VERSIONCMD = "`git describe --exact-match --tags $(git log -n1 --pretty='%h') 2>/dev/null`"
VERSION := $(shell echo $(VERSIONCMD))
ifeq ($(strip $(VERSION)),)
   BRANCHCMD := "`git describe --contains --all HEAD`-`git rev-parse HEAD`"
   VERSION = $(shell echo $(BRANCHCMD))
else
   TAGCMD := "`git describe --exact-match --tags $(git log -n1 --pretty='%h') 2>/dev/null`-`git rev-parse HEAD`"
   VERSION =  $(shell echo $(TAGCMD))
endif
VERSION ?= $(VERSION)


BRANCHCMD := "`git describe --contains --all HEAD`"
BRANCH := $(shell echo $(BRANCHCMD))
BRANCH  ?= $(BRANCH)
COMMITCMD = "`git rev-parse HEAD`"
COMMIT := $(shell echo $(COMMITCMD))
DATE := $(shell echo `date +%FT%T%z`)
CHANGES := $(shell echo `git status --porcelain | wc -l`)
ifneq ($(strip $(CHANGES)), 0)
        VERSION := dirty-build-$(VERSION)
        COMMIT := dirty-build-$(COMMIT)
endif

REMOVESYMBOL := -w -s
ifeq (true, $(DEBUG))
        REMOVESYMBOL =
        GCFLAGS=-gcflags=all="-N -l "
endif
LDFLAGSPREFIX := "github.com/mgtv-tech/redis-GunYu/pkg"
LDFLAGS += -X $(LDFLAGSPREFIX)/version.version=$(VERSION) -X $(LDFLAGSPREFIX)/version.date=$(DATE) -X $(LDFLAGSPREFIX)/version.commit=$(COMMIT) -X $(LDFLAGSPREFIX)/version.branch=$(BRANCH) $(REMOVESYMBOL)


### build

.PHONY: build
build: tidy
	go build -ldflags "$(LDFLAGS)" $(GCFLAGS) -o $(BINARY_NAME) main.go
	@echo -e "\033[32mbuild $(BINARY_NAME) successfully\033[0m"


.PHONY: tidy
tidy:
	go mod tidy -v


.PHONY: demo
demo:
	docker build -t redisgunyu-demo -f docker/demo/Dockerfile .


.PHONY: allos
allos:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -ldflags "$(LDFLAGS)" $(GCFLAGS) -o $(BINARY_NAME)".darwin-amd64" main.go
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -ldflags "$(LDFLAGS)" $(GCFLAGS) -o $(BINARY_NAME)".darwin-arm64" main.go
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" $(GCFLAGS) -o $(BINARY_NAME)".linux-amd64" main.go
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags "$(LDFLAGS)" $(GCFLAGS) -o $(BINARY_NAME)".win-amd64" main.go
	@echo -e "\033[32mbuild $(BINARY_NAME) successfully\033[0m"


### test

.PHONY: test-static
test-static:
	@files="$$(find . -type f -name '*.go' \
		-not -path './vendor/*' -not -path './.artifacts/*' -not -path './.tools/*')"; \
		test -z "$$(gofmt -l $$files)" || \
		{ echo "gofmt is required for:"; gofmt -l $$files; exit 1; }
	go vet ./...
	go build ./...
	@for file in $$(find tests -type f -name '*.sh'); do bash -n "$$file" || exit 1; done
	bash ./tests/integration/test_portability.sh

.PHONY: test-prereqs
test-prereqs:
	@bash -c 'source ./tests/bisync/lib/redis_env.sh; require_test_commands'

.PHONY: test-unit
test-unit:
	mkdir -p "$(ARTIFACT_ROOT)/unit"
	go test ./... -count=1 -coverprofile="$(ARTIFACT_ROOT)/unit/coverage.out"
	go tool cover -func="$(ARTIFACT_ROOT)/unit/coverage.out" > "$(ARTIFACT_ROOT)/unit/coverage.txt"

.PHONY: test-race
test-race:
	go test -race -count=1 ./pkg/io/pipe ./pkg/store ./pkg/redis/... ./syncer

.PHONY: test-race-all
test-race-all:
	go test -race -count=1 ./...

.PHONY: test-integration
test-integration: test-prereqs
	ARTIFACT_ROOT="$(ARTIFACT_ROOT)/integration" bash ./tests/integration/run_go_integration.sh

.PHONY: test-e2e-smoke
test-e2e-smoke: test-prereqs
	ARTIFACT_ROOT="$(ARTIFACT_ROOT)/e2e-smoke" bash ./tests/integration/run_e2e_smoke.sh

.PHONY: test-nightly
test-nightly: test-prereqs
	NIGHTLY_SUITE=nonbisync-core bash ./tests/integration/run_nightly.sh
	NIGHTLY_SUITE=bisync-core bash ./tests/integration/run_nightly.sh

.PHONY: test-etcd
test-etcd: test-prereqs
	ENABLE_ETCD_TESTS=1 NIGHTLY_SUITE=etcd bash ./tests/integration/run_nightly.sh

.PHONY: test-release
test-release: test-prereqs test-static test-unit test-race-all test-integration test-e2e-smoke

.PHONY: test-coverage
test-coverage: test-unit
	@total=$$(tail -n 1 "$(ARTIFACT_ROOT)/unit/coverage.txt" | sed -n 's/.*[[:space:]]\([0-9][0-9.]*\)%$$/\1/p'); \
		awk -v actual="$$total" -v minimum="$(COVERAGE_MIN)" 'BEGIN { if (actual + 0 < minimum + 0) { printf "coverage %.1f%% is below %.1f%%\n", actual, minimum; exit 1 } printf "coverage %.1f%% (minimum %.1f%%)\n", actual, minimum }'

.PHONY: test-upgrade-rollback
test-upgrade-rollback:
	ARTIFACT_ROOT="$(ARTIFACT_ROOT)/upgrade-rollback" bash ./tests/integration/run_upgrade_rollback.sh

.PHONY: test-clean
test-clean:
	@if [[ -z "$(ARTIFACT_ROOT)" || "$(ARTIFACT_ROOT)" != "$(CURDIR)/.artifacts/tests"* ]]; then \
		echo "refusing to clean ARTIFACT_ROOT=$(ARTIFACT_ROOT)" >&2; exit 1; \
	fi
	@if [[ -d "$(ARTIFACT_ROOT)" ]]; then \
		find "$(ARTIFACT_ROOT)" -type f -delete; \
		find "$(ARTIFACT_ROOT)" -depth -type d -empty -delete; \
	fi

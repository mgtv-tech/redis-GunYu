
### definition
BINARY_NAME := "redisGunYu"


GOPATHVAR=${GOPATH}
SHELL=/usr/bin/env bash

### version,branch,commit,date,changes,
VERSION := ""
VERSIONCMD = "`git describe --exact-match --tags $(git log -n1 --pretty='%h')`"
VERSION := $(shell echo $(VERSIONCMD))
ifeq ($(strip $(VERSION)),)
   BRANCHCMD := "`git describe --contains --all HEAD`-`git rev-parse HEAD`"
   VERSION = $(shell echo $(BRANCHCMD))
else
   TAGCMD := "`git describe --exact-match --tags $(git log -n1 --pretty='%h')`-`git rev-parse HEAD`"
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
LDFLAGSPREFIX := "github.com/ikenchina/redis-GunYu/pkg"
LDFLAGS += -X $(LDFLAGSPREFIX)/version.version=$(VERSION) -X $(LDFLAGSPREFIX)/version.date=$(DATE) -X $(LDFLAGSPREFIX)/version.commit=$(COMMIT) -X $(LDFLAGSPREFIX)/version.branch=$(BRANCH) $(REMOVESYMBOL)


### build

.PHONY: build

build: tidy
	go build -ldflags "$(LDFLAGS)" $(GCFLAGS) -o $(BINARY_NAME) main.go
	@echo -e "\033[32mbuild $(BINARY_NAME) successfully\033[0m"


.PHONY: tidy
tidy:
	go mod tidy -v

# Version announced to peers. Taken from an exact Git tag on HEAD, so a
# checkout of v2.1.0 builds a binary announcing 2.1.0. Off a tag it is empty,
# which keeps the value compiled into internal/tenderseed.Version. CI and the
# Dockerfile pass it explicitly, which takes precedence over this default.
VERSION ?= $(shell git describe --tags --exact-match 2>/dev/null | sed "s/^v//")
LDFLAGS :=
ifneq ($(VERSION),)
LDFLAGS := -X github.com/AviaOne/tenderseed/internal/tenderseed.Version=$(VERSION)
endif

all: build

# build binaries for current platform
build: build/tenderseed

build/tenderseed: cmd/tenderseed/main.go $(wildcard internal/**/*.go) go.mod
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o ./build/tenderseed ./cmd/tenderseed

# build linux binaries
build-linux: build/tenderseed.elf

build/tenderseed.elf: cmd/tenderseed/main.go $(wildcard internal/**/*.go) go.mod
	CGO_ENABLED=0 GOOS=linux go build -ldflags "$(LDFLAGS)" -o ./build/tenderseed.elf ./cmd/tenderseed

test:
	go test ./...

lint:
	@echo "--> Running linter"
	@golangci-lint run ./...

# run the bench: make bench HOME_DIR=/path/to/home SECONDS=300
bench: build
	@scripts/bench.sh $(HOME_DIR) $(SECONDS)

clean:
	rm -rf build

.PHONY: all bench clean test lint build-linux build

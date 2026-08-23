# Version announced to peers. Empty means the value compiled into
# internal/tenderseed.Version. Release builds pass the Git tag.
VERSION ?=
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

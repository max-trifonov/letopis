VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w \
	-X github.com/max-trifonov/letopis/internal/version.Version=$(VERSION) \
	-X github.com/max-trifonov/letopis/internal/version.Commit=$(COMMIT) \
	-X github.com/max-trifonov/letopis/internal/version.Date=$(DATE)

IMAGE ?= ghcr.io/max-trifonov/letopis

LOAD_COMPOSE := docker compose -f test/load/docker-compose.load.yml

.PHONY: build run test test-integration bench lint proto tidy docker \
	load-up load-down load-durable load-fast load-strict load-overload

build:
	go build -trimpath -ldflags "$(LDFLAGS)" -o bin/letopis ./cmd/letopis

run: build
	./bin/letopis serve --config config.example.yaml

test:
	go test -race ./...

test-integration:
	go test -tags=integration -race ./...

# Pipeline/diff CPU benchmarks (S2-07). -race is intentionally off: it distorts
# timings.
bench:
	go test -run '^$$' -bench . -benchmem ./internal/diff/ ./internal/service/

# Load test (S2-07). load-up builds the service image and brings up the stack;
# the per-mode targets run a k6 scenario against it (results in
# test/load/results/). See test/load/README.md.
load-up:
	$(LOAD_COMPOSE) up -d --build letopis

load-down:
	$(LOAD_COMPOSE) down -v

load-durable:
	$(LOAD_COMPOSE) run --rm k6 run /scripts/durable.js

load-fast:
	$(LOAD_COMPOSE) run --rm k6 run /scripts/fast.js

load-strict:
	$(LOAD_COMPOSE) run --rm k6 run /scripts/strict.js

load-overload:
	$(LOAD_COMPOSE) run --rm k6 run /scripts/overload.js

lint:
	golangci-lint run
	buf lint

proto:
	buf generate

tidy:
	go mod tidy

docker:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg DATE=$(DATE) \
		-t $(IMAGE):$(VERSION) -t $(IMAGE):latest .

# qsrv. Start with:  make setup  (asks everything), then  make up.
# make help lists every target.

.DEFAULT_GOAL := help
SHELL := bash
GO ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

# Pick up the mode chosen by make setup (binary, docker or monitoring).
-include .qsrv/config.env

##@ Run qsrv

.PHONY: setup
setup: ## Ask what qsrv needs (how to run it, ports, storage, backups, API keys) and write .qsrv/
	@scripts/setup.sh

.PHONY: up
up: $(if $(filter binary,$(MODE)),build) $(if $(filter qsrv:local,$(IMAGE)),image) ## Start qsrv as configured, in the background
	@scripts/qsrv.sh up

.PHONY: down
down: ## Stop qsrv (pending writes are committed first)
	@scripts/qsrv.sh down

.PHONY: restart
restart: $(if $(filter binary,$(MODE)),build) ## Stop and start again (applies a new make setup)
	@scripts/qsrv.sh restart

.PHONY: run
run: build ## Run in the foreground (binary mode), Ctrl-C to stop
	@scripts/qsrv.sh run

.PHONY: status
status: ## Is it up? And its URLs
	@scripts/qsrv.sh status

.PHONY: logs
logs: ## Follow the logs
	@scripts/qsrv.sh logs

.PHONY: keys
keys: ## Show the API keys
	@scripts/qsrv.sh keys

.PHONY: add-key
add-key: ## New API key without restarting: make add-key TENANT=acme NAME=billing (TENANT='*' = admin)
	@scripts/qsrv.sh add-key "$(TENANT)" "$(NAME)"

##@ Build

.PHONY: build
build: ## Build bin/qsrv, bin/qctl and bin/qbench (pure Go, static)
	@mkdir -p bin
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/ ./cmd/...

.PHONY: build-cgo
build-cgo: ## Build bin/qsrv with native SQLite (cgo, static; needs a C compiler)
	@mkdir -p bin
	CGO_ENABLED=1 $(GO) build -trimpath -tags sqlite_cgo,sqlite_omit_load_extension,netgo,osusergo \
	  -ldflags "$(LDFLAGS) -linkmode external -extldflags -static" -o bin/qsrv ./cmd/qsrv

.PHONY: image
image: ## Build the Docker image qsrv:local
	docker build --build-arg VERSION=$(VERSION) -t qsrv:local .

##@ Develop

.PHONY: test
test: ## Tests with -race, WASM driver
	$(GO) test -race ./...

.PHONY: test-cgo
test-cgo: ## Tests with -race, cgo driver
	$(GO) test -race -tags sqlite_cgo ./...

.PHONY: crash-test
crash-test: ## Kill qsrv with SIGKILL under load and check nothing confirmed is lost
	$(GO) test -tags e2e -run Crash -v ./internal/e2e/

.PHONY: lint
lint: ## gofmt and go vet, both drivers
	@test -z "$$(gofmt -l .)" || { echo "not gofmt-ed:"; gofmt -l .; exit 1; }
	$(GO) vet ./...
	$(GO) vet -tags sqlite_cgo ./...

.PHONY: bench
bench: ## Go benchmarks
	$(GO) test -run '^$$' -bench . -benchmem ./...

.PHONY: dashboards
dashboards: ## Regenerate the Grafana dashboards from deploy/grafana/generate.py
	python3 deploy/grafana/generate.py deploy/grafana/dashboards

.PHONY: clean
clean: ## Remove bin/ (keeps .qsrv/ and data)
	rm -rf bin

.PHONY: help
help: ## This list
	@awk 'BEGIN {FS = ":.*## "} \
	  /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } \
	  /^[a-zA-Z_-]+:.*## / { printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo
	@if [ -f .qsrv/config.env ]; then echo "Configured: mode $(MODE). make up to start, make status to check."; \
	  else echo "Not set up yet: run make setup."; fi

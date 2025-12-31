# Convex Cluster Manager Makefile

# Binary name
BINARY_NAME=convex-cluster-manager

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOCLEAN=$(GOCMD) clean
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOVET=$(GOCMD) vet
GOFMT=$(GOCMD) fmt

# Build directory
BUILD_DIR=./bin

# Version info
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

# Linker flags
LDFLAGS=-ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildTime=$(BUILD_TIME)"

# Default target
.DEFAULT_GOAL := build

.PHONY: all build build-linux clean test test-short test-coverage coverage lint fmt vet tidy install help \
        test-e2e test-failover test-failover-basic test-failover-graceful test-failover-rapid \
        test-failover-data-integrity test-failover-network test-failover-all

## help: Show this help message
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@sed -n 's/^## //p' $(MAKEFILE_LIST) | column -t -s ':'

## all: Run tests and build
all: test build

## build: Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/convex-cluster-manager

## build-linux-amd64: Build for Linux amd64
build-linux-amd64:
	@echo "Building $(BINARY_NAME) for Linux amd64..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 ./cmd/convex-cluster-manager

## build-linux-arm64: Build for Linux arm64
build-linux-arm64:
	@echo "Building $(BINARY_NAME) for Linux arm64..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-arm64 ./cmd/convex-cluster-manager

## build-linux: Build for both Linux architectures
build-linux: build-linux-amd64 build-linux-arm64

## clean: Clean build artifacts
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html
	rm -rf ./test-cluster-e2e-output

## test: Run all unit tests
test:
	@echo "Running tests..."
	$(GOTEST) -v ./...

## test-short: Run tests (skip integration tests)
test-short:
	@echo "Running short tests..."
	$(GOTEST) -v -short ./...

## test-coverage: Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -v -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -func=coverage.out

## coverage: Generate HTML coverage report
coverage: test-coverage
	@echo "Generating coverage report..."
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

## lint: Run golangci-lint (requires golangci-lint to be installed)
lint:
	@echo "Running linter..."
	@which golangci-lint > /dev/null || (echo "golangci-lint not installed. Run: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest" && exit 1)
	golangci-lint run ./...

## fmt: Format code
fmt:
	@echo "Formatting code..."
	$(GOFMT) ./...

## vet: Run go vet
vet:
	@echo "Running go vet..."
	$(GOVET) ./...

## tidy: Tidy go modules
tidy:
	@echo "Tidying modules..."
	$(GOMOD) tidy

## install: Install the binary to GOPATH/bin
install:
	@echo "Installing $(BINARY_NAME)..."
	$(GOCMD) install $(LDFLAGS) ./cmd/convex-cluster-manager

## check: Run all checks (fmt, vet, lint, test)
check: fmt vet lint test

# ============================================
# E2E and Failover Test Targets
# ============================================

## test-e2e: Run the cluster e2e test (sets up a 2-node cluster with real backend)
test-e2e:
	@echo "Running cluster e2e test..."
	./scripts/test-cluster-e2e.sh

## test-e2e-keep: Run the cluster e2e test and keep containers running
test-e2e-keep:
	@echo "Running cluster e2e test (keeping containers)..."
	./scripts/test-cluster-e2e.sh --keep

## test-failover: Run all failover tests (alias for test-failover-all)
test-failover: test-failover-all

## test-failover-all: Run the complete failover test suite
test-failover-all:
	@echo "Running complete failover test suite..."
	./scripts/test-cluster-failover-all.sh

## test-failover-basic: Run basic failover test (kill primary, verify passive takes over)
test-failover-basic:
	@echo "Running basic failover test..."
	./scripts/test-cluster-failover-basic.sh

## test-failover-graceful: Run graceful failover test (demote command)
test-failover-graceful:
	@echo "Running graceful failover test..."
	./scripts/test-cluster-failover-graceful.sh

## test-failover-rapid: Run rapid failover stress test
test-failover-rapid:
	@echo "Running rapid failover test..."
	./scripts/test-cluster-failover-rapid.sh

## test-failover-data-integrity: Run data integrity failover test
test-failover-data-integrity:
	@echo "Running data integrity failover test..."
	./scripts/test-cluster-failover-data-integrity.sh

## test-failover-network: Run network partition failover test
test-failover-network:
	@echo "Running network partition failover test..."
	./scripts/test-cluster-failover-network.sh

## docker-build: Build Docker images for cluster e2e testing
docker-build:
	@echo "Building Docker images..."
	docker build -t convex-cluster-e2e:latest -f docker/Dockerfile.cluster-e2e docker/

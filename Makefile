# Sensor Alert Engine
# Makefile for building, testing, and deploying

BINARY := alert-engine
GOOS ?= linux
GOARCH ?= amd64
DEPLOY_HOST ?= <services-tailscale-ip>
DEPLOY_USER ?= <user>
REMOTE_BIN := /usr/local/bin/$(BINARY)
REMOTE_CONFIG := /etc/alert-engine/rules.yaml
SERVICE := alert-engine.service

# Container image. Must match alert_engine_image in the Ansible role
# (tools/ansible/roles/alert-engine/vars/main.yml) -- the role resolves the
# tag from alert_engine_version, so a mismatch here deploys the wrong thing.
#
# The deploy target is amd64 (services LXC on Proxmox); this is built
# explicitly rather than inherited from the build host, which is arm64.
IMAGE := ghcr.io/trv-enterprises/$(BINARY)
IMAGE_PLATFORM ?= linux/amd64

# Image tags carry no leading "v" even though git tags do -- GHCR convention.
# Pass the git tag and it is stripped: VERSION=v0.2.0-rc.8 -> 0.2.0-rc.8
VERSION ?=
IMAGE_TAG := $(patsubst v%,%,$(VERSION))

SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ Development

.PHONY: fmt
fmt: ## Run go fmt
	go fmt ./...

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: test
test: fmt vet ## Run tests with coverage
	go test ./... -coverprofile cover.out
	@echo "Coverage report: cover.out"

.PHONY: run
run: build-local ## Build and run locally
	./$(BINARY) -config rules.yaml

.PHONY: build-local
build-local: ## Build for local platform
	go build -o $(BINARY) ./cmd/alert-engine/

##@ Build

.PHONY: build
build: ## Build linux binary (CGO_ENABLED=0)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build \
		-ldflags="-s -w" \
		-o $(BINARY) ./cmd/alert-engine/

.PHONY: docker-build
docker-build: ## Build Docker image (local tag, no registry)
	docker build --platform $(IMAGE_PLATFORM) -t $(BINARY):latest .

.PHONY: docker-push
docker-push: require-version ## Build and push VERSION=x.y.z to GHCR
	@echo "Building $(IMAGE):$(IMAGE_TAG) for $(IMAGE_PLATFORM)"
	docker build --platform $(IMAGE_PLATFORM) -t $(IMAGE):$(IMAGE_TAG) .
	@# Requires a GHCR login with write:packages on the trv-enterprises org:
	@#   echo $$TRVE_GH_TOKEN | docker login ghcr.io -u <user> --password-stdin
	docker push $(IMAGE):$(IMAGE_TAG)
	@echo
	@echo "Pushed $(IMAGE):$(IMAGE_TAG)"
	@echo "Deploy with: make deploy-alert-engine ALERT_ENGINE_VERSION=$(IMAGE_TAG)  (from homelab-deploy)"

.PHONY: require-version
require-version:
	@if [ -z "$(VERSION)" ]; then \
		echo "VERSION is required, e.g. make docker-push VERSION=v0.2.0-rc.8"; \
		exit 1; \
	fi

##@ Deploy

.PHONY: deploy
deploy: build ## Build and deploy to services LXC
	scp $(BINARY) $(DEPLOY_USER)@$(DEPLOY_HOST):/tmp/$(BINARY)
	scp rules.yaml $(DEPLOY_USER)@$(DEPLOY_HOST):/tmp/rules.yaml
	scp $(SERVICE) $(DEPLOY_USER)@$(DEPLOY_HOST):/tmp/$(SERVICE)
	ssh $(DEPLOY_USER)@$(DEPLOY_HOST) '\
		sudo mv /tmp/$(BINARY) $(REMOTE_BIN) && \
		sudo chmod +x $(REMOTE_BIN) && \
		sudo mkdir -p /etc/alert-engine && \
		sudo mv /tmp/rules.yaml $(REMOTE_CONFIG) && \
		sudo mv /tmp/$(SERVICE) /etc/systemd/system/$(SERVICE) && \
		sudo systemctl daemon-reload && \
		sudo systemctl enable $(SERVICE) && \
		sudo systemctl restart $(SERVICE)'
	@echo "Deployed and started $(SERVICE)"

.PHONY: deploy-config
deploy-config: ## Deploy config only and reload (SIGHUP)
	scp rules.yaml $(DEPLOY_USER)@$(DEPLOY_HOST):/tmp/rules.yaml
	ssh $(DEPLOY_USER)@$(DEPLOY_HOST) '\
		sudo mv /tmp/rules.yaml $(REMOTE_CONFIG) && \
		sudo systemctl reload $(SERVICE)'
	@echo "Config deployed and reloaded"

##@ Remote Operations

.PHONY: logs
logs: ## View remote logs (follow mode)
	ssh $(DEPLOY_USER)@$(DEPLOY_HOST) 'sudo journalctl -u $(SERVICE) -f'

.PHONY: status
status: ## Show remote service status
	ssh $(DEPLOY_USER)@$(DEPLOY_HOST) 'sudo systemctl status $(SERVICE)'

.PHONY: restart
restart: ## Restart remote service
	ssh $(DEPLOY_USER)@$(DEPLOY_HOST) 'sudo systemctl restart $(SERVICE)'

.PHONY: stop
stop: ## Stop remote service
	ssh $(DEPLOY_USER)@$(DEPLOY_HOST) 'sudo systemctl stop $(SERVICE)'

##@ Helpers

.PHONY: clean
clean: ## Remove build artifacts
	rm -f $(BINARY) cover.out

.PHONY: deps
deps: ## Download dependencies
	go mod download
	go mod tidy

.PHONY: help
help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

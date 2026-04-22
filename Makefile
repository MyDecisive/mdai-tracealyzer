KIND_CLUSTER_NAME ?= greptime
KUBECTL_CONTEXT   ?= kind-$(KIND_CLUSTER_NAME)
NAMESPACE         ?= mdai
DOCKER_TAG        ?= 0.1.0
CHART_VERSION     ?= $(shell git describe --tags --abbrev=0 2>/dev/null | sed 's/^v//')
ifeq ($(CHART_VERSION),)
CHART_VERSION := 0.1.0
endif
BUILD_PLATFORMS   ?= linux/arm64,linux/amd64
GOTOOLCHAIN       ?= go1.25.0
TEST_GATEWAY_NAME ?= datadog-agent-test-gateway
TEST_GATEWAY_SAMPLE ?= collector-datadog-agent-test-gateway.yaml
TEST_STAND_DIR    ?= test-stand
DEMO_CONTROL_URL  ?= http://localhost:8081
DEMO_SCENARIO     ?= browse

KUBECTL               = kubectl --context=$(KUBECTL_CONTEXT) -n $(NAMESPACE)
GO                    = CGO_ENABLED=0 GOTOOLCHAIN=$(GOTOOLCHAIN) go
GO_TEST               = $(GO) test -count=1
AWS_ECR_REPO          = public.ecr.aws/decisiveai
REPO_NAME             = mdai-tracealyzer
DOCKER_IMAGE          = $(AWS_ECR_REPO)/$(REPO_NAME):$(DOCKER_TAG)
TEST_GATEWAY_MANIFEST = samples/$(TEST_GATEWAY_SAMPLE)
TEST_GATEWAY_SERVICE  = $(TEST_GATEWAY_NAME)-collector
COMPOSE               = docker compose -f $(TEST_STAND_DIR)/docker-compose.yaml
TRACKING_ID_NAMESPACE_REWRITE = sed -E 's|(argocd.argoproj.io/tracking-id: [^:]+:opentelemetry.io/OpenTelemetryCollector:)[^/]+/|\1$(NAMESPACE)/|'

define apply_manifest
$(TRACKING_ID_NAMESPACE_REWRITE) $(1) | $(KUBECTL) apply -f -
endef

.PHONY: build test testv test-race cover coverv coverhtml clean-coverage tidy tidy-check \
	docker-login docker-build docker-push docker-build-local kind-load deploy-local redeploy \
	logs metrics-forward \
	helm helm-package helm-publish status \
	demo-deploy-gateway demo-delete-gateway demo-port-forward \
	demo-up demo-down demo-emit demo-agent-logs demo-gateway-logs

build: tidy
	$(GO) build -trimpath -ldflags="-w -s" -o mdai-tracealyzer ./cmd/mdai-tracealyzer

test: tidy
	$(GO_TEST) ./...

testv: tidy
	$(GO_TEST) -v ./...

test-race: tidy
	CGO_ENABLED=1 GOTOOLCHAIN=$(GOTOOLCHAIN) go test -count=1 -race -v ./...

cover: tidy
	$(GO_TEST) -cover ./...

coverv: tidy
	$(GO_TEST) -v -cover ./...

coverhtml:
	@trap 'rm -f coverage.out' EXIT; \
	$(GO_TEST) -coverprofile=coverage.out ./... && \
	$(GO) tool cover -html=coverage.out -o coverage.html && \
	( open coverage.html || xdg-open coverage.html )

clean-coverage:
	@rm -f coverage.out coverage.html

tidy:
	@$(GO) mod tidy

tidy-check:
	@$(GO) mod tidy -diff

docker-login:
	aws ecr-public get-login-password | docker login --username AWS --password-stdin $(AWS_ECR_REPO)

docker-build: tidy
	docker buildx build --platform $(BUILD_PLATFORMS) -t $(DOCKER_IMAGE) . --load

docker-push: tidy docker-login
	docker buildx build --platform $(BUILD_PLATFORMS) -t $(DOCKER_IMAGE) . --push

docker-build-local: tidy
	docker build -t $(DOCKER_IMAGE) .

kind-load: docker-build-local
	kind load docker-image $(DOCKER_IMAGE) --name $(KIND_CLUSTER_NAME)

deploy-local: kind-load
	helm upgrade --install mdai-tracealyzer ./deployment \
		--kube-context $(KUBECTL_CONTEXT) \
		--namespace $(NAMESPACE) \
		--create-namespace \
		--set image.tag=$(DOCKER_TAG) \
		--set image.pullPolicy=Never \
		--set config.service.logLevel=debug
	$(KUBECTL) rollout restart deployment/mdai-tracealyzer

redeploy: kind-load
	$(KUBECTL) rollout restart deployment/mdai-tracealyzer

logs:
	$(KUBECTL) logs -f deployment/mdai-tracealyzer

metrics-forward:
	$(KUBECTL) port-forward deployment/mdai-tracealyzer 9090:9090

helm:
	@echo "Usage: make helm-<command>"
	@echo "Available commands:"
	@echo "  helm-package   Package the Helm chart"
	@echo "  helm-publish   Publish the Helm chart"

helm-package:
	@echo "Packaging Helm chart..."
	@helm package -u --version $(CHART_VERSION) --app-version $(CHART_VERSION) ./deployment > /dev/null

helm-publish: CHART_NAME := $(REPO_NAME)
helm-publish: CHART_REPO := git@github.com:MyDecisive/mdai-helm-charts.git
helm-publish: CHART_PACKAGE := $(CHART_NAME)-$(CHART_VERSION).tgz
helm-publish: BASE_BRANCH := gh-pages
helm-publish: TARGET_BRANCH := $(CHART_NAME)-v$(CHART_VERSION)
helm-publish: CLONE_DIR := $(shell mktemp -d /tmp/mdai-helm-charts.XXXXXX)
helm-publish: REPO_DIR := $(shell pwd)
helm-publish: helm-package
	@echo "Cloning $(CHART_REPO)..."
	@rm -rf $(CLONE_DIR)
	@git clone -q --branch $(BASE_BRANCH) $(CHART_REPO) $(CLONE_DIR)

	@echo "Creating branch $(TARGET_BRANCH) from $(BASE_BRANCH)..."
	@cd $(CLONE_DIR) && git checkout -q -b $(TARGET_BRANCH)

	@echo "Copying and indexing chart..."
	@cd $(CLONE_DIR) && \
		helm repo index $(REPO_DIR) --merge index.yaml && \
		mv $(REPO_DIR)/$(CHART_PACKAGE) $(CLONE_DIR)/ && \
		mv $(REPO_DIR)/index.yaml $(CLONE_DIR)/

	@echo "Committing changes..."
	@cd $(CLONE_DIR) && \
		git add $(CHART_PACKAGE) index.yaml && \
		git commit -q -m "chore: publish $(CHART_PACKAGE)" && \
		git push -q origin $(TARGET_BRANCH) && \
		rm -rf $(CLONE_DIR)

	@echo "Chart published"

demo-deploy-gateway:
	$(call apply_manifest,$(TEST_GATEWAY_MANIFEST))

demo-delete-gateway:
	$(KUBECTL) delete -f $(TEST_GATEWAY_MANIFEST) --ignore-not-found

demo-port-forward:
	$(KUBECTL) port-forward service/$(TEST_GATEWAY_SERVICE) 18126:8126

demo-up:
	$(COMPOSE) up --build -d

demo-down:
	$(COMPOSE) down --remove-orphans

demo-emit:
	curl -sS -X POST $(DEMO_CONTROL_URL)/emit -H 'Content-Type: application/json' -d '{"scenario":"$(DEMO_SCENARIO)"}'

demo-gateway-logs:
	$(KUBECTL) logs -l app=$(TEST_GATEWAY_NAME) --tail=200 -f

demo-agent-logs:
	$(COMPOSE) logs -f agent

status:
	$(KUBECTL) get opentelemetrycollectors

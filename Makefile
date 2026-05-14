# =============================================================================
# Configuration (override via env or `make VAR=value`)
# =============================================================================

# Kind cluster + namespaces.
# NAMESPACE holds tracealyzer and (in Mode A) the in-project sample collector.
# DEMO_NAMESPACE holds the demo apps and the Datadog agent DaemonSet.
KIND_CLUSTER_NAME ?= mdai
KUBECTL_CONTEXT   ?= kind-$(KIND_CLUSTER_NAME)
NAMESPACE         ?= mdai
DEMO_NAMESPACE    ?= test-stand-demo

# Build / release.
DOCKER_TAG        ?= 0.1.2
BUILD_PLATFORMS   ?= linux/arm64,linux/amd64
GOTOOLCHAIN       ?= go1.25.0
LOCAL_VALUES      ?= deployment/values-local.yaml
CHART_VERSION     ?= $(shell git describe --tags --abbrev=0 2>/dev/null | sed 's/^v//')
ifeq ($(CHART_VERSION),)
CHART_VERSION := 0.1.2
endif

# Mode A sample collector. Apply path is `kubectl apply -k samples/`; to
# deploy a renamed instance, add a kustomize overlay rather than editing the
# base. The default forwarder URL targets the Service the OTel Operator
# creates for the base CR.
TEST_GATEWAY_KUSTOMIZE_DIR ?= samples
TEST_GATEWAY_NAME           = datadog-agent-test-gateway

# Datadog agent forwarder URL substituted into test-stand/deployment/datadog-agent.yaml.
# Default points at the Mode A collector's Service; override for Mode B.
DEMO_DATADOG_FORWARDER_URL ?= http://$(TEST_GATEWAY_NAME)-collector.$(NAMESPACE).svc.cluster.local:8126

# Datadog API-key Secret. Sourced from $(NAMESPACE) and copied into
# $(DEMO_NAMESPACE) by demo-apply (Pod secretKeyRef cannot cross namespaces).
# The Secret must have an `api-key` data field.
DEMO_DATADOG_SECRET_NAME ?= test-vv-integration-secret

# Demo scenario for one-shot emits.
DEMO_SCENARIO ?= browse

# Load generator knobs substituted into test-stand/deployment/load-generator.yaml.
# Empty values for LOAD_MIX / LOAD_ERROR_RATE_PCT are honored (load-generator
# treats them as unset). Override per-invocation via `make demo-load-up VAR=…`.
DEMO_LOAD_RPS             ?= 10
DEMO_LOAD_PROFILE         ?= demo
DEMO_LOAD_MIX             ?=
DEMO_LOAD_ERROR_RATE_PCT  ?=

# =============================================================================
# Derived values
# =============================================================================

AWS_ECR_REPO       = public.ecr.aws/decisiveai
REPO_NAME          = mdai-tracealyzer
DOCKER_IMAGE       = $(AWS_ECR_REPO)/$(REPO_NAME):$(DOCKER_TAG)

GO                 = CGO_ENABLED=0 GOTOOLCHAIN=$(GOTOOLCHAIN) go
GO_TEST            = $(GO) test -count=1
KUBECTL            = kubectl --context=$(KUBECTL_CONTEXT) -n $(NAMESPACE)
DEMO_KUBECTL       = kubectl --context=$(KUBECTL_CONTEXT) -n $(DEMO_NAMESPACE)

TEST_STAND_DIR     = test-stand
DEMO_DEPLOY_DIR    = $(TEST_STAND_DIR)/deployment
DEMO_SVC_IMAGE     = test-stand-demo-svc:local
DEMO_LOADGEN_IMAGE = test-stand-load-generator:local

DEMO_CORE_MANIFESTS = \
	$(DEMO_DEPLOY_DIR)/postgres-init.yaml \
	$(DEMO_DEPLOY_DIR)/stateful.yaml \
	$(DEMO_DEPLOY_DIR)/demo-services.yaml

DEMO_ROLE_DEPLOYMENTS = \
	deployment/catalog-api \
	deployment/payments-api \
	deployment/inventory-http-api \
	deployment/inventory-grpc-service \
	deployment/checkout-api \
	deployment/gateway-api \
	deployment/notifier

define ensure_namespace
kubectl --context=$(KUBECTL_CONTEXT) create namespace $(1) --dry-run=client -o yaml | kubectl --context=$(KUBECTL_CONTEXT) apply -f -
endef

.PHONY: \
	build test test-race cover coverhtml tidy tidy-check \
	docker-login docker-build docker-build-local docker-push \
	kind-load deploy-local redeploy logs metrics-forward \
	helm-package helm-publish \
	demo-deploy-gateway demo-delete-gateway demo-collector-logs demo-collector-port-forward \
	demo-svc-kind-load demo-loadgen-kind-load demo-kind-load \
	demo-apply demo-apply-external demo-up demo-down demo-rollout \
	demo-load-up demo-load-down demo-load-logs demo-load-rollout \
	demo-emit demo-scenarios demo-agent-logs demo-app-port-forward

# =============================================================================
# Go: build, test, modules
# =============================================================================

build: tidy
	$(GO) build -trimpath -ldflags="-w -s" -o mdai-tracealyzer ./cmd/mdai-tracealyzer

test: tidy
	$(GO_TEST) ./...

test-race: tidy
	CGO_ENABLED=1 GOTOOLCHAIN=$(GOTOOLCHAIN) go test -count=1 -race ./...

cover: tidy
	$(GO_TEST) -cover ./...

coverhtml:
	@trap 'rm -f coverage.out' EXIT; \
	$(GO_TEST) -coverprofile=coverage.out ./... && \
	$(GO) tool cover -html=coverage.out -o coverage.html && \
	( open coverage.html || xdg-open coverage.html )

tidy:
	@$(GO) mod tidy

tidy-check:
	@$(GO) mod tidy -diff

# =============================================================================
# Docker image
# =============================================================================

docker-login:
	aws ecr-public get-login-password | docker login --username AWS --password-stdin $(AWS_ECR_REPO)

docker-build: tidy
	docker buildx build --platform $(BUILD_PLATFORMS) -t $(DOCKER_IMAGE) . --load

docker-build-local: tidy
	docker build -t $(DOCKER_IMAGE) .

docker-push: tidy docker-login
	docker buildx build --platform $(BUILD_PLATFORMS) -t $(DOCKER_IMAGE) . --push

# =============================================================================
# Local Helm deploy (kind)
# =============================================================================

kind-load: docker-build-local
	kind load docker-image $(DOCKER_IMAGE) --name $(KIND_CLUSTER_NAME)

deploy-local: kind-load
	helm upgrade --install mdai-tracealyzer ./deployment \
		--kube-context $(KUBECTL_CONTEXT) \
		--namespace $(NAMESPACE) \
		--create-namespace \
		-f $(LOCAL_VALUES) \
		--set image.tag=$(DOCKER_TAG)
	$(KUBECTL) rollout restart deployment/mdai-tracealyzer

redeploy: kind-load
	$(KUBECTL) rollout restart deployment/mdai-tracealyzer

logs:
	$(KUBECTL) logs -f deployment/mdai-tracealyzer

metrics-forward:
	$(KUBECTL) port-forward deployment/mdai-tracealyzer 9090:9090

# =============================================================================
# Helm chart release
# =============================================================================

helm-package:
	@helm package -u --version $(CHART_VERSION) --app-version $(CHART_VERSION) ./deployment > /dev/null

helm-publish: CHART_PACKAGE := $(REPO_NAME)-$(CHART_VERSION).tgz
helm-publish: TARGET_BRANCH := $(REPO_NAME)-v$(CHART_VERSION)
helm-publish: CLONE_DIR     := $(shell mktemp -d /tmp/mdai-helm-charts.XXXXXX)
helm-publish: REPO_DIR      := $(shell pwd)
helm-publish: helm-package
	@git clone -q --branch gh-pages git@github.com:MyDecisive/mdai-helm-charts.git $(CLONE_DIR)
	@cd $(CLONE_DIR) && git checkout -q -b $(TARGET_BRANCH)
	@cd $(CLONE_DIR) && \
		helm repo index $(REPO_DIR) --merge index.yaml && \
		mv $(REPO_DIR)/$(CHART_PACKAGE) $(CLONE_DIR)/ && \
		mv $(REPO_DIR)/index.yaml $(CLONE_DIR)/
	@cd $(CLONE_DIR) && \
		git add $(CHART_PACKAGE) index.yaml && \
		git commit -q -m "chore: publish $(CHART_PACKAGE)" && \
		git push -q origin HEAD
	@rm -rf $(CLONE_DIR)
	@echo "Chart published: $(CHART_PACKAGE)"

# =============================================================================
# Mode A: in-project sample collector (samples/collector-datadog-agent-test-gateway.yaml)
#
# Deploys an OpenTelemetryCollector named $(TEST_GATEWAY_NAME) into $(NAMESPACE).
# The OTel Operator creates Service $(TEST_GATEWAY_NAME)-collector, which is
# the default DEMO_DATADOG_FORWARDER_URL target.
# =============================================================================

demo-deploy-gateway:
	$(call ensure_namespace,$(NAMESPACE))
	$(KUBECTL) apply -k $(TEST_GATEWAY_KUSTOMIZE_DIR)

demo-delete-gateway:
	$(KUBECTL) delete -k $(TEST_GATEWAY_KUSTOMIZE_DIR) --ignore-not-found

demo-collector-logs:
	$(KUBECTL) logs -l app=$(TEST_GATEWAY_NAME) --tail=200 -f

demo-collector-port-forward:
	$(KUBECTL) port-forward service/$(TEST_GATEWAY_NAME)-collector 18126:8126

# =============================================================================
# Demo stack — build/load images, apply manifests, lifecycle
# =============================================================================

demo-svc-kind-load:
	docker build --build-arg SERVICE=demo-svc -t $(DEMO_SVC_IMAGE) $(TEST_STAND_DIR)/demo
	kind load docker-image $(DEMO_SVC_IMAGE) --name $(KIND_CLUSTER_NAME)

demo-loadgen-kind-load:
	docker build --build-arg SERVICE=load-generator -t $(DEMO_LOADGEN_IMAGE) $(TEST_STAND_DIR)/demo
	kind load docker-image $(DEMO_LOADGEN_IMAGE) --name $(KIND_CLUSTER_NAME)

demo-kind-load: demo-svc-kind-load demo-loadgen-kind-load

# demo-apply uses DEMO_DATADOG_FORWARDER_URL (Mode A default points at the
# in-project sample collector). demo-apply-external is a guarded entrypoint
# for Mode B that requires the URL to be set explicitly.
demo-apply:
	$(call ensure_namespace,$(NAMESPACE))
	$(call ensure_namespace,$(DEMO_NAMESPACE))
	@kubectl --context=$(KUBECTL_CONTEXT) -n $(NAMESPACE) get secret $(DEMO_DATADOG_SECRET_NAME) >/dev/null 2>&1 || { \
		echo "Source secret '$(DEMO_DATADOG_SECRET_NAME)' is missing in namespace $(NAMESPACE)."; \
		echo "Create it before re-running, e.g.:"; \
		echo "  kubectl --context=$(KUBECTL_CONTEXT) -n $(NAMESPACE) create secret generic $(DEMO_DATADOG_SECRET_NAME) --from-literal=api-key=<DD_API_KEY>"; \
		exit 1; \
	}
	@kubectl --context=$(KUBECTL_CONTEXT) -n $(NAMESPACE) get secret $(DEMO_DATADOG_SECRET_NAME) -o yaml \
		| sed -e '/^  namespace:/d' -e '/^  uid:/d' -e '/^  resourceVersion:/d' -e '/^  creationTimestamp:/d' \
		| $(DEMO_KUBECTL) apply -f -
	$(DEMO_KUBECTL) apply $(addprefix -f ,$(DEMO_CORE_MANIFESTS))
	sed -e 's|__DATADOG_FORWARDER_URL__|$(DEMO_DATADOG_FORWARDER_URL)|g' \
		-e 's|__DATADOG_SECRET_NAME__|$(DEMO_DATADOG_SECRET_NAME)|g' \
		-e 's|__DEMO_NAMESPACE__|$(DEMO_NAMESPACE)|g' \
		$(DEMO_DEPLOY_DIR)/datadog-agent.yaml | $(DEMO_KUBECTL) apply -f -

demo-apply-external:
	@case "$(origin DEMO_DATADOG_FORWARDER_URL)" in \
		"command line"|"environment"|"environment override") ;; \
		*) echo "demo-apply-external requires DEMO_DATADOG_FORWARDER_URL=http://… (e.g. http://mdai-envoy.$(NAMESPACE).svc.cluster.local:8126)"; exit 1 ;; \
	esac
	$(MAKE) demo-apply

demo-up: demo-kind-load demo-apply

demo-down:
	kubectl --context=$(KUBECTL_CONTEXT) delete namespace $(DEMO_NAMESPACE) --ignore-not-found
	kubectl --context=$(KUBECTL_CONTEXT) delete clusterrole,clusterrolebinding \
		datadog-agent-$(DEMO_NAMESPACE) --ignore-not-found

demo-rollout:
	$(DEMO_KUBECTL) rollout restart $(DEMO_ROLE_DEPLOYMENTS)

# =============================================================================
# Load generator (optional, sustained traffic)
# =============================================================================

demo-load-up: demo-loadgen-kind-load
	sed -e 's|__LOAD_PROFILE__|$(DEMO_LOAD_PROFILE)|g' \
		-e 's|__LOAD_RPS__|$(DEMO_LOAD_RPS)|g' \
		-e 's|__LOAD_ERROR_RATE_PCT__|$(DEMO_LOAD_ERROR_RATE_PCT)|g' \
		-e 's|__LOAD_MIX__|$(DEMO_LOAD_MIX)|g' \
		$(DEMO_DEPLOY_DIR)/load-generator.yaml | $(DEMO_KUBECTL) apply -f -

demo-load-down:
	$(DEMO_KUBECTL) delete deployment/load-generator --ignore-not-found

demo-load-logs:
	$(DEMO_KUBECTL) logs -f -l app.kubernetes.io/name=load-generator

demo-load-rollout:
	$(DEMO_KUBECTL) rollout restart deployment/load-generator

# =============================================================================
# Demo introspection (one-shot emit, scenario list, logs, port-forward)
# =============================================================================

demo-emit:
	$(DEMO_KUBECTL) run demo-emit --rm --restart=Never --attach \
		--image=$(DEMO_LOADGEN_IMAGE) --image-pull-policy=IfNotPresent \
		--env=GATEWAY_URL=http://gateway-api:8080 \
		-- --once $(DEMO_SCENARIO)

demo-scenarios:
	@cd $(TEST_STAND_DIR)/demo && $(GO) run ./cmd/load-generator -list

demo-agent-logs:
	$(DEMO_KUBECTL) logs -f -l app.kubernetes.io/name=datadog-agent

demo-app-port-forward:
	$(DEMO_KUBECTL) port-forward service/gateway-api 8081:8080

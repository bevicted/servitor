GO ?= go
DOCKER ?= docker
RSYNC ?= rsync
OPERATOR_IMAGE ?= registry.example.invalid/servitor-operator:dev
TASK_IMAGE ?= registry.example.invalid/servitor-task:dev
IMAGE_PLATFORM ?= linux/amd64
ICT_SOURCE ?= ../ict
LOCALBIN ?= $(CURDIR)/bin
ENVTEST ?= $(LOCALBIN)/setup-envtest
ENVTEST_VERSION ?= 32e5e9e948a572779280969aaaf7db92f76d8252
ENVTEST_K8S_VERSION ?= 1.32.0

.PHONY: test test-unit test-integration envtest build operator-image task-image manifests

test: test-unit test-integration

test-unit:
	$(GO) test ./...

test-integration: envtest
	KUBEBUILDER_ASSETS="$$($(ENVTEST) use -i --bin-dir $(LOCALBIN) -p path $(ENVTEST_K8S_VERSION))" \
		$(GO) test -tags=integration ./internal/controller -run '^TestRuntimeContract$$' -count=1

envtest: $(ENVTEST)
	@$(ENVTEST) use --bin-dir $(LOCALBIN) -p path $(ENVTEST_K8S_VERSION) >/dev/null

$(ENVTEST):
	@mkdir -p $(LOCALBIN)
	GOBIN=$(LOCALBIN) $(GO) install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(ENVTEST_VERSION)

build:
	$(GO) build ./cmd/servitor ./cmd/servitor-task

operator-image:
	$(DOCKER) build --platform $(IMAGE_PLATFORM) --file build/operator.Dockerfile --tag $(OPERATOR_IMAGE) .

task-image:
	IMAGE_PLATFORM="$(IMAGE_PLATFORM)" ICT_SOURCE="$(ICT_SOURCE)" DOCKER="$(DOCKER)" RSYNC="$(RSYNC)" ./build/task-image.sh "$(TASK_IMAGE)"

manifests:
	kubectl kustomize config/default

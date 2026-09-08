GO ?= go
DOCKER ?= docker
RSYNC ?= rsync
OPERATOR_IMAGE ?= registry.example.invalid/servitor-operator:dev
TASK_IMAGE ?= registry.example.invalid/servitor-task:dev
ICT_SOURCE ?= ../ict

.PHONY: test build operator-image task-image manifests

test:
	$(GO) test ./...

build:
	$(GO) build ./cmd/servitor ./cmd/servitor-task

operator-image:
	$(DOCKER) build --file build/operator.Dockerfile --tag $(OPERATOR_IMAGE) .

task-image:
	ICT_SOURCE="$(ICT_SOURCE)" DOCKER="$(DOCKER)" RSYNC="$(RSYNC)" ./build/task-image.sh "$(TASK_IMAGE)"

manifests:
	kubectl kustomize config/default

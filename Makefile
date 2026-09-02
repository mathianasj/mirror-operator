# VERSION defines the project version for the bundle.
# Update this value when you upgrade the version of your project.
# To re-generate a bundle for another specific version without changing the standard setup, you can:
# - use the VERSION as arg of the bundle target (e.g make bundle VERSION=0.0.2)
# - use environment variables to overwrite this value (e.g export VERSION=0.0.2)
VERSION ?= 0.1.3

# CHANNELS define the bundle channels used in the bundle.
# Add a new line here if you would like to change its default config. (E.g CHANNELS = "candidate,fast,stable")
# To re-generate a bundle for other specific channels without changing the standard setup, you can:
# - use the CHANNELS as arg of the bundle target (e.g make bundle CHANNELS=candidate,fast,stable)
# - use environment variables to overwrite this value (e.g export CHANNELS="candidate,fast,stable")
ifneq ($(origin CHANNELS), undefined)
BUNDLE_CHANNELS := --channels=$(CHANNELS)
endif

# DEFAULT_CHANNEL defines the default channel used in the bundle.
# Add a new line here if you would like to change its default config. (E.g DEFAULT_CHANNEL = "stable")
# To re-generate a bundle for any other default channel without changing the default setup, you can:
# - use the DEFAULT_CHANNEL as arg of the bundle target (e.g make bundle DEFAULT_CHANNEL=stable)
# - use environment variables to overwrite this value (e.g export DEFAULT_CHANNEL="stable")
ifneq ($(origin DEFAULT_CHANNEL), undefined)
BUNDLE_DEFAULT_CHANNEL := --default-channel=$(DEFAULT_CHANNEL)
endif
BUNDLE_METADATA_OPTS ?= $(BUNDLE_CHANNELS) $(BUNDLE_DEFAULT_CHANNEL)

# IMAGE_TAG_BASE defines the docker.io namespace and part of the image name for remote images.
# This variable is used to construct full image tags for bundle and catalog images.
#
# For example, running 'make bundle-build bundle-push catalog-build catalog-push' will build and push both
# quay.io/mathianasj/mirror-operator-bundle:$VERSION and quay.io/mathianasj/mirror-operator-catalog:$VERSION.
IMAGE_TAG_BASE ?= quay.io/mathianasj/mirror-operator

# BUNDLE_IMG defines the image:tag used for the bundle.
# You can use it as an arg. (E.g make bundle-build BUNDLE_IMG=<some-registry>/<project-name-bundle>:<tag>)
BUNDLE_IMG ?= $(IMAGE_TAG_BASE)-bundle:v$(VERSION)

# BUNDLE_GEN_FLAGS are the flags passed to the operator-sdk generate bundle command
BUNDLE_GEN_FLAGS ?= -q --overwrite --version $(VERSION) $(BUNDLE_METADATA_OPTS)

# USE_IMAGE_DIGESTS defines if images are resolved via tags or digests
# You can enable this value if you would like to use SHA Based Digests
# To enable set flag to true
USE_IMAGE_DIGESTS ?= false
ifeq ($(USE_IMAGE_DIGESTS), true)
	BUNDLE_GEN_FLAGS += --use-image-digests
endif

# Set the Operator SDK version to use. By default, what is installed on the system is used.
# This is useful for CI or a project to utilize a specific version of the operator-sdk toolkit.
OPERATOR_SDK_VERSION ?= v1.39.2
# Image URL to use all building/pushing image targets
IMG ?= $(IMAGE_TAG_BASE):latest

# Operand images managed by the operator (used for relatedImages in the CSV).
# Override these with digest-pinned references for release bundles.
MIRROR_IMG ?= quay.io/mathianasj/oc-mirror:v2
ARCHITECT_FRONTEND_IMG ?= quay.io/mathianasj/openshift-airgap-architect-frontend:latest
ARCHITECT_BACKEND_IMG ?= quay.io/mathianasj/openshift-airgap-architect-backend:latest
ARCHITECT_CONSOLE_PLUGIN_IMG ?= quay.io/mathianasj/openshift-airgap-architect-console-plugin:latest

GIT_COMMIT ?= $(shell git rev-parse --short HEAD)
BUILD_DATE ?= $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
LDFLAGS ?= -X main.version=$(VERSION) -X main.gitCommit=$(GIT_COMMIT) -X main.buildDate=$(BUILD_DATE)
# ENVTEST_K8S_VERSION refers to the version of kubebuilder assets to be downloaded by envtest binary.
ENVTEST_K8S_VERSION = 1.31.0

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# Utilize Kind or modify the e2e tests to load the image locally, enabling compatibility with other vendors.
.PHONY: test-e2e  # Run the e2e tests against a Kind k8s instance that is spun up.
test-e2e:
	go test ./test/e2e/ -v -ginkgo.v

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) run --fix

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -ldflags "$(LDFLAGS)" -o bin/manager cmd/main.go

.PHONY: kill
kill: ## Kill any running operator instances
	@echo "Killing existing operator processes..."
	@ps aux | grep -E "go run.*cmd/main.go|controller-gen" | grep -v grep | awk '{print $$2}' | xargs kill -9 2>/dev/null || true
	@lsof -ti:8080 | xargs kill -9 2>/dev/null || true
	@lsof -ti:8081 | xargs kill -9 2>/dev/null || true
	@echo "All operator processes killed"

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run -ldflags "$(LDFLAGS)" ./cmd/main.go || true

.PHONY: restart
restart: kill run ## Kill existing instances and restart the operator

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build --build-arg GIT_COMMIT=$(GIT_COMMIT) --build-arg BUILD_DATE=$(BUILD_DATE) --build-arg VERSION=$(VERSION) -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name mirror-operator-builder
	$(CONTAINER_TOOL) buildx use mirror-operator-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm mirror-operator-builder
	rm Dockerfile.cross

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default > dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) apply -f -

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default | $(KUBECTL) apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/default | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
KUBECTL ?= kubectl
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint
YQ = $(LOCALBIN)/yq

## Tool Versions
KUSTOMIZE_VERSION ?= v5.4.3
CONTROLLER_TOOLS_VERSION ?= v0.16.1
ENVTEST_VERSION ?= release-0.19
GOLANGCI_LINT_VERSION ?= v1.59.1
YQ_VERSION ?= v4.44.3

.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

.PHONY: yq
yq: $(YQ) ## Download yq locally if necessary.
$(YQ): $(LOCALBIN)
	$(call go-install-tool,$(YQ),github.com/mikefarah/yq/v4,$(YQ_VERSION))

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f $(1) || true ;\
GOBIN=$(LOCALBIN) go install $${package} ;\
mv $(1) $(1)-$(3) ;\
} ;\
ln -sf $(1)-$(3) $(1)
endef

.PHONY: operator-sdk
OPERATOR_SDK ?= $(LOCALBIN)/operator-sdk
operator-sdk: ## Download operator-sdk locally if necessary.
ifeq (,$(wildcard $(OPERATOR_SDK)))
ifeq (, $(shell which operator-sdk 2>/dev/null))
	@{ \
	set -e ;\
	mkdir -p $(dir $(OPERATOR_SDK)) ;\
	OS=$(shell go env GOOS) && ARCH=$(shell go env GOARCH) && \
	curl -sSLo $(OPERATOR_SDK) https://github.com/operator-framework/operator-sdk/releases/download/$(OPERATOR_SDK_VERSION)/operator-sdk_$${OS}_$${ARCH} ;\
	chmod +x $(OPERATOR_SDK) ;\
	}
else
OPERATOR_SDK = $(shell which operator-sdk)
endif
endif

.PHONY: bundle
bundle: manifests kustomize operator-sdk ## Generate bundle manifests and metadata, then validate generated files.
	$(OPERATOR_SDK) generate kustomize manifests -q --interactive=false
	cd config/manager && $(KUSTOMIZE) edit set image controller=$(IMG)
	$(KUSTOMIZE) build config/manifests | $(OPERATOR_SDK) generate bundle $(BUNDLE_GEN_FLAGS)
	$(OPERATOR_SDK) bundle validate ./bundle
	$(MAKE) bundle-related-images

CSV_PATH = bundle/manifests/mirror-operator.clusterserviceversion.yaml

.PHONY: bundle-related-images
bundle-related-images: yq ## Resolve image digests and inject relatedImages + env vars into the CSV.
	@echo "Resolving image digests..."
	$(eval IMG_DIGEST := $(shell skopeo inspect --format '{{.Digest}}' docker://$(IMG) 2>/dev/null))
	$(eval MIRROR_DIGEST := $(shell skopeo inspect --format '{{.Digest}}' docker://$(MIRROR_IMG) 2>/dev/null))
	$(eval FRONTEND_DIGEST := $(shell skopeo inspect --format '{{.Digest}}' docker://$(ARCHITECT_FRONTEND_IMG) 2>/dev/null))
	$(eval BACKEND_DIGEST := $(shell skopeo inspect --format '{{.Digest}}' docker://$(ARCHITECT_BACKEND_IMG) 2>/dev/null))
	$(eval CONSOLE_PLUGIN_DIGEST := $(shell skopeo inspect --format '{{.Digest}}' docker://$(ARCHITECT_CONSOLE_PLUGIN_IMG) 2>/dev/null))
	$(eval IMG_BASE := $(firstword $(subst :, ,$(IMG))))
	$(eval MIRROR_BASE := $(firstword $(subst :, ,$(MIRROR_IMG))))
	$(eval FRONTEND_BASE := $(firstword $(subst :, ,$(ARCHITECT_FRONTEND_IMG))))
	$(eval BACKEND_BASE := $(firstword $(subst :, ,$(ARCHITECT_BACKEND_IMG))))
	$(eval CONSOLE_PLUGIN_BASE := $(firstword $(subst :, ,$(ARCHITECT_CONSOLE_PLUGIN_IMG))))
	@echo "Injecting relatedImages into CSV..."
	$(YQ) -i '.spec.relatedImages = [{"name": "manager", "image": "$(IMG_BASE)@$(IMG_DIGEST)"}, {"name": "oc-mirror", "image": "$(MIRROR_BASE)@$(MIRROR_DIGEST)"}, {"name": "architect-frontend", "image": "$(FRONTEND_BASE)@$(FRONTEND_DIGEST)"}, {"name": "architect-backend", "image": "$(BACKEND_BASE)@$(BACKEND_DIGEST)"}, {"name": "architect-console-plugin", "image": "$(CONSOLE_PLUGIN_BASE)@$(CONSOLE_PLUGIN_DIGEST)"}]' $(CSV_PATH)
	@echo "Injecting RELATED_IMAGE env vars into CSV..."
	$(YQ) -i '(.spec.install.spec.deployments[0].spec.template.spec.containers[] | select(.name == "manager") | .env) += [{"name": "RELATED_IMAGE_OC_MIRROR", "value": "$(MIRROR_BASE)@$(MIRROR_DIGEST)"}, {"name": "RELATED_IMAGE_ARCHITECT_FRONTEND", "value": "$(FRONTEND_BASE)@$(FRONTEND_DIGEST)"}, {"name": "RELATED_IMAGE_ARCHITECT_BACKEND", "value": "$(BACKEND_BASE)@$(BACKEND_DIGEST)"}, {"name": "RELATED_IMAGE_ARCHITECT_CONSOLE_PLUGIN", "value": "$(CONSOLE_PLUGIN_BASE)@$(CONSOLE_PLUGIN_DIGEST)"}]' $(CSV_PATH)
	$(OPERATOR_SDK) bundle validate ./bundle

.PHONY: bundle-build
bundle-build: ## Build the bundle image.
	docker build -f bundle.Dockerfile -t $(BUNDLE_IMG) .

.PHONY: bundle-push
bundle-push: ## Push the bundle image.
	$(MAKE) docker-push IMG=$(BUNDLE_IMG)

.PHONY: opm
OPM = $(LOCALBIN)/opm
opm: ## Download opm locally if necessary.
ifeq (,$(wildcard $(OPM)))
ifeq (,$(shell which opm 2>/dev/null))
	@{ \
	set -e ;\
	mkdir -p $(dir $(OPM)) ;\
	OS=$(shell go env GOOS) && ARCH=$(shell go env GOARCH) && \
	curl -sSLo $(OPM) https://github.com/operator-framework/operator-registry/releases/download/v1.71.0/$${OS}-$${ARCH}-opm ;\
	chmod +x $(OPM) ;\
	}
else
OPM = $(shell which opm)
endif
endif

# The bundle image(s) to render into the FBC catalog.
BUNDLE_IMGS ?= $(BUNDLE_IMG)

# The image tag given to the resulting catalog image.
CATALOG_IMG ?= $(IMAGE_TAG_BASE)-catalog:v$(VERSION)

# Directory for the File-Based Catalog content.
CATALOG_DIR ?= catalog/mirror-operator

# Dev channel name (e.g., "master"). When set, a dev channel is added to the catalog.
DEV_CHANNEL ?=

# Release bundle images from the tracking file.
RELEASE_BUNDLE_IMGS = $(shell cat catalog-templates/bundle-images.txt 2>/dev/null | tr '\n' ' ')

.PHONY: fbc-render
fbc-render: opm ## Render bundle image(s) into a File-Based Catalog.
	mkdir -p $(CATALOG_DIR)
	@echo '{"schema":"olm.package","name":"mirror-operator","defaultChannel":"alpha"}' > $(CATALOG_DIR)/catalog.json
	@cat catalog-templates/alpha-channel.json >> $(CATALOG_DIR)/catalog.json
ifneq ($(DEV_CHANNEL),)
	@echo '{"schema":"olm.channel","name":"$(DEV_CHANNEL)","package":"mirror-operator","entries":[{"name":"mirror-operator.v$(VERSION)","skipRange":">=0.0.0-0"}]}' >> $(CATALOG_DIR)/catalog.json
	$(OPM) render $(RELEASE_BUNDLE_IMGS) $(BUNDLE_IMGS) -o json >> $(CATALOG_DIR)/catalog.json
else
	$(OPM) render $(RELEASE_BUNDLE_IMGS) -o json >> $(CATALOG_DIR)/catalog.json
endif

.PHONY: fbc-validate
fbc-validate: opm ## Validate the File-Based Catalog.
	$(OPM) validate catalog/

.PHONY: catalog-build
catalog-build: fbc-render fbc-validate ## Build a catalog image from the FBC.
	$(OPM) generate dockerfile catalog/ --binary-image=quay.io/operator-framework/opm:v1.71.0
	$(CONTAINER_TOOL) build -t $(CATALOG_IMG) -f catalog.Dockerfile .
	rm -f catalog.Dockerfile

.PHONY: catalog-push
catalog-push: ## Push a catalog image.
	$(MAKE) docker-push IMG=$(CATALOG_IMG)

.PHONY: catalog-clean
catalog-clean: ## Clean generated catalog files.
	rm -rf catalog/ catalog.Dockerfile

##@ Release

.PHONY: release
release: ## Create a versioned release (usage: make release VERSION=x.y.z)
	@if [ "$(VERSION)" = "0.0.1" ]; then \
		echo "ERROR: specify VERSION, e.g.  make release VERSION=1.0.0"; exit 1; \
	fi
	@if ! echo "$(VERSION)" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$$'; then \
		echo "ERROR: VERSION must be semver (x.y.z), got '$(VERSION)'"; exit 1; \
	fi
	@if git rev-parse "v$(VERSION)" >/dev/null 2>&1; then \
		echo "ERROR: tag v$(VERSION) already exists"; exit 1; \
	fi
	@if [ -n "$$(git status --porcelain)" ]; then \
		echo "ERROR: working tree is dirty — commit or stash changes first"; exit 1; \
	fi
	@command -v gh >/dev/null 2>&1 || { echo "ERROR: gh CLI is required but not installed"; exit 1; }
	$(eval OLD_VERSION := $(shell grep '^VERSION ?=' Makefile | sed 's/VERSION ?= //'))
	@echo "Releasing v$(VERSION)  (previous: $(OLD_VERSION))"
	@# Update VERSION in Makefile
	@if sed --version >/dev/null 2>&1; then \
		sed -i 's/^VERSION ?= .*/VERSION ?= $(VERSION)/' Makefile; \
	else \
		sed -i '' 's/^VERSION ?= .*/VERSION ?= $(VERSION)/' Makefile; \
	fi
	@# Update replaces in release-config.yaml to point to previous version
	@if sed --version >/dev/null 2>&1; then \
		sed -i 's/^    replaces: mirror-operator\.v.*/    replaces: mirror-operator.v$(OLD_VERSION)/' community-operators/release-config.yaml; \
	else \
		sed -i '' 's/^    replaces: mirror-operator\.v.*/    replaces: mirror-operator.v$(OLD_VERSION)/' community-operators/release-config.yaml; \
	fi
	@# Update alpha-channel.json with new version entry
	@if sed --version >/dev/null 2>&1; then \
		sed -i 's/\]}$$/,{"name":"mirror-operator.v$(VERSION)","replaces":"mirror-operator.v$(OLD_VERSION)"}]}/' catalog-templates/alpha-channel.json; \
	else \
		sed -i '' 's/\]}$$/,{"name":"mirror-operator.v$(VERSION)","replaces":"mirror-operator.v$(OLD_VERSION)"}]}/' catalog-templates/alpha-channel.json; \
	fi
	@# Add new bundle image to tracking file
	@echo "$(IMAGE_TAG_BASE)-bundle:v$(VERSION)" >> catalog-templates/bundle-images.txt
	git add Makefile community-operators/release-config.yaml catalog-templates/alpha-channel.json catalog-templates/bundle-images.txt
	git commit -m "Release v$(VERSION)"
	git tag -a "v$(VERSION)" -m "Release v$(VERSION)"
	git push origin HEAD "v$(VERSION)"
	@NOTES=$$(hack/release-notes.sh $(VERSION)); \
	gh release create "v$(VERSION)" --title "v$(VERSION)" --notes-file "$$NOTES"; \
	rm -f "$$NOTES"
	hack/close-milestone.sh $(VERSION)

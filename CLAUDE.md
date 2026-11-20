# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

The mirror-operator is a cluster-scoped Kubernetes operator for managing disconnected/airgapped OpenShift environments. It automates content collection from internet sources, bundle creation for physical transfer (DVD/USB), import into airgapped registries, and cluster bootstrapping. The operator also deploys and manages the **Airgap Architect UI** (a connected Node.js/React config tool).

**Language**: Go 1.22+ | **Framework**: Kubebuilder v4 / controller-runtime | **API group**: `mirror.mirror.mathianasj.github.com/v1`

## Key Commands

```bash
make test           # Run all tests (also runs manifests, generate, fmt, vet)
make run            # Run controller locally against current kubeconfig
make restart        # Kill existing local instance and restart
make manifests      # Regenerate CRD/RBAC YAML + sync bundledCrds/ (run after api/v1 changes)
make generate       # Regenerate deepcopy functions
make install        # Install CRDs into cluster
make bundle IMG=... VERSION=x.y.z  # Build OLM bundle
make release VERSION=x.y.z         # Cut a versioned release (requires clean working tree)
```

**Single test**: `go test ./internal/controller -v --ginkgo.focus="<describe block name>"`

**`make test` already runs `manifests generate fmt vet`** — do not run them separately before `make test`.

After any `api/v1/*_types.go` change, run `make manifests generate` before committing.

## Architecture

### Four CRDs

| CRD | Scope | Purpose | Controller |
|-----|-------|---------|-----------|
| `DisconnectedPlatform` | Cluster | Top-level orchestrator; deploys Airgap Architect UI, manages OLM subscriptions, aggregates status | `disconnectedplatform_controller.go` + `disconnectedplatform_airgapped.go` |
| `CollectionPipeline` | Namespaced | Triggers Tekton pipelines to run oc-mirror and produce bundles | `collectionpipeline_controller.go` |
| `MirrorImport` | Namespaced | Imports bundles into airgapped Quay, verifies cosign signatures | `mirrorimport_controller.go` |
| `ClusterBootstrap` | Namespaced | Provisions new OpenShift clusters from mirrored content (stub) | `clusterbootstrap_controller.go` |

### DisconnectedPlatform — the monolith

`disconnectedplatform_controller.go` is intentionally large (~12k lines). It embeds entire Tekton PipelineRun definitions as Go string literals (the `buildPipelineTasks()` family). The file owns: Airgap Architect deployments, Quay config, RHTAS/Keycloak/RHTPA wiring, OSUS, console plugin, OLM subscriptions, and the collection pipeline template. **Do not grow it further** — extract new domain logic into packages under `internal/controller/`.

The airgapped reconcile path lives in `disconnectedplatform_airgapped.go` (import pipeline, mirror-to-nodes, UpdateService, ACM).

### CollectionPipeline — imageset enrichment

`collectionpipeline_controller.go` enriches the user-supplied `spec.imageSetConfig` YAML before passing it to the Tekton pipeline:
- `injectMirrorOperator(configYAML, catalogOverride, channelOverride)` — adds mirror-operator package to the imageset; when a custom catalog is set via `platform.Spec.Connected.MirrorOperatorCatalog`, also adds the catalog image to `additionalImages` so oc-mirror explicitly mirrors it
- `injectArchitectImages()` — adds frontend/backend images
- `injectRHCOSServerImage()` — adds RHCOS server image for ACM host inventory
- `injectDefaultArchitecture()` — pins to `amd64` to avoid OSUS graph mismatches

### Console Plugin Sidecar

The `airgap-architect-plugin` Deployment has **two containers**:
- `plugin` — nginx on port 9001 (HTTPS) serving the React dist files; proxies `/api/` to `localhost:4001`
- `plugin-backend` — Express.js sidecar on port 4001 handling `/api/mirror-import/*` and `/api/collections/*` routes; proxies shared architect routes to `OAA_BACKEND_URL`

Source in `console-plugin/` (TypeScript/React UI + Express backend in `console-plugin/server/`). Images built by CI and referenced via `RELATED_IMAGE_ARCHITECT_CONSOLE_PLUGIN` / `RELATED_IMAGE_ARCHITECT_CONSOLE_PLUGIN_BACKEND`.

### Import Script

`config/scripts/import-airgap-architect.sh` is embedded into the operator binary via `//go:embed` in `config/scripts/scripts.go` and stored as a Kubernetes ConfigMap (`airgap-architect-import-script`). The collection pipeline's `copy-import-script` Tekton task bundles it alongside CRD YAMLs from `config/scripts/bundledCrds/` into the airgapped bundle tar.

The script's `start_containers` function (and lightweight `restart_containers` sibling) start the Airgap Architect backend+frontend via podman on the bastion. `setup_bootstrap_manifests()` generates BOOTSTRAP_MANIFESTS_DIR content (CRDs, operator manifests, DisconnectedPlatform CR) for injecting into agent ISOs.

### Auto-Detection: MirrorOperatorCatalog

`syncMirrorOperatorCatalogFromSubscription()` runs in reconcile and reads the operator's own `Subscription` to detect which catalog source (community-operators vs custom) and channel were used at install time. It auto-populates `platform.Spec.Connected.MirrorOperatorCatalog` and `MirrorOperatorChannel`, which the CollectionPipeline imageset injection uses.

### Release Process

```bash
gh workflow run release.yaml --repo mathianasj/mirror-operator -f version=0.1.9
```

The `make release VERSION=x.y.z` target updates `Makefile`, `community-operators/release-config.yaml`, `catalog-templates/alpha-channel.json`, `catalog-templates/bundle-images.txt`, commits, tags, pushes, creates the GitHub release, and closes the milestone. Requires a clean working tree and the `gh` CLI.

**Branch protection caveat**: master requires the `test` check to pass. The release workflow must bypass this — either via a PAT with bypass permission stored as `RELEASE_PAT` secret, or by adding `github-actions[bot]` as a bypass actor in branch protection settings.

### CRD bundledCrds sync

`make manifests` automatically runs `make sync-crds` which copies `config/crd/bases/*.yaml` → `config/scripts/bundledCrds/`. Always run `make manifests` after API changes to keep them in sync.

## Operator Conventions

- **Logging**: `log.FromContext(ctx)` only — never `fmt.Println`/`log.Printf`
- **Requeue**: `ctrl.Result{RequeueAfter: 30 * time.Second}` while waiting on external deps; `ctrl.Result{}` on success; return error for exponential backoff
- **Finalizer format**: `mirror.mathianasj.github.com/<resource>-finalizer`
- **Owner references**: `ctrl.SetControllerReference(owner, owned, r.Scheme)` — not manual construction. `DisconnectedPlatform` is cluster-scoped so it can own namespace-scoped resources in any namespace
- **Unstructured resources**: Always call `SetGroupVersionKind()` before Get/Create/Update
- **Status conditions**: `metav1.Condition` with `Ready` / `Degraded` types. Set `ObservedGeneration: obj.Generation`. Only update `LastTransitionTime` when condition status actually changes (use `Reason: "HealthCheckPassed"` when clearing Degraded)
- **Status update conflicts**: Ignore `apierrors.IsConflict(err)` — next reconcile will pick it up
- **CRD-not-found errors**: Track `crdMissing` flag and requeue 30s instead of erroring (for Tekton, OLM, OpenShift-specific CRDs)
- **`ensureSubscription` pattern**: always pass `platform` for owner reference; call `reconcileSubscriptionProxy` on existing subscriptions to keep proxy env vars current

## Go Best Practices

- `fmt.Errorf("reconciling quay config: %w", err)` — always wrap errors with context
- `errors.Is()` / `errors.As()` for error comparison — never string-match `err.Error()`
- No `init()` functions — pass dependencies explicitly
- Nil slices are fine for `append`/`len`/`range`; don't `make` unless you need capacity
- Table-driven tests with `t.Run()` for unit helpers; Ginkgo/Gomega for controller tests
- TDD: write failing test (RED) → minimum code to pass (GREEN) → refactor. Run `make test` before every commit

## Testing Patterns

Controller tests use the fake client or envtest (`suite_test.go` wires up envtest for integration tests). Most new tests should use `fake.NewClientBuilder().WithScheme(scheme).WithObjects(...).Build()`.

```go
// Ginkgo style (controllers)
var _ = Describe("MyReconciler", func() {
    It("does something", func() {
        r := &MyReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build(), Scheme: scheme}
        result, err := r.Reconcile(ctx, req)
        Expect(err).NotTo(HaveOccurred())
    })
})
```

Run a focused test: `go test ./internal/controller -v --ginkgo.focus="description text"`

## Git Workflow

- Every commit must reference a GitHub issue: create one if none exists
- Branch off master, open a PR — no direct pushes to master
- Rebase on master before opening/merging PRs: `git fetch origin master && git rebase origin/master`
- Branch name convention: `fix/<issue-number>-<short-desc>` or `feat/<issue-number>-<short-desc>`

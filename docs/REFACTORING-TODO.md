# Refactoring TODO - Go Best Practices

This document tracks anti-patterns identified in the codebase and refactoring work needed to align with Go best practices.

## Analysis Summary

**Date**: 2026-07-08  
**Current State**:
- `disconnectedplatform_controller.go`: 9,630 lines (should be <500)
- `collectionpipeline_controller.go`: 1,125 lines
- Multiple functions >100 lines (should be <50-100)
- All logic in single `internal/controller` package

## HIGH Priority Tasks

### 1. Split disconnectedplatform_controller.go (9,630 lines)

**Problem**: Massive god-object file mixing multiple concerns.

**Target Structure**:
```
internal/
├── controller/
│   └── disconnectedplatform_controller.go  (<300 lines, orchestration only)
├── keycloak/
│   ├── client.go      (HTTP client & authentication)
│   ├── realm.go       (realm management)
│   ├── oidc.go        (OIDC client configuration)
│   └── rbac.go        (role/scope assignment)
├── quay/
│   ├── config.go      (Quay config management)
│   └── clair.go       (Clair VEX configuration)
├── architect/
│   ├── deployment.go  (Airgap Architect deployments)
│   └── route.go       (Routes/Services/Ingress)
├── pipeline/
│   ├── builder.go     (Pipeline template builder)
│   └── tasks.go       (Individual task definitions)
└── rhtpa/
    ├── config.go      (TPA configuration)
    └── postgres.go    (PostgreSQL setup)
```

**Benefits**:
- Each package <500 lines, focused on single concern
- Independently testable components
- Clear separation of concerns
- Easier onboarding for new developers

**Estimated Effort**: 2-3 days

---

### 2. Break Down Long Functions (>100 lines)

**Problem**: Functions too long to understand/test at a glance.

**Current Violators**:
- `reconcileRHTPAConfig`: **464 lines** → break into:
  - `ensureRHTPAPostgreSQL()`
  - `ensureKeycloakRealm()`
  - `ensureOIDCClients()`
  - `ensureRBACConfig()`

- `ensureTrustifyOIDCClient`: **225 lines** → extract:
  - `createOIDCClient()`
  - `updateClientRedirects()`
  - `configureClientScopes()`

- `Reconcile`: **184 lines** → extract reconciliation phases:
  - `reconcileArchitect()`
  - `reconcileOperators()`
  - `reconcilePipelines()`

- `updateTrustifyRedirectURIs`: **148 lines**
- `ensureTrustifyRealmAndOIDC`: **118 lines**

**Pattern to Follow**:
```go
// Good: Orchestration function delegates to helpers
func (r *Reconciler) reconcileRHTPAConfig(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
    if err := r.ensureRHTPAPostgreSQL(ctx); err != nil {
        return fmt.Errorf("failed to setup PostgreSQL: %w", err)
    }
    if err := r.ensureKeycloakRealm(ctx, platform); err != nil {
        return fmt.Errorf("failed to setup Keycloak: %w", err)
    }
    if err := r.ensureOIDCClients(ctx, platform); err != nil {
        return fmt.Errorf("failed to setup OIDC clients: %w", err)
    }
    return r.ensureRBACConfig(ctx, platform)
}

// Each helper function: <50 lines, single responsibility
func (r *Reconciler) ensureRHTPAPostgreSQL(ctx context.Context) error {
    // ... focused implementation
}
```

**Target**: All functions <50 lines (max 100 for complex logic)

**Estimated Effort**: 3-5 days

---

## MEDIUM Priority Tasks

### 3. Extract Tekton Pipeline Building to Separate Package

**Problem**: ~1,600 lines of inline map[string]interface{} for Tekton pipelines in `buildPipelineTasks()`

**Create**: `internal/pipeline/builder.go`

**Proposed API**:
```go
package pipeline

import pipelinev1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"

type Builder struct {
    params     []pipelinev1.ParamSpec
    tasks      []pipelinev1.PipelineTask
    workspaces []pipelinev1.PipelineWorkspaceDeclaration
}

func NewBuilder() *Builder { ... }

// Fluent API for building pipeline
func (b *Builder) WithDryRun(intermediateRegistry string) *Builder { ... }
func (b *Builder) WithOcMirror(config OcMirrorConfig) *Builder { ... }
func (b *Builder) WithSigning(config SigningConfig) *Builder { ... }
func (b *Builder) WithSBOM(config SBOMConfig) *Builder { ... }
func (b *Builder) WithUpload(config S3Config) *Builder { ... }
func (b *Builder) Build() *pipelinev1.Pipeline { ... }

// Usage in controller
pipeline := pipeline.NewBuilder().
    WithOcMirror(ocMirrorCfg).
    WithSigning(signingCfg).
    WithSBOM(sbomCfg).
    WithUpload(s3Cfg).
    Build()
```

**Benefits**:
- Testable pipeline construction
- Reusable across controllers
- Type-safe vs map[string]interface{}
- Clear builder pattern
- Easy to add/remove tasks

**Estimated Effort**: 2-3 days

---

### 4. Add Error Wrapping with Context

**Problem**: Many places return `err` without context, making debugging hard.

**Current (Bad)**:
```go
if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
    return err  // ❌ No context about what failed
}
```

**Improved (Good)**:
```go
if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
    return fmt.Errorf("failed to get DisconnectedPlatform %s: %w", req.NamespacedName, err)
}
```

**Apply to**:
- All controller reconcilers
- All helper functions
- All package-level functions

**Tool to Find Issues**:
```bash
# Find naked error returns
grep -rn "return err$" internal/controller/
grep -rn "return nil, err$" internal/controller/
```

**Estimated Effort**: 1-2 days (mechanical change)

---

## LOW Priority Tasks

### 5. Group Related Constants into Structs

**Problem**: 20+ scattered constants hard to manage.

**Current (Scattered)**:
```go
const (
    platformFinalizer     = "mirror.mathianasj.github.com/platform-finalizer"
    architectNamespace    = "mirror-operator-system"
    defaultPullSecretName = "pull-secret"
    defaultPullSecretNS   = "openshift-config"
    pullSecretVolumeName  = "pull-secret"
    pullSecretMountPath   = "/var/run/secrets/openshift.io/pull-secret"
)
```

**Improved (Grouped)**:
```go
// internal/config/defaults.go
package config

type PullSecretConfig struct {
    Name       string
    Namespace  string
    VolumeName string
    MountPath  string
}

var DefaultPullSecret = PullSecretConfig{
    Name:       "pull-secret",
    Namespace:  "openshift-config",
    VolumeName: "pull-secret",
    MountPath:  "/var/run/secrets/openshift.io/pull-secret",
}

type FinalizerConfig struct {
    Platform string
    Pipeline string
    Import   string
}

var Finalizers = FinalizerConfig{
    Platform: "mirror.mathianasj.github.com/platform-finalizer",
    Pipeline: "mirror.mathianasj.github.com/pipeline-finalizer",
    Import:   "mirror.mathianasj.github.com/import-finalizer",
}

type NamespaceConfig struct {
    Architect string
    Operator  string
}

var Namespaces = NamespaceConfig{
    Architect: "mirror-operator-system",
    Operator:  "mirror-operator-system",
}
```

**Benefits**:
- Easier to override for testing
- Clear relationships between constants
- Better IDE autocomplete
- Type safety
- Can marshal/unmarshal as config

**Estimated Effort**: 1 day

---

### 6. Add Package Documentation and godoc Comments

**Problem**: Missing package-level and exported function documentation.

**Add Package Docs**:
```go
// Package controller implements Kubernetes controllers for the Mirror Operator.
//
// The Mirror Operator manages disconnected/airgapped OpenShift environments through
// four main controllers:
//   - DisconnectedPlatform: Orchestrates connected and airgapped platform components
//   - CollectionPipeline: Automates content collection via Tekton pipelines
//   - MirrorImport: Manages bundle imports into airgapped registries
//   - ClusterBootstrap: Provisions OpenShift clusters from mirrored content
//
// Each controller follows standard Kubernetes operator patterns using controller-runtime.
package controller
```

**Add Function Docs**:
```go
// Reconcile implements the reconciliation loop for DisconnectedPlatform resources.
//
// In connected mode, it manages:
//   - Airgap Architect UI (frontend/backend/plugin)
//   - Quay registry for intermediate storage
//   - Keycloak for authentication
//   - RHTPA integration for SBOM/signing
//   - Collection pipeline automation
//
// In airgapped mode, it manages:
//   - Airgap Architect UI for bundle imports
//   - Import pipeline automation
//   - Cluster bootstrapping
//
// The reconciler is idempotent and handles resource lifecycle including
// finalizer-based cleanup on deletion.
func (r *DisconnectedPlatformReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    // ...
}
```

**Check with**:
```bash
# View generated docs
go doc ./internal/controller
go doc ./internal/controller DisconnectedPlatformReconciler.Reconcile

# Serve docs locally
godoc -http=:6060
# Open http://localhost:6060/pkg/github.com/mathianasj/mirror-operator/
```

**Estimated Effort**: 1 day

---

## Tools to Help

### Find Issues:
```bash
# Find files >1000 lines
find . -name "*.go" -type f -exec wc -l {} \; | awk '$1 > 1000' | sort -rn

# Find long functions (install gocyclo)
go install github.com/fzipp/gocyclo/cmd/gocyclo@latest
gocyclo -over 15 .

# Run linter
golangci-lint run

# Check cognitive complexity
gocognit -over 15 .
```

### During Refactoring:
```bash
# Run tests after each change
make test

# Verify no regressions
make vet
make fmt

# Test locally
make run
```

---

## Recommended Execution Order

### Phase 1: Quick Wins (1-2 weeks)
1. **Task #4**: Add error wrapping (mechanical, immediate value)
2. **Task #2**: Break down long functions (visible improvement)
3. **Task #3**: Extract pipeline builder (clear boundaries)

### Phase 2: Major Refactor (2-3 weeks)
4. **Task #1**: Split disconnectedplatform_controller.go (requires planning)

### Phase 3: Polish (1 week)
5. **Task #5**: Group constants
6. **Task #6**: Add documentation

**Total Estimated Effort**: 4-6 weeks

---

## Success Metrics

After completion:
- ✅ No files >500 lines
- ✅ No functions >100 lines
- ✅ All errors wrapped with context
- ✅ Clear package structure (controller, keycloak, quay, pipeline, rhtpa, architect)
- ✅ All exported functions documented
- ✅ Pipeline building is type-safe and testable
- ✅ Pass `golangci-lint run` with no warnings
- ✅ Cognitive complexity <15 for all functions

---

## References

- [Effective Go](https://go.dev/doc/effective_go)
- [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md)
- [Standard Go Project Layout](https://github.com/golang-standards/project-layout)
- [Kubebuilder Best Practices](https://book.kubebuilder.io/)

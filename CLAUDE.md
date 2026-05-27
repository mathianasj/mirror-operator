# Mirror Operator - Claude Integration Guide

This file provides guidance for Claude Code and other AI assistants working with the mirror-operator codebase.

## Project Overview

The mirror-operator is a Kubernetes operator for managing disconnected/airgapped OpenShift environments. It automates:
- Content collection from internet sources
- Bundle creation for physical transfer
- Import into airgapped registries
- Cluster bootstrapping from mirrored content

**Language**: Go 1.22+  
**Framework**: Kubebuilder v4  
**Primary Dependencies**: controller-runtime, Kubernetes client-go

## Repository Structure

```
mirror-operator/
├── api/v1/                          # CRD type definitions
│   ├── disconnectedplatform_types.go   # Main orchestration resource
│   ├── collectionpipeline_types.go     # Collection automation
│   ├── mirrorimport_types.go           # Import automation
│   └── clusterbootstrap_types.go       # Cluster provisioning
├── internal/controller/             # Controller reconciliation logic
│   ├── disconnectedplatform_controller.go
│   ├── collectionpipeline_controller.go
│   ├── mirrorimport_controller.go
│   └── clusterbootstrap_controller.go
├── config/                          # Kubernetes manifests
│   ├── crd/bases/                     # Generated CRD YAML
│   ├── rbac/                          # RBAC roles
│   ├── samples/                       # Example CRs
│   └── manager/                       # Operator deployment
├── docs/                            # Documentation
│   ├── crd-reference.md              # API documentation
│   ├── integration-guide.md          # Integration patterns
│   └── reference-architecture-mapping.md
└── cmd/main.go                      # Operator entrypoint
```

## Custom Resource Definitions (CRDs)

### DisconnectedPlatform
**Purpose**: Top-level orchestrator for connected/airgapped workflows  
**Key Fields**:
- `spec.mode`: `connected` | `airgapped`
- `spec.architect`: Airgap Architect UI configuration
- `spec.connected.collectionSchedule`: Cron schedule for collections
- `spec.airgapped.importPath`: Bundle import location

**Controller**: `internal/controller/disconnectedplatform_controller.go`

### CollectionPipeline
**Purpose**: Triggers Tekton pipelines to collect images from internet sources  
**Key Fields**:
- `spec.imageSetConfig`: oc-mirror ImageSetConfiguration YAML
- `spec.storage`: PVC or S3 output location
- `spec.signing`: Cosign signing configuration

**Controller**: `internal/controller/collectionpipeline_controller.go`

### MirrorImport
**Purpose**: Imports bundles into airgapped registries  
**Key Fields**:
- `spec.bundle`: Source bundle location (PVC path)
- `spec.targetRegistry`: Destination registry
- `spec.publish`: CatalogSource/ICSP creation flags

**Controller**: `internal/controller/mirrorimport_controller.go`

### ClusterBootstrap
**Purpose**: Provisions new OpenShift clusters from mirrored content  
**Key Fields**:
- `spec.version`: OpenShift version
- `spec.platform`: `vsphere` | `baremetal` | `none`
- `spec.mirrorRegistry`: Registry for cluster installation

**Controller**: `internal/controller/clusterbootstrap_controller.go`

## Development Workflow

### Making Changes to CRDs

1. **Edit types**: Modify `api/v1/*_types.go`
2. **Regenerate manifests**: `make manifests`
3. **Regenerate deepcopy**: `make generate`
4. **Update CRDs in cluster**: `make install`

### Testing Controllers Locally

```bash
# Run operator locally (connects to current kubeconfig context)
make run

# Apply sample CR
kubectl apply -f config/samples/mirror_v1_disconnectedplatform_airgapped.yaml

# Watch logs in operator output
# Check status
kubectl get disconnectedplatform -o yaml
```

### Key Makefile Targets

```bash
make manifests      # Generate CRD/RBAC YAML from Go types
make generate       # Generate deepcopy code
make fmt            # Format Go code
make vet            # Run Go vet
make test           # Run unit tests
make install        # Install CRDs into cluster
make uninstall      # Remove CRDs from cluster
make deploy         # Deploy operator to cluster
make undeploy       # Remove operator from cluster
make run            # Run controller locally
make docker-build   # Build container image
```

## Architecture Patterns

### Controller Pattern
All controllers follow standard Kubernetes controller-runtime patterns:

```go
func (r *SomeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    // 1. Fetch resource
    obj := &v1.SomeResource{}
    if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
        return ctrl.Result{}, client.IgnoreNotFound(err)
    }
    
    // 2. Handle deletion (finalizers)
    if !obj.DeletionTimestamp.IsZero() {
        return r.cleanup(ctx, obj)
    }
    
    // 3. Add finalizer if missing
    if !containsString(obj.Finalizers, finalizer) {
        obj.Finalizers = append(obj.Finalizers, finalizer)
        return ctrl.Result{}, r.Update(ctx, obj)
    }
    
    // 4. Reconcile desired state
    if err := r.reconcileResources(ctx, obj); err != nil {
        return ctrl.Result{}, err
    }
    
    // 5. Update status
    return ctrl.Result{}, r.Status().Update(ctx, obj)
}
```

### Using Unstructured for Dynamic Resources

The operator frequently uses `unstructured.Unstructured` to work with Tekton, OLM, and OpenShift resources without importing their type definitions:

```go
deployment := &unstructured.Unstructured{}
deployment.SetGroupVersionKind(schema.GroupVersionKind{
    Group:   "apps",
    Version: "v1",
    Kind:    "Deployment",
})
deployment.SetName("my-deployment")
deployment.SetNamespace("my-namespace")

unstructured.SetNestedField(deployment.Object, int64(3), "spec", "replicas")
```

### Secret Handling

OpenShift pull secrets and registry credentials are handled via:
- `corev1.LocalObjectReference` in CRD specs
- Automatic copying between namespaces when needed
- Volume mounts in pods for consumption

Example from `disconnectedplatform_controller.go`:
```go
// Copy pull secret from openshift-config to operator namespace
sourceSecret := &corev1.Secret{}
r.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: "openshift-config"}, sourceSecret)

targetSecret := &corev1.Secret{
    ObjectMeta: metav1.ObjectMeta{
        Name: "pull-secret",
        Namespace: "mirror-operator-system",
    },
    Data: sourceSecret.Data,
    Type: sourceSecret.Type,
}
r.Create(ctx, targetSecret)
```

## Common Tasks

### Adding a New Field to a CRD

1. Add field to struct in `api/v1/*_types.go`:
   ```go
   type DisconnectedPlatformSpec struct {
       Mode      PlatformMode     `json:"mode"`
       NewField  string           `json:"newField,omitempty"`  // Add here
   }
   ```

2. Regenerate:
   ```bash
   make manifests generate
   ```

3. Update controller to use new field in `internal/controller/*.go`

4. Update sample manifests in `config/samples/`

### Adding RBAC Permissions

Add kubebuilder markers to controller file:
```go
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
```

Then regenerate:
```bash
make manifests
```

### Watching Additional Resources

In `SetupWithManager()`:
```go
func (r *SomeReconciler) SetupWithManager(mgr ctrl.Manager) error {
    return ctrl.NewControllerManagedBy(mgr).
        For(&v1.SomeResource{}).
        Owns(&appsv1.Deployment{}).        // Watch owned deployments
        Watches(&corev1.Secret{}, &secretEventHandler{}).  // Watch secrets
        Complete(r)
}
```

### Creating/Updating Kubernetes Resources

Use owner references for garbage collection:
```go
deployment.SetOwnerReferences([]metav1.OwnerReference{
    {
        APIVersion: platform.APIVersion,
        Kind:       platform.Kind,
        Name:       platform.Name,
        UID:        platform.UID,
        Controller: func() *bool { b := true; return &b }(),
    },
})
```

Check-create-update pattern:
```go
existing := &appsv1.Deployment{}
if err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing); err == nil {
    // Update existing
    desired.SetResourceVersion(existing.GetResourceVersion())
    return r.Update(ctx, desired)
} else if apierrors.IsNotFound(err) {
    // Create new
    return r.Create(ctx, desired)
} else {
    return err
}
```

## Testing

### Unit Tests
Located in `*_test.go` files alongside controllers. Use `envtest` for controller testing:

```go
var _ = Describe("DisconnectedPlatformReconciler", func() {
    It("adds finalizer on first reconcile", func() {
        platform := &mirrorv1.DisconnectedPlatform{...}
        r := &DisconnectedPlatformReconciler{Client: fakeClient}
        
        _, err := r.Reconcile(ctx, req)
        Expect(err).NotTo(HaveOccurred())
        Expect(platform.Finalizers).To(ContainElement(finalizer))
    })
})
```

### Integration Testing
Run operator locally with:
```bash
make run
```

Then interact with real cluster:
```bash
kubectl apply -f config/samples/
kubectl get disconnectedplatforms
kubectl describe disconnectedplatform my-platform
```

## Special Considerations

### Airgap Architect UI
The `DisconnectedPlatform` controller manages frontend/backend deployments for the Airgap Architect web UI:
- Frontend: React/Vite serving at port 5173
- Backend: Node.js API at port 4000
- Pull secret automatically mounted from OpenShift global secret
- Routes created for external access

### Pull Secret Management
The operator automatically:
1. Copies `openshift-config/pull-secret` to `mirror-operator-system` namespace
2. Watches for updates to source secret
3. Triggers pod restarts when secret changes
4. Allows custom pull secret override via `spec.architect.pullSecret`

Environment variables set in backend pod:
- `PULL_SECRET_FILE`: Path to mounted secret
- `OPENSHIFT_OPERATOR_MANAGED=true`: Signals operator management

### GitOps Considerations
Resources are designed for GitOps workflows:
- All specs are declarative
- Status is separate from spec
- Controllers are idempotent
- Resources can be safely re-applied

## Troubleshooting

### Operator Not Reconciling
```bash
# Check operator is running
kubectl get pods -n mirror-operator-system

# View logs
kubectl logs -n mirror-operator-system -l control-plane=controller-manager -f

# Check for RBAC issues
kubectl auth can-i create deployments --as=system:serviceaccount:mirror-operator-system:mirror-operator-controller-manager
```

### CRD Changes Not Applying
```bash
# Reinstall CRDs
make uninstall install

# Verify CRD version
kubectl get crd disconnectedplatforms.mirror.mirror.mathianasj.github.com -o yaml | grep version
```

### Status Not Updating
Status subresource must be updated separately:
```go
// Wrong - updates spec and status together
r.Update(ctx, obj)

// Right - only updates status
r.Status().Update(ctx, obj)
```

## References

- [Kubebuilder Book](https://book.kubebuilder.io/) - Operator framework documentation
- [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime) - Core reconciliation library
- [CRD Reference](docs/crd-reference.md) - Complete API documentation
- [Integration Guide](docs/integration-guide.md) - How to integrate with this operator

## Contact

For questions about this codebase:
- Review existing issues: https://github.com/mathianasj/mirror-operator/issues
- Documentation: See `docs/` directory
- Code patterns: Follow existing controller implementations

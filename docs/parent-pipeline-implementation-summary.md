# Parent Pipeline Feature - Implementation Summary

## Overview

This document summarizes the implementation of the parent pipeline reference feature for CollectionPipeline resources. This feature enables delta collections by allowing child pipelines to reuse the oc-mirror cache from completed parent pipelines.

## What Was Changed

### 1. API Changes (`api/v1/collectionpipeline_types.go`)

#### New Spec Fields
```go
type CollectionPipelineSpec struct {
    // ... existing fields ...
    
    // ParentPipeline is the name of a parent CollectionPipeline to inherit cache from.
    // When set, this pipeline will reuse the parent's working PVC for incremental collection.
    // +optional
    ParentPipeline string `json:"parentPipeline,omitempty"`
}
```

#### New Status Fields
```go
type CollectionPipelineStatus struct {
    // ... existing fields ...
    
    // WorkingPVCName is the name of the PVC used for working storage during collection.
    // This is the PVC that contains oc-mirror's cache.
    // +optional
    WorkingPVCName string `json:"workingPvcName,omitempty"`
    
    // ParentPipelineVersion is the version of the parent pipeline at the time this pipeline started.
    // Populated from parent's status.version when this pipeline begins.
    // +optional
    ParentPipelineVersion string `json:"parentPipelineVersion,omitempty"`
}
```

### 2. Controller Changes (`internal/controller/collectionpipeline_controller.go`)

#### A. Parent Validation (Lines ~160-197)

**What it does:**
- Validates parent pipeline exists in same namespace
- Ensures parent has completed successfully (phase == "Complete")
- Captures parent's version in child's status for audit trail

**Error Handling:**
- Parent not found: Sets Failed status with condition
- Parent not complete: Requeues with 30s delay

**Code Location:**
```go
// Added after trigger annotation check, before incremental validation
if pipeline.Spec.ParentPipeline != "" {
    parentPipeline := &mirrorv1.CollectionPipeline{}
    if err := r.Get(ctx, types.NamespacedName{Name: pipeline.Spec.ParentPipeline, Namespace: pipeline.Namespace}, parentPipeline); err != nil {
        // Handle parent not found
    }
    
    if parentPipeline.Status.Phase != string(mirrorv1.CollectionPhaseComplete) {
        // Wait for parent to complete
    }
    
    // Capture parent version
    pipeline.Status.ParentPipelineVersion = parentPipeline.Status.Version
}
```

#### B. PVC Reuse Logic (Lines ~482-579)

**Modified Function:** `ensurePVC()`

**Priority Order for Working PVC:**
1. Parent pipeline's PVC (if `spec.parentPipeline` set)
2. Base version's PVC (if `spec.incremental` true)
3. Own PVC (new collections)

**Key Changes:**
```go
var workingPVCName string
if pipeline.Spec.ParentPipeline != "" {
    // Fetch parent and use its working PVC
    parentPipeline := &mirrorv1.CollectionPipeline{}
    r.Get(ctx, types.NamespacedName{Name: pipeline.Spec.ParentPipeline, Namespace: pipeline.Namespace}, parentPipeline)
    workingPVCName = parentPipeline.Status.WorkingPVCName
} else if pipeline.Spec.Incremental && pipeline.Spec.BaseVersion != "" {
    workingPVCName = fmt.Sprintf("collection-storage-%s", pipeline.Spec.BaseVersion)
} else {
    workingPVCName = fmt.Sprintf("collection-storage-%s", pipeline.Name)
}

// Track working PVC in status
pipeline.Status.WorkingPVCName = workingPVCName
```

**PVC Creation:**
- Only creates new PVC for non-parent, non-incremental collections
- Returns error if parent/base PVC doesn't exist

#### C. PipelineRun Building (Lines ~556-581)

**Modified Function:** `buildPipelineRun()`

**What Changed:**
- Uses `pipeline.Status.WorkingPVCName` instead of computing PVC name
- Fallback logic mirrors `ensurePVC()` for consistency

**Code:**
```go
// Use the working PVC name from status (set by ensurePVC)
workingPVCName := pipeline.Status.WorkingPVCName
if workingPVCName == "" {
    // Fallback: same logic as ensurePVC
}
```

### 3. CRD Manifest Changes

**File:** `config/crd/bases/mirror.mirror.mathianasj.github.com_collectionpipelines.yaml`

**Generated Changes:**
- Added `spec.parentPipeline` field with description
- Added `status.workingPvcName` field with description
- Added `status.parentPipelineVersion` field with description

### 4. Sample Manifests

**New File:** `config/samples/mirror_v1_collectionpipeline_parent.yaml`

**Contents:**
```yaml
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: CollectionPipeline
metadata:
  name: collection-v4-17-update
spec:
  parentPipeline: collection-v4-17  # References parent
  imageSetConfig: |
    # Updated configuration with new operators or versions
  triggerType: manual
  # ... other config
```

**Updated:** `config/samples/kustomization.yaml`
- Added `mirror_v1_collectionpipeline_parent.yaml` to resources list

### 5. Documentation

**New Files:**
- `docs/parent-pipeline-ui-integration.md` - Complete UI integration guide
- `docs/parent-pipeline-implementation-summary.md` - This document

**Sections in UI Integration Guide:**
- Overview and benefits
- CRD fields reference
- How parent pipelines work
- UI integration requirements
- API operations
- Example workflows
- Testing procedures

## How It Works

### Sequence Diagram

```
User                Controller              Kubernetes API
 |                      |                         |
 | Create child CR      |                         |
 |--------------------->|                         |
 |                      |                         |
 |                      | Get parent pipeline     |
 |                      |------------------------>|
 |                      |<------------------------|
 |                      |                         |
 |                      | Validate: phase=Complete|
 |                      |                         |
 |                      | Update child status:    |
 |                      | - parentPipelineVersion |
 |                      |------------------------>|
 |                      |                         |
 |                      | Get parent PVC name     |
 |                      |------------------------>|
 |                      |<------------------------|
 |                      |                         |
 |                      | Update child status:    |
 |                      | - workingPvcName        |
 |                      |------------------------>|
 |                      |                         |
 |                      | Create PipelineRun      |
 |                      | (uses parent's PVC)     |
 |                      |------------------------>|
 |                      |                         |
 |<---------------------|                         |
 | Child collecting     |                         |
```

### Data Flow

1. **User creates child CollectionPipeline** with `spec.parentPipeline = "collection-v4-17"`

2. **Controller validates parent:**
   - Fetches parent: `GET /apis/.../collectionpipelines/collection-v4-17`
   - Checks: `parent.status.phase == "Complete"`
   - Captures: `parent.status.version → child.status.parentPipelineVersion`

3. **Controller determines PVC:**
   - Gets: `parent.status.workingPvcName`
   - Sets: `child.status.workingPvcName = parent.status.workingPvcName`

4. **Controller creates PipelineRun:**
   - Workspace binding: `claimName: <parent's PVC>`
   - oc-mirror runs and finds cached blobs
   - Only downloads delta content

5. **Result:**
   - Bundle contains only new/changed images
   - Significantly smaller than full collection
   - Faster collection time

## Key Design Decisions

### 1. Parent Reference vs Incremental Flag

These are **complementary**, not mutually exclusive:

| Feature | Scope | Purpose | When to Use |
|---------|-------|---------|-------------|
| `parentPipeline` | Connected side | Reuse collection cache | Creating updated bundles |
| `incremental` + `baseVersion` | Airgapped side | Validate import history | Ensuring prerequisites exist |

**Can use both:**
```yaml
spec:
  parentPipeline: collection-v4-16      # Reuse cache
  incremental: true                     # Also validate imported
  baseVersion: v2026.05.26.001-manual   # Base version
```

### 2. Status Fields vs Computed Values

**Decision:** Store `workingPvcName` in status rather than computing each time.

**Rationale:**
- Single source of truth
- Easier debugging (inspect status to see which PVC is used)
- Prevents race conditions if parent changes
- Simplifies PipelineRun building

### 3. Validation Timing

**Decision:** Validate parent before creating PipelineRun.

**Rationale:**
- Fail fast if parent doesn't exist
- Prevent wasted resources (PVC creation, ConfigMap creation)
- Clear error messages to user
- Requeue if parent not ready yet (non-destructive wait)

### 4. PVC Ownership

**Decision:** Parent owns its PVC, child references it (no owner reference).

**Rationale:**
- Parent PVC must not be deleted while children use it
- Prevents accidental deletion via garbage collection
- Future: Could add reference counting for cleanup

### 5. Namespace Scoping

**Decision:** Parent reference is name-only (same namespace implied).

**Rationale:**
- Consistent with `baseVersion` (string, not object reference)
- Simpler API
- PVCs are namespace-scoped anyway
- Most users don't need cross-namespace references

## Testing

### Unit Tests Needed

**Location:** `internal/controller/collectionpipeline_controller_test.go`

**Test Cases:**
1. Parent validation succeeds when parent exists and is Complete
2. Parent validation fails when parent not found
3. Parent validation waits when parent not Complete
4. PVC reuse uses parent's working PVC
5. Parent version captured in child status
6. Error when parent PVC doesn't exist
7. Priority order: parent > incremental > own

### Integration Testing

**Manual Steps:**
```bash
# 1. Deploy operator
make run

# 2. Create parent
kubectl apply -f config/samples/mirror_v1_collectionpipeline.yaml

# 3. Wait for completion
kubectl wait --for=jsonpath='{.status.phase}'=Complete \
  collectionpipeline/collection-v4-17 --timeout=60m

# 4. Verify parent status
kubectl get cp collection-v4-17 -o jsonpath='{.status.workingPvcName}'
# Should output: collection-storage-collection-v4-17

# 5. Create child
kubectl apply -f config/samples/mirror_v1_collectionpipeline_parent.yaml

# 6. Verify child status
kubectl get cp collection-v4-17-update -o yaml | grep -A5 status:
# Should show:
#   parentPipelineVersion: v2026.07.10.001-manual
#   workingPvcName: collection-storage-collection-v4-17

# 7. Check PipelineRun workspace
kubectl get pipelinerun -l mirror.mathianasj.github.com/collection-pipeline=collection-v4-17-update \
  -o jsonpath='{.items[0].spec.workspaces}' | jq
# Should show workspace using parent's PVC
```

### Expected Behaviors

**Success Path:**
1. Parent created and completes
2. Child references parent
3. Child status shows: `parentPipelineVersion`, `workingPvcName`
4. PipelineRun uses parent's PVC
5. Collection completes with smaller bundle

**Error Path 1 - Parent Not Found:**
1. Child created with invalid parent name
2. Status: `phase: Failed`
3. Condition: `ParentPipelineValid = False, Reason: ParentNotFound`

**Error Path 2 - Parent Not Complete:**
1. Child created while parent still collecting
2. Status: `phase: Pending` (not Failed)
3. Requeues every 30s until parent completes
4. Once parent completes, child proceeds

**Error Path 3 - Parent PVC Missing:**
1. Parent completed but PVC was deleted
2. Child attempts to start
3. ensurePVC returns error: "parent working PVC ... must exist"
4. Reconcile fails, requeues

## Compatibility

### Backward Compatibility

**Existing CollectionPipelines:** No changes required
- `spec.parentPipeline` is optional
- If not set, behavior identical to before
- Existing pipelines continue to work

**API Compatibility:** Additive changes only
- No fields removed or renamed
- No breaking changes to existing fields
- CRD version unchanged (v1)

### Forward Compatibility

**Future Enhancements:**
- Multi-generation chains (grandparent → parent → child)
- Cross-namespace references (if needed)
- PVC reference counting for cleanup
- Automatic parent selection (latest completed)

## Metrics and Observability

### Recommended Metrics to Add (Future)

```
collectionpipeline_parent_references_total
  - Total child pipelines with parent references

collectionpipeline_cache_reuse_bytes_saved
  - Bytes saved by reusing parent cache

collectionpipeline_delta_collection_duration_seconds
  - Duration of delta collections vs full collections

collectionpipeline_parent_validation_failures_total
  - Count of parent validation failures by reason
```

### Log Messages Added

**Parent Validation:**
```
"Waiting for parent pipeline to complete", 
  "parent", pipeline.Spec.ParentPipeline, 
  "parentPhase", parentPipeline.Status.Phase
```

**PVC Reuse:**
```
logger.Info("Reusing parent working PVC",
  "parent", pipeline.Spec.ParentPipeline,
  "pvc", workingPVCName)
```

## Files Modified

```
api/v1/collectionpipeline_types.go                           | Added 3 fields
config/crd/bases/mirror.mirror...collectionpipelines.yaml    | Generated CRD
internal/controller/collectionpipeline_controller.go         | Added validation, PVC reuse
config/samples/mirror_v1_collectionpipeline_parent.yaml      | New sample
config/samples/kustomization.yaml                            | Added sample reference
docs/parent-pipeline-ui-integration.md                       | New documentation
docs/parent-pipeline-implementation-summary.md               | This file
```

## Migration Path

### For Existing Users

No migration needed. Feature is opt-in:
1. Existing pipelines work unchanged
2. New pipelines can use `parentPipeline` field
3. No data migration required

### For New Users

Recommended workflow:
1. Create initial full collection
2. Wait for completion
3. Create subsequent collections with parent reference
4. Benefit from delta collections

## Known Limitations

1. **Same Namespace Only:** Parent must be in same namespace as child
2. **No PVC Cleanup:** Parent PVC persists even if children deleted (manual cleanup needed)
3. **No Cross-Cluster:** Parent and child must be in same cluster
4. **Serial Dependency:** Child cannot start until parent completes (by design)
5. **No Automatic Updates:** If parent re-runs, children don't automatically re-run

## Future Enhancements

### Short Term
- [ ] Add metrics for cache reuse
- [ ] Unit tests for parent validation
- [ ] Integration tests for full workflow

### Medium Term
- [ ] PVC reference counting for cleanup
- [ ] UI integration (covered in separate doc)
- [ ] Automatic parent selection based on latest complete

### Long Term
- [ ] Multi-generation lineage support
- [ ] Cross-namespace parent references (if needed)
- [ ] Automatic child re-creation when parent updates
- [ ] Bundle diff visualization

## Summary

The parent pipeline feature is fully implemented and ready for use. It enables:
- ✅ Reuse of oc-mirror cache across collections
- ✅ Delta bundles with only new/changed content
- ✅ Audit trail of parent-child relationships
- ✅ Faster collection times and smaller bundles
- ✅ Backward compatible with existing pipelines

**Next Steps:**
1. UI integration (see `docs/parent-pipeline-ui-integration.md`)
2. User testing and feedback
3. Documentation updates (CRD reference, user guides)
4. Metrics and observability enhancements

# Parent Pipeline Feature - UI Integration Guide

## Overview

The CollectionPipeline CRD now supports parent references, allowing users to create delta collections that reuse the oc-mirror cache from a completed parent pipeline. This significantly reduces collection time and bundle size by only downloading new/changed content.

**Key Benefits:**
- Reuses oc-mirror cache from parent pipeline's working PVC
- Generates delta bundles containing only new/changed images
- Tracks parent-child relationships for audit trail
- Enables iterative collection workflows (add operators, update versions)

## What Was Implemented

### New CRD Fields

#### Spec Fields (User-Provided)

**`spec.parentPipeline`** (string, optional)
- Name of parent CollectionPipeline to inherit cache from
- Must reference a pipeline in the same namespace
- Parent must be in `Complete` status before child can start
- Example: `"collection-v4-17"`

#### Status Fields (System-Populated)

**`status.workingPvcName`** (string)
- Name of the PVC used for working storage during collection
- Contains the oc-mirror cache
- Child pipelines inherit this from parent
- Example: `"collection-storage-collection-v4-17"`

**`status.parentPipelineVersion`** (string)
- Version of parent pipeline at the time child started
- Captured from parent's `status.version` field
- Provides audit trail of lineage
- Example: `"v2026.07.10.001-manual"`

## How Parent Pipelines Work

### Workflow Sequence

1. **Parent Collection (Initial)**
   ```yaml
   apiVersion: mirror.mirror.mathianasj.github.com/v1
   kind: CollectionPipeline
   metadata:
     name: collection-v4-17
   spec:
     imageSetConfig: |
       # Full config with operators, platform, etc.
     triggerType: manual
   ```
   - Creates working PVC: `collection-storage-collection-v4-17`
   - oc-mirror downloads all content
   - Generates full bundle

2. **Child Collection (Delta)**
   ```yaml
   apiVersion: mirror.mirror.mathianasj.github.com/v1
   kind: CollectionPipeline
   metadata:
     name: collection-v4-17-update
   spec:
     parentPipeline: collection-v4-17  # References parent
     imageSetConfig: |
       # Updated config with new operators or versions
     triggerType: manual
   ```
   - Reuses PVC: `collection-storage-collection-v4-17`
   - oc-mirror sees cached blobs, only downloads deltas
   - Generates smaller bundle with changes only

### Validation Rules

The controller validates:
1. **Parent Exists**: `spec.parentPipeline` must reference an existing CollectionPipeline
2. **Parent Completed**: Parent must have `status.phase == "Complete"`
3. **Working PVC Exists**: Parent's working PVC must exist before child starts

If validation fails:
- Phase set to `Failed`
- Condition added explaining the issue
- Child pipeline does not start

## UI Integration Requirements

### Data the UI Needs to Display

#### 1. List of Completed Collections (Candidates for Parent)

**Query:**
```yaml
GET /apis/mirror.mirror.mathianasj.github.com/v1/namespaces/{namespace}/collectionpipelines
Filter: status.phase == "Complete"
```

**Display Fields:**
- `metadata.name` - Pipeline name
- `status.version` - Version identifier
- `status.completionTime` - When it finished
- `status.bundleUrl` - Location of bundle
- `spec.imageSetConfig` - Configuration (for pre-population)

#### 2. Parent-Child Relationships (Lineage View)

**Fields to Track:**
- `spec.parentPipeline` - Parent name
- `status.parentPipelineVersion` - Parent version at collection time
- `status.workingPvcName` - Shared cache PVC

**Visual Representation:**
```
collection-v4-17 (v2026.07.10.001)
  ├─ collection-storage-collection-v4-17 (100Gi PVC)
  │
  └─ collection-v4-17-update (v2026.07.10.002)
       └─ Uses same PVC (delta collection)
```

### Suggested UI Workflow

#### Option 1: "Update Collection" Button

When viewing a completed CollectionPipeline:

**UI Elements:**
1. **Button**: "Create Update Collection" or "Create Delta Collection"
2. **Click Action**:
   - Pre-populate form with parent's `spec.imageSetConfig`
   - Set parent reference automatically
   - Allow user to modify imageSetConfig
   - Generate unique name (e.g., `{parent-name}-update-{timestamp}`)

**Form Fields:**
```
Parent Pipeline: [collection-v4-17] (read-only, auto-filled)
Name: [collection-v4-17-update-20260710] (editable)
Image Set Configuration: [YAML editor with parent's config] (editable)
Trigger Type: [manual/scheduled] (editable)
Storage: [inherited from platform or editable]
Signing: [inherited or editable]
```

#### Option 2: Create Collection with Parent Selector

When creating any new CollectionPipeline:

**UI Elements:**
1. **Checkbox**: "Create as delta collection from parent"
2. **Dropdown** (if checked): Select parent from completed pipelines
3. **YAML Editor**: Pre-populate from parent when selected
4. **Visual Indicator**: Show cache reuse benefit

**Example Flow:**
```
┌─────────────────────────────────────┐
│ Create Collection Pipeline          │
├─────────────────────────────────────┤
│ ☑ Create delta from parent          │
│                                     │
│ Parent Pipeline:                    │
│ [collection-v4-17 ▼]                │
│   collection-v4-16                  │
│ ▸ collection-v4-17 (selected)       │
│   collection-v4-18                  │
│                                     │
│ Image Set Configuration:            │
│ ┌─────────────────────────────────┐ │
│ │ # Pre-populated from parent     │ │
│ │ kind: ImageSetConfiguration     │ │
│ │ apiVersion: mirror.openshift... │ │
│ └─────────────────────────────────┘ │
│                                     │
│ [Cancel] [Create Delta Collection] │
└─────────────────────────────────────┘
```

### API Operations for UI

#### 1. List Completed Pipelines (for parent selection)

**Kubernetes Client Call:**
```typescript
const completedPipelines = await k8sListResource({
  model: CollectionPipelineModel,
  queryOptions: {
    ns: namespace,
  },
}).then(response => 
  response.items.filter(p => p.status?.phase === 'Complete')
);
```

#### 2. Fetch Parent Configuration (for pre-population)

**Kubernetes Client Call:**
```typescript
const parentPipeline = await k8sGetResource({
  model: CollectionPipelineModel,
  name: parentPipelineName,
  ns: namespace,
});

// Pre-populate form
const imageSetConfig = parentPipeline.spec.imageSetConfig;
```

#### 3. Create Child Pipeline

**Kubernetes Client Call:**
```typescript
const childPipeline = {
  apiVersion: 'mirror.mirror.mathianasj.github.com/v1',
  kind: 'CollectionPipeline',
  metadata: {
    name: `${parentPipelineName}-update`,
    namespace: namespace,
  },
  spec: {
    parentPipeline: parentPipelineName,  // NEW FIELD
    imageSetConfig: modifiedYaml,         // User's changes
    triggerType: 'manual',
    storage: parentPipeline.spec.storage,
    signing: parentPipeline.spec.signing,
  },
};

await k8sCreateResource({
  model: CollectionPipelineModel,
  data: childPipeline,
});
```

#### 4. Monitor Child Pipeline Status

**Watch for Status Updates:**
```typescript
// Watch for status changes
const pipeline = await k8sGetResource({
  model: CollectionPipelineModel,
  name: childPipelineName,
  ns: namespace,
});

// Display these fields
const status = {
  phase: pipeline.status?.phase,                          // "Pending" | "Collecting" | "Complete" | "Failed"
  parentPipelineVersion: pipeline.status?.parentPipelineVersion,  // e.g., "v2026.07.10.001-manual"
  workingPvcName: pipeline.status?.workingPvcName,        // e.g., "collection-storage-collection-v4-17"
  bundleUrl: pipeline.status?.bundleUrl,                  // When complete
};
```

### Status Display in UI

#### Collection Pipeline Detail Page

**Parent Information Section** (show when `spec.parentPipeline` is set):
```
┌──────────────────────────────────────────┐
│ Delta Collection                         │
├──────────────────────────────────────────┤
│ Parent Pipeline:  collection-v4-17       │
│ Parent Version:   v2026.07.10.001-manual │
│ Shared Cache PVC: collection-storage-... │
│ Cache Status:     Reusing (delta mode)   │
└──────────────────────────────────────────┘
```

#### Collection List Page

**Visual Indicators:**
- Icon/badge for delta collections
- Parent name shown in list
- Lineage tree view (optional)

**Example List Item:**
```
┌────────────────────────────────────────────────────────┐
│ collection-v4-17-update                         ◀ 🔗   │
│ Status: Complete  |  Delta from: collection-v4-17     │
│ Version: v2026.07.10.002-manual                       │
│ Size: 2.3 GB (delta) | Parent: 45 GB (full)          │
└────────────────────────────────────────────────────────┘
```

### Validation and Error Handling

#### Error Conditions to Display

1. **Parent Not Found**
   ```
   Condition Type: ParentPipelineValid
   Status: False
   Reason: ParentNotFound
   Message: "parent pipeline collection-v4-17 not found"
   ```
   **UI Display**: Red alert with message and link to select valid parent

2. **Parent Not Complete**
   ```
   Status: Pipeline is waiting for parent to complete
   Parent Phase: Collecting (75% complete)
   ```
   **UI Display**: Info banner with parent progress and "Waiting..." status

3. **Working PVC Missing**
   ```
   Error: parent pipeline collection requires parent working PVC 
          collection-storage-collection-v4-17 to exist
   ```
   **UI Display**: Error alert explaining cache PVC not found

### Recommended UI Components

#### 1. Parent Pipeline Selector Component

**Props:**
```typescript
interface ParentPipelineSelectorProps {
  namespace: string;
  selectedParent?: string;
  onParentSelect: (parentName: string, parentConfig: string) => void;
  onClear: () => void;
}
```

**Features:**
- Dropdown of completed pipelines
- Shows version and completion date
- Preview of parent's imageSetConfig
- Clear selection option

#### 2. Delta Collection Badge Component

**Props:**
```typescript
interface DeltaCollectionBadgeProps {
  parentName: string;
  parentVersion?: string;
}
```

**Display:**
```
[🔗 Delta from: collection-v4-17]
```

#### 3. Lineage Tree Component

**Props:**
```typescript
interface LineageTreeProps {
  pipelines: CollectionPipeline[];
  selectedPipeline?: string;
}
```

**Display:**
```
collection-v4-16 (v2026.05.01.001) ────┐
                                       │
collection-v4-17 (v2026.07.10.001) ◄───┤
  └─ collection-v4-17-update (v2026.07.10.002)
```

## Example Workflows

### Workflow 1: Add New Operators

**User Journey:**
1. Navigate to completed pipeline "collection-v4-17"
2. Click "Create Update Collection"
3. UI pre-fills form:
   - Parent: collection-v4-17
   - Name: collection-v4-17-update-20260710
   - ImageSetConfig: (parent's config)
4. User edits imageSetConfig to add:
   ```yaml
   operators:
     - catalog: registry.redhat.io/redhat/redhat-operator-index:v4.17
       packages:
         - name: advanced-cluster-management
         - name: openshift-gitops-operator  # NEW
   ```
5. Click "Create"
6. UI shows:
   - Status: Pending → Collecting
   - Parent: collection-v4-17 (v2026.07.10.001-manual)
   - Cache: Reusing (delta mode)
7. Completion shows smaller bundle (only new operator)

### Workflow 2: Update OpenShift Version Range

**User Journey:**
1. Create new collection from parent "collection-v4-17"
2. Edit platform channel:
   ```yaml
   platform:
     channels:
       - name: stable-4.17
         minVersion: 4.17.5  # Updated from 4.17.0
         maxVersion: 4.17.10 # Updated from 4.17.5
   ```
3. Create and monitor
4. Bundle contains only new patch releases

## Testing the Feature

### Manual Testing (kubectl)

**Step 1: Create parent pipeline**
```bash
kubectl apply -f config/samples/mirror_v1_collectionpipeline.yaml

# Wait for completion
kubectl wait --for=jsonpath='{.status.phase}'=Complete \
  collectionpipeline/collection-v4-17 --timeout=60m
```

**Step 2: Create child pipeline**
```bash
kubectl apply -f config/samples/mirror_v1_collectionpipeline_parent.yaml
```

**Step 3: Verify parent reference**
```bash
kubectl get collectionpipeline collection-v4-17-update -o yaml

# Check status fields:
# - status.parentPipelineVersion (should match parent's version)
# - status.workingPvcName (should match parent's PVC)
```

**Step 4: Verify PVC reuse**
```bash
# Get parent's PVC
kubectl get collectionpipeline collection-v4-17 \
  -o jsonpath='{.status.workingPvcName}'

# Get child's PVC (should be same)
kubectl get collectionpipeline collection-v4-17-update \
  -o jsonpath='{.status.workingPvcName}'
```

### UI Testing Checklist

- [ ] Parent selector shows only completed pipelines
- [ ] Selecting parent pre-populates imageSetConfig
- [ ] Created pipeline shows parent reference in details
- [ ] Status page displays parentPipelineVersion
- [ ] List view shows delta badge/indicator
- [ ] Error handling for invalid parent works
- [ ] Waiting state shown when parent incomplete
- [ ] Bundle size comparison shown (parent full vs child delta)

## Additional Features to Consider

### Future Enhancements

1. **Visual Diff of ImageSetConfig**
   - Show diff between parent and child config
   - Highlight added/removed operators
   - Show version changes

2. **Bundle Size Predictions**
   - Estimate delta bundle size based on changes
   - Show cache hit rate

3. **Automatic Scheduling**
   - Create child pipelines on schedule
   - Auto-update to latest versions

4. **Multi-Generation Lineage**
   - Support grandparent → parent → child chains
   - Visualize full collection history

5. **Smart Defaults**
   - Suggest parent based on latest completed collection
   - Auto-name child pipelines with timestamps
   - Copy parent's storage/signing config

## Reference Files

### Implementation Files
- `api/v1/collectionpipeline_types.go` - CRD type definitions
- `internal/controller/collectionpipeline_controller.go` - Controller logic
- `config/samples/mirror_v1_collectionpipeline_parent.yaml` - Example usage
- `config/crd/bases/mirror.mirror.mathianasj.github.com_collectionpipelines.yaml` - Generated CRD

### Key Code Locations

**Parent Validation** (collectionpipeline_controller.go:160-197):
```go
if pipeline.Spec.ParentPipeline != "" {
    // Validate parent exists and is Complete
    // Capture parent version in status
}
```

**PVC Reuse Logic** (collectionpipeline_controller.go:482-579):
```go
func (r *CollectionPipelineReconciler) ensurePVC(ctx, pipeline) {
    // Priority: parent > incremental > own
    if pipeline.Spec.ParentPipeline != "" {
        workingPVCName = parent.Status.WorkingPVCName
    }
}
```

## Questions or Issues

For questions about this feature:
1. Review sample manifest: `config/samples/mirror_v1_collectionpipeline_parent.yaml`
2. Check controller logs: `kubectl logs -n mirror-operator-system -l control-plane=controller-manager -f`
3. Inspect pipeline status: `kubectl describe collectionpipeline <name>`
4. Review CRD documentation: `docs/crd-reference.md`

## Summary

The parent pipeline feature is fully implemented and ready for UI integration. The UI should:
1. **List** completed pipelines as parent candidates
2. **Pre-populate** forms with parent's imageSetConfig
3. **Create** child pipelines with `spec.parentPipeline` set
4. **Display** parent relationships and delta indicators
5. **Monitor** parent validation and collection progress

This enables users to iteratively build collections with minimal data transfer and storage overhead.

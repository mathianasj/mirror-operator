# Airgap Architect Bundle Integration

This document describes how to integrate Airgap Architect container images into the collection bundle.

## TODO

- **Pipeline Parameter Defaults**: The architect-frontend-image and architect-backend-image parameters should have default values in the pipeline template definition. This allows standalone PipelineRuns (manual or GitOps-driven) to work without requiring the controller to pass these values. Current defaults should be:
  - `architect-frontend-image`: `quay.io/mirror-operator/airgap-architect-frontend:latest`
  - `architect-backend-image`: `quay.io/mirror-operator/airgap-architect-backend:latest`

- **Shared SBOM Cache Between Collection Pipelines**: Currently each CollectionPipeline has its own SBOM cache in `/workspace/output/sbom-cache`. This means the same images get scanned multiple times across different collections, wasting time and resources. 
  - **Problem**: The syft-sbom task stores cached SBOMs by digest in the working PVC, but each collection uses a separate PVC (1:1 mapping).
  - **Solution**: Create a shared PVC for SBOM cache that all collection pipelines can mount. The cache is keyed by image digest (sha256), so it's safe to share across collections.
  - **Implementation**: 
    - Create a single `sbom-cache` PVC (RWX or RWO with proper scheduling)
    - Mount it to all collection pipeline runs at `/workspace/sbom-cache`
    - Update the syft-sbom task to use this shared cache location
    - Benefits: First collection scans everything, subsequent collections reuse cached SBOMs for unchanged images
  - **Impact**: Significant time savings for incremental collections and collections with overlapping image sets

## Overview

The collection pipeline needs to export the Airgap Architect frontend and backend container images as tarballs and include them in the final bundle tar.gz file, along with an import script that can run them in an airgapped environment.

## Changes Required

### 1. CRD Status Fields

Added to `CollectionPipelineStatus`:
- `ArchitectFrontendImageURL` - URL to the frontend image tarball (inside the bundle)
- `ArchitectBackendImageURL` - URL to the backend image tarball (inside the bundle)
- `ArchitectImportScriptURL` - URL to the import script (inside the bundle)

### 2. Import Script

Created at `config/scripts/import-airgap-architect.sh`:
- Automatically imports images from tarballs if not already present
- Starts containers using podman (mimics docker-compose.yml behavior)
- Default command is `start` which auto-imports images
- Configurable via environment variables (DATA_DIR, MOCK_MODE, etc.)

### 3. Tekton Pipeline Changes

The `collection-pipeline-template` needs to be modified to:

1. **Export Airgap Architect Images** (new task after oc-mirror completes):
   ```yaml
   - name: export-architect-images
     taskSpec:
       params:
         - name: frontend-image
         - name: backend-image
       workspaces:
         - name: output
       steps:
         - name: save-frontend
           image: quay.io/podman/stable:latest
           script: |
             #!/bin/bash
             set -ex
             podman pull $(params.frontend-image)
             podman save $(params.frontend-image) | gzip > $(workspaces.output.path)/airgap-architect-frontend.tar.gz
         - name: save-backend
           image: quay.io/podman/stable:latest
           script: |
             #!/bin/bash
             set -ex
             podman pull $(params.backend-image)
             podman save $(params.backend-image) | gzip > $(workspaces.output.path)/airgap-architect-backend.tar.gz
   ```

2. **Copy Import Script** (new task):
   ```yaml
   - name: copy-import-script
     taskSpec:
       workspaces:
         - name: output
       steps:
         - name: copy-script
           image: registry.access.redhat.com/ubi9/ubi-minimal:latest
           script: |
             #!/bin/bash
             cat > $(workspaces.output.path)/import-airgap-architect.sh <<'EOF'
             # Script content here (embedded inline or fetched from configmap)
             EOF
             chmod +x $(workspaces.output.path)/import-airgap-architect.sh
   ```

3. **Update Bundle Creation** (modify existing bundle task):
   
   The existing bundle creation step needs to include the new files:
   ```bash
   # Existing: tar -czf bundle.tar.gz oc-mirror-output/
   # Updated:
   tar -czf bundle.tar.gz \
     oc-mirror-output/ \
     airgap-architect-frontend.tar.gz \
     airgap-architect-backend.tar.gz \
     import-airgap-architect.sh
   ```

4. **Pipeline Parameters** (add to buildPipelineRun):
   - `architect-frontend-image` - Frontend container image reference
   - `architect-backend-image` - Backend container image reference

### 4. Controller Changes

`buildPipelineRun()` needs to pass architect image parameters:

```go
params = append(params,
    pipelinev1.Param{
        Name: "architect-frontend-image",
        Value: pipelinev1.ParamValue{
            Type: "string",
            StringVal: r.getArchitectFrontendImage(),
        },
    },
    pipelinev1.Param{
        Name: "architect-backend-image",
        Value: pipelinev1.ParamValue{
            Type: "string",
            StringVal: r.getArchitectBackendImage(),
        },
    },
)
```

Helper methods:
```go
func (r *CollectionPipelineReconciler) getArchitectFrontendImage() string {
    // Get from DisconnectedPlatform or use default
    platform, _ := r.findPlatform(ctx)
    if platform != nil && platform.Spec.Architect != nil && platform.Spec.Architect.FrontendImage != "" {
        return platform.Spec.Architect.FrontendImage
    }
    return "quay.io/mirror-operator/airgap-architect-frontend:latest"
}

func (r *CollectionPipelineReconciler) getArchitectBackendImage() string {
    // Get from DisconnectedPlatform or use default
    platform, _ := r.findPlatform(ctx)
    if platform != nil && platform.Spec.Architect != nil && platform.Spec.Architect.BackendImage != "" {
        return platform.Spec.Architect.BackendImage
    }
    return "quay.io/mirror-operator/airgap-architect-backend:latest"
}
```

## Bundle Structure

After these changes, the bundle will contain:

```
bundle.tar.gz
├── oc-mirror-output/
│   ├── mirror_seq1_000000.tar
│   ├── publish/
│   └── ...
├── airgap-architect-frontend.tar.gz   # Frontend container image
├── airgap-architect-backend.tar.gz    # Backend container image
└── import-airgap-architect.sh         # Import and run script
```

## Usage in Airgapped Environment

1. Transfer bundle.tar.gz to airgapped environment
2. Extract bundle:
   ```bash
   tar -xzf bundle.tar.gz
   cd bundle/
   ```

3. Run Airgap Architect:
   ```bash
   ./import-airgap-architect.sh
   ```
   
   This will:
   - Check if images are already imported
   - Import them from tarballs if needed
   - Start both frontend and backend containers
   - Display access URLs (http://localhost:5173 and http://localhost:4000)

4. Import mirror content using the UI or oc-mirror directly

## Notes

- Images are exported from the same registry used by the operator (quay.io/mirror-operator by default)
- The script uses podman (not docker) to match RHEL/OpenShift environments
- Data persists in a podman volume (backend-data) across restarts
- The import script is idempotent - safe to run multiple times

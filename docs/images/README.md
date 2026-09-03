# Screenshot Capture Checklist

This directory contains screenshots for the [End-to-End Guide](../end-to-end-guide.md). Each screenshot should be captured from a live OpenShift cluster with the Airgap Architect console plugin enabled.

## Conventions

- Format: PNG
- Naming: `{phase}-{description}.png`
- Consistent cluster and namespace naming (see the guide's naming conventions)
- Browser chrome should be cropped out; show only the OpenShift console content area

## Phase 1: Installation

| # | Filename | What to Capture |
|---|----------|-----------------|
| 1 | `01-operatorhub-mirror-operator.png` | OperatorHub search results showing the Mirror Operator available for installation |
| 2 | `01-operator-installed.png` | Installed Operators view confirming mirror-operator is running |
| 3 | `01-console-nav-airgap-architect.png` | OpenShift console sidebar showing the Airgap Architect menu item under Administrator |
| 4 | `01-architect-dashboard.png` | Airgap Architect dashboard showing platform status, collection history, and import history |

## Phase 2: Content Collection

| # | Filename | What to Capture |
|---|----------|-----------------|
| 5 | `02-imageset-editor.png` | Architect UI ImageSetConfiguration editor wizard with platform channels and operator selection |
| 6 | `02-collection-create.png` | Collection creation form with ImageSetConfiguration, storage, and signing options |
| 7 | `02-collection-list.png` | Collection list view showing CollectionPipeline resources with phase, version, and timestamps |
| 8 | `02-collection-detail.png` | Collection detail view showing pipeline progress, task status, and log output |
| 9 | `02-collection-complete.png` | Completed collection showing bundle URL, signature URL, and SBOM reference |
| 10 | `02-delta-collection.png` | Delta collection view showing parent pipeline reference and incremental content difference |

## Phase 3: Transfer and Import — Path A (Bootstrap)

| # | Filename | What to Capture |
|---|----------|-----------------|
| 11 | `03a-architect-landing.png` | Airgap Architect standalone landing page showing "Start new install" card |
| 12 | `03a-blueprint-preloaded.png` | Blueprint step with pre-populated OpenShift version from bundle and platform selection |
| 13 | `03a-identity-preloaded.png` | Identity & Access step showing pre-configured mirror registry fields (read-only) |
| 14 | `03a-hosts-inventory.png` | Hosts/Inventory step with control plane and worker node definitions |
| 15 | `03a-review-assets.png` | Assets & Guide step showing generated install-config.yaml and agent-config.yaml |
| 16 | `03a-generate-iso.png` | Generate Agent ISO step with ISO download button and kubeadmin credentials |

## Phase 3: Transfer and Import — Path B (Ongoing)

| # | Filename | What to Capture |
|---|----------|-----------------|
| 17 | `03b-import-list.png` | Console plugin import list view on the airgapped side with "Import Bundle" button |
| 18 | `03b-import-wizard-source.png` | Import wizard Bundle Source step showing upload and import volume options |
| 19 | `03b-import-wizard-review.png` | Import wizard Review & Import step with summary of settings |
| 20 | `03b-import-detail.png` | MirrorImport detail view showing pipeline progress, task status, and registry target |
| 21 | `03b-operatorhub-mirrored.png` | OperatorHub displaying operators from the mirrored CatalogSource after successful import |
| 22 | `03b-catalogsource-resources.png` | OpenShift console showing CatalogSource and ImageDigestMirrorSet resources created by the import |

## Phase 4: Cluster Bootstrap

| # | Filename | What to Capture |
|---|----------|-----------------|
| 14 | `04-bootstrap-status.png` | ClusterBootstrap resource status view showing cluster provisioning phase and configuration |

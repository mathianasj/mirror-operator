# Prerequisites and Environment Setup

This document covers the requirements for deploying and using the mirror-operator on both connected and airgapped OpenShift clusters.

## Cluster Requirements

### Connected Side

| Requirement | Details |
|-------------|---------|
| OpenShift | 4.10 or later |
| Access level | `cluster-admin` |
| OLM | Operator Lifecycle Manager must be available |
| cert-manager | Required when using RHTAS with managed Keycloak. A `ClusterIssuer` must be configured for TLS certificate provisioning. |
| Internet access | Must reach `registry.redhat.io`, `quay.io`, and `github.com` |

### Airgapped Side

| Requirement | Details |
|-------------|---------|
| OpenShift | 4.10 or later |
| Access level | `cluster-admin` |
| OLM | Operator Lifecycle Manager with mirrored catalog sources |
| Mirror registry | A container registry accessible from the cluster (e.g., Quay, Harbor, or mirror-registry) |
| Internet access | None (by definition) |

## Required Credentials

The operator uses the cluster's existing pull secret from `openshift-config/pull-secret` (created during OpenShift installation) and automatically copies it to the `mirror-operator-system` namespace for collection and import workloads.

## Storage Sizing Guide

| Component | Minimum | Recommended | Notes |
|-----------|---------|-------------|-------|
| Artifact storage (`artifactStorage.size`) | 100Gi | 500Gi | Scales with the number of operators and platform versions mirrored |
| Collection working PVC | 50Gi | 200Gi | oc-mirror cache and working space |
| Bundle output PVC | 50Gi | 200Gi | Must fit the largest generated bundle |
| Import PVC | 50Gi | 200Gi | Must fit the largest bundle being imported |
| Quay storage (if managed) | 100Gi | 500Gi | Registry storage for all mirrored images |
| RHTPA storage (if enabled) | 50Gi | 200Gi | SBOM analysis data |

Storage classes with `ReadWriteOnce` (RWO) access mode are sufficient for most components. The SBOM cache benefits from `ReadWriteMany` (RWX) if sharing across concurrent pipelines.

## Network Requirements

### Connected Side

The following endpoints must be reachable from the cluster:

| Endpoint | Purpose |
|----------|---------|
| `registry.redhat.io` | Red Hat container images and operator catalogs |
| `quay.io` | Community and third-party images |
| `cdn.redhat.com` | Red Hat content delivery |
| `github.com` | Operator catalog metadata |
| S3 endpoint (if using S3 storage) | Bundle output destination |

### Airgapped Side

No internet connectivity is required. All traffic is internal:

| Endpoint | Purpose |
|----------|---------|
| Mirror registry (e.g., `quay.airgap.local`) | Destination for imported images |
| PVC mount path (e.g., `/mnt/physical-media`) | Source for bundle imports |

## CLI Tools

| Tool | Required | Purpose |
|------|----------|---------|
| `oc` | Yes | OpenShift CLI, matching your cluster version |
| `kubectl` | Yes | Kubernetes CLI (bundled with `oc`) |
| `cosign` | Optional | Manual signature verification |
| `tkn` | Optional | Tekton CLI for pipeline log inspection |
| `podman` | Optional | Running Airgap Architect locally in airgapped environments |

## Next Steps

Once your environment meets these prerequisites, proceed to the [End-to-End Guide](end-to-end-guide.md) to install the operator and begin mirroring content.

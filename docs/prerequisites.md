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

## Cluster Node Sizing

The mirror-operator installs several operators and workloads onto the cluster. The tables below show recommended per-node sizes for each supported topology.

### Operator Workload Summary

The following workloads are deployed by the operator and consume cluster resources in addition to the OpenShift platform baseline.

**Connected mode** (all default operators):

| Component | Pods | CPU Request | Memory Request |
|-----------|------|------------|----------------|
| Mirror operator | 1 | 250m | 256Mi |
| OpenShift Pipelines (operator + controllers) | 3 | 600m | 1.5Gi |
| RHBK operator + Keycloak + PostgreSQL | 3 | 1 core | 1.8Gi |
| RHTAS operator + Fulcio + Rekor + CTLog + Trillian (3) + TUF | 8 | 1.2 cores | 2.4Gi |
| RHTPA operator + Trustify importer + server + PostgreSQL | 4 | 2.5 cores | 17Gi |
| Quay operator + app + mirror + Clair + PostgreSQL + Redis | 6 | 1.9 cores | 5.5Gi |
| OSUS operator + UpdateService (2 replicas) | 3 | 300m | 768Mi |
| Architect frontend + backend | 2 | 500m | 1Gi |
| **Total** | **~30** | **~8 cores** | **~30Gi** |

Tekton pipeline pods add ~1-2 cores and 2-4 Gi during active collection runs.

**Airgapped mode** (default operators, no ACM):

| Component | Pods | CPU Request | Memory Request |
|-----------|------|------------|----------------|
| Mirror operator | 1 | 250m | 256Mi |
| OpenShift Pipelines (operator + controllers) | 3 | 600m | 1.5Gi |
| Quay operator + app + mirror + Clair + PostgreSQL + Redis | 6 | 1.9 cores | 5.5Gi |
| OSUS operator + UpdateService (2 replicas) | 3 | 300m | 768Mi |
| Architect frontend + backend | 2 | 500m | 1Gi |
| RHCOS server | 1 | 100m | 128Mi |
| **Total** | **~16** | **~3.7 cores** | **~9Gi** |

**Airgapped mode with ACM** adds:

| Component | Pods | CPU Request | Memory Request |
|-----------|------|------------|----------------|
| ACM operator + MultiClusterHub | 12-15 | 3 cores | 8Gi |
| Assisted Installer | 2-3 | 500m | 1.5Gi |
| **ACM subtotal** | **~15** | **~3.5 cores** | **~9.5Gi** |
| **Grand total (airgapped + ACM)** | **~31** | **~7.2 cores** | **~18.5Gi** |

### Connected Cluster — Per-Node Sizing

| Resource | Single Node (SNO) | Two Node (per node) | Compact 3-Node (per node) |
|----------|-------------------|---------------------|---------------------------|
| **vCPU** | 32 | 16 | 16 |
| **RAM** | 128 GB | 64 GB | 64 GB |
| **OS disk** | 200 GB | 200 GB | 200 GB |
| **PVC storage** | 1 TB | 500 GB | 500 GB |

RHTPA Trustify is the largest single consumer (2 cores, 16 Gi). On compact clusters, ensure each node has enough RAM for Trustify to schedule if it lands there.

### Airgapped Cluster — Per-Node Sizing (no ACM)

| Resource | Single Node (SNO) | Two Node (per node) | Compact 3-Node (per node) |
|----------|-------------------|---------------------|---------------------------|
| **vCPU** | 24 | 16 | 8 |
| **RAM** | 64 GB | 32 GB | 32 GB |
| **OS disk** | 200 GB | 200 GB | 200 GB |
| **PVC storage** | 500 GB | 300 GB | 300 GB |

### Airgapped Cluster — Per-Node Sizing (with ACM)

| Resource | Single Node (SNO) | Two Node (per node) | Compact 3-Node (per node) |
|----------|-------------------|---------------------|---------------------------|
| **vCPU** | 32 | 16 | 16 |
| **RAM** | 96 GB | 64 GB | 32 GB |
| **OS disk** | 200 GB | 200 GB | 200 GB |
| **PVC storage** | 1 TB | 500 GB | 500 GB |

### PVC Storage Breakdown

**Connected cluster PVCs:**

| PVC | Size | Notes |
|-----|------|-------|
| RHTPA PostgreSQL | 50Gi (auto-expands to 200Gi) | SBOM analysis database |
| Keycloak PostgreSQL | 5Gi | Identity provider |
| Quay PostgreSQL | 50Gi | Managed by Quay operator |
| Quay object storage | 100-200Gi | Image blob storage (or OBC) |
| Quay Redis | 10Gi | Cache |
| Collection working PVC | 100-200Gi | oc-mirror cache per pipeline |
| NooBaa / ODF | 50Gi | S3-compatible object storage |

**Airgapped cluster PVCs (with ACM):**

| PVC | Size | Notes |
|-----|------|-------|
| Quay PostgreSQL | 50Gi | Managed by Quay operator |
| Quay object storage | 200Gi | Image blob storage |
| Quay Redis | 10Gi | Cache |
| Import working PVC | 100-200Gi | Per MirrorImport |
| AgentServiceConfig database | 200Gi | ACM host inventory |
| AgentServiceConfig filesystem | 200Gi | ACM host inventory |
| AgentServiceConfig image | 100Gi | ACM ISO storage |

On SNO all PVCs reside on the single node. On multi-node clusters PVCs distribute across nodes based on the storage class.

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

## Importer Machine Requirements

The importer machine (bastion host) runs the `import-airgap-architect.sh` script in the airgapped environment. It installs a mirror-registry (Quay), mirrors images from the bundle archives, and runs the Airgap Architect UI containers via podman.

### Compute

| Resource | Recommended |
|----------|-------------|
| **CPU** | 8 cores |
| **RAM** | 32 GB |
| **OS** | RHEL 9 |
| **Software** | podman, openssl |

### Storage — Non-STIG Machine

Assumes a 150 GB bundle: download tar to `/opt/bundle`, extract, delete tar, then run the script.

| Mount | Contents | Peak | Steady-State | Recommended |
|-------|----------|------|-------------|-------------|
| `/opt` | Bundle (150 GB) + Quay data at `/opt/quay` (150 GB) + oc-mirror workspace (75 GB temp) | 375 GB | 300 GB | **500 GB** |
| `/home` | Podman container images (`~/.local/share/containers/storage`) + CLI tools (`~/.local/bin`) | 7 GB | 7 GB | **20 GB** |
| `/` | OS base | 20 GB | 20 GB | **50 GB** |
| **Total** | | | | **570 GB** |

### Storage — STIG Machine

Same assumptions. STIG adds separate partitions and relocates CLI tools to `/usr/local/bin`.

| Mount | Contents | Peak | Steady-State | Recommended |
|-------|----------|------|-------------|-------------|
| `/opt` | Bundle (150 GB) + Quay data at `/opt/quay` (150 GB) + oc-mirror workspace (75 GB temp) | 375 GB | 300 GB | **500 GB** |
| `/home` | Podman container images (`~/.local/share/containers/storage`); noexec OK for storage | 5 GB | 5 GB | **20 GB** |
| `/var` | Podman runtime data, logs | 5 GB | 3 GB | **20 GB** |
| `/var/tmp` | oc-mirror temp files (set `TMPDIR=/opt/bundle/tmp` to avoid this) | 10 GB | 0 | **10 GB** |
| `/tmp` | General temp; STIG mounts noexec, nosuid | 2 GB | 0 | **5 GB** |
| `/usr` | CLI tools at `/usr/local/bin` (oc, oc-mirror, openshift-install) | 2 GB | 2 GB | **+2 GB** over base |
| `/` | OS base | 15 GB | 15 GB | **30 GB** |
| **Total** | | | | **~587 GB** |

### Peak Storage Timeline at `/opt`

```
Step 1: Download tar           -> 150 GB  (tar file)
Step 2: Extract bundle         -> 300 GB  (tar + extracted)       <- PEAK during extraction
Step 3: Delete tar             -> 150 GB  (extracted only)
Step 4: Script starts mirror   -> 375 GB  (bundle + workspace + Quay filling)  <- PEAK during operation
Step 5: Mirror complete        -> 300 GB  (bundle + Quay data)
Step 6: Optional bundle delete -> 150 GB  (Quay data only, if bundle no longer needed)
```

> **STIG note**: On STIG machines with small `/tmp` and `/var/tmp`, set `TMPDIR=/opt/bundle/tmp` before running the script to keep oc-mirror temp files on the large `/opt` partition.

### Scaling the Estimates

The storage formula scales linearly with bundle size:

- **`/opt` recommended** = `bundle size x 2.5 + 125 GB`
- **Peak at `/opt`** = `bundle size x 2.5`
- **Steady-state at `/opt`** = `bundle size x 2`

All other partition sizes remain constant regardless of bundle size.

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

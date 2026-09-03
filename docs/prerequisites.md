# Prerequisites and Environment Setup

This document covers the requirements for deploying and using the mirror-operator on both connected and airgapped OpenShift clusters.

## Cluster Requirements

### Connected Side

| Requirement | Details |
|-------------|---------|
| OpenShift | 4.10 or later |
| Access level | `cluster-admin` |
| OLM | Operator Lifecycle Manager must be available |
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

### Red Hat Pull Secret

Obtain your pull secret from [console.redhat.com/openshift/install/pull-secret](https://console.redhat.com/openshift/install/pull-secret).

Create the secret in the `openshift-config` namespace:

```bash
oc create secret generic pull-secret \
  -n openshift-config \
  --from-file=.dockerconfigjson=pull-secret.json \
  --type=kubernetes.io/dockerconfigjson
```

The operator automatically copies this secret to the `mirror-operator-system` namespace and mounts it into collection and import workloads.

### Registry Credentials

For the mirror registry (airgapped side), create a `dockerconfigjson` secret:

```bash
oc create secret docker-registry mirror-registry-creds \
  -n mirror-operator-system \
  --docker-server=quay.airgap.local \
  --docker-username=admin \
  --docker-password=<password>
```

### Cosign Signing Key (Optional)

If using image signing, generate a cosign key pair and create secrets:

```bash
cosign generate-key-pair

oc create secret generic cosign-signing-key \
  -n mirror-operator-system \
  --from-file=cosign.key=cosign.key \
  --from-literal=cosign.password=<password>

oc create secret generic cosign-public-key \
  -n mirror-operator-system \
  --from-file=cosign.pub=cosign.pub
```

For keyless signing via RHTAS, see the [RHTAS Integration Guide](rhtas-integration.md).

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

### S3 Storage Alternative

Collection output can be directed to S3-compatible storage instead of PVCs. This requires an S3 credentials secret:

```bash
oc create secret generic s3-credentials \
  -n mirror-operator-system \
  --from-literal=AWS_ACCESS_KEY_ID=<key> \
  --from-literal=AWS_SECRET_ACCESS_KEY=<secret>
```

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

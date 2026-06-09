# Keyless Image Signing with Intermediate Registry

This document describes how the mirror-operator signs container images using keyless signatures (RHTAS/Fulcio) with an intermediate Quay registry to ensure signatures are included in the tar bundles for airgapped environments.

## Overview

The mirror-operator implements a **three-phase mirroring workflow** when keyless signing is enabled with an intermediate registry:

```
┌─────────────────────────────────────────────────────────────────┐
│                    CONNECTED ENVIRONMENT                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Internet Sources (Red Hat CDN, Quay.io, etc.)                 │
│        ↓                                                        │
│   [1] oc-mirror m2m (Mirror-to-Mirror)                         │
│        ↓                                                        │
│  Intermediate Quay Registry                                    │
│  (quay.apps.example.com/mirror/*)                             │
│        ↓                                                        │
│   [2] cosign sign + attest (Keyless with RHTAS)               │
│        • Sign each image with Fulcio certificates             │
│        • Attach SBOM as CycloneDX attestation                  │
│        ↓                                                        │
│  Intermediate Quay Registry (WITH SIGNATURES)                 │
│  • Images                                                       │
│  • Signatures (OCI artifacts)                                  │
│  • SBOM Attestations (in-toto format)                         │
│        ↓                                                        │
│   [3] oc-mirror m2d (Mirror-to-Disk FROM intermediate)        │
│        ↓                                                        │
│  Tar Archive (INCLUDES EVERYTHING!)                           │
│  • Images                                                       │
│  • Signatures ✓                                                │
│  • SBOM Attestations ✓                                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                          ↓
                  [Physical Transport]
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│                    AIRGAPPED ENVIRONMENT                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Tar Archive → [oc-mirror d2m] → Airgapped Registry           │
│                                   (WITH SIGNATURES!)            │
│                                                                 │
│  Verification:                                                 │
│  ✓ cosign verify --certificate-oidc-issuer=...                │
│  ✓ cosign verify-attestation --type=cyclonedx ...             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Why Intermediate Registry?

### The Problem with Direct Mirroring

When you mirror directly to disk (`oc-mirror --config=config.yaml file:///output`):

1. Images are pulled from internet sources
2. Images are stored in local cache
3. **oc-mirror creates tar archive immediately**
4. ❌ No opportunity to sign images before archiving
5. ❌ Signatures cannot be added to the tar

Even if you sign images in the local cache after mirroring, oc-mirror's archiving process has already completed and won't include the signatures.

### The Solution: Intermediate Registry

By using an intermediate Quay registry:

1. **Phase 1**: Mirror FROM internet TO intermediate registry (`m2m`)
2. **Phase 2**: Sign images IN the intermediate registry
3. **Phase 3**: Mirror FROM intermediate TO disk (`m2d`)

✅ Signatures are in the source registry when oc-mirror creates the tar  
✅ oc-mirror naturally includes all OCI artifacts (images + signatures + attestations)  
✅ Clean, supported workflow using standard oc-mirror operations  

## Architecture Components

### 1. Managed Quay Registry

The operator can deploy and manage a Quay registry for you:

```yaml
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: DisconnectedPlatform
spec:
  mode: connected
  connected:
    quay:
      managed:
        enabled: true
        organizationName: "mirror"
        storage:
          type: "filesystem"  # or "s3"
          size: "500Gi"
```

**Prerequisites:**
- **ObjectBucketClaim CRD** must be available on the cluster
  - Provided by OpenShift Data Foundation (ODF) or Noobaa operators
  - Required even for filesystem storage due to Quay operator architecture
  - If not available, use S3 storage or external Quay (see below)

**What it does:**
- Installs Quay Operator
- Creates QuayRegistry CR
- Configures storage (managed objectstorage with OBC support)
- Exposes via OpenShift Route
- Auto-updates `spec.connected.mirrorRegistry` field

**Alternative: S3 Storage (no ObjectBucketClaim required):**

```yaml
spec:
  connected:
    quay:
      managed:
        enabled: true
        organizationName: "mirror"
        storage:
          type: "s3"
          s3Bucket: "my-quay-bucket"
          s3AccessKey: "AKIA..."
          s3SecretKey: "secret..."
          s3Endpoint: "s3.amazonaws.com"
```

When using S3 storage, the operator creates a config bundle secret with S3 credentials, bypassing the ObjectBucketClaim requirement.

**Note on StorageClass:**
- For `type: filesystem` - The `storageClass` field is **not used** by the Quay operator when using managed objectstorage with ObjectBucketClaim. The OBC controller determines the StorageClass based on your ODF/Noobaa configuration.
- For `type: s3` - The `storageClass` field is not applicable as storage is external.

### 2. RHTAS (Keyless Signing)

```yaml
spec:
  connected:
    rhtas:
      oidc:
        managed:
          enabled: true
          realm: "trusted-artifact-signer"
```

**Components:**
- **Fulcio**: Issues short-lived signing certificates
- **Rekor**: Transparency log for signature records
- **TUF**: Distributes root of trust

### 3. CollectionPipeline Signing

```yaml
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: CollectionPipeline
spec:
  signing:
    keyless:
      fulcioURL: "https://fulcio.mirror-operator-system.svc"
      rekorURL: "https://rekor.mirror-operator-system.svc"
      tufURL: "http://tuf.mirror-operator-system.svc"
      oidcIssuer: "https://keycloak.apps.example.com/realms/..."
      oidcClientID: "mirror-operator"
      oidcClientSecret:
        name: "rhtpa-oidc-secret"
```

## Tekton Pipeline Workflow

When a CollectionPipeline runs with intermediate registry:

### Task 1: dry-run
```bash
oc-mirror --v2 --config=config.yaml --dry-run file:///output
```
**Purpose**: Generate `mapping.txt` with list of all images to mirror

### Task 2: mirror-to-intermediate
```bash
oc-mirror --v2 \
  --config=config.yaml \
  docker://quay.apps.example.com/mirror
```
**Purpose**: Mirror images from internet to intermediate Quay registry

**Result**: All images now in `quay.apps.example.com/mirror/*`

### Task 3: syft-sbom
```bash
# For each image in mapping.txt:
syft image@digest -o cyclonedx-json > /attestations/image.json
```
**Purpose**: Generate per-image SBOMs for attestation

**Result**: Individual SBOM files for each image

### Task 4: sign-images
```bash
# Get OIDC token
TOKEN=$(curl -X POST "$OIDC_ISSUER/protocol/openid-connect/token" \
  -d "grant_type=client_credentials" \
  -d "client_id=$OIDC_CLIENT_ID" \
  -d "client_secret=$OIDC_SECRET" | jq -r '.access_token')

# Initialize TUF
cosign initialize --mirror="$TUF_URL" --root="$TUF_URL/root.json"

# For each image in mapping.txt:
IMAGE_REF="quay.apps.example.com/mirror/openshift/release@sha256:..."

# Sign image
cosign sign \
  --fulcio-url "$FULCIO_URL" \
  --rekor-url "$REKOR_URL" \
  --fulcio-auth-flow=client_credentials \
  --oidc-issuer "$OIDC_ISSUER" \
  --oidc-token "$TOKEN" \
  --yes \
  --registry-referrers-mode=oci-1-1 \
  "$IMAGE_REF"

# Attach SBOM attestation
cosign attest \
  --fulcio-url "$FULCIO_URL" \
  --rekor-url "$REKOR_URL" \
  --fulcio-auth-flow=client_credentials \
  --oidc-issuer "$OIDC_ISSUER" \
  --oidc-token "$TOKEN" \
  --yes \
  --type cyclonedx \
  --predicate /attestations/image.json \
  --registry-referrers-mode=oci-1-1 \
  "$IMAGE_REF"
```

**Purpose**: Sign images and attach SBOM attestations IN the intermediate registry

**Result**: 
- Each image has a signature stored as OCI artifact
- Each image has an SBOM attestation (in-toto format)
- All stored in Quay alongside the images

### Task 5: mirror-from-intermediate
```bash
# Generate new ImageSetConfiguration that points to intermediate registry
cat > intermediate-config.yaml <<EOF
kind: ImageSetConfiguration
apiVersion: mirror.openshift.io/v2alpha1
mirror:
  platform:
    release: "quay.apps.example.com/mirror/openshift/release-images"
  operators:
    - catalog: quay.apps.example.com/mirror/redhat/redhat-operator-index:v4.18
  additionalImages:
    - name: quay.apps.example.com/mirror/*
EOF

# Mirror FROM intermediate TO disk
oc-mirror --v2 \
  --config=intermediate-config.yaml \
  file:///output
```

**Purpose**: Create tar archive by mirroring FROM intermediate registry (which has signatures)

**Result**: 
- `mirror_TIMESTAMP.tar` file
- **Includes images, signatures, and attestations!**

### Task 6: sign-bundles
```bash
# Sign the tar file itself
cosign sign-blob \
  --fulcio-url "$FULCIO_URL" \
  --rekor-url "$REKOR_URL" \
  --fulcio-auth-flow=client_credentials \
  --oidc-issuer "$OIDC_ISSUER" \
  --oidc-token "$TOKEN" \
  --yes \
  mirror_TIMESTAMP.tar \
  --bundle mirror_TIMESTAMP.bundle
```

**Purpose**: Sign the tar bundle file for transport integrity

**Result**: Bundle signature for verifying the tar wasn't tampered with during transport

## Airgapped Environment Workflow

### Import with Signatures

```bash
# On airgapped side
oc-mirror --v2 \
  --from file:///path/to/bundle \
  docker://airgapped-registry.local
```

**What happens:**
1. oc-mirror extracts tar archive
2. Pushes images to airgapped registry
3. **Pushes signature OCI artifacts** 
4. **Pushes attestation artifacts**

### Verification

#### Verify Image Signature
```bash
cosign verify \
  --certificate-identity-regexp=".*mirror-operator.*" \
  --certificate-oidc-issuer="https://keycloak.apps.example.com/realms/trusted-artifact-signer" \
  airgapped-registry.local/openshift/release@sha256:...
```

**Output:**
```json
{
  "critical": {
    "identity": {
      "docker-reference": "quay.apps.example.com/mirror/openshift/release"
    },
    "image": {
      "docker-manifest-digest": "sha256:..."
    },
    "type": "cosign container image signature"
  },
  "optional": {
    "Issuer": "https://keycloak.apps.example.com/realms/trusted-artifact-signer",
    "Subject": "mirror-operator"
  }
}
```

#### Verify SBOM Attestation
```bash
cosign verify-attestation \
  --certificate-identity-regexp=".*mirror-operator.*" \
  --certificate-oidc-issuer="https://keycloak.apps.example.com/realms/trusted-artifact-signer" \
  --type cyclonedx \
  airgapped-registry.local/openshift/release@sha256:... \
  | jq -r '.payload' | base64 -d
```

**Output:** Full CycloneDX SBOM
```json
{
  "bomFormat": "CycloneDX",
  "specVersion": "1.4",
  "components": [
    {
      "type": "library",
      "name": "openssl",
      "version": "3.0.7",
      "purl": "pkg:rpm/rhel/openssl@3.0.7"
    },
    ...
  ]
}
```

## Configuration Reference

### DisconnectedPlatform with Managed Quay

```yaml
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: DisconnectedPlatform
metadata:
  name: connected-platform
spec:
  mode: connected
  connected:
    # Managed Quay instance
    quay:
      managed:
        enabled: true
        organizationName: "mirror"
        
        # Filesystem storage (uses PVC)
        storage:
          type: "filesystem"
          size: "500Gi"
          storageClass: "gp3-csi"
        
        # OR S3 storage
        # storage:
        #   type: "s3"
        #   s3Bucket: "my-quay-bucket"
        #   s3AccessKey: "AKIA..."
        #   s3SecretKey: "secret..."
        #   s3Endpoint: "s3.amazonaws.com"
        
        # Optional TLS
        # tlsSecret:
        #   name: quay-tls-cert
        
        # Optional cert-manager integration
        # certIssuer:
        #   name: letsencrypt-prod
        #   kind: ClusterIssuer
    
    # RHTAS for signing
    rhtas:
      oidc:
        managed:
          enabled: true
          realm: "trusted-artifact-signer"
    
    # Operator configuration
    operators:
      quayOperator:
        channel: "stable-3.13"
        approvalStrategy: "Automatic"
```

### Using External Quay

If you have an existing Quay instance or ObjectBucketClaim is not available:

```yaml
spec:
  connected:
    quay:
      externalURL: "quay.example.com/mirror"
```

The operator will use this URL as the intermediate registry without deploying Quay.

**When to use external Quay:**
- ObjectBucketClaim CRD is not available and you cannot install ODF/Noobaa
- You already have a Quay instance deployed
- You prefer to manage Quay separately from the operator

## Benefits

### Security
- ✅ **Keyless signing**: No private key management
- ✅ **Short-lived certificates**: Fulcio certs expire quickly
- ✅ **Transparency log**: All signatures recorded in Rekor
- ✅ **SBOM attestations**: Know what's in your images
- ✅ **Verification in airgap**: Trust established on connected side

### Operational
- ✅ **Fully automated**: No manual steps
- ✅ **Scalable**: Quay handles thousands of images
- ✅ **Observable**: RHTPA ingests SBOMs for analysis
- ✅ **Compliant**: Meet supply chain security requirements

### Technical
- ✅ **Standard workflow**: Uses oc-mirror's native m2m and m2d
- ✅ **No cache hacks**: Clean separation of concerns
- ✅ **OCI compliant**: Signatures follow OCI artifact spec
- ✅ **Works with incremental**: Supports incremental mirroring

## Troubleshooting

### ObjectBucketClaim API not found

**Error:**
```
error checking for object storage support: unable to list object bucket claims: 
failed to get API group resources: unable to retrieve the complete list of server APIs: 
objectbucket.io/v1alpha1: the server could not find the requested resource
```

**Solution 1 - Install ObjectBucket operator:**
```bash
# Install OpenShift Data Foundation operator (provides OBC)
oc apply -f - <<EOF
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: odf-operator
  namespace: openshift-storage
spec:
  channel: stable-4.18
  name: odf-operator
  source: redhat-operators
  sourceNamespace: openshift-marketplace
EOF
```

**Solution 2 - Use S3 storage:**
```yaml
spec:
  connected:
    quay:
      managed:
        enabled: true
        storage:
          type: "s3"
          s3Bucket: "my-bucket"
          s3AccessKey: "AKIA..."
          s3SecretKey: "secret..."
          s3Endpoint: "s3.amazonaws.com"
```

**Solution 3 - Use external Quay:**
```yaml
spec:
  connected:
    quay:
      externalURL: "quay.example.com/mirror"
```

### Quay not ready
```bash
# Check QuayRegistry status
oc get quayregistry -n mirror-operator-system mirror-operator-quay -o yaml

# Check Quay pods
oc get pods -n mirror-operator-system | grep quay
```

### Signatures not in tar
```bash
# Verify signatures exist in intermediate registry
cosign tree quay.apps.example.com/mirror/openshift/release:4.18.1

# Should show:
# └── 📦 sha256:abc... (image)
#     ├── 🔐 sha256:def... (signature)
#     └── 📄 sha256:ghi... (attestation)
```

### OIDC token issues
```bash
# Test OIDC token retrieval
curl -X POST "https://keycloak.apps.example.com/realms/tas/protocol/openid-connect/token" \
  -d "grant_type=client_credentials" \
  -d "client_id=mirror-operator" \
  -d "client_secret=SECRET"
```

## See Also

- [CRD Reference](crd-reference.md)
- [Integration Guide](integration-guide.md)
- [RHTAS Documentation](https://docs.redhat.com/en/documentation/red_hat_trusted_artifact_signer)
- [Cosign Documentation](https://docs.sigstore.dev/cosign/overview/)

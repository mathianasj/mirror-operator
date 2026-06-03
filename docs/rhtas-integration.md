# Red Hat Trusted Artifact Signer (RHTAS) Integration

This document describes how the mirror-operator integrates with Red Hat Trusted Artifact Signer (RHTAS) to provide artifact signing and verification in connected and airgapped environments.

## Overview

The mirror-operator uses RHTAS for signing mirrored artifacts in connected environments and provides verification capabilities in airgapped environments through bundled public keys.

### Architecture

- **Connected Environment**: Full RHTAS stack with TUF for secure key distribution and automatic key rotation
- **Airgapped Environment**: Verification using bundled static keys (Fulcio root CA + Rekor public key)

## Connected Environment Setup

### Prerequisites

1. OpenShift cluster with internet access
2. PostgreSQL database (for Trillian and Rekor)
3. OIDC provider (Keycloak, Red Hat SSO, GitHub, Google, etc.)

### Configuration

The RHTAS integration is configured through the `DisconnectedPlatform` CRD when running in `connected` mode:

```yaml
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: DisconnectedPlatform
metadata:
  name: disconnected-platform
spec:
  mode: connected
  connected:
    operators:
      rhtas: {}  # Enable RHTAS operator installation
    rhtas:
      oidc:
        issuer: "https://keycloak.example.com/realms/trusted-artifact-signer"
        clientId: "trusted-artifact-signer"
        type: "email"
      database:
        host: "postgres.example.com"
        name: "rhtas"
        port: 5432
        username: "rhtas"
        password: "change-me"
```

### OIDC Configuration

The operator supports multiple OIDC providers:

#### Red Hat SSO / Keycloak

```yaml
rhtas:
  oidc:
    issuer: "https://keycloak.example.com/realms/trusted-artifact-signer"
    clientId: "trusted-artifact-signer"
    type: "email"
```

#### GitHub

```yaml
rhtas:
  oidc:
    issuer: "https://github.com/login/oauth"
    clientId: "your-github-client-id"
    type: "github-workflow"
```

#### Google

```yaml
rhtas:
  oidc:
    issuer: "https://accounts.google.com"
    clientId: "your-google-client-id"
    type: "email"
```

### Database Configuration

RHTAS requires a PostgreSQL database for Trillian (transparency log) and Rekor (signature transparency):

```yaml
rhtas:
  database:
    host: "postgres.example.com"
    name: "rhtas"
    port: 5432
    username: "rhtas"
    password: "change-me"
```

### Components Deployed

When RHTAS is enabled, the operator creates a `Securesign` CR that deploys:

1. **Fulcio**: Certificate authority for code signing
   - Issues short-lived signing certificates based on OIDC identity
   - Binds identity to the certificate

2. **Rekor**: Transparency log for signatures
   - Records all signatures for audit trail
   - Provides public verification of signature existence

3. **CTlog**: Certificate transparency log
   - Records all certificates issued by Fulcio
   - Enables monitoring and auditing of certificate issuance

4. **Trillian**: Merkle tree implementation
   - Provides cryptographic verification of log integrity
   - Backend for Rekor and CTlog

5. **TUF**: The Update Framework
   - Secure distribution of root keys
   - Automatic key rotation
   - Protection against key compromise

## Signing Process

### During Collection

When a `CollectionPipeline` runs with RHTAS enabled:

1. **oc-mirror** collects images and operators
2. **syft** generates SBOM (Software Bill of Materials)
3. **cosign** signs artifacts using RHTAS:
   - User authenticates via OIDC (Keycloak, GitHub, etc.)
   - Fulcio issues short-lived signing certificate
   - cosign signs artifact with certificate
   - Signature and certificate stored in Rekor
4. **TUF key extraction**: Current Fulcio root CA and Rekor public key extracted from TUF repository and bundled with artifacts

### TUF Key Extraction Task

The collection pipeline includes a task to extract the current public keys from the TUF repository:

```yaml
- name: extract-tuf-keys
  runAfter: ["cosign-sign"]
  taskSpec:
    steps:
      - name: extract-keys
        image: quay.io/mathianasj/oc-mirror:v2
        command: ["/bin/sh"]
        args:
          - -c
          - |
            # Download TUF root metadata
            cosign initialize --mirror https://tuf-repo-url --root /workspace/output/tuf-root.json
            
            # Extract Fulcio root CA
            cosign download fulcio-root --out /workspace/output/fulcio-root.pem
            
            # Extract Rekor public key
            cosign download rekor-public-key --out /workspace/output/rekor-public-key.pem
```

These keys are bundled with the mirror artifacts for use in airgapped verification.

## Airgapped Environment

### Pre-shared Trusted Root Keys

For secure airgapped verification, the RHTAS root keys are distributed **separately** from mirror bundles to establish a trusted root before any artifact verification.

#### Key Extraction on Connected Side

When RHTAS is deployed, the operator automatically extracts the current root keys from the Securesign deployment and stores them in a ConfigMap:

```bash
# On connected cluster
kubectl get configmap -n mirror-operator-system rhtas-trusted-root -o yaml

# Contents:
# - fulcio-root.pem: Fulcio root CA certificate
# - rekor-public-key.pem: Rekor public key
# - tuf-repository-url: TUF repository URL (for reference)
# - extraction-timestamp: When keys were extracted
```

The DisconnectedPlatform status tracks key information:

```bash
kubectl get disconnectedplatform disconnected-platform -o jsonpath='{.status.rhtasRootKeys}'
```

Output:
```json
{
  "configMap": "rhtas-trusted-root",
  "fulcioRootHash": "a1b2c3d4e5f6g7h8",
  "rekorKeyHash": "9i0j1k2l3m4n5o6p",
  "lastUpdated": "2026-05-28T14:30:00Z",
  "tufRepositoryUrl": "https://tuf.rhtas.svc.cluster.local"
}
```

#### Distributing Keys to Airgapped Environment

**Option 1: Manual Transfer (Most Secure)**

1. Export the ConfigMap from connected cluster:
   ```bash
   kubectl get configmap -n mirror-operator-system rhtas-trusted-root -o yaml > rhtas-trusted-root.yaml
   ```

2. Verify key fingerprints through out-of-band channel (phone, encrypted email, etc.):
   ```bash
   # On connected side
   kubectl get configmap -n mirror-operator-system rhtas-trusted-root \
     -o jsonpath='{.data.fulcio-root\.pem}' | sha256sum
   kubectl get configmap -n mirror-operator-system rhtas-trusted-root \
     -o jsonpath='{.data.rekor-public-key\.pem}' | sha256sum
   ```

3. Transfer the ConfigMap file to airgapped environment (USB drive, CD, etc.)

4. Apply to airgapped cluster:
   ```bash
   kubectl apply -f rhtas-trusted-root.yaml
   ```

**Option 2: Bake into Operator Installation**

Include the trusted root ConfigMap in the operator deployment manifests for airgapped environments:

```bash
# On connected side, generate installation bundle
kubectl kustomize config/default > mirror-operator-install.yaml

# Append trusted root
kubectl get configmap -n mirror-operator-system rhtas-trusted-root -o yaml >> mirror-operator-install.yaml

# Transfer and install in airgapped
kubectl apply -f mirror-operator-install.yaml
```

**Option 3: Reference in DisconnectedPlatform Spec**

Point to pre-deployed ConfigMap:

```yaml
spec:
  mode: airgapped
  airgapped:
    rhtas:
      trustedRootKeys:
        name: rhtas-trusted-root
```

### Verification Without OIDC

Airgapped environments do not need access to the OIDC provider or TUF repository for verification. Verification only requires:

1. **Fulcio Root CA**: To verify the signing certificate chain
2. **Rekor Public Key**: To verify the Rekor log entry

These keys are **pre-installed** separately from the mirror bundles.

### Verification Process

```bash
# Verify signature using pre-shared keys from ConfigMap
cosign verify \
  --certificate-identity-regexp ".*" \
  --certificate-oidc-issuer-regexp ".*" \
  --rekor-url "none" \
  --key /etc/rhtas-root/rekor-public-key.pem \
  --cert-chain /etc/rhtas-root/fulcio-root.pem \
  image:tag
```

The MirrorImport controller can mount the ConfigMap and use the keys for automatic verification.

## Security Considerations

### Trust Model

The security model relies on **separation of key distribution from artifact distribution**:

1. **Trusted Root Keys**: Distributed separately through secure out-of-band channels
   - Manual transfer with fingerprint verification
   - Baked into operator installation
   - Pre-deployed before any mirror operations

2. **Mirror Artifacts**: Verified against the pre-shared trusted root
   - If attacker compromises bundle, they cannot forge signatures
   - Verification proves artifacts were signed by someone with access to connected RHTAS
   - Tampering detection through signature mismatch

3. **Chain of Trust**:
   - Connected: OIDC identity → Fulcio cert → signature → Rekor log
   - Airgapped: Pre-shared Fulcio root → cert validation → signature verification
   - No circular dependency: keys ≠ bundled with artifacts

### Connected Environment

- TUF provides secure key rotation and distribution
- Compromised keys can be revoked through TUF
- Rekor provides transparency and audit trail
- OIDC binds signatures to verified identities
- Automatic key extraction to ConfigMap for distribution

### Airgapped Environment

- Static keys provide offline verification
- Keys distributed separately from artifacts (prevents circular trust issue)
- Keys should be rotated periodically (re-distribute ConfigMap)
- No phone-home or external validation required
- Verification limited to pre-shared trusted root
- Fingerprint verification recommended during key distribution

### Key Rotation

When keys rotate on the connected side:

1. TUF automatically distributes new keys to connected systems
2. Next mirror bundle includes updated keys
3. Airgapped environment receives updated keys through normal mirror process
4. Old signatures remain verifiable with historical keys (Rekor immutability)

## Troubleshooting

### RHTAS Operator Not Installing

Check component status:
```bash
kubectl get disconnectedplatform disconnected-platform -o jsonpath='{.status.components}' | jq
```

Check subscription:
```bash
kubectl get subscription -n openshift-operators mirror-operator-trusted-artifact-signer -o yaml
```

### Securesign CR Not Created

Check operator logs:
```bash
kubectl logs -n mirror-operator-system -l control-plane=controller-manager
```

Verify OIDC configuration:
```bash
kubectl get securesign -n openshift-operators mirror-operator-securesign -o yaml
```

### Signing Failures

Check OIDC authentication:
```bash
# Verify OIDC issuer is accessible
curl https://keycloak.example.com/realms/trusted-artifact-signer/.well-known/openid-configuration
```

Check Fulcio certificate issuance:
```bash
kubectl logs -n openshift-operators -l app.kubernetes.io/component=fulcio
```

### Verification Failures in Airgapped

Verify bundled keys exist:
```bash
ls -la /path/to/bundle/
# Should show:
# - fulcio-root.pem
# - rekor-public-key.pem
```

Test key format:
```bash
openssl x509 -in fulcio-root.pem -text -noout
openssl pkey -in rekor-public-key.pem -pubin -text -noout
```

## Reference

- [RHTAS Documentation](https://docs.redhat.com/en/documentation/red_hat_trusted_artifact_signer/1.4/html-single/deployment_guide/index)
- [Sigstore Documentation](https://docs.sigstore.dev/)
- [Cosign Documentation](https://docs.sigstore.dev/cosign/overview/)
- [TUF Specification](https://theupdateframework.io/)

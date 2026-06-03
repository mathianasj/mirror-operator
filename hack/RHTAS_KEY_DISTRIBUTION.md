# RHTAS Trusted Root Key Distribution

This directory contains scripts for securely distributing RHTAS trusted root keys from connected to airgapped environments.

## Overview

To prevent circular trust issues, RHTAS root keys (Fulcio root CA and Rekor public key) are distributed **separately** from mirror bundles. This ensures that an attacker who compromises a mirror bundle cannot also replace the verification keys.

## Workflow

### 1. Connected Environment: Extract Keys

On the connected cluster where RHTAS is deployed:

```bash
# Extract keys from the rhtas-trusted-root ConfigMap
./hack/extract-rhtas-keys.sh

# Output directory: ./rhtas-trusted-root/
# Contains:
# - configmap.yaml           # Full ConfigMap for easy k8s deployment
# - fulcio-root.pem         # Fulcio root CA certificate
# - rekor-public-key.pem    # Rekor public key
# - fingerprints.txt        # SHA256 fingerprints for verification
# - extraction-timestamp.txt # When keys were extracted
# - tuf-repository-url.txt  # TUF repository URL (reference only)
```

### 2. Verify Fingerprints Out-of-Band

**CRITICAL SECURITY STEP**: Verify the key fingerprints through a separate, trusted channel:

```bash
# On connected cluster, get fingerprints
cat ./rhtas-trusted-root/fingerprints.txt

# Communicate these fingerprints through:
# - Phone call (read the hash aloud)
# - Signal/encrypted messaging
# - Physical meeting
# - Signed email (GPG/PGP)
# - Any channel OTHER than the mirror bundle transfer method
```

The person receiving the keys in the airgapped environment should record these fingerprints for verification.

### 3. Transfer to Airgapped Environment

Transfer the `rhtas-trusted-root/` directory to the airgapped environment using your organization's approved physical media transfer process:

- USB drive (encrypted)
- DVD/CD-ROM
- Dedicated secure file transfer system
- Physical courier

**Do NOT** bundle these keys with the mirror artifacts. They must travel separately.

### 4. Airgapped Environment: Verify Keys

On the airgapped cluster, before applying the keys:

```bash
# Verify the keys match expected fingerprints
./hack/verify-rhtas-keys.sh ./rhtas-trusted-root/

# This script will:
# 1. Check all required files are present
# 2. Compute current fingerprints
# 3. Compare against fingerprints.txt
# 4. Validate PEM formats
# 5. Display verification results
```

**Manual Verification**: Compare the displayed fingerprints with those you received through the out-of-band channel in step 2.

### 5. Apply Keys to Airgapped Cluster

After successful verification:

```bash
# Apply the ConfigMap
kubectl apply -f ./rhtas-trusted-root/configmap.yaml

# Verify it was created
kubectl get configmap -n mirror-operator-system rhtas-trusted-root
```

### 6. Configure DisconnectedPlatform

Reference the pre-deployed keys in your DisconnectedPlatform CR:

```yaml
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: DisconnectedPlatform
metadata:
  name: disconnected-platform-airgapped
spec:
  mode: airgapped
  airgapped:
    rhtas:
      trustedRootKeys:
        name: rhtas-trusted-root
```

## Key Rotation

When RHTAS keys rotate on the connected side (through TUF):

1. Re-run `extract-rhtas-keys.sh` on connected cluster
2. Verify new fingerprints out-of-band
3. Transfer new keys to airgapped environment
4. Verify and apply updated ConfigMap
5. Old signatures remain verifiable (Rekor immutability)

**Recommended rotation schedule**: Every 6-12 months, or when:
- Suspected key compromise
- Organizational security policy requires it
- Major infrastructure changes

## Security Considerations

### Why Separate Distribution?

If keys are bundled with mirror artifacts:
```
Attacker → Replace mirror bundle
        → Replace verification keys in same bundle  
        → Verification passes but content is malicious ❌
```

With separate distribution:
```
Attacker → Replace mirror bundle
        → Cannot replace pre-deployed verification keys
        → Verification fails, tampering detected ✓
```

### Fingerprint Verification is Critical

The fingerprint verification step (step 2) is what makes this secure:

- **Without it**: Keys could be tampered during physical transfer
- **With it**: Tampering is detected when fingerprints don't match

The out-of-band channel must be:
- Independent from the physical media transfer
- Authenticated (you know who you're talking to)
- Secure (can't be intercepted/modified)

### Trust Anchors

After deployment, the ConfigMap becomes your **trust anchor** for all mirror verification:

- Stored in etcd (encrypted at rest)
- RBAC-protected (only admins can modify)
- Auditable (changes tracked in audit logs)
- Version-controlled (can track updates via GitOps)

## Troubleshooting

### Keys Not Found in Securesign Status

```bash
# Check Securesign is ready
kubectl get securesign -n openshift-operators mirror-operator-securesign

# Check status details
kubectl get securesign -n openshift-operators mirror-operator-securesign -o yaml

# Wait for phase: Ready
```

### Fingerprint Mismatch

**DO NOT PROCEED** if fingerprints don't match:

1. Verify you're comparing the right files
2. Check for file corruption (re-transfer if needed)
3. Verify source keys haven't rotated (re-extract if needed)
4. Contact security team if suspicious

### ConfigMap Already Exists

To update existing ConfigMap:

```bash
# Delete old ConfigMap
kubectl delete configmap -n mirror-operator-system rhtas-trusted-root

# Apply new one
kubectl apply -f ./rhtas-trusted-root/configmap.yaml
```

Or use the scripts which handle updates automatically.

## Alternative: Bake into Operator Installation

For initial deployment, you can include the trusted root in the operator installation manifests:

```bash
# On connected cluster
./hack/extract-rhtas-keys.sh

# Generate operator installation bundle with keys
kubectl kustomize config/default > mirror-operator-install.yaml
cat ./rhtas-trusted-root/configmap.yaml >> mirror-operator-install.yaml

# Transfer mirror-operator-install.yaml to airgapped
# Verify fingerprints
# Apply to airgapped cluster
kubectl apply -f mirror-operator-install.yaml
```

This ensures keys are pre-deployed before any mirror operations.

## References

- [RHTAS Integration Documentation](../docs/rhtas-integration.md)
- [TUF Specification](https://theupdateframework.io/)
- [NIST Guidelines on Key Management](https://csrc.nist.gov/publications/detail/sp/800-57-part-1/rev-5/final)

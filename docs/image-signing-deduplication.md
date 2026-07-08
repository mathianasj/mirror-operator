# Image Signing Deduplication

## Problem

The `sign-images` task in the collection pipeline was signing **every image** in the mapping file, even if it had already been signed in a previous collection run.

This caused:
- **Wasted time** - Re-signing hundreds of images that already have signatures
- **Wasted Rekor entries** - Creating duplicate transparency log entries for the same content
- **Unnecessary load** - Extra requests to Fulcio, Rekor, and the OIDC provider
- **Slower pipeline runs** - Signing is one of the slowest tasks in the pipeline

## Why This Happened

Cosign signatures are tied to the **image digest** (content hash), not the tag. Once an image digest is signed, that signature is valid regardless of which tags point to it.

For example:
```
quay.io/openshift-release-dev/ocp-release@sha256:abc123...
```

If this digest is signed once, it doesn't need to be signed again, even if:
- A new tag points to it
- It's mirrored to a different registry  
- It appears in multiple collection runs

## Solution Implemented

Added signature verification **before** signing in the `sign-images` task.

### Before
```bash
while IFS='=' read -r source dest; do
  echo "Signing $image_ref"
  cosign sign ... "$image_ref"
done
```

**Result**: Signs every image, every time.

### After
```bash
# Initialize TUF root for OUR private Sigstore instance
cosign initialize --mirror="$(params.tuf-url)" --root="$(params.tuf-url)/root.json"

while IFS='=' read -r source dest; do
  # Check if already signed BY OUR PRIVATE SIGSTORE INSTANCE
  # IMPORTANT: Verify against OUR Rekor, not upstream public Sigstore
  if cosign verify \
    --rekor-url=$(params.rekor-url) \
    --certificate-oidc-issuer=$(params.oidc-issuer) \
    --certificate-identity-regexp=".*" \
    "$image_ref" >/dev/null 2>&1; then
    echo "  ✓ Already signed by our Sigstore: $image_ref (skipping)"
    skipped_count=$((skipped_count + 1))
    continue
  fi
  
  echo "  Signing $image_ref"
  cosign sign ... "$image_ref"
done
```

**Result**: Only signs images that don't have signatures from OUR private Sigstore instance.

## How `cosign verify` Works

The verification command:
```bash
cosign verify \
  --rekor-url=$(params.rekor-url) \
  --certificate-oidc-issuer=$(params.oidc-issuer) \
  --certificate-identity-regexp=".*" \
  "$image_ref"
```

- **Returns 0 (success)** if the image has valid signatures from OUR Sigstore instance
- **Returns non-zero (failure)** if no signatures exist or verification fails
- `--rekor-url`: Points to OUR private Rekor (not public sigstore.dev)
- `--certificate-oidc-issuer`: Verifies signature was created by OUR Keycloak/OIDC provider
- `--certificate-identity-regexp=".*"`: Accepts any identity from our OIDC issuer
- Checks against OUR configured Rekor transparency log

## Security Considerations

### ⚠️ **CRITICAL: Verify Against YOUR Private Sigstore**

**The Problem:**
Many images from upstream (Red Hat, vendors) already have signatures from **public Sigstore** (sigstore.dev). If you don't explicitly verify against YOUR private Sigstore instance, cosign will find the upstream signatures and skip re-signing.

**Why This Matters:**
- ❌ Upstream signatures are NOT in YOUR Rekor transparency log
- ❌ You can't verify them in an airgapped environment (no access to sigstore.dev)
- ❌ You can't apply YOUR signing policies to them
- ❌ You lose the audit trail in YOUR environment

**The Fix:**
Always verify with:
- `--rekor-url=$(params.rekor-url)` - YOUR private Rekor
- `--certificate-oidc-issuer=$(params.oidc-issuer)` - YOUR Keycloak/OIDC

**Without these flags:**
```bash
# BAD: Checks against public Sigstore
cosign verify \
  --certificate-identity-regexp=".*" \
  --certificate-oidc-issuer-regexp=".*" \
  quay.io/openshift-release-dev/ocp-release:4.21.21-x86_64

# ✓ Success (found Red Hat's signature from public Sigstore)
# Result: Skips re-signing, no entry in YOUR Rekor
```

**With the flags:**
```bash
# GOOD: Checks against YOUR private Sigstore
cosign verify \
  --rekor-url=https://rekor.mycompany.com \
  --certificate-oidc-issuer=https://keycloak.mycompany.com/realms/trustify \
  --certificate-identity-regexp=".*" \
  quay.io/openshift-release-dev/ocp-release:4.21.21-x86_64

# ✗ Failure (Red Hat's signature not in YOUR Rekor)
# Result: Re-signs with YOUR private key, creates entry in YOUR Rekor
```

### TUF Root Initialization

Before verifying, initialize the TUF root for YOUR Sigstore:
```bash
cosign initialize --mirror="$(params.tuf-url)" --root="$(params.tuf-url)/root.json"
```

This downloads the TUF metadata from YOUR Sigstore instance, ensuring all subsequent `cosign verify` operations trust YOUR root CA, not the public Sigstore root.

## Expected Behavior

### First Collection Run

```
Processing 191 images...
  Signing quay.io/mirror/openshift/release:4.21.21-x86_64
  Signing quay.io/mirror/openshift/release:4.21.21-x86_64-agent-installer
  ...
  
=== Image signing complete ===
Total: 191 | Signed: 191 | Already signed: 0 | Failed: 0
```

### Second Collection Run (same content)

```
Processing 191 images...
  ✓ Already signed: quay.io/mirror/openshift/release:4.21.21-x86_64 (skipping)
  ✓ Already signed: quay.io/mirror/openshift/release:4.21.21-x86_64-agent-installer (skipping)
  ...
  
=== Image signing complete ===
Total: 191 | Signed: 0 | Already signed: 191 | Failed: 0
```

### Incremental Collection Run (some new images)

```
Processing 195 images...
  ✓ Already signed: quay.io/mirror/openshift/release:4.21.21-x86_64 (skipping)
  ✓ Already signed: quay.io/mirror/openshift/release:4.21.21-x86_64-agent-installer (skipping)
  ...
  Signing quay.io/mirror/openshift/release:4.21.22-x86_64  # New version
  Signing quay.io/mirror/openshift/release:4.21.22-x86_64-agent-installer
  ...
  
=== Image signing complete ===
Total: 195 | Signed: 4 | Already signed: 191 | Failed: 0
```

## Performance Impact

Rough estimates for a typical OCP release:

| Scenario | Before | After | Time Saved |
|----------|--------|-------|------------|
| First run (191 images) | ~25 minutes | ~25 minutes | 0 (nothing to skip) |
| Second run (same content) | ~25 minutes | ~2 minutes | ~23 minutes (92% faster) |
| Incremental (4 new images) | ~25 minutes | ~3 minutes | ~22 minutes (88% faster) |

**Why verification is fast:**
- `cosign verify` only checks metadata (no image download)
- Queries Rekor transparency log (fast index lookup)
- No cryptographic signing operations (CPU-intensive)

## Edge Cases Handled

### 1. Image exists but has no signatures
**Behavior**: `cosign verify` fails → image gets signed  
**Correct**: ✅ New image needs signing

### 2. Image has signatures from different identity
**Behavior**: Verification succeeds with relaxed regexp match → skipped  
**Correct**: ✅ Already signed (doesn't matter who signed it for mirroring purposes)

### 3. Image was signed but signature was deleted from Rekor
**Behavior**: `cosign verify` fails → image gets re-signed  
**Correct**: ✅ Need to restore signature

### 4. Network error when checking Rekor
**Behavior**: `cosign verify` fails → image gets signed  
**Correct**: ✅ Better to re-sign than skip (fail-safe)

### 5. Image digest changed (content update)
**Behavior**: New digest has no signature → gets signed  
**Correct**: ✅ Different content needs new signature

## Verification

To manually check if an image is signed:

```bash
# Check for signatures
cosign verify \
  --certificate-identity-regexp=".*" \
  --certificate-oidc-issuer-regexp=".*" \
  quay.io/mirror/openshift/release@sha256:abc123...

# List all signatures
cosign tree quay.io/mirror/openshift/release@sha256:abc123...
```

## Limitations

### Does NOT prevent re-signing if:
- The signature is in a different Rekor instance (different transparency log)
- The verification fails due to trust root mismatch
- Custom verification policies are in place

For these cases, the pipeline will re-sign, which is safe but redundant.

### Requires:
- Rekor transparency log is accessible from the pipeline
- TUF root is initialized (`cosign initialize`)
- Network connectivity to Rekor server

## Related Changes

This optimization complements the storage configuration fixes:
- [Mirror-From-Intermediate Storage Fix](mirror-from-intermediate-storage-fix.md)
- [Upstream Blocking Implementation](upstream-blocking-implementation.md)

Together these changes significantly reduce:
1. Storage usage (PVC optimization)
2. Network traffic (upstream blocking)  
3. Time and resources (signing deduplication)

## Configuration

No configuration needed - the deduplication is automatic.

To **disable** deduplication (force re-sign everything):
- Remove the `cosign verify` check from the `sign-images` task
- Or set a pipeline parameter like `force-resign: "true"` (not currently implemented)

## Troubleshooting

### Issue: Images keep getting re-signed even though they should be skipped

**Diagnosis**:
```bash
# Check if Rekor is accessible
curl https://<rekor-url>/api/v1/log

# Check if signatures exist
cosign verify --certificate-identity-regexp=".*" \
  --certificate-oidc-issuer-regexp=".*" \
  <image-ref>
```

**Possible causes**:
1. Rekor transparency log is not accessible
2. TUF root not initialized (`cosign initialize` not run)
3. Signatures were created with a different Rekor instance
4. Verification policy mismatch

### Issue: "Already signed" but no signatures found

**Diagnosis**:
```bash
# Check signature attachments
cosign tree <image-ref>

# Check Rekor for entries
rekor-cli search --artifact <image-ref>
```

**Explanation**: Signatures are stored as OCI artifacts attached to the image. The verification checks Rekor for the transparency log entry, not the registry.

## References

- [Cosign Documentation](https://docs.sigstore.dev/cosign/overview/)
- [Rekor Transparency Log](https://docs.sigstore.dev/rekor/overview/)
- [Sigstore Architecture](https://docs.sigstore.dev/about/overview/)

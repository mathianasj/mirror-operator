# Security Advisory: Signature Verification in Image Signing Deduplication

## Severity: HIGH

## Summary

The image signing deduplication feature had a critical security flaw that allowed upstream public Sigstore signatures to satisfy the "already signed" check, preventing re-signing with the private Sigstore instance.

## Vulnerability Details

### Vulnerable Code (Before Fix)

```bash
# Check if already signed
if cosign verify \
  --certificate-identity-regexp=".*" \
  --certificate-oidc-issuer-regexp=".*" \
  "$image_ref" >/dev/null 2>&1; then
  echo "Already signed, skipping"
  continue
fi
```

**Problem**: This verification checked against **public Sigstore** (sigstore.dev), not the private Sigstore instance.

### Impact

When mirroring images from upstream sources (Red Hat, vendors):

1. ✅ **Upstream images with public Sigstore signatures** (e.g., Red Hat-signed OCP releases)
   - Verification **succeeds** (finds Red Hat's signature in public Sigstore)
   - Pipeline **skips re-signing** with private key
   - **No entry created in private Rekor**
   - ❌ **Result**: Images in airgapped environment have NO verifiable signatures

2. ✅ **Images without any signatures**
   - Verification **fails** (no signature found)
   - Pipeline **signs** with private key
   - Entry created in private Rekor
   - ✅ **Result**: Properly signed and verifiable

### Attack Scenarios

#### Scenario 1: Signature Bypass
**Attacker Goal**: Inject malicious images that won't be re-signed

1. Attacker publishes malicious image with ANY valid Sigstore signature (even self-signed via public Sigstore)
2. Organization mirrors this image
3. Pipeline finds signature, skips re-signing
4. Malicious image deployed to airgapped environment
5. No audit trail in organization's private Rekor

#### Scenario 2: Trust Boundary Violation
**Problem**: Trusting external signatures without verification

1. Upstream vendor signs images with public Sigstore
2. Organization mirrors vendor images
3. Pipeline accepts vendor signatures without re-verification
4. If vendor's signing key is compromised AFTER mirroring, organization has no way to detect it
5. Compromised images already in airgapped environment with "valid" signatures

#### Scenario 3: Airgap Verification Failure
**Problem**: Signatures can't be verified in airgapped environment

1. Images mirrored with upstream public Sigstore signatures
2. Pipeline skips re-signing
3. In airgapped environment, attempt to verify signatures
4. Verification **fails** because airgap has no access to public Sigstore Rekor
5. All images appear unsigned/unverifiable

## Fixed Code

```bash
# Initialize TUF root for OUR private Sigstore instance
cosign initialize --mirror="$(params.tuf-url)" --root="$(params.tuf-url)/root.json"

# Check if already signed BY OUR PRIVATE SIGSTORE INSTANCE
if cosign verify \
  --rekor-url=$(params.rekor-url) \
  --certificate-oidc-issuer=$(params.oidc-issuer) \
  --certificate-identity-regexp=".*" \
  "$image_ref" >/dev/null 2>&1; then
  echo "Already signed by our Sigstore, skipping"
  continue
fi
```

**Changes:**
1. ✅ `cosign initialize` with private TUF URL (not public)
2. ✅ `--rekor-url` points to private Rekor instance
3. ✅ `--certificate-oidc-issuer` verifies signature from private OIDC provider

## Verification

### Before Fix

```bash
# Example: Red Hat signed OCP release image
IMAGE="quay.io/openshift-release-dev/ocp-release:4.21.21-x86_64"

# Verify against public Sigstore (WRONG)
cosign verify \
  --certificate-identity-regexp=".*" \
  --certificate-oidc-issuer-regexp=".*" \
  "$IMAGE"

# Output: ✓ Verification successful
# (Found Red Hat's signature in public sigstore.dev Rekor)

# Verify against YOUR private Sigstore (CORRECT)
cosign verify \
  --rekor-url=https://rekor.mycompany.com \
  --certificate-oidc-issuer=https://keycloak.mycompany.com/realms/trustify \
  --certificate-identity-regexp=".*" \
  "$IMAGE"

# Output: ✗ Verification failed
# (Red Hat's signature not in YOUR private Rekor)
# Result: Image will be re-signed with YOUR private key
```

### After Fix

All images are verified against YOUR private Sigstore:
- Images with upstream public signatures → Re-signed with your private key ✅
- Images already signed by you → Skipped (deduplication still works) ✅
- All signatures verifiable in airgapped environment ✅
- Complete audit trail in YOUR private Rekor ✅

## Affected Versions

- **Vulnerable**: All versions before 2026-07-08 that included signature deduplication
- **Fixed**: 2026-07-08 and later

## Remediation

### Immediate Actions

1. **Update to fixed version** immediately
2. **Re-run signing** on all previously mirrored images:
   ```bash
   # Force re-sign all images (don't skip any)
   # Temporarily disable deduplication or use --force flag
   ```

3. **Audit Rekor** for missing signatures:
   ```bash
   # List all images in intermediate registry
   skopeo list-tags docker://intermediate-registry/mirror
   
   # For each image, check if signature exists in YOUR Rekor
   rekor-cli search --artifact <image-digest>
   
   # Images missing from YOUR Rekor were skipped due to this vulnerability
   ```

### Long-term Actions

1. **Policy Enforcement**: Configure admission controllers to ONLY accept signatures from YOUR private Sigstore
2. **Monitoring**: Alert when images are deployed without signatures in YOUR Rekor
3. **Regular Audits**: Verify all mirrored images have corresponding Rekor entries

## Detection

### Check if You Were Affected

```bash
# Count images in your intermediate registry
IMAGE_COUNT=$(skopeo list-tags docker://your-registry/mirror | jq '.Tags | length')

# Count signatures in YOUR Rekor
SIGNATURE_COUNT=$(rekor-cli search --public-key <your-fulcio-ca> | wc -l)

# If SIGNATURE_COUNT < IMAGE_COUNT, you may have been affected
echo "Images: $IMAGE_COUNT"
echo "Signatures in your Rekor: $SIGNATURE_COUNT"
echo "Gap: $((IMAGE_COUNT - SIGNATURE_COUNT))"
```

### Identify Unsigned Images

```bash
#!/bin/bash
# check-unsigned-images.sh

REGISTRY="your-intermediate-registry/mirror"
REKOR_URL="https://rekor.yourcompany.com"
OIDC_ISSUER="https://keycloak.yourcompany.com/realms/trustify"

# Get all images
skopeo list-tags docker://$REGISTRY | jq -r '.Tags[]' | while read tag; do
  IMAGE="$REGISTRY:$tag"
  
  # Try to verify against YOUR Sigstore
  if ! cosign verify \
    --rekor-url=$REKOR_URL \
    --certificate-oidc-issuer=$OIDC_ISSUER \
    --certificate-identity-regexp=".*" \
    "$IMAGE" >/dev/null 2>&1; then
    echo "NOT SIGNED IN YOUR REKOR: $IMAGE"
  fi
done
```

## Timeline

- **2026-07-08 14:00 UTC**: Vulnerability identified during code review
- **2026-07-08 14:30 UTC**: Fix implemented and tested
- **2026-07-08 15:00 UTC**: Fix deployed, documentation updated
- **2026-07-08 15:30 UTC**: Security advisory published

## References

- [Sigstore Documentation](https://docs.sigstore.dev/)
- [Cosign Verify Documentation](https://docs.sigstore.dev/cosign/verify/)
- [Image Signing Deduplication](./image-signing-deduplication.md)
- [Trust Root Management](https://docs.sigstore.dev/trust_root/)

## Credits

Identified during security review of signature deduplication implementation.

## Contact

For questions about this security advisory, please review the operator documentation or file an issue.

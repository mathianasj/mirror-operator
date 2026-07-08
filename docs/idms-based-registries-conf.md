# IDMS-Based registries.conf Generation

## Problem

When mirroring FROM an intermediate registry, the `registries.conf` file needs to map source registries to the **exact paths** that were created during the mirror-to-intermediate step.

**The Issue:** oc-mirror modifies repository paths when mirroring to a registry. For example:
- **Source**: `quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:abc123`
- **Pushed to**: `intermediate-registry/mirror/openshift/release-dev/ocp-v4.0-art-dev@sha256:abc123`

The path structure changes! So a static registries.conf that just maps:
```toml
[[registry]]
location="quay.io"
[[registry.mirror]]
location="intermediate-registry"
```

Won't work, because oc-mirror will try to pull from the wrong path.

## Solution: Parse IDMS Files

The **ImageDigestMirrorSet (IDMS)** file that oc-mirror generates during mirror-to-intermediate contains the ACTUAL source → mirror mappings.

### IDMS File Format

```yaml
apiVersion: config.openshift.io/v1
kind: ImageDigestMirrorSet
metadata:
  name: oc-mirror
spec:
  imageDigestMirrors:
  - mirrors:
    - intermediate-registry/mirror/openshift/release-dev/ocp-v4.0-art-dev
    source: quay.io/openshift-release-dev/ocp-v4.0-art-dev
  - mirrors:
    - intermediate-registry/mirror/openshift/release
    source: quay.io/openshift-release-dev/ocp-release
```

Each entry shows:
- **source**: The original upstream registry/repository
- **mirrors**: Where that content was actually pushed in the intermediate registry

### Generated registries.conf

From the IDMS above, we generate:

```toml
[[registry]]
location="quay.io/openshift-release-dev/ocp-v4.0-art-dev"
blocked = true
[[registry.mirror]]
location="intermediate-registry/mirror/openshift/release-dev/ocp-v4.0-art-dev"

[[registry]]
location="quay.io/openshift-release-dev/ocp-release"
blocked = true
[[registry.mirror]]
location="intermediate-registry/mirror/openshift/release"
```

Note: We only extract the **registry hostname** from the mirror path for the `location=` field, but the full path mapping is preserved in IDMS.

## Implementation

### Location in Pipeline

File: `internal/controller/disconnectedplatform_controller.go`  
Task: `mirror-from-intermediate`  
Step: Before running `oc-mirror`

### Parsing Logic

```bash
# 1. Check IDMS file exists
IDMS_FILE="/workspace/output/working-dir/cluster-resources/idms-oc-mirror.yaml"
if [ ! -f "$IDMS_FILE" ]; then
  echo "ERROR: IDMS file not found"
  exit 1
fi

# 2. Parse IDMS using yq (YAML parser) or fallback to grep/awk
if command -v yq >/dev/null 2>&1; then
  # Extract source and mirror pairs
  yq eval '.spec.imageDigestMirrors[] | 
    .source as $source | 
    .mirrors[] | 
    [$source, .] | @tsv' "$IDMS_FILE"
else
  # Fallback parsing with grep/awk
  grep -E "^\s+source:|^\s+-\s+" "$IDMS_FILE"
fi

# 3. Generate registries.conf
cat > /workspace/output/tmp/containers/registries.conf <<EOF
[[registry]]
location="<source-from-idms>"
blocked = true
[[registry.mirror]]
location="<mirror-registry-from-idms>"
EOF
```

### Why Two Parsing Methods?

1. **yq (preferred)**: Properly parses YAML, handles edge cases, more reliable
2. **grep/awk (fallback)**: Works even if yq isn't installed in the mirror-image

The mirror-image container should include `yq` for best results.

## Benefits

### 1. Automatic Path Mapping
No manual configuration needed - the paths are automatically extracted from what oc-mirror actually created.

### 2. Handles oc-mirror Path Transformations
oc-mirror applies various path transformations:
- Shortening paths (e.g., dropping `-dev` from repository names)
- Namespace consolidation
- Digest-based organization

IDMS captures the ACTUAL result, so registries.conf will always be correct.

### 3. Multi-Registry Support
If you mirror from multiple source registries (quay.io, registry.redhat.io, gcr.io), the IDMS will have entries for all of them, and registries.conf will be generated with all the correct mappings.

### 4. Version Independent
As oc-mirror evolves and changes its path transformation logic, this approach continues to work because we're reading what oc-mirror actually did, not predicting what it will do.

## Verification

### Check IDMS Was Created

```bash
oc exec <mirror-to-intermediate-pod> -- ls -la /workspace/output/working-dir/cluster-resources/
# Should show: idms-oc-mirror.yaml
```

### Check Generated registries.conf

```bash
oc logs <mirror-from-intermediate-pod> | grep -A 50 "Generated registries.conf from IDMS"
```

Expected output:
```
=== Generated registries.conf from IDMS ===
# Generated from IDMS file created by oc-mirror

[[registry]]
location="quay.io/openshift-release-dev/ocp-v4.0-art-dev"
blocked = true
[[registry.mirror]]
location="intermediate-registry"

[[registry]]
location="quay.io/openshift-release-dev/ocp-release"
blocked = true
[[registry.mirror]]
location="intermediate-registry"
...
```

### Verify Mirror Succeeds

```bash
oc logs <mirror-from-intermediate-pod> | tail -20
```

Should show:
```
✓ 191 / 191 release images mirrored successfully
✓ 3 / 3 additional images mirrored successfully
```

NOT:
```
✗ 0 / 191 release images mirrored: Some release images failed
error: repository not found
```

## Troubleshooting

### Issue: "IDMS file not found"

**Symptom:**
```
ERROR: IDMS file not found at /workspace/output/working-dir/cluster-resources/idms-oc-mirror.yaml
```

**Cause:** The mirror-to-intermediate step didn't run, failed, or the working-dir was cleared.

**Solution:**
1. Check mirror-to-intermediate step succeeded
2. Verify working-dir is on persistent storage (PVC), not ephemeral
3. Ensure mirror-from-intermediate runs immediately after mirror-to-intermediate (not in a separate PipelineRun)

### Issue: "No registries found in IDMS"

**Symptom:**
```
=== Generated registries.conf from IDMS ===
# Generated from IDMS file created by oc-mirror

# Block CDN access...
```
(No `[[registry]]` blocks)

**Cause:** IDMS file is empty or malformed.

**Solution:**
```bash
# Check IDMS content
oc exec <pod> -- cat /workspace/output/working-dir/cluster-resources/idms-oc-mirror.yaml
```

Should contain `spec.imageDigestMirrors` section.

### Issue: Still getting "repository not found"

**Symptom:**
```
error: reading manifest in intermediate-registry/mirror/openshift-release-dev/...
repository not found
```

**Diagnosis:**
1. The repository path is still wrong
2. Check what's actually in IDMS vs what registries.conf generated

**Debug:**
```bash
# Compare IDMS vs registries.conf
oc exec <pod> -- cat /workspace/output/working-dir/cluster-resources/idms-oc-mirror.yaml
oc exec <pod> -- cat /workspace/output/tmp/containers/registries.conf
```

Look for mismatches in paths.

### Issue: yq not found, fallback parser failed

**Symptom:**
```
oc-mirror: line XX: yq: command not found
[Empty or incorrect registries.conf generated]
```

**Solution:** Ensure the mirror-image includes `yq`:
```dockerfile
RUN microdnf install -y yq
```

Or update the fallback grep/awk parser to handle your IDMS format.

## Related Documentation

- [Mirror-From-Intermediate Storage Fix](mirror-from-intermediate-storage-fix.md) - Storage configuration
- [Upstream Blocking Implementation](upstream-blocking-implementation.md) - Defense-in-depth blocking
- [oc-mirror IDMS Documentation](https://docs.openshift.com/container-platform/4.14/installing/disconnected_install/installing-mirroring-installation-images.html#installation-adding-registry-pull-secret_installing-mirroring-installation-images)

## Future Improvements

### 1. Support ITMS (ImageTagMirrorSet)
Currently only parses IDMS (digest-based). Could also parse ITMS (tag-based) if needed.

### 2. Validation
Add validation that every source in the ImageSetConfiguration has a corresponding IDMS entry.

### 3. Path Verification
Before running mirror-from-intermediate, verify that the paths in IDMS actually exist in the intermediate registry:
```bash
for mirror_path in $(extract_from_idms); do
  skopeo inspect docker://$mirror_path || echo "WARNING: $mirror_path not found"
done
```

### 4. Cache IDMS
Store IDMS in a ConfigMap so it persists even if working-dir is cleared, enabling mirror-from-intermediate to run in a separate PipelineRun.

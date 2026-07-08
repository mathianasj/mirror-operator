# Mirror-From-Intermediate Storage Configuration Fix

## Problem

The `mirror-from-intermediate` pipeline step was failing with:

```
readfrom tcp [::1]:42240->[::1]:55000: happened during read: unexpected EOF
```

This error indicated that oc-mirror's internal `localhost:55000` registry was failing, but the root cause was **temp storage exhaustion**, not network issues.

## Root Cause Analysis

### How oc-mirror's Internal Registry Works

When running `oc-mirror --v2 file:///<path>` (mirrorToDisk mode), oc-mirror internally:

1. **Starts a temporary registry** on `localhost:55000` 
2. **Pulls images** from source registries (or intermediate registry via registries.conf)
3. **Stages images** in the localhost:55000 registry  
4. **Writes images** from localhost:55000 to disk archives

The localhost:55000 registry is NOT about network connectivity - it's oc-mirror's internal staging mechanism for the disk workflow.

### Storage Locations Used by oc-mirror

Based on [oc-mirror source code analysis](https://github.com/openshift/oc-mirror):

| Component | Default Location | Configured Via |
|-----------|------------------|----------------|
| **Internal registry storage** | `$CACHE_DIR/.oc-mirror/.cache` | `--cache-dir` flag or `$HOME` |
| **Container storage (graphroot)** | `/var/lib/containers/storage` (rootful) or `$HOME/.local/share/containers/storage` (rootless) | `CONTAINERS_STORAGE_CONF` |
| **Temp files** | `$TMPDIR` or `/tmp` | `TMPDIR` environment variable |
| **Working directory** | `<destination>/working-dir` | `--workspace` flag |

### The Problem in Tekton Pods

In a Tekton pod:
- **Ephemeral storage** (`/tmp`, `/var/tmp`) is typically **small** (few GB)  
- **PVC mounts** (`/workspace/output`) have **large capacity** (100GB+)
- oc-mirror was using **default locations** which pointed to **ephemeral storage**
- Large image blobs filled `/tmp` → `localhost:55000` registry writes failed → "unexpected EOF"

## Solution Implemented

### Changes to `mirror-from-intermediate` Task

**File**: `internal/controller/disconnectedplatform_controller.go`

#### 1. Environment Variables

Added to the task step:

```go
"env": []map[string]interface{}{
    // Set TMPDIR to workspace to avoid using small ephemeral storage
    {"name": "TMPDIR", "value": "/workspace/output/tmp"},
    // Set HOME to workspace (oc-mirror defaults cache-dir to $HOME)
    {"name": "HOME", "value": "/workspace/output"},
    // Configure container storage to use workspace
    {"name": "CONTAINERS_STORAGE_CONF", "value": "/workspace/output/storage.conf"},
},
```

**Purpose**:
- `TMPDIR=/workspace/output/tmp` → Temp files go to PVC, not `/tmp`
- `HOME=/workspace/output` → oc-mirror's default cache-dir becomes `/workspace/output/.oc-mirror/.cache`
- `CONTAINERS_STORAGE_CONF` → Podman uses custom storage config

#### 2. Container Storage Configuration

Added script block to create `storage.conf`:

```bash
# Configure container storage to use workspace instead of /tmp or /var/tmp
# This ensures the localhost:55000 internal registry uses the PVC
mkdir -p /workspace/output/containers
cat > /workspace/output/storage.conf <<STORAGE_EOF
[storage]
  driver = "overlay"
  graphroot = "/workspace/output/containers/storage"
  runroot = "/workspace/output/containers/run"

[storage.options]
  additionalimagestores = []

[storage.options.overlay]
  mountopt = "nodev,metacopy=on"
STORAGE_EOF

export CONTAINERS_STORAGE_CONF=/workspace/output/storage.conf
```

**Effect**:
- Podman/containers storage root (`graphroot`) → `/workspace/output/containers/storage` (on PVC)
- Runtime state (`runroot`) → `/workspace/output/containers/run` (on PVC)
- The localhost:55000 registry's blob storage → PVC instead of `/var/lib/containers` or `/tmp`

#### 3. Diagnostic Output

Added logging to verify configuration:

```bash
echo "=== Container storage configured ==="
echo "Storage root: /workspace/output/containers/storage"
echo "Temp directory: $TMPDIR"
echo "Cache directory: $HOME/.oc-mirror/.cache"
df -h /workspace/output
```

This helps operators verify:
- Storage paths are correctly set
- PVC has sufficient free space
- Configuration loaded before oc-mirror runs

#### 4. Updated registries.conf Path

Changed from:
```bash
mkdir -p /tmp/containers
cat > /tmp/containers/registries.conf <<EOF
...
export CONTAINERS_REGISTRIES_CONF=/tmp/containers/registries.conf
```

To:
```bash
mkdir -p /workspace/output/tmp/containers
cat > /workspace/output/tmp/containers/registries.conf <<EOF
...
export CONTAINERS_REGISTRIES_CONF=/workspace/output/tmp/containers/registries.conf
```

**Reason**: Consistency - keep all configuration files on the PVC.

## Expected Storage Usage

With a typical OCP 4.21 full release mirror:

| Component | Approximate Size | Location (Before) | Location (After) |
|-----------|------------------|-------------------|------------------|
| Image blobs | 50-80 GB | `/tmp` or `/var/tmp` ❌ | `/workspace/output/containers/storage` ✅ |
| oc-mirror cache | 5-10 GB | `$HOME/.oc-mirror/.cache` ❌ | `/workspace/output/.oc-mirror/.cache` ✅ |
| Working dir | 1-2 GB | `/workspace/output/working-dir` ✅ | `/workspace/output/working-dir` ✅ |
| Temp files | 1-5 GB | `/tmp` ❌ | `/workspace/output/tmp` ✅ |
| **Total** | **60-100 GB** | **Mixed** | **All on PVC** ✅ |

## Verification

### Before the Fix

```
[ERROR] Failed to copy ocpReleaseContent...
error: writing blob: Patch "http://localhost:55000/...": 
readfrom tcp [::1]:42240->[::1]:55000: happened during read: unexpected EOF
```

```
✗ 31 / 191 release images mirrored
```

### After the Fix

Expected output:

```
=== Container storage configured ===
Storage root: /workspace/output/containers/storage
Temp directory: /workspace/output/tmp
Cache directory: /workspace/output/.oc-mirror/.cache
Filesystem      Size  Used Avail Use% Mounted on
/dev/sda1       200G   45G  155G  23% /workspace/output

=== Created registries.conf with upstream blocking ===
[[registry]]
location="registry.redhat.io"
blocked = true
[[registry.mirror]]
location="quay.example.com/mirror"
...

✓ Intermediate registry is accessible
✓ Found repositories in intermediate registry

=== Starting mirror from intermediate registry ===
Source: quay.example.com/mirror
Destination: file:///workspace/output
Upstream access: BLOCKED (registries.conf + /etc/hosts)

[oc-mirror successfully pulls and writes all images]

✓ 191 / 191 release images mirrored
✓ 3 / 3 additional images mirrored

=== Mirror from intermediate complete ===
```

## PVC Sizing Recommendations

Based on typical mirroring scenarios:

| Use Case | Content | Recommended PVC Size |
|----------|---------|---------------------|
| Single OCP release | 1 version, minimal operators | 100 GB |
| Full OCP release | 1 version, all content | 150 GB |
| Multiple OCP versions | 2-3 versions | 250 GB |
| Continuous mirroring | Incremental updates | 300 GB+ |

**Formula**: `(Expected tar archive size) × 2 + 50 GB overhead`

The 2× multiplier accounts for:
- Intermediate storage in localhost:55000 registry
- Final tar archives
- Working directory and temp files

## Troubleshooting

### Issue: Still getting "unexpected EOF" errors

**Check 1**: Verify PVC has sufficient space

```bash
oc exec <pipeline-pod> -- df -h /workspace/output
```

If usage is >90%, increase PVC size or clean up old collections.

**Check 2**: Verify environment variables are set

```bash
oc logs <pipeline-pod> | grep "Container storage configured"
```

Should show:
```
Storage root: /workspace/output/containers/storage
Temp directory: /workspace/output/tmp
```

**Check 3**: Verify storage.conf is being used

```bash
oc exec <pipeline-pod> -- cat /workspace/output/storage.conf
```

Should show `graphroot = "/workspace/output/containers/storage"`.

### Issue: "no space left on device" on PVC

**Symptoms**:
```
mkdir: cannot create directory '/workspace/output/containers/storage': No space left on device
```

**Solutions**:

1. **Increase PVC size** (if storage class supports expansion):
   ```bash
   oc patch pvc <pvc-name> -p '{"spec":{"resources":{"requests":{"storage":"300Gi"}}}}'
   ```

2. **Clean up old collections**:
   ```bash
   oc exec <pipeline-pod> -- rm -rf /workspace/output/mirror_seq*.tar
   oc exec <pipeline-pod> -- rm -rf /workspace/output/containers/storage
   ```

3. **Use a larger PVC** in CollectionPipeline spec:
   ```yaml
   spec:
     storage:
       pvc:
         size: 300Gi
   ```

### Issue: Pod crashes with OOM (Out of Memory)

**Symptoms**:
```
OOMKilled (exit code 137)
```

**Root Cause**: Large blobs being processed in memory.

**Solution**: Increase pod memory limits in CollectionPipeline:

```yaml
spec:
  pipeline:
    resources:
      limits:
        memory: "8Gi"
      requests:
        memory: "4Gi"
```

## Related Documentation

- [oc-mirror Source Code](https://github.com/openshift/oc-mirror) - Storage configuration internals
- [Upstream Blocking Implementation](upstream-blocking-implementation.md) - Defense-in-depth blocking
- [Troubleshooting Mirror-From-Intermediate](troubleshooting-mirror-from-intermediate.md) - General troubleshooting guide

## References

From oc-mirror source code analysis:

- **Cache path constant**: `/internal/pkg/cli/const.go:cacheRelativePath = ".oc-mirror/.cache"`
- **Storage setup**: `/internal/pkg/cli/executor.go:setupLocalStorageDir()`
- **Registry config template**: `/internal/pkg/cli/executor.go:registryConfigYaml`
- **Default port**: `/internal/pkg/mirror/options.go:Port uint16 // HTTP port used by oc-mirror's local storage instance`

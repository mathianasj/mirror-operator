# Troubleshooting Mirror-From-Intermediate Failures

## Overview

The `mirror-from-intermediate` step implements the oc-mirror enclave workflow pattern, where content is mirrored FROM an intermediate registry TO a local disk archive.

## How It Works

### Enclave Workflow Pattern

Based on [oc-mirror enclave support](https://github.com/openshift/oc-mirror/blob/main/docs/features/enclave_support.md):

1. **registries.conf Configuration**: A `registries.conf` file redirects public registry requests to the intermediate registry **and blocks upstream access**:
   ```toml
   [[registry]]
   location="registry.redhat.io"
   blocked = true  # Prevents fallback to upstream
   [[registry.mirror]]
   location="<intermediate-registry>"
   ```

   **Important**: The `blocked = true` directive ensures that if the mirror fails, the operation fails immediately instead of falling back to the upstream registry.

2. **Mirror to Disk**: `oc-mirror` runs in `mirrorToDisk` mode:
   ```bash
   export CONTAINERS_REGISTRIES_CONF=/path/to/registries.conf
   oc-mirror --v2 --config=imageset.yaml file:///workspace/output
   ```

3. **Internal Localhost Registry**: oc-mirror starts a temporary registry on `localhost:55000` to stage images during the mirror-to-disk process

4. **Image Flow**: 
   - oc-mirror requests image from public registry (e.g., `registry.redhat.io/image`)
   - registries.conf redirects to intermediate registry
   - Image is pulled from intermediate → staged in localhost:55000 → written to disk

## Common Failure: "unexpected EOF" at localhost:55000

### Symptoms
```
[ERROR] Failed to copy ocpReleaseContent ...
error: writing blob: Patch "http://localhost:55000/v2/.../blobs/uploads/...": 
readfrom tcp [::1]:42240->[::1]:55000: happened during read: unexpected EOF
```

### What This Means

**This is NOT a storage issue.** The error indicates:

1. The connection to `localhost:55000` (oc-mirror's internal staging registry) broke
2. This happens **because** the upstream pull from the intermediate registry failed
3. When the source stream fails (EOF from intermediate registry), the write to localhost:55000 also fails

### Root Causes

#### 1. Intermediate Registry Network Issues
- Intermittent connectivity to the intermediate Quay registry
- Network timeouts or packet loss
- Route/ingress issues in OpenShift

#### 2. Intermediate Registry Configuration
- The intermediate registry may be **proxying to upstream** instead of serving cached content
- Check if images exist in the intermediate registry:
  ```bash
  podman login <intermediate-registry>
  skopeo inspect docker://<intermediate-registry>/openshift/release:<tag>
  ```

#### 3. Pull Secret Issues
- Expired or invalid pull secret for intermediate registry
- Missing credentials for intermediate registry in `/workspace/pull-secret/.dockerconfigjson`

#### 4. CDN Redirect Issues
- Some registries (like Quay) redirect to CDN URLs for blob downloads
- If the intermediate registry redirects to `cdn01.quay.io`, the pipeline needs access to that CDN
- Check error messages for CDN URLs like `cdn01.quay.io/quayio-production-s3/...`

#### 5. Resource Constraints
- Pod memory limits causing OOM during large blob transfers
- CPU throttling during image processing
- Disk I/O bottlenecks on the PVC

## Diagnostic Steps

### 1. Verify Intermediate Registry Has Content

```bash
# List repositories
curl -k -u <user>:<pass> https://<intermediate-registry>/v2/_catalog

# Check specific image exists
skopeo inspect --tls-verify=false \
  docker://<intermediate-registry>/openshift/release:4.21.21-x86_64
```

### 2. Check registries.conf Is Being Used

Look for this in pipeline logs:
```
=== Created registries.conf ===
[[registry]]
location="registry.redhat.io"
[[registry.mirror]]
location="<your-intermediate-registry>"
```

Verify `CONTAINERS_REGISTRIES_CONF` environment variable is set:
```bash
echo $CONTAINERS_REGISTRIES_CONF
```

### 3. Test Pull from Intermediate Registry

From inside the pipeline pod:
```bash
podman pull --tls-verify=false \
  --authfile=/workspace/pull-secret/.dockerconfigjson \
  <intermediate-registry>/openshift/release@sha256:...
```

### 4. Check for CDN Redirects

```bash
# Enable debug logging
export CONTAINERS_CONF_OVERRIDE=/dev/null
export CONTAINERS_TRANSPORTS_OPTIONS=debug

# Pull with debug output
podman pull --tls-verify=false <intermediate-registry>/image
```

Look for HTTP 302/307 redirects to CDN URLs.

### 5. Monitor Resource Usage

```bash
# While pipeline is running:
oc adm top pod <pipeline-pod>

# Check PVC usage:
oc exec <pipeline-pod> -- df -h /workspace/output
```

## Built-in Upstream Blocking

The mirror-operator implements **defense-in-depth** blocking of upstream registries:

### 1. registries.conf blocking
All upstream registries are configured with `blocked = true`:
- `registry.redhat.io`
- `quay.io`
- `registry.access.redhat.com`
- `cdn01.quay.io`, `cdn02.quay.io`, `cdn03.quay.io`
- `docker.io`, `registry-1.docker.io`
- `gcr.io`, `ghcr.io`

This prevents podman/oc-mirror from falling back to upstream if the mirror fails.

### 2. /etc/hosts blocking
Common upstream registry hostnames are redirected to `127.0.0.1` in `/etc/hosts`:
```
127.0.0.1 registry.redhat.io
127.0.0.1 quay.io
127.0.0.1 cdn01.quay.io
...
```

This provides a second layer of protection against any tools that bypass registries.conf.

### 3. Pre-flight verification
Before starting the mirror, the pipeline:
1. Verifies the intermediate registry is accessible
2. Attempts to list repositories
3. Provides clear error messages if content is missing

## Solutions

### Solution 1: Ensure Intermediate Registry Has Cached Content

The intermediate registry must have already mirrored the content:

```bash
# On connected environment, mirror TO intermediate registry first:
oc-mirror --v2 --config=imageset.yaml \
  docker://<intermediate-registry>/mirror
```

**Critical**: Verify images are stored locally, not just proxied.

If using Quay as the intermediate registry:
```yaml
# Quay config.yaml - disable pull-through proxy to force local storage
FEATURE_PROXY_CACHE: false
FEATURE_DIRECT_DOWNLOAD: false
```

### Solution 2: Increase Timeouts and Retries

Modify the pipeline task to add retry logic:

```bash
# In mirror-from-intermediate task
for attempt in 1 2 3; do
  if oc-mirror --v2 --config=/workspace/config/imageset-config.yaml \
    --authfile=/workspace/pull-secret/.dockerconfigjson \
    file:///workspace/output; then
    break
  fi
  echo "Attempt $attempt failed, retrying..."
  sleep 30
done
```

### Solution 3: Disable CDN Redirects

If the intermediate registry is Quay, configure it to serve blobs directly:

```yaml
# Quay config.yaml
FEATURE_DIRECT_DOWNLOAD: false
```

This prevents redirects to S3/CDN URLs.

### Solution 4: Increase Resource Limits

In the CollectionPipeline CR:

```yaml
spec:
  pipeline:
    resources:
      requests:
        memory: "4Gi"
        cpu: "2"
      limits:
        memory: "8Gi"
        cpu: "4"
```

### Solution 5: Use Larger PVC

Ensure the output PVC has sufficient space:

```yaml
spec:
  storage:
    pvc:
      size: 500Gi  # Increase as needed
```

### Solution 6: Network Policy / Firewall

Ensure the pipeline pod can reach:
- Intermediate registry hostname and port
- Any CDN URLs the registry redirects to (e.g., `cdn01.quay.io`)

```bash
# Test connectivity
oc exec <pipeline-pod> -- curl -I https://<intermediate-registry>/health
oc exec <pipeline-pod> -- curl -I https://cdn01.quay.io
```

## Verification

After applying fixes, check:

1. **Success rate**: Should see `✓ 191 / 191 release images mirrored` (not `✗ 31 / 191`)
2. **No EOF errors**: Check logs for absence of `unexpected EOF` messages
3. **Complete bundle**: Verify output tar files are created:
   ```bash
   oc exec <pipeline-pod> -- ls -lh /workspace/output/mirror_seq*.tar
   ```

## Related Documentation

- [oc-mirror Enclave Support](https://github.com/openshift/oc-mirror/blob/main/docs/features/enclave_support.md)
- [Mirror Operator CLAUDE.md](../CLAUDE.md)
- [Integration Guide](integration-guide.md)

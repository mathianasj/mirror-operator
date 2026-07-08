# Upstream Registry Blocking Implementation

## Overview

This document describes the defense-in-depth approach to blocking upstream registry access in the `mirror-from-intermediate` pipeline task, ensuring all content is pulled exclusively from the intermediate registry.

## Problem Statement

When using the oc-mirror enclave workflow to mirror FROM an intermediate registry TO disk, there are risks:

1. **Fallback behavior**: If content is missing from the intermediate registry, podman/oc-mirror might fall back to upstream registries
2. **CDN redirects**: Some registries (especially Quay) redirect blob downloads to CDN URLs like `cdn01.quay.io`
3. **Pull-through proxies**: Intermediate registries configured as pull-through proxies will fetch from upstream on cache miss
4. **Bypass mechanisms**: Some tools may not respect `registries.conf` configuration

## Implementation

### Layer 1: registries.conf with blocking

File: `internal/controller/disconnectedplatform_controller.go` (mirror-from-intermediate task)

```toml
[[registry]]
location="registry.redhat.io"
blocked = true  # KEY: Prevents fallback to upstream
[[registry.mirror]]
location="<intermediate-registry>"
```

**Blocked Registries:**
- `registry.redhat.io` - Red Hat Container Catalog
- `quay.io` - Quay.io public registry
- `registry.access.redhat.com` - Legacy Red Hat registry
- `cdn01.quay.io`, `cdn02.quay.io`, `cdn03.quay.io` - Quay CDN endpoints
- `docker.io` - Docker Hub
- `registry-1.docker.io` - Docker Hub registry endpoint
- `gcr.io` - Google Container Registry
- `ghcr.io` - GitHub Container Registry

**How it works:**
- The `blocked = true` directive tells podman/skopeo/oc-mirror to **fail** if the mirror cannot serve the content
- Without `blocked = true`, the tools would attempt the upstream registry as a fallback
- All image pulls are redirected to `$(params.intermediate-registry)` via the `[[registry.mirror]]` section

### Layer 2: /etc/hosts poisoning

Defense-in-depth mechanism that blocks DNS resolution:

```bash
cat >> /etc/hosts <<EOF
127.0.0.1 registry.redhat.io
127.0.0.1 quay.io
127.0.0.1 cdn01.quay.io
...
EOF
```

**Purpose:**
- Provides protection even if registries.conf is bypassed
- Ensures that any tool attempting to resolve these hostnames gets `127.0.0.1`
- Causes immediate connection failure instead of reaching internet

**Trade-off:**
- Makes debugging harder if legitimate local services run on these domains
- Should be combined with clear error messages (implemented in Layer 3)

### Layer 3: Pre-flight verification and error messages

Before starting the mirror operation:

```bash
# Verify intermediate registry is accessible
if ! curl -f -k "https://<intermediate-registry>/v2/"; then
  echo "ERROR: Cannot reach intermediate registry"
  exit 1
fi

# Attempt to list repositories
curl -k "https://<intermediate-registry>/v2/_catalog?n=10"
```

After mirror failure:

```bash
if ! oc-mirror ...; then
  echo "Common causes:"
  echo "  1. Content not present in intermediate registry"
  echo "  2. Authentication failed"
  echo "  3. Intermediate registry proxying to blocked upstream CDNs"
  echo ""
  echo "Verify intermediate was populated with:"
  echo "  oc-mirror --v2 docker://<intermediate-registry>/mirror"
  exit 1
fi
```

**Benefits:**
- Detects misconfiguration early (before starting expensive mirror operation)
- Provides actionable error messages
- Helps operators diagnose whether content is missing vs. network issues

## Expected Behavior

### Success Case

```
=== Created registries.conf with upstream blocking ===
[[registry]]
location="registry.redhat.io"
blocked = true
[[registry.mirror]]
location="quay.example.com/mirror"
...

=== Blocked upstream registries in /etc/hosts ===
127.0.0.1 registry.redhat.io
127.0.0.1 quay.io
127.0.0.1 cdn01.quay.io
...

=== Verifying intermediate registry access ===
✓ Intermediate registry is accessible

=== Checking intermediate registry content ===
openshift/release
openshift/release-images
...

=== Starting mirror from intermediate registry ===
Source: quay.example.com/mirror
Destination: file:///workspace/output
Upstream access: BLOCKED (registries.conf + /etc/hosts)

[oc-mirror output]
✓ 191 / 191 release images mirrored

=== Mirror from intermediate complete ===
```

### Failure Case: Missing Content

```
=== Verifying intermediate registry access ===
✓ Intermediate registry is accessible

=== Starting mirror from intermediate registry ===
[oc-mirror attempts to pull image]
ERROR: manifest unknown: manifest unknown
ERROR: Failed to copy image

=== Mirror from intermediate FAILED ===

Common causes:
  1. Content not present in intermediate registry (quay.example.com/mirror)
  2. Authentication failed (check pull secret)
  3. Intermediate registry is proxying/redirecting to blocked upstream CDNs

Check that the intermediate registry has been populated with:
  oc-mirror --v2 --config=imageset.yaml docker://quay.example.com/mirror
```

### Failure Case: Blocked Upstream Attempt

```
=== Starting mirror from intermediate registry ===
[oc-mirror attempts to pull image]
ERROR: error pinging docker registry registry.redhat.io: 
Get "https://registry.redhat.io/v2/": dial tcp 127.0.0.1:443: connect: connection refused

=== Mirror from intermediate FAILED ===
```

This indicates the tool tried to reach upstream (blocked by /etc/hosts → 127.0.0.1).

## Intermediate Registry Configuration

For this blocking to work correctly, the intermediate registry **must** have content stored locally, not configured as a pull-through proxy.

### Quay Configuration

Disable pull-through features:

```yaml
# Quay config.yaml
FEATURE_PROXY_CACHE: false
FEATURE_DIRECT_DOWNLOAD: false
```

- `FEATURE_PROXY_CACHE: false` - Disables pulling missing images from upstream on demand
- `FEATURE_DIRECT_DOWNLOAD: false` - Serves blobs directly from Quay storage instead of redirecting to S3/CDN

### Mirror Registry (Quay-based)

The `mirror-registry` installer does not enable proxy features by default - content must be explicitly mirrored:

```bash
# Populate the intermediate registry on connected network
oc-mirror --v2 --config=imageset.yaml docker://registry.example.com:8443/mirror
```

### Verification

Check that images are actually stored:

```bash
# List repositories
curl -k https://registry.example.com:8443/v2/_catalog

# Check specific image manifest is stored locally
curl -k https://registry.example.com:8443/v2/openshift/release/manifests/4.21.21-x86_64
```

If the registry returns HTTP 404 for a manifest that oc-mirror expects, the content is missing.

## Testing

### Test 1: Verify Blocking Works

```bash
# In the pipeline pod, try to reach upstream (should fail)
curl -v https://registry.redhat.io/v2/
# Expected: Connection refused (127.0.0.1)

# Try to pull from upstream directly (should fail)
podman pull registry.redhat.io/ubi9/ubi:latest
# Expected: error pinging docker registry
```

### Test 2: Verify Mirror Works

```bash
# Check registries.conf is loaded
echo $CONTAINERS_REGISTRIES_CONF
cat $CONTAINERS_REGISTRIES_CONF

# Pull image that should redirect to intermediate
podman pull --authfile=/workspace/pull-secret/.dockerconfigjson \
  registry.redhat.io/ubi9/ubi:latest

# Expected: Pulls from intermediate registry, not upstream
```

### Test 3: End-to-End Mirror

Run a CollectionPipeline that triggers `mirror-from-intermediate`:

```yaml
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: CollectionPipeline
metadata:
  name: test-intermediate-blocking
spec:
  imageSetConfig: |
    apiVersion: mirror.openshift.io/v1alpha2
    kind: ImageSetConfiguration
    mirror:
      platform:
        channels:
          - name: stable-4.21
            minVersion: 4.21.21
            maxVersion: 4.21.21
```

Expected outcome:
- All images pulled from intermediate registry
- No upstream connections attempted
- Clear error if content missing from intermediate

## Troubleshooting

### Issue: "connection refused" errors

**Symptom:**
```
dial tcp 127.0.0.1:443: connect: connection refused
```

**Diagnosis:** Tool attempted to contact blocked upstream registry

**Resolution:** This is expected behavior - the blocking is working. The root cause is likely missing content in the intermediate registry.

### Issue: "manifest unknown" errors

**Symptom:**
```
manifest unknown: manifest unknown
```

**Diagnosis:** Image not present in intermediate registry

**Resolution:**
1. Verify intermediate registry was populated:
   ```bash
   curl -k https://<intermediate-registry>/v2/_catalog
   ```
2. Re-run mirror TO intermediate:
   ```bash
   oc-mirror --v2 --config=imageset.yaml docker://<intermediate-registry>/mirror
   ```

### Issue: CDN redirect errors (should not occur)

**Symptom:**
```
Get "https://cdn01.quay.io/...": connection refused
```

**Diagnosis:** Intermediate registry redirected to CDN (which is now blocked)

**Resolution:** Configure intermediate registry to disable direct download:
```yaml
# Quay config
FEATURE_DIRECT_DOWNLOAD: false
```

## Related Documentation

- [oc-mirror Enclave Support](https://github.com/openshift/oc-mirror/blob/main/docs/features/enclave_support.md)
- [containers-registries.conf(5)](https://github.com/containers/image/blob/main/docs/containers-registries.conf.5.md)
- [Troubleshooting Mirror-From-Intermediate](troubleshooting-mirror-from-intermediate.md)

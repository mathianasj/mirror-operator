package pipelines

import "fmt"

// DownloadRHCOSTask returns the Tekton embedded task definition for downloading
// RHCOS ISO and rootFS images. The task checks the intermediate registry first
// and skips the download when the image already exists.
func DownloadRHCOSTask(ubi9Image string) map[string]interface{} {
	return map[string]interface{}{
		"name": "download-rhcos-images",
		"taskSpec": map[string]interface{}{
			"steps": []map[string]interface{}{
				{
					"name":    "download-rhcos",
					"image":   ubi9Image,
					"command": []string{"/bin/bash", "-c"},
					"args":    []string{downloadRHCOSScript()},
				},
			},
		},
		"workspaces": []map[string]interface{}{
			{"name": "output"},
			{"name": "pull-secret"},
		},
	}
}

// BuildRHCOSServerTask returns the Tekton embedded task definition for building
// and pushing the RHCOS server OCI image. It runs after download-rhcos-images
// and also skips when the image already exists in the intermediate registry.
func BuildRHCOSServerTask() map[string]interface{} {
	return map[string]interface{}{
		"name":     "build-rhcos-server",
		"runAfter": []string{"download-rhcos-images"},
		"taskSpec": map[string]interface{}{
			"steps": []map[string]interface{}{
				{
					"name":    "build-and-push",
					"image":   "quay.io/buildah/stable:latest",
					"command": []string{"/bin/bash", "-c"},
					"env": []map[string]interface{}{
						{"name": "BUILDAH_ISOLATION", "value": "chroot"},
						{"name": "STORAGE_DRIVER", "value": "vfs"},
					},
					"securityContext": map[string]interface{}{
						"capabilities": map[string]interface{}{
							"add": []string{"SETFCAP"},
						},
					},
					"args": []string{buildRHCOSServerScript()},
				},
			},
		},
		"workspaces": []map[string]interface{}{
			{"name": "output"},
			{"name": "pull-secret"},
		},
	}
}

// RegistryV2ManifestURL constructs the V2 registry API URL for checking
// whether an image manifest exists. It correctly separates the registry
// host from the path component so that the /v2/ prefix is placed after
// the host, not after the full registry string.
//
// Examples:
//
//	RegistryV2ManifestURL("registry.example.com", "", "rhcos-server", "4.18")
//	  => "https://registry.example.com/v2/rhcos-server/manifests/4.18"
//
//	RegistryV2ManifestURL("registry.example.com", "org", "rhcos-server", "4.18")
//	  => "https://registry.example.com/v2/org/rhcos-server/manifests/4.18"
func RegistryV2ManifestURL(host, path, repository, tag string) string {
	if path != "" {
		return fmt.Sprintf("https://%s/v2/%s/%s/manifests/%s", host, path, repository, tag)
	}
	return fmt.Sprintf("https://%s/v2/%s/manifests/%s", host, repository, tag)
}

// registryExistsCheckSnippet returns the shell snippet that extracts
// REGISTRY_HOST and REGISTRY_PATH from an INTERMEDIATE_REGISTRY variable,
// looks up auth credentials from a pull secret, and checks the V2 manifest
// endpoint. Used by both download and build tasks for consistency.
func registryExistsCheckSnippet() string {
	return `
REGISTRY_HOST=$(echo "$INTERMEDIATE_REGISTRY" | cut -d/ -f1)
REGISTRY_PATH=$(echo "$INTERMEDIATE_REGISTRY" | cut -sd/ -f2-)

PULL_SECRET="/workspace/pull-secret/.dockerconfigjson"
AUTH=$(cat "$PULL_SECRET" | python3 -c "
import sys,json
d=json.load(sys.stdin)
host = '${INTERMEDIATE_REGISTRY}'.split('/')[0]
for key in [host, '${INTERMEDIATE_REGISTRY}', 'https://' + host, 'https://${INTERMEDIATE_REGISTRY}']:
  if key in d.get('auths',{}):
    print(d['auths'][key].get('auth',''))
    break
" 2>/dev/null || true)
AUTH_HEADER=""
if [ -n "$AUTH" ]; then
  AUTH_HEADER="Authorization: Basic ${AUTH}"
fi
HTTP_CODE=$(curl -sk -o /dev/null -w '%{http_code}' \
  ${AUTH_HEADER:+-H "$AUTH_HEADER"} \
  "https://${REGISTRY_HOST}/v2/${REGISTRY_PATH:+${REGISTRY_PATH}/}rhcos-server/manifests/${RHCOS_VERSION}" 2>/dev/null || echo "000")
if [ -z "$HTTP_CODE" ]; then HTTP_CODE="000"; fi
`
}

func downloadRHCOSScript() string {
	return `
set -ex

if [ "$(params.rhcos-download-enabled)" != "true" ]; then
  echo "RHCOS download skipped (rhcos-download-enabled=$(params.rhcos-download-enabled))"
  exit 0
fi

OC_VERSION="$(params.oc-version)"
# Extract major.minor (e.g., "4.18.3" -> "4.18", "stable-4.18" -> "4.18")
RHCOS_VERSION=$(echo "$OC_VERSION" | grep -oP '\d+\.\d+')
if [ -z "$RHCOS_VERSION" ]; then
  echo "ERROR: Cannot extract major.minor version from $OC_VERSION"
  exit 1
fi

INTERMEDIATE_REGISTRY="$(params.intermediate-registry)"
if [ -n "$INTERMEDIATE_REGISTRY" ]; then
` + registryExistsCheckSnippet() + `
  if [ "$HTTP_CODE" = "200" ]; then
    echo "RHCOS server image already exists: ${INTERMEDIATE_REGISTRY}/rhcos-server:${RHCOS_VERSION} — skipping download"
    exit 0
  fi
  echo "RHCOS server image not found (HTTP $HTTP_CODE), proceeding with download..."
fi

echo "=== Downloading RHCOS boot images for ACM Host Inventory ==="

RHCOS_DIR="/workspace/output/rhcos"
mkdir -p "$RHCOS_DIR"

RHCOS_BASE="https://mirror.openshift.com/pub/openshift-v4/dependencies/rhcos/${RHCOS_VERSION}/latest"

echo "Downloading RHCOS live ISO for version $RHCOS_VERSION..."
if ! curl -L -f "${RHCOS_BASE}/rhcos-live-iso.x86_64.iso" \
  -o "$RHCOS_DIR/rhcos-live.x86_64.iso" 2>/dev/null; then
  echo "Trying versioned filename..."
  curl -L -f "${RHCOS_BASE}/rhcos-${RHCOS_VERSION}.0-x86_64-live-iso.x86_64.iso" \
    -o "$RHCOS_DIR/rhcos-live.x86_64.iso"
fi

echo "Downloading RHCOS rootFS for version $RHCOS_VERSION..."
if ! curl -L -f "${RHCOS_BASE}/rhcos-live-rootfs.x86_64.img" \
  -o "$RHCOS_DIR/rhcos-live-rootfs.x86_64.img" 2>/dev/null; then
  echo "Trying versioned filename..."
  curl -L -f "${RHCOS_BASE}/rhcos-${RHCOS_VERSION}.0-x86_64-live-rootfs.x86_64.img" \
    -o "$RHCOS_DIR/rhcos-live-rootfs.x86_64.img"
fi

echo "RHCOS downloads:"
ls -lh "$RHCOS_DIR/"

# Validate file sizes (ISO and rootFS should each be > 500MB)
for file in "$RHCOS_DIR"/*.iso "$RHCOS_DIR"/*.img; do
  if [ -f "$file" ]; then
    size=$(stat -c%s "$file" 2>/dev/null || stat -f%z "$file" 2>/dev/null)
    if [ "$size" -lt 500000000 ]; then
      echo "ERROR: $(basename $file) is only $(($size / 1048576))MB - expected >500MB"
      exit 1
    fi
  fi
done

# Write version metadata
cat > "$RHCOS_DIR/RHCOS_VERSION.txt" <<EOFV
rhcos_version=${RHCOS_VERSION}
ocp_version=${OC_VERSION}
EOFV

echo "=== RHCOS download complete ==="
`
}

func buildRHCOSServerScript() string {
	return `
set -ex

if [ "$(params.rhcos-download-enabled)" != "true" ] || [ -z "$(params.intermediate-registry)" ]; then
  echo "RHCOS server build skipped (rhcos-download-enabled=$(params.rhcos-download-enabled), intermediate-registry=$(params.intermediate-registry))"
  exit 0
fi

RHCOS_DIR="/workspace/output/rhcos"
BASE_IMAGE="$(params.rhcos-server-base-image)"
INTERMEDIATE_REGISTRY="$(params.intermediate-registry)"

# Get RHCOS version from metadata file, or derive from OCP version if download was skipped
if [ -f "$RHCOS_DIR/RHCOS_VERSION.txt" ]; then
  RHCOS_VERSION=$(cat "$RHCOS_DIR/RHCOS_VERSION.txt" | grep rhcos_version | cut -d= -f2)
else
  RHCOS_VERSION=$(echo "$(params.oc-version)" | grep -oP '\d+\.\d+')
fi

FULL_IMAGE="${INTERMEDIATE_REGISTRY}/rhcos-server:${RHCOS_VERSION}"
REGISTRY_HOST=$(echo "$INTERMEDIATE_REGISTRY" | cut -d/ -f1)
REGISTRY_PATH=$(echo "$INTERMEDIATE_REGISTRY" | cut -sd/ -f2-)

# Check if RHCOS server image already exists in intermediate registry
AUTH=$(cat /workspace/pull-secret/.dockerconfigjson | python3 -c "
import sys,json
d=json.load(sys.stdin)
host = '${INTERMEDIATE_REGISTRY}'.split('/')[0]
for key in [host, '${INTERMEDIATE_REGISTRY}', 'https://' + host, 'https://${INTERMEDIATE_REGISTRY}']:
  if key in d.get('auths',{}):
    print(d['auths'][key].get('auth',''))
    break
" 2>/dev/null || true)
AUTH_HEADER=""
if [ -n "$AUTH" ]; then
  AUTH_HEADER="Authorization: Basic ${AUTH}"
fi
HTTP_CODE=$(curl -sk -o /dev/null -w '%{http_code}' \
  ${AUTH_HEADER:+-H "$AUTH_HEADER"} \
  "https://${REGISTRY_HOST}/v2/${REGISTRY_PATH:+${REGISTRY_PATH}/}rhcos-server/manifests/${RHCOS_VERSION}" 2>/dev/null || true)
if [ -z "$HTTP_CODE" ]; then HTTP_CODE="000"; fi
if [ "$HTTP_CODE" = "200" ]; then
  echo "RHCOS server image already exists: ${FULL_IMAGE} — skipping build"
  exit 0
fi

# If download was skipped but image doesn't exist, we need the RHCOS files
if [ ! -f "$RHCOS_DIR/rhcos-live.x86_64.iso" ]; then
  echo "ERROR: RHCOS files not found and image not in registry. Re-run with download enabled."
  exit 1
fi

echo "=== Building RHCOS server OCI image ==="

cat > "$RHCOS_DIR/Containerfile" <<CEOF
FROM ${BASE_IMAGE}
ADD --chown=1001:0 rhcos-live.x86_64.iso /tmp/src/rhcos-live.x86_64.iso
ADD --chown=1001:0 rhcos-live-rootfs.x86_64.img /tmp/src/rhcos-live-rootfs.x86_64.img
ADD --chown=1001:0 RHCOS_VERSION.txt /tmp/src/RHCOS_VERSION.txt
RUN /usr/libexec/s2i/assemble
CMD /usr/libexec/s2i/run
LABEL io.openshift.rhcos.version="${RHCOS_VERSION}"
LABEL io.openshift.mirror-operator/component="rhcos-server"
CEOF

buildah bud --storage-driver=vfs --isolation=chroot \
  --authfile=/workspace/pull-secret/.dockerconfigjson \
  -t "$FULL_IMAGE" \
  -f "$RHCOS_DIR/Containerfile" \
  "$RHCOS_DIR"

# Push directly to internal Quay service (bypasses external route/HAProxy)
NAMESPACE=$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace 2>/dev/null || echo "mirror-operator-system")
INTERNAL_HOST="mirror-operator-quay-quay-app.${NAMESPACE}.svc.cluster.local"

if [ -n "$REGISTRY_PATH" ]; then
  PUSH_IMAGE="${INTERNAL_HOST}/${REGISTRY_PATH}/rhcos-server:${RHCOS_VERSION}"
else
  PUSH_IMAGE="${INTERNAL_HOST}/rhcos-server:${RHCOS_VERSION}"
fi

# Extract credentials from pull secret
AUTH_B64=$(cat /workspace/pull-secret/.dockerconfigjson | \
  grep -o "\"${REGISTRY_HOST}[^\"]*\"[[:space:]]*:[[:space:]]*{[^}]*}" | head -1 | \
  grep -o '"auth":"[^"]*"' | cut -d'"' -f4)
if [ -n "$AUTH_B64" ]; then
  CREDS=$(echo "$AUTH_B64" | base64 -d)
  CRED_USER=$(echo "$CREDS" | cut -d: -f1)
  CRED_PASS=$(echo "$CREDS" | cut -d: -f2-)
  buildah login --tls-verify=false -u "$CRED_USER" -p "$CRED_PASS" "$INTERNAL_HOST"
fi

# Map external hostname to internal service IP so Quay's auth/upload
# redirects (which use SERVER_HOSTNAME) stay cluster-internal
INTERNAL_IP=$(getent hosts "$INTERNAL_HOST" | awk '{print $1}' | head -1)
if [ -n "$INTERNAL_IP" ]; then
  echo "$INTERNAL_IP $REGISTRY_HOST" >> /etc/hosts
  echo "Mapped $REGISTRY_HOST -> $INTERNAL_IP to bypass CLB"
  buildah login --tls-verify=false -u "$CRED_USER" -p "$CRED_PASS" "$REGISTRY_HOST" 2>/dev/null || true
fi

echo "Pushing via internal service: ${PUSH_IMAGE}"
buildah push --storage-driver=vfs --tls-verify=false --retry 3 \
  "$FULL_IMAGE" "docker://${PUSH_IMAGE}"

echo "=== RHCOS server image build and push complete ==="
`
}

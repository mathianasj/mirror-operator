#!/bin/bash
set -euo pipefail

# Extract RHTAS trusted root keys from connected cluster
# and prepare them for transfer to airgapped environment

NAMESPACE="${NAMESPACE:-mirror-operator-system}"
CONFIGMAP="${CONFIGMAP:-rhtas-trusted-root}"
OUTPUT_DIR="${OUTPUT_DIR:-./rhtas-trusted-root}"

echo "Extracting RHTAS trusted root keys..."
echo "Namespace: ${NAMESPACE}"
echo "ConfigMap: ${CONFIGMAP}"
echo "Output directory: ${OUTPUT_DIR}"

# Create output directory
mkdir -p "${OUTPUT_DIR}"

# Extract ConfigMap
echo ""
echo "Extracting ConfigMap..."
kubectl get configmap -n "${NAMESPACE}" "${CONFIGMAP}" -o yaml > "${OUTPUT_DIR}/configmap.yaml"

# Extract individual key files
echo "Extracting Fulcio root CA..."
kubectl get configmap -n "${NAMESPACE}" "${CONFIGMAP}" \
  -o jsonpath='{.data.fulcio-root\.pem}' > "${OUTPUT_DIR}/fulcio-root.pem"

echo "Extracting Rekor public key..."
kubectl get configmap -n "${NAMESPACE}" "${CONFIGMAP}" \
  -o jsonpath='{.data.rekor-public-key\.pem}' > "${OUTPUT_DIR}/rekor-public-key.pem"

# Extract metadata
kubectl get configmap -n "${NAMESPACE}" "${CONFIGMAP}" \
  -o jsonpath='{.data.tuf-repository-url}' > "${OUTPUT_DIR}/tuf-repository-url.txt"

kubectl get configmap -n "${NAMESPACE}" "${CONFIGMAP}" \
  -o jsonpath='{.data.extraction-timestamp}' > "${OUTPUT_DIR}/extraction-timestamp.txt"

# Generate fingerprints
echo ""
echo "Generating fingerprints..."
FULCIO_FINGERPRINT=$(sha256sum "${OUTPUT_DIR}/fulcio-root.pem" | awk '{print $1}')
REKOR_FINGERPRINT=$(sha256sum "${OUTPUT_DIR}/rekor-public-key.pem" | awk '{print $1}')

cat > "${OUTPUT_DIR}/fingerprints.txt" <<EOF
RHTAS Trusted Root Key Fingerprints
====================================

Fulcio Root CA (SHA256):
${FULCIO_FINGERPRINT}

Rekor Public Key (SHA256):
${REKOR_FINGERPRINT}

Extraction Timestamp:
$(cat "${OUTPUT_DIR}/extraction-timestamp.txt")

TUF Repository:
$(cat "${OUTPUT_DIR}/tuf-repository-url.txt")

Verification Instructions
=========================

On the airgapped cluster, verify the fingerprints match:

  sha256sum fulcio-root.pem
  sha256sum rekor-public-key.pem

Then apply the ConfigMap:

  kubectl apply -f configmap.yaml

EOF

echo ""
echo "✓ Extraction complete!"
echo ""
echo "Files created in ${OUTPUT_DIR}:"
ls -lh "${OUTPUT_DIR}"
echo ""
echo "Fingerprints:"
echo "-------------"
cat "${OUTPUT_DIR}/fingerprints.txt"
echo ""
echo "Next steps:"
echo "1. Verify fingerprints through out-of-band channel (phone, Signal, etc.)"
echo "2. Transfer ${OUTPUT_DIR} to airgapped environment via trusted media (USB, CD)"
echo "3. On airgapped cluster, verify fingerprints and apply configmap.yaml"

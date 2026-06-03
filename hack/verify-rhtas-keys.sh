#!/bin/bash
set -euo pipefail

# Verify RHTAS trusted root keys in airgapped environment
# before applying them to the cluster

INPUT_DIR="${1:-.}"

if [ ! -d "${INPUT_DIR}" ]; then
  echo "Error: Directory ${INPUT_DIR} not found"
  echo "Usage: $0 <directory-with-rhtas-keys>"
  exit 1
fi

echo "Verifying RHTAS trusted root keys..."
echo "Input directory: ${INPUT_DIR}"
echo ""

# Check required files exist
REQUIRED_FILES=(
  "fulcio-root.pem"
  "rekor-public-key.pem"
  "fingerprints.txt"
  "configmap.yaml"
)

for file in "${REQUIRED_FILES[@]}"; do
  if [ ! -f "${INPUT_DIR}/${file}" ]; then
    echo "Error: Required file ${file} not found in ${INPUT_DIR}"
    exit 1
  fi
done

echo "✓ All required files present"
echo ""

# Compute current fingerprints
FULCIO_CURRENT=$(sha256sum "${INPUT_DIR}/fulcio-root.pem" | awk '{print $1}')
REKOR_CURRENT=$(sha256sum "${INPUT_DIR}/rekor-public-key.pem" | awk '{print $1}')

echo "Current fingerprints:"
echo "--------------------"
echo "Fulcio Root CA:   ${FULCIO_CURRENT}"
echo "Rekor Public Key: ${REKOR_CURRENT}"
echo ""

# Extract expected fingerprints from file
FULCIO_EXPECTED=$(grep -A 1 "Fulcio Root CA" "${INPUT_DIR}/fingerprints.txt" | tail -1 | tr -d ' ')
REKOR_EXPECTED=$(grep -A 1 "Rekor Public Key" "${INPUT_DIR}/fingerprints.txt" | tail -1 | tr -d ' ')

echo "Expected fingerprints (from fingerprints.txt):"
echo "----------------------------------------------"
echo "Fulcio Root CA:   ${FULCIO_EXPECTED}"
echo "Rekor Public Key: ${REKOR_EXPECTED}"
echo ""

# Verify fingerprints match
if [ "${FULCIO_CURRENT}" != "${FULCIO_EXPECTED}" ]; then
  echo "❌ ERROR: Fulcio Root CA fingerprint mismatch!"
  echo "This could indicate tampering or corruption."
  echo "DO NOT PROCEED. Verify keys through out-of-band channel."
  exit 1
fi

if [ "${REKOR_CURRENT}" != "${REKOR_EXPECTED}" ]; then
  echo "❌ ERROR: Rekor Public Key fingerprint mismatch!"
  echo "This could indicate tampering or corruption."
  echo "DO NOT PROCEED. Verify keys through out-of-band channel."
  exit 1
fi

echo "✓ Fingerprints match!"
echo ""

# Validate PEM formats
echo "Validating key formats..."
if ! openssl x509 -in "${INPUT_DIR}/fulcio-root.pem" -text -noout > /dev/null 2>&1; then
  echo "❌ ERROR: Invalid Fulcio root CA certificate format"
  exit 1
fi
echo "✓ Fulcio root CA is valid X.509 certificate"

if ! openssl pkey -in "${INPUT_DIR}/rekor-public-key.pem" -pubin -text -noout > /dev/null 2>&1; then
  echo "❌ ERROR: Invalid Rekor public key format"
  exit 1
fi
echo "✓ Rekor public key is valid"
echo ""

# Display extraction metadata
echo "Extraction metadata:"
echo "-------------------"
if [ -f "${INPUT_DIR}/extraction-timestamp.txt" ]; then
  echo "Timestamp: $(cat "${INPUT_DIR}/extraction-timestamp.txt")"
fi
if [ -f "${INPUT_DIR}/tuf-repository-url.txt" ]; then
  echo "TUF Repo:  $(cat "${INPUT_DIR}/tuf-repository-url.txt")"
fi
echo ""

echo "======================================"
echo "✓ Verification successful!"
echo "======================================"
echo ""
echo "IMPORTANT: Before applying, verify these fingerprints"
echo "through an out-of-band channel (phone call, Signal, etc.)"
echo "with someone who has access to the connected cluster."
echo ""
echo "To apply the trusted root keys:"
echo "  kubectl apply -f ${INPUT_DIR}/configmap.yaml"
echo ""

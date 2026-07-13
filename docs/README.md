# Documentation

## Getting Started

- [Project Overview](../README.md) -- What mirror-operator does and how to get started
- [Architecture Guide](architecture.md) -- How the operator works, with diagrams

## API Reference

- [CRD Reference](crd-reference.md) -- Complete field reference for all Custom Resources
- [Reference Architecture Mapping](reference-architecture-mapping.md) -- How this operator maps to the OCP Disconnected Pipeline reference architecture

## Integration

- [Integration Guide](integration-guide.md) -- Programmatic integration with Go, Python, and Ansible examples
- [Console Plugin Integration](console-plugin-integration.md) -- Embedding the Airgap Architect UI in the OpenShift console

## Supply Chain Security

- [RHTAS Integration](rhtas-integration.md) -- Sigstore/Fulcio/Rekor setup and configuration
- [Keyless Signing with Intermediate Registry](KEYLESS_SIGNING_WITH_INTERMEDIATE_REGISTRY.md) -- Three-phase mirroring workflow with keyless signing
- [Keycloak TLS Configuration](keycloak-tls-configuration.md) -- TLS setup for managed Keycloak (cert-manager and custom secret)
- [Image Signing Deduplication](image-signing-deduplication.md) -- Optimization to skip re-signing already-signed images
- [Signature Verification Security Advisory](SECURITY-signature-verification.md) -- Security fix for upstream signature bypass

## STIG/FIPS Compliance

- [STIG and FIPS Compliance Guide](stig-fips-compliance.md) -- How the operator handles DISA STIG-hardened environments
- [STIG Quick Reference](stig-quick-reference.md) -- Quick-start card for STIG systems
- [STIG Update README](STIG-UPDATE-README.md) -- Release notes for the STIG compliance update
- [Import Script STIG Changes](import-script-stig-update-summary.md) -- Technical summary of import script changes for STIG

## Advanced Features

- [Parent Pipeline (Delta Collections)](parent-pipeline-implementation-summary.md) -- PVC sharing and delta collection support
- [Parent Pipeline UI Integration](parent-pipeline-ui-integration.md) -- UI integration guide for the parent pipeline feature
- [Airgap Architect Bundle Integration](airgap-architect-bundle-integration.md) -- Including Architect images in collection bundles
- [Upstream Registry Blocking](upstream-blocking-implementation.md) -- Defense-in-depth blocking of upstream registries during mirroring
- [IDMS-Based registries.conf](idms-based-registries-conf.md) -- Generating registries.conf from ImageDigestMirrorSet

## Troubleshooting

- [Mirror-From-Intermediate Troubleshooting](troubleshooting-mirror-from-intermediate.md) -- Diagnosing mirror-from-intermediate failures
- [Mirror-From-Intermediate Storage Fix](mirror-from-intermediate-storage-fix.md) -- Root cause analysis and fix for "unexpected EOF" errors

## Development

- [Refactoring TODO](REFACTORING-TODO.md) -- Tracked anti-patterns and refactoring work
- [RHTAS Key Distribution](../hack/RHTAS_KEY_DISTRIBUTION.md) -- Operational guide for distributing RHTAS root keys

# Agent Context — mirror-operator

## Goal
Build an OpenShift operator (mirror-operator) implementing the [OCP Disconnected Pipeline reference architecture](https://github.com/mathianasj/ocp-disco-pipeline-arch). The operator manages the disconnected OpenShift lifecycle with four CRDs:

- **DisconnectedPlatform** (cluster-scoped) — top-level orchestrator, aggregated status and ownership of child resources
- **CollectionPipeline** (namespaced) — builds oc-mirror v2 bundles (connected cluster) via Tekton Pipelines
- **MirrorImport** (namespaced) — imports bundles into an airgapped Quay instance and publishes CatalogSource/ICSP
- **ClusterBootstrap** (namespaced) — bootstraps new OpenShift clusters in the airgap (stub)

## Supply Chain Security (RHTAP Foundation)
The operator uses [Red Hat Trusted Application Pipeline](https://docs.redhat.com/en/documentation/red_hat_trusted_application_pipeline/1.0/html-single/getting_started_with_red_hat_trusted_application_pipeline/index) tooling to secure the bundle lifecycle:

- **Syft** (via RHTPA) — generates CycloneDX SBOMs for all mirrored container images
- **Cosign** (via RHTAS) — signs bundle tarballs and attaches SBOMs as in-toto attestations
- **Tekton Chains** — creates signed attestations of the PipelineRun itself (SLSA level 3)
- **Enterprise Contract** — validates attestations against release policies before import

### CollectionPipeline (connected)
After `oc-mirror` produces the bundle:
1. **Syft** scans all mirrored images in `/workspace/output` → `sbom.cyclonedx.json`
2. **Cosign** signs the bundle tarball → `<bundle>.sig`
3. **Cosign** creates a signed **attestation file** (`attestation.json.sig`) linking the bundle SHA256 to the SBOM SHA256 — cryptographically proves the SBOM belongs to that specific bundle
4. **Tekton Chains** signs the PipelineRun attestation automatically
5. All artifacts (tarball, .sig, attestation.json, attestation.json.sig, sbom) stored in the output PVC

Fields: `spec.signing.keySecretRef`, `spec.signing.passwordSecretRef` (both `LocalObjectReference`)

### MirrorImport (airgapped)
Before extracting the bundle:
1. **Cosign verify** — validates `<bundle>.sig` against a trusted public key
2. **Cosign verify** — validates `attestation.json.sig` against the same public key
3. **Hash comparison** — extracts bundle + SBOM SHA256 from `attestation.json` and compares against actual artifacts; mismatch fails the import
4. **Enterprise Contract** — checks the attestation against policies (stub — `ec` CLI not yet bundled)
5. On failure → phase `"Failed"` with condition `SignatureVerification`
6. On success → import proceeds; SBOM published to RHTPA

Fields: `spec.verify.publicKeySecretRef`, `spec.verify.enterpriseContractPolicy.configMapRef`

### DisconnectedPlatform
In connected mode, deploys and manages via OLM Subscriptions:
- **OpenShift Pipelines** — Tekton Pipelines for bundle collection
- **RHTAS** (Trusted Artifact Signer) — Sigstore/Cosign signing service
- **RHTPA** (Trusted Profile Analyzer) — SBOM storage and visualization

Fields: `spec.connected.operators.{openshiftPipelines,rhtas,rhtpa}` with `{disabled,channel,approvalStrategy,catalogSource,catalogSourceNamespace,startingCSV}` on each.

## Constraints
- Connected and airgapped clusters cannot talk to each other; bundles must be physically transferred (DVD/USB).
- Frontend UI (openshift-airgap-architect) generates the oc-mirror ImageSetConfiguration YAML — operator accepts it as raw embedded YAML.
- Single tooling image with `oc-mirror` + `cosign` + `syft` serves both workflows (m2d and d2m).
- Domain: `mirror.mathianasj.github.com`, API group: `mirror.mirror.mathianasj.github.com/v1`.
- Golang, Operator SDK, Tekton Pipelines v0.63.0, Quay, OpenShift.
- Three-repository pattern: reference architecture (ocp-disco-pipeline-arch), production operator (this repo), upstream config tool (openshift-airgap-architect).
- All OLM-managed dependencies are Apache 2.0 or UBI EULA — fully redistributable.

## Images
- **Controller image** (`controller:latest`): operator controller Go binary on UBI 9 minimal (Dockerfile, multi-stage build with `golang:1.24`)
- **Tooling image** (`quay.io/mirror-operator/oc-mirror:v2`): oc-mirror 4.21.0, cosign v2.4.3, syft v1.21.0, oc 4.21.17, kubectl (Dockerfile.tooling, multi-stage build for linux/amd64)
- Single `--mirror-image` flag (default `quay.io/mirror-operator/oc-mirror:v2`) references the tooling image.

## CRDs

### DisconnectedPlatform (`disconnectedplatforms.mirror.mirror.mathianasj.github.com`)
- **Scope**: Cluster
- **Spec**
  - `mode` (PlatformMode) — `connected` or `airgapped`
  - `connected` (optional ConnectedConfig) — collection schedule, mirror registry, artifact storage, trigger types, operators
  - `airgapped` (optional AirgappedConfig) — management cluster flag, mirror registry, bootstrap enabled, import path, registryCredentials
  - `architect` (optional AirgapArchitectConfig) — enable/disable airgap-architect UI deployment
  - `gitOps` (optional GitOpsConfig) — GitOps integration (stub)
- **Status**
  - `phase` (Ready / Collecting / Importing / Error)
  - `lastCollection` / `lastImport` — version tracking for incremental collections
  - `collectionHistory` / `importHistory` — full ordered history of collection/import versions
  - `components` — aggregated status of airgap-architect, pipeline, registry, etc.
- **Reconciler**
  1. Adds finalizer on create, sets phase to Ready.
  2. Aggregates `collectionHistory` from all CollectionPipeline.status.complete entries.
  3. Aggregates `importHistory` from all MirrorImport.status.complete entries.
  4. Sets `lastCollection`/`lastImport` to the most recent entry.
  5. `reconcileSubscriptions`: when `mode: connected` and `operators` configured, creates OperatorGroup + Subscription in `openshift-operators` for each enabled operator (openshift-pipelines, trusted-artifact-signer, trusted-profile-analyzer). Supports override fields per operator.

### CollectionPipeline (`collectionpipelines.mirror.mirror.mathianasj.github.com`)
- Replaces deprecated MirrorRelease.
- **Spec**
  - `imageSetConfig` (string) — raw ImageSetConfiguration YAML
  - `triggerType` (TriggerType) — scheduled, manual, or event
  - `incremental` (bool) — incremental collection mode
  - `baseVersion` (string) — base version for incremental collections
  - `storage.output` (optional BundleOutput) — PVC name or S3 config (bucket, region, endpoint, secretRef)
  - `signing` (optional CosignSigningConfig) — `keySecretRef` + `passwordSecretRef`
- **Status**
  - `phase` (Pending → Collecting → Complete / Failed)
  - `version` — auto-generated version string (format `vYYYY.MM.DD.001-trigger`)
  - `pipelineRunRef`, `configMapRef`, `sbomRef`, `startTime`, `completionTime`
- **Reconciler**
  1. Adds finalizer on create.
  2. Generates version string from current date, trigger type (build number hardcoded to 001).
  3. If `incremental` and `baseVersion` set, validates that the base version exists in DisconnectedPlatform.status.importHistory.
  4. Creates ConfigMap from `imageSetConfig`.
  5. Creates inline Tekton PipelineRun with 3 tasks:
     - **Task 1 — oc-mirror**: `oc-mirror --config /workspace/config/imageset-config.yaml file:///workspace/output --v2`
     - **Task 2 — syft-sbom**: `syft dir:/workspace/output -o cyclonedx-json > /workspace/output/sbom.cyclonedx.json`
      - **Task 3 — cosign-sign**: `cosign sign-blob --key /workspace/cosign/cosign.key ...` (key/password from secrets); creates signed attestation.json linking bundle hash to SBOM hash
     - S3 env vars injected from referenced Secret when configured.
  6. On completion, reads pod logs for SBOM and stores in ConfigMap, updates DisconnectedPlatform.status.collectionHistory.

### MirrorImport (`mirrorimports.mirror.mirror.mathianasj.github.com`)
- **Spec**
  - `imageSetConfig` (string) — same ImageSetConfiguration YAML used to build the bundle
  - `bundle` — PVC name + filename of the transferred tarball
  - `targetRegistry` — URL + optional CAConfigMap
  - `publish` — catalogSource bool, imageContentSourcePolicy bool
  - `collectionVersion` (string) — optional reference to the CollectionPipeline version this import corresponds to
  - `verify` (optional CosignVerificationConfig) — `publicKeySecretRef` + `enterpriseContractPolicy.configMapRef`
- **Status**
  - `phase` (empty → Importing → Publishing → Complete / Failed), `conditions`
- **Reconciler** (state machine)
  1. Adds finalizer on first reconcile.
  2. `""` → if `collectionVersion` set, validates it's not already in platform import history; sets phase `Importing`.
  3. `"Importing"` — creates ConfigMap from `imageSetConfig`, creates Job:
     - Mounts bundle PVC at `/data`, ConfigMap at `/config`.
      - If `verify.publicKeySecretRef` set: prepends `cosign verify-blob --key ...` for bundle + attestation, then compares attested hashes against actual artifacts.
     - Main command: `tar -xvf /data/<bundle.tar> -C /workspace && oc-mirror --config /config/imageset-config.yaml --from file:///workspace docker://<registry> --v2`
     - Watches Job: `Complete` → phase `Publishing`, `Failed` → phase `Failed` (+ `SignatureVerification` condition on verify failure).
  4. `"Publishing"` — calls `ensureCatalogSource()` (creates CatalogSource in openshift-marketplace) and `ensureICSP()` (creates ImageDigestMirrorSet for registry.redhat.io + registry.connect.redhat.com), then sets phase `Complete`.
  5. On complete, updates DisconnectedPlatform.status.importHistory with version info.

### ClusterBootstrap (`clusterbootstraps.mirror.mirror.mathianasj.github.com`)
- **Spec**: `version`, `platform`, `installConfig`, `mirrorRegistry`, `pullSecret`, `trustBundle`, `network`, `controlPlane`, `compute`, `postInstall`
- **Status**: `phase` (Pending → Validating → Installing → Complete / Failed)
- **Reconciler** (stub) — adds finalizer, sets phase to Pending.

### MirrorRelease (deprecated)
- Kept for backward compatibility with existing CRs. No active controller — use CollectionPipeline instead.

## CLI Flags
| Flag | Default | Description |
|---|---|---|
| `--mirror-image` | `quay.io/mirror-operator/oc-mirror:v2` | Container image for CollectionPipeline PipelineRun and Import Job |
| `--metrics-bind-address` | `0` | Metrics endpoint |
| `--health-probe-bind-address` | `:8081` | Health probe endpoint |
| `--leader-elect` | `false` | Leader election for controller manager |

## Tests
- 61 tests, all passing (`make test`), coverage 64.3%.
- Uses envtest (k8s 1.31.0-darwin-arm64) + fake client.
- Tests cover: ConfigMap creation, PipelineRun 3-task construction (oc-mirror, syft-sbom, cosign-sign with key/password workspaces), S3 env vars, Job construction (d2m path + registry URL + cosign verify-blob), finalizer lifecycle, phase transitions, default image fallback, DisconnectedPlatform/ClusterBootstrap finalizer/phase lifecycle, version generation (`generateVersion`), dependency validation (`versionExists`), platform history tracking, collectionVersionComplete helper, OLM subscription creation with overrides, cosign workspace/password conditions, SBOM reader Job.

## Stubs (not yet implemented)
- Enterprise Contract `ec` CLI — not yet bundled in tooling image (cosign verify-blob works, full EC validation is stub).
- ClusterBootstrap openshift-install orchestration.
- DisconnectedPlatform sub-component scheduling (airgap-architect, full RHTAS/RHTPA beyond OLM Subscriptions).
- S3 import path for MirrorImport.
- ImageSetConfiguration YAML parsing to auto-detect mirror sources for IDMS.
- Incremental build number in version strings.

## Build Notes
- `make bundle` generates OLM bundle to `./bundle/` — uses `--interactive=false` flag to avoid prompts.
- `config/crd/kustomization.yaml` must include all CRDs (previously only had 2 of 5).
- `config/samples/kustomization.yaml` must not have duplicate CR names (esp. cluster-scoped DisconnectedPlatform).
- `make docker-build` builds controller image; `podman build -f Dockerfile.tooling -t <tag> .` builds the sidecar.

## Next Steps
1. Deploy operator to a cluster and test end-to-end.
2. Bundle Enterprise Contract `ec` CLI into tooling image for full EC validation on MirrorImport.
3. Implement DisconnectedPlatform sub-component lifecycle (airgap-architect, full RHTAS/RHTPA integration beyond OLM subscriptions).
4. Implement ClusterBootstrap openshift-install orchestration.
5. (Optional) Wire S3 import path for MirrorImport.
6. (Optional) Parse ImageSetConfiguration YAML to auto-detect mirror sources for IDMS.
7. (Optional) Increment build number in version strings.

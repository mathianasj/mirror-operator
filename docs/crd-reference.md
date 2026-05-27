# Mirror Operator CRD Reference

This document provides a complete reference for all Custom Resource Definitions (CRDs) provided by the mirror-operator.

## Table of Contents

- [DisconnectedPlatform](#disconnectedplatform)
- [CollectionPipeline](#collectionpipeline)
- [MirrorImport](#mirrorimport)
- [ClusterBootstrap](#clusterbootstrap)

---

## DisconnectedPlatform

The `DisconnectedPlatform` CRD is the primary resource that orchestrates the entire disconnected OpenShift workflow. It manages both connected (collection) and airgapped (import) environments.

**API Group:** `mirror.mirror.mathianasj.github.com/v1`  
**Kind:** `DisconnectedPlatform`  
**Short Name:** `dp`  
**Scope:** Cluster

### Spec Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `mode` | string | Yes | Operating mode: `connected` or `airgapped` |
| `connected` | [ConnectedConfig](#connectedconfig) | No | Configuration for connected environments (collection mode) |
| `airgapped` | [AirgappedConfig](#airgappedconfig) | No | Configuration for airgapped environments (import mode) |
| `architect` | [AirgapArchitectConfig](#airgaparchitectconfig) | No | Configuration for the Airgap Architect UI |
| `gitOps` | [GitOpsConfig](#gitopsconfig) | No | GitOps integration configuration |

#### ConnectedConfig

Configuration for connected environments where content is collected from the internet.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `collectionSchedule` | string | Yes | Cron schedule for automated collection (e.g., `0 2 * * *`) |
| `mirrorRegistry` | string | Yes | Registry URL for storing collected images |
| `artifactStorage` | [ArtifactStorageConfig](#artifactstorageconfig) | Yes | Storage configuration for collection bundles |
| `triggerTypes` | []string | No | Allowed trigger types: `scheduled`, `manual`, `event` |
| `operators` | [OperatorConfig](#operatorconfig) | No | OLM operator installation overrides |
| `rhtpa` | [RHTPAInstallerConfig](#rhtpainstallerconfig) | No | Red Hat Trusted Profile Analyzer configuration |

#### AirgappedConfig

Configuration for airgapped environments where content is imported from bundles.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `managementCluster` | bool | Yes | Whether this is the management cluster for the airgap |
| `mirrorRegistry` | string | Yes | Registry URL for importing images |
| `bootstrapEnabled` | bool | Yes | Enable cluster bootstrap capabilities |
| `importPath` | string | No | Path for importing bundles (e.g., `/mnt/physical-media`) |
| `registryCredentials` | LocalObjectReference | No | Secret containing registry credentials |

#### AirgapArchitectConfig

Configuration for the Airgap Architect web UI.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `enabled` | bool | Yes | Enable the Airgap Architect UI |
| `frontendImage` | string | No | Custom frontend image (defaults to operator-managed image) |
| `backendImage` | string | No | Custom backend image (defaults to operator-managed image) |
| `replicas` | int32 | No | Number of replicas (default: 1) |
| `route` | [RouteConfig](#routeconfig) | No | OpenShift Route configuration |
| `pullSecret` | LocalObjectReference | No | Custom pull secret (defaults to `openshift-config/pull-secret`) |

#### GitOpsConfig

Configuration for GitOps integration.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `enabled` | bool | Yes | Enable GitOps integration |
| `repositoryURL` | string | No | Git repository URL |
| `branch` | string | No | Git branch (default: `main`) |
| `path` | string | No | Path within the repository |
| `credentials` | LocalObjectReference | No | Secret containing Git credentials |

#### ArtifactStorageConfig

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `size` | string | Yes | Storage size (e.g., `100Gi`) |
| `storageClass` | string | No | Storage class name |

#### OperatorConfig

Override default operator installation settings.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `openshiftPipelines` | [OLMSubscriptionConfig](#olmsubscriptionconfig) | No | OpenShift Pipelines operator config |
| `rhtas` | [OLMSubscriptionConfig](#olmsubscriptionconfig) | No | Red Hat Trusted Artifact Signer config |
| `rhtpa` | [OLMSubscriptionConfig](#olmsubscriptionconfig) | No | Red Hat Trusted Profile Analyzer config |

#### OLMSubscriptionConfig

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `disabled` | bool | No | Disable this operator installation |
| `channel` | string | No | Subscription channel |
| `approvalStrategy` | string | No | `Automatic` or `Manual` |
| `catalogSource` | string | No | Catalog source name |
| `catalogSourceNamespace` | string | No | Catalog source namespace |
| `startingCSV` | string | No | Starting CSV version |

#### RouteConfig

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `host` | string | No | Custom hostname (auto-generated if not specified) |
| `tls` | [TLSConfig](#tlsconfig) | No | TLS configuration |

#### TLSConfig

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `termination` | string | No | TLS termination type: `edge`, `passthrough`, `reencrypt` (default: `edge`) |

### Status Fields

| Field | Type | Description |
|-------|------|-------------|
| `phase` | string | Current phase: `Ready`, `Collecting`, `Importing`, `Error` |
| `conditions` | []Condition | Standard Kubernetes conditions |
| `lastCollection` | [CollectionInfo](#collectioninfo) | Most recent collection metadata |
| `collectionHistory` | [][CollectionInfo](#collectioninfo) | Historical collection records |
| `lastImport` | [ImportInfo](#importinfo) | Most recent import metadata |
| `importHistory` | [][ImportInfo](#importinfo) | Historical import records |
| `components` | [][ComponentStatus](#componentstatus) | Status of platform components |

#### CollectionInfo

| Field | Type | Description |
|-------|------|-------------|
| `version` | string | Collection version identifier |
| `timestamp` | Time | Collection timestamp |
| `size` | string | Bundle size |
| `status` | string | Collection status |

#### ImportInfo

| Field | Type | Description |
|-------|------|-------------|
| `version` | string | Import version identifier |
| `timestamp` | Time | Import timestamp |
| `status` | string | Import status |

#### ComponentStatus

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Component name |
| `status` | string | Component status |
| `url` | string | Component URL (if applicable) |
| `lastCheck` | Time | Last health check time |

### Examples

#### Connected Platform

```yaml
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: DisconnectedPlatform
metadata:
  name: connected-platform
spec:
  mode: connected
  connected:
    collectionSchedule: "0 2 * * *"
    mirrorRegistry: "quay.io/myorg/mirror"
    artifactStorage:
      size: 500Gi
      storageClass: fast-ssd
    triggerTypes:
      - scheduled
      - manual
    operators:
      openshiftPipelines:
        channel: latest
      rhtas:
        channel: stable
      rhtpa:
        channel: stable-v1.1
    rhtpa:
      storage:
        type: pvc
        size: 100Gi
      database:
        host: postgres.example.com
        name: rhtpa
        username: rhtpa_user
        password: changeme
```

#### Airgapped Platform with Architect UI

```yaml
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: DisconnectedPlatform
metadata:
  name: airgapped-platform
spec:
  mode: airgapped
  airgapped:
    managementCluster: true
    mirrorRegistry: "registry.airgap.local:5000/mirror"
    bootstrapEnabled: true
    importPath: /mnt/usb-drive
    registryCredentials:
      name: registry-creds
  architect:
    enabled: true
    replicas: 2
    route:
      host: architect.apps.airgap.local
      tls:
        termination: edge
    # Optional: use custom pull secret
    # pullSecret:
    #   name: my-pull-secret
```

---

## CollectionPipeline

The `CollectionPipeline` CRD triggers and manages the collection of container images, operators, and metadata from internet sources into a portable bundle.

**API Group:** `mirror.mirror.mathianasj.github.com/v1`  
**Kind:** `CollectionPipeline`  
**Short Name:** `cp`

### Spec Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `imageSetConfig` | string | Yes | ImageSetConfiguration YAML defining what to collect |
| `triggerType` | string | No | How this collection was triggered: `scheduled`, `manual`, `event` |
| `incremental` | bool | No | Perform incremental collection based on `baseVersion` |
| `baseVersion` | string | No | Base version for incremental collection |
| `storage` | [ArtifactOutput](#artifactoutput) | No | Where to store the output bundle |
| `signing` | [CosignSigningConfig](#cosignsigningconfig) | No | Cosign signing configuration |

#### ArtifactOutput

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `output` | [BundleOutput](#bundleoutput) | No | Bundle output configuration |

#### BundleOutput

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `pvc` | string | No | PVC name for storage |
| `filename` | string | No | Bundle filename |
| `s3` | [S3Config](#s3config) | No | S3 storage configuration |

#### S3Config

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `bucket` | string | Yes | S3 bucket name |
| `region` | string | Yes | AWS region |
| `endpoint` | string | No | Custom S3 endpoint |
| `secretRef` | LocalObjectReference | Yes | Secret with AWS credentials |

#### CosignSigningConfig

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `keySecretRef` | LocalObjectReference | No | Secret containing cosign private key |
| `passwordSecretRef` | LocalObjectReference | No | Secret containing key password |

### Status Fields

| Field | Type | Description |
|-------|------|-------------|
| `phase` | string | `Pending`, `Collecting`, `Complete`, `Failed` |
| `version` | string | Generated collection version |
| `pipelineRunRef` | string | Tekton PipelineRun reference |
| `configMapRef` | string | ConfigMap containing ImageSetConfig |
| `sbomRef` | string | SBOM artifact reference |
| `sbomReaderRef` | string | SBOM reader service account |
| `startTime` | Time | Collection start time |
| `completionTime` | Time | Collection completion time |
| `conditions` | []Condition | Standard Kubernetes conditions |

### Example

```yaml
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: CollectionPipeline
metadata:
  name: ocp-4-17-collection
spec:
  triggerType: manual
  incremental: false
  imageSetConfig: |
    apiVersion: mirror.openshift.io/v1alpha2
    kind: ImageSetConfiguration
    mirror:
      platform:
        channels:
          - name: stable-4.17
            minVersion: 4.17.0
            maxVersion: 4.17.5
      operators:
        - catalog: registry.redhat.io/redhat/redhat-operator-index:v4.17
          packages:
            - name: openshift-pipelines-operator-rh
            - name: rhtas-operator
  storage:
    output:
      pvc: collection-storage
      filename: ocp-4.17-bundle.tar
  signing:
    keySecretRef:
      name: cosign-key
    passwordSecretRef:
      name: cosign-password
```

---

## MirrorImport

The `MirrorImport` CRD imports a previously collected bundle into an airgapped registry and optionally creates CatalogSources and ImageContentSourcePolicies.

**API Group:** `mirror.mirror.mathianasj.github.com/v1`  
**Kind:** `MirrorImport`  
**Short Name:** `mi`

### Spec Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `imageSetConfig` | string | Yes | ImageSetConfiguration YAML (must match collection) |
| `bundle` | [BundleSource](#bundlesource) | Yes | Location of the bundle to import |
| `targetRegistry` | [RegistryConfig](#registryconfig) | Yes | Target registry configuration |
| `publish` | [PublishConfig](#publishconfig) | Yes | What resources to publish |
| `collectionVersion` | string | No | Version identifier from collection |
| `verify` | [CosignVerificationConfig](#cosignverificationconfig) | No | Bundle verification configuration |

#### BundleSource

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `pvc` | string | Yes | PVC containing the bundle |
| `filename` | string | Yes | Bundle filename |

#### RegistryConfig

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `url` | string | Yes | Target registry URL |
| `caConfigMap` | string | No | ConfigMap with registry CA certificate |

#### PublishConfig

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `catalogSource` | bool | Yes | Create CatalogSource resources |
| `imageContentSourcePolicy` | bool | Yes | Create ImageContentSourcePolicy resources |

#### CosignVerificationConfig

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `publicKeySecretRef` | LocalObjectReference | No | Secret with cosign public key |
| `enterpriseContractPolicy` | [EnterpriseContractPolicy](#enterprisecontractpolicy) | No | Enterprise Contract policy |

#### EnterpriseContractPolicy

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `configMapRef` | LocalObjectReference | Yes | ConfigMap with EC policy |

### Status Fields

| Field | Type | Description |
|-------|------|-------------|
| `phase` | string | Current import phase |
| `conditions` | []Condition | Standard Kubernetes conditions |

### Example

```yaml
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: MirrorImport
metadata:
  name: ocp-4-17-import
spec:
  collectionVersion: v2026.05.26.001
  imageSetConfig: |
    apiVersion: mirror.openshift.io/v1alpha2
    kind: ImageSetConfiguration
    mirror:
      platform:
        channels:
          - name: stable-4.17
  bundle:
    pvc: import-storage
    filename: ocp-4.17-bundle.tar
  targetRegistry:
    url: registry.airgap.local:5000
    caConfigMap: registry-ca
  publish:
    catalogSource: true
    imageContentSourcePolicy: true
  verify:
    publicKeySecretRef:
      name: cosign-public-key
```

---

## ClusterBootstrap

The `ClusterBootstrap` CRD provisions new OpenShift clusters in airgapped environments using collected images.

**API Group:** `mirror.mirror.mathianasj.github.com/v1`  
**Kind:** `ClusterBootstrap`  
**Short Name:** `cb`

### Spec Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `version` | string | Yes | OpenShift version (from collection) |
| `platform` | string | Yes | Platform type: `vsphere`, `baremetal`, `none` |
| `installConfig` | LocalObjectReference | Yes | Secret containing install-config.yaml |
| `mirrorRegistry` | string | Yes | Mirror registry URL |
| `pullSecret` | LocalObjectReference | Yes | Secret containing pull secret |
| `trustBundle` | LocalObjectReference | No | Secret containing additional CA trust bundle |
| `network` | [NetworkConfig](#networkconfig) | No | Network configuration overrides |
| `controlPlane` | [NodePoolConfig](#nodepoolconfig) | No | Control plane node configuration |
| `compute` | [NodePoolConfig](#nodepoolconfig) | No | Compute node configuration |
| `postInstall` | [PostInstallConfig](#postinstallconfig) | No | Post-installation tasks |

#### NetworkConfig

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `clusterNetwork` | string | No | Cluster network CIDR |
| `serviceNetwork` | string | No | Service network CIDR |
| `networkType` | string | No | Network plugin: `OVNKubernetes`, `OpenShiftSDN` |

#### NodePoolConfig

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `replicas` | int32 | Yes | Number of nodes |
| `resources` | [NodeResources](#noderesources) | No | Node resource requirements |

#### NodeResources

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `cpu` | int32 | No | CPU cores |
| `memory` | int32 | No | Memory in GB |
| `disk` | int32 | No | Disk size in GB |

#### PostInstallConfig

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `operators` | [][OperatorInstall](#operatorinstall) | No | Operators to install post-bootstrap |

#### OperatorInstall

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Operator package name |
| `namespace` | string | No | Target namespace |
| `channel` | string | No | Subscription channel |

### Status Fields

| Field | Type | Description |
|-------|------|-------------|
| `phase` | string | `Pending`, `Validating`, `Installing`, `Complete`, `Failed` |
| `kubeconfig` | LocalObjectReference | Secret containing cluster kubeconfig |
| `consoleUrl` | string | OpenShift console URL |
| `startTime` | Time | Bootstrap start time |
| `completionTime` | Time | Bootstrap completion time |
| `conditions` | []Condition | Standard Kubernetes conditions |

### Example

```yaml
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: ClusterBootstrap
metadata:
  name: prod-cluster-01
spec:
  version: "4.17.5"
  platform: vsphere
  installConfig:
    name: install-config
  mirrorRegistry: "registry.airgap.local:5000/mirror"
  pullSecret:
    name: pull-secret
  trustBundle:
    name: ca-bundle
  network:
    networkType: OVNKubernetes
  controlPlane:
    replicas: 3
    resources:
      cpu: 8
      memory: 32
      disk: 120
  compute:
    replicas: 3
    resources:
      cpu: 16
      memory: 64
      disk: 500
  postInstall:
    operators:
      - name: openshift-pipelines-operator-rh
        channel: latest
      - name: rhtas-operator
        namespace: openshift-operators
        channel: stable
```

---

## Common Patterns

### Secret References

Many CRDs reference Kubernetes Secrets via `LocalObjectReference`:

```yaml
pullSecret:
  name: my-secret  # Must exist in the same namespace as the CR
```

### Version Correlation

Collection and import workflows use version identifiers:

1. `CollectionPipeline` generates a `version` in its status
2. `MirrorImport` references that version via `collectionVersion`
3. `ClusterBootstrap` uses the version to select images

### Status Conditions

All CRDs follow standard Kubernetes condition conventions:

```yaml
status:
  conditions:
    - type: Ready
      status: "True"
      reason: CollectionComplete
      message: "Collection completed successfully"
      lastTransitionTime: "2026-05-27T10:00:00Z"
```

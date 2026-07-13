# Architecture Guide

The mirror-operator automates the lifecycle of disconnected (airgapped) OpenShift environments. In these environments, production clusters have no internet access -- all container images, operators, and platform updates must be collected on a connected system, transferred via physical media, and imported into the airgapped cluster's registry.

This operator replaces manual `oc-mirror` workflows with a declarative, Kubernetes-native automation layer built on Tekton Pipelines, with integrated supply chain security via Red Hat Trusted Artifact Signer (RHTAS).

## Custom Resource Relationships

The operator defines four Custom Resource Definitions that work together in a layered architecture. `DisconnectedPlatform` is the cluster-scoped root resource that installs prerequisites and manages configuration. The namespace-scoped resources (`CollectionPipeline`, `MirrorImport`, `ClusterBootstrap`) handle specific workflow stages.

```mermaid
graph TD
    subgraph Cluster Scoped
        DP["DisconnectedPlatform<br/><i>Root orchestrator</i>"]
    end

    subgraph Namespace Scoped
        CP["CollectionPipeline<br/><i>Content collection</i>"]
        MI["MirrorImport<br/><i>Bundle import</i>"]
        CB["ClusterBootstrap<br/><i>Cluster provisioning (planned)</i>"]
    end

    DP -- "pushes signing config" --> CP
    DP -- "aggregates collection history" --> CP
    DP -- "aggregates import history" --> MI

    CP -- "reports completion" --> DP
    MI -- "reports completion" --> DP
    MI -- "checks import history<br/>(dedup)" --> DP

    CP -- "parent pipeline<br/>PVC sharing" --> CP

    subgraph "CollectionPipeline Child Resources"
        CM1["ConfigMap<br/><i>ImageSetConfiguration</i>"]
        PVC["PersistentVolumeClaim<br/><i>oc-mirror working storage</i>"]
        PR["Tekton PipelineRun<br/><i>Collection execution</i>"]
    end

    subgraph "MirrorImport Child Resources"
        CM2["ConfigMap<br/><i>Import configuration</i>"]
        JOB["Job<br/><i>oc-mirror import</i>"]
        CS["CatalogSource<br/><i>OLM catalog</i>"]
        IDMS["ImageDigestMirrorSet<br/><i>Image pull redirection</i>"]
    end

    CP --> CM1
    CP --> PVC
    CP --> PR

    MI --> CM2
    MI --> JOB
    MI --> CS
    MI --> IDMS

    PR -. "bundle artifact" .-> JOB

    style DP fill:#326CE5,color:#fff
    style CP fill:#4A90D9,color:#fff
    style MI fill:#4A90D9,color:#fff
    style CB fill:#999,color:#fff,stroke-dasharray: 5 5
```

The dashed border on `ClusterBootstrap` indicates it is a planned feature -- the controller currently only sets an initial `Pending` phase.

## End-to-End Workflow

The complete disconnected mirror workflow spans two environments separated by an airgap. The connected environment collects content from the internet, signs it, and packages it into a transferable bundle. The airgapped environment verifies and imports the bundle into its local registry.

```mermaid
sequenceDiagram
    actor User
    participant DP as DisconnectedPlatform
    participant CP as CollectionPipeline
    participant TR as Tekton PipelineRun
    participant S3 as S3 / Artifact Storage
    participant Media as Physical Media
    participant MI as MirrorImport
    participant Job as Import Job
    participant Reg as Airgapped Registry

    Note over DP,S3: Connected Environment

    User->>DP: Create DisconnectedPlatform CR (mode: connected)
    DP->>DP: Install operators (Tekton, Keycloak, RHTAS, Quay)
    DP->>DP: Deploy Airgap Architect UI
    DP->>DP: Configure Keycloak OIDC
    DP->>DP: Create Securesign (Fulcio, Rekor, TUF)
    DP->>DP: Create Tekton Pipeline template

    User->>CP: Create CollectionPipeline CR
    DP-->>CP: Push signing config (Fulcio/Rekor/OIDC)
    CP->>TR: Create PipelineRun
    TR->>TR: oc-mirror to intermediate registry
    TR->>TR: Sign images (Fulcio keyless)
    TR->>TR: Generate SBOM (Syft)
    TR->>TR: Mirror from intermediate to disk (tar)
    TR->>TR: Sign bundle (cosign)
    TR->>S3: Upload bundle + signature + SBOM
    TR-->>CP: Report completion
    CP-->>DP: Update collection history

    Note over Media: Airgap Transfer

    S3->>Media: Download bundle, signature, SBOM
    Media->>MI: Load onto airgapped PVC

    Note over MI,Reg: Airgapped Environment

    User->>MI: Create MirrorImport CR
    MI->>MI: Check import history (dedup)
    MI->>Job: Create import Job
    Job->>Job: Verify cosign signature
    Job->>Job: Verify attestation hashes
    Job->>Job: oc-mirror import to registry
    Job-->>Reg: Images available in registry
    MI->>MI: Create CatalogSource
    MI->>MI: Create ImageDigestMirrorSet
    MI-->>DP: Update import history
```

## DisconnectedPlatform Reconciliation Flow

The `DisconnectedPlatform` controller is the most complex controller in the operator. It manages the entire infrastructure stack on the connected side and aggregates status from all child resources. The reconciliation follows a sequential dependency chain -- each component depends on the previous ones being ready.

```mermaid
flowchart TD
    Start([Reconcile triggered]) --> Fetch[Fetch DisconnectedPlatform CR]
    Fetch --> CheckDeletion{Marked for<br/>deletion?}
    CheckDeletion -- Yes --> Cleanup[Run cleanup / remove finalizer]
    CheckDeletion -- No --> EnsureFinalizer[Ensure finalizer present]
    EnsureFinalizer --> CheckMode{Mode?}

    CheckMode -- connected --> Subs[Reconcile OLM Subscriptions<br/>Tekton, Keycloak, RHTAS, RHTPA, Quay]
    CheckMode -- airgapped --> AirgapSetup[Configure airgapped<br/>registry and RHTAS roots]

    Subs --> Architect[Deploy Airgap Architect UI<br/>Frontend + Backend + Routes]
    Architect --> CheckRHBK{Keycloak<br/>operator ready?}
    CheckRHBK -- No --> SkipKC[Skip Keycloak setup, requeue]
    CheckRHBK -- Yes --> KC[Reconcile Managed Keycloak<br/>Create realm, clients, OIDC config]
    KC --> CheckTAS{RHTAS<br/>operator ready?}
    CheckTAS -- No --> SkipTAS[Skip RHTAS setup, requeue]
    CheckTAS -- Yes --> TAS[Create Securesign CR<br/>Fulcio, Rekor, CTLog, TUF]
    TAS --> ExtractKeys[Extract RHTAS root keys<br/>Fulcio cert hash, Rekor key hash]
    ExtractKeys --> PushSigning[Push signing config to<br/>all CollectionPipeline CRs]
    PushSigning --> CheckRHTPA{RHTPA<br/>configured?}
    CheckRHTPA -- No --> SkipRHTPA[Skip RHTPA]
    CheckRHTPA -- Yes --> RHTPA[Create TrustedProfileAnalyzer CR<br/>with Keycloak OIDC + S3]
    RHTPA --> CheckQuay{Quay<br/>configured?}
    SkipRHTPA --> CheckQuay
    CheckQuay -- No --> SkipQuay[Skip Quay]
    CheckQuay -- Yes --> Quay[Create QuayRegistry CR<br/>Ensure robot account<br/>Merge into pull secret]
    Quay --> Artifacts
    SkipQuay --> Artifacts

    Artifacts[Create ObjectBucketClaim<br/>for artifact S3 storage] --> Pipeline[Create Tekton Pipeline template<br/>collection-pipeline-template]
    Pipeline --> AggregateHistory[Aggregate collection + import<br/>history from child CRs]
    AggregateHistory --> UpdateStatus[Update status + conditions]
    AirgapSetup --> AggregateHistory

    style Start fill:#326CE5,color:#fff
    style Subs fill:#4A90D9,color:#fff
    style KC fill:#D4A017,color:#fff
    style TAS fill:#E8630A,color:#fff
    style RHTPA fill:#7B68EE,color:#fff
    style Quay fill:#D71920,color:#fff
```

## Collection Pipeline Lifecycle

The `CollectionPipeline` controller manages individual collection runs. Each CollectionPipeline CR goes through a series of phases as the Tekton PipelineRun executes.

```mermaid
flowchart TD
    Start([CollectionPipeline created<br/>or triggered]) --> EnsureFinalizer[Ensure finalizer]
    EnsureFinalizer --> CheckTrigger{Trigger annotation<br/>present?}
    CheckTrigger -- Yes --> ResetStatus[Reset status fields<br/>for new run]
    CheckTrigger -- No --> CheckExisting{PipelineRun<br/>already exists?}
    ResetStatus --> CheckExisting

    CheckExisting -- Yes --> TrackRun[Track PipelineRun status]
    TrackRun --> MapStatus{PipelineRun<br/>condition?}
    MapStatus -- Running --> SetCollecting[Phase: Collecting]
    MapStatus -- Succeeded --> ExtractResults[Extract results<br/>bundle-url, signature-url, sbom-url]
    MapStatus -- Failed --> SetFailed[Phase: Failed]
    ExtractResults --> SetComplete[Phase: Complete]
    SetComplete --> ReportHistory[Update DisconnectedPlatform<br/>collection history]

    CheckExisting -- No --> ValidateParent{Parent pipeline<br/>configured?}
    ValidateParent -- Yes --> CheckParent{Parent<br/>complete?}
    CheckParent -- No --> Requeue30[Requeue in 30s]
    CheckParent -- Yes --> CheckIncremental
    ValidateParent -- No --> CheckIncremental{Incremental<br/>collection?}
    CheckIncremental -- Yes --> CheckBase{Base version<br/>imported?}
    CheckBase -- No --> RequeueBase[Requeue in 30s]
    CheckBase -- Yes --> GenerateVersion
    CheckIncremental -- No --> GenerateVersion[Generate version string<br/>e.g. v2026.07.13.001-manual]

    GenerateVersion --> EnsureConfigMap[Create/update ConfigMap<br/>with ImageSetConfiguration]
    EnsureConfigMap --> EnsurePVC[Ensure working PVC<br/>or share parent PVC]
    EnsurePVC --> CheckSigning{Signing config<br/>applied?}
    CheckSigning -- No --> WaitSigning[Wait for DisconnectedPlatform<br/>to push signing config]
    CheckSigning -- Yes --> CreateRun[Create Tekton PipelineRun<br/>with all parameters]
    CreateRun --> TrackRun

    style Start fill:#326CE5,color:#fff
    style SetComplete fill:#28A745,color:#fff
    style SetFailed fill:#DC3545,color:#fff
```

## Supply Chain Security

The operator implements a complete supply chain security model using Red Hat Trusted Artifact Signer (RHTAS), which provides a private Sigstore deployment. Every collection produces signed artifacts with full provenance, and every import can verify the chain before trusting the content.

```mermaid
flowchart LR
    subgraph "Connected Side - Signing"
        direction TB
        Mirror[oc-mirror collects<br/>images to intermediate<br/>Quay registry]
        Mirror --> SignImg[Sign images in registry<br/>Fulcio keyless via OIDC]
        SignImg --> SBOM[Generate SBOM<br/>Syft scan of images]
        SBOM --> MirrorDisk[Mirror from intermediate<br/>to disk - create tar bundle]
        MirrorDisk --> SignBundle[Sign bundle with cosign<br/>Fulcio keyless certificate]
        SignBundle --> Attest[Create attestation.json<br/>SHA256 of bundle + SBOM]
        Attest --> Upload[Upload to S3:<br/>bundle.tar, .sig, sbom.json,<br/>attestation.json]
    end

    subgraph "RHTAS Infrastructure"
        direction TB
        Fulcio[Fulcio<br/>Certificate authority]
        Rekor[Rekor<br/>Transparency log]
        CTLog[CT Log<br/>Certificate transparency]
        TUF[TUF<br/>Root of trust distribution]
        KC[Keycloak<br/>OIDC identity provider]
        KC --> Fulcio
    end

    subgraph "Airgapped Side - Verification"
        direction TB
        Verify1[Verify bundle signature<br/>cosign verify-blob]
        Verify1 --> Verify2[Verify attestation signature<br/>cosign verify-blob]
        Verify2 --> Verify3[Compare SHA256 hashes<br/>bundle vs attestation]
        Verify3 --> Import[oc-mirror import<br/>to local registry]
    end

    SignImg -.-> Fulcio
    SignImg -.-> Rekor
    SignBundle -.-> Fulcio
    SignBundle -.-> Rekor

    Upload -- "Physical<br/>media" --> Verify1

    style Fulcio fill:#E8630A,color:#fff
    style Rekor fill:#E8630A,color:#fff
    style CTLog fill:#E8630A,color:#fff
    style TUF fill:#E8630A,color:#fff
    style KC fill:#D4A017,color:#fff
```

### Key Security Properties

- **Keyless signing**: No long-lived signing keys to manage. Fulcio issues short-lived certificates based on OIDC identity, and all signing events are recorded in Rekor's immutable transparency log.
- **Private Sigstore**: The operator deploys its own RHTAS instance rather than using the public Sigstore infrastructure. This means verification on the airgapped side uses your organization's root of trust, not a public one.
- **Attestation verification**: The import side verifies not just the bundle signature, but also an attestation document that binds the bundle's SHA256 hash to the SBOM's hash -- ensuring the SBOM matches the exact bundle being imported.
- **RHTAS root key distribution**: Root keys (Fulcio certificate hash, Rekor public key hash) must be securely transferred to the airgapped environment out-of-band. See [RHTAS Key Distribution](../hack/RHTAS_KEY_DISTRIBUTION.md) for the operational procedure.

## Managed Components

On the connected side, the `DisconnectedPlatform` controller installs and configures the following operator ecosystem via OLM subscriptions:

| Component | Operator | Purpose |
|-----------|----------|---------|
| **Tekton Pipelines** | `openshift-pipelines-operator-rh` | Runs the collection pipeline (oc-mirror, signing, packaging) |
| **Red Hat Build of Keycloak** | `rhbk-operator` | Provides OIDC identity for Fulcio keyless signing |
| **RHTAS** | `rhtas-operator` | Deploys Fulcio, Rekor, CTLog, Trillian, and TUF for supply chain signing |
| **RHTPA** | `trustification-operator` | Deploys Trustify for SBOM storage and vulnerability analysis |
| **Quay** | `quay-operator` | Provides an intermediate registry for the three-phase signing workflow |

Each operator subscription can be individually disabled or customized (channel, source, namespace) via the `spec.connected.operators` configuration.

In addition to operator subscriptions, the controller deploys the **Airgap Architect** web UI (React frontend + Node.js backend) with optional OpenShift console plugin integration, and manages all supporting infrastructure: managed PostgreSQL instances for Keycloak and RHTPA, S3 storage via ObjectBucketClaims, TLS certificates, and the cluster pull secret.

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:validation:Enum=connected;airgapped
type PlatformMode string

const (
	PlatformModeConnected PlatformMode = "connected"
	PlatformModeAirgapped PlatformMode = "airgapped"
)

// +kubebuilder:validation:Enum=scheduled;manual;event
type TriggerType string

const (
	TriggerTypeScheduled TriggerType = "scheduled"
	TriggerTypeManual    TriggerType = "manual"
	TriggerTypeEvent     TriggerType = "event"
)

type ArtifactStorageConfig struct {
	// Kubernetes StorageClass to use for artifact PVCs
	// +optional
	StorageClass string `json:"storageClass,omitempty"`
	// Storage size for collection artifacts (e.g. '500Gi')
	// +kubebuilder:default="500Gi"
	Size string `json:"size"`
}

type ConnectedConfig struct {
	// Cron schedule for automated content collection (e.g. '0 2 * * 0' for weekly Sunday 2am). Required only when triggerTypes includes 'scheduled'.
	// +optional
	CollectionSchedule string `json:"collectionSchedule,omitempty"`
	// Target mirror registry URL (auto-configured when managed Quay is enabled)
	// +optional
	MirrorRegistry string `json:"mirrorRegistry,omitempty"`
	// Storage configuration for collection artifacts
	ArtifactStorage ArtifactStorageConfig `json:"artifactStorage"`
	// Collection trigger types: scheduled (cron), manual (on-demand), event (webhook)
	// +optional
	TriggerTypes []TriggerType `json:"triggerTypes,omitempty"`
	// OLM operator subscriptions managed by this platform
	// +optional
	Operators *OperatorConfig `json:"operators,omitempty"`
	// Red Hat Trusted Artifact Signer configuration for supply chain security
	// +optional
	RHTAS *RHTASInstallerConfig `json:"rhtas,omitempty"`
	// Red Hat Trusted Profile Analyzer configuration for SBOM analysis
	// +optional
	RHTPA *RHTPAInstallerConfig `json:"rhtpa,omitempty"`
	// Quay registry configuration for intermediate image storage
	// +optional
	Quay *QuayInstallerConfig `json:"quay,omitempty"`
}

type OLMSubscriptionConfig struct {
	// Set to true to skip installation of this operator
	// +optional
	Disabled bool `json:"disabled,omitempty"`
	// OLM subscription channel
	// +optional
	Channel string `json:"channel,omitempty"`
	// OLM install plan approval strategy
	// +kubebuilder:validation:Enum=Automatic;Manual
	// +kubebuilder:default="Automatic"
	// +optional
	ApprovalStrategy string `json:"approvalStrategy,omitempty"`
	// OLM catalog source name
	// +optional
	CatalogSource string `json:"catalogSource,omitempty"`
	// OLM catalog source namespace
	// +optional
	CatalogSourceNS string `json:"catalogSourceNamespace,omitempty"`
	// Starting CSV version for the subscription
	// +optional
	StartingCSV string `json:"startingCSV,omitempty"`
}

type OperatorConfig struct {
	// OpenShift Pipelines operator subscription (provides Tekton)
	// +optional
	OpenShiftPipelines *OLMSubscriptionConfig `json:"openshiftPipelines,omitempty"`
	// Red Hat Build of Keycloak operator subscription
	// +optional
	Keycloak *OLMSubscriptionConfig `json:"keycloak,omitempty"`
	// Red Hat Trusted Artifact Signer operator subscription
	// +optional
	RHTAS *OLMSubscriptionConfig `json:"rhtas,omitempty"`
	// Red Hat Trusted Profile Analyzer operator subscription
	// +optional
	RHTPA *OLMSubscriptionConfig `json:"rhtpa,omitempty"`
	// Quay operator subscription for managed registry
	// +optional
	QuayOperator *OLMSubscriptionConfig `json:"quayOperator,omitempty"`
}

type RHTASInstallerConfig struct {
	// OIDC configuration for Trusted Artifact Signer authentication
	// +optional
	OIDC *RHTASOIDCConfig `json:"oidc,omitempty"`
	// External database configuration for RHTAS
	// +optional
	Database *RHTASDatabaseConfig `json:"database,omitempty"`
	// Reference to a Secret containing trusted root keys for signature verification
	// +optional
	TrustedRootKeys *corev1.LocalObjectReference `json:"trustedRootKeys,omitempty"`
}

type RHTASOIDCConfig struct {
	// OIDC issuer URL
	// +optional
	Issuer string `json:"issuer,omitempty"`
	// OIDC client ID
	// +optional
	ClientID string `json:"clientId,omitempty"`
	// OIDC client secret
	// +optional
	ClientSecret string `json:"clientSecret,omitempty"`
	// OIDC identity claim type
	// +optional
	Type string `json:"type,omitempty"`
	// Managed Keycloak instance for OIDC (auto-provisions Keycloak)
	// +optional
	Managed *ManagedKeycloakConfig `json:"managed,omitempty"`
}

type ManagedKeycloakConfig struct {
	// Enable managed Keycloak deployment
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// Keycloak realm name
	// +optional
	Realm string `json:"realm,omitempty"`
	// Keycloak admin username
	// +optional
	AdminUser string `json:"adminUser,omitempty"`
	// Keycloak admin password
	// +optional
	AdminPassword string `json:"adminPassword,omitempty"`
	// Reference to a pre-existing TLS secret for Keycloak
	// +optional
	TLSSecret *corev1.LocalObjectReference `json:"tlsSecret,omitempty"`
	// cert-manager issuer reference for automatic TLS certificate provisioning
	// +optional
	CertIssuer *CertIssuerReference `json:"certIssuer,omitempty"`
}

type CertIssuerReference struct {
	// Name of the cert-manager Issuer or ClusterIssuer
	Name string `json:"name"`
	// Kind of the cert-manager issuer resource
	// +kubebuilder:validation:Enum=ClusterIssuer;Issuer
	// +kubebuilder:default="ClusterIssuer"
	// +optional
	Kind string `json:"kind,omitempty"`
}

type RHTASDatabaseConfig struct {
	// Database hostname
	Host string `json:"host"`
	// Database name
	Name string `json:"name"`
	// Database port
	// +kubebuilder:validation:Minimum=1
	// +optional
	Port int32 `json:"port,omitempty"`
	// Database username
	// +optional
	Username string `json:"username,omitempty"`
	// Database password
	// +optional
	Password string `json:"password,omitempty"`
}

type RHTPAInstallerConfig struct {
	// Storage configuration for Trusted Profile Analyzer
	// +optional
	Storage *RHTPAStorageConfig `json:"storage,omitempty"`
	// External database configuration for RHTPA
	// +optional
	Database *RHTPADatabaseConfig `json:"database,omitempty"`
	// OIDC configuration for RHTPA authentication
	// +optional
	OIDC *RHTPAOIDCConfig `json:"oidc,omitempty"`
}

type QuayInstallerConfig struct {
	// Deploy and manage a Quay registry instance
	// +optional
	Managed *ManagedQuayConfig `json:"managed,omitempty"`
	// External Quay URL if not using a managed instance
	// +optional
	ExternalURL string `json:"externalURL,omitempty"`
}

type ManagedQuayConfig struct {
	// Enable managed Quay deployment
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// Quay organization name for mirrored content
	// +kubebuilder:default="mirror"
	// +optional
	OrganizationName string `json:"organizationName,omitempty"`
	// Storage configuration for Quay registry
	// +optional
	Storage *QuayStorageConfig `json:"storage,omitempty"`
	// External database configuration for Quay
	// +optional
	Database *QuayDatabaseConfig `json:"database,omitempty"`
	// Quay admin username
	// +optional
	AdminUser string `json:"adminUser,omitempty"`
	// Quay admin password
	// +optional
	AdminPassword string `json:"adminPassword,omitempty"`
	// Reference to a pre-existing TLS secret for Quay
	// +optional
	TLSSecret *corev1.LocalObjectReference `json:"tlsSecret,omitempty"`
	// cert-manager issuer reference for automatic TLS certificate provisioning
	// +optional
	CertIssuer *CertIssuerReference `json:"certIssuer,omitempty"`
	// Clair vulnerability scanner configuration
	// +optional
	Clair *ClairConfig `json:"clair,omitempty"`
}

type ClairConfig struct {
	// Configure Clair to use only Red Hat VEX data for vulnerability scanning
	// +optional
	UseRedHatVEXOnly bool `json:"useRedHatVEXOnly,omitempty"`
}

type QuayStorageConfig struct {
	// Storage backend: filesystem (PVC) or s3 (ODF BucketClaim)
	// +kubebuilder:validation:Enum=filesystem;s3
	// +kubebuilder:default="s3"
	Type string `json:"type"`
	// Storage size (for filesystem type)
	// +optional
	Size string `json:"size,omitempty"`
	// Kubernetes StorageClass (for filesystem type)
	// +optional
	StorageClass string `json:"storageClass,omitempty"`
	// S3 bucket name (auto-provisioned via ODF BucketClaim if not set)
	// +optional
	S3Bucket string `json:"s3Bucket,omitempty"`
	// S3 access key (auto-provisioned via ODF BucketClaim if not set)
	// +optional
	S3AccessKey string `json:"s3AccessKey,omitempty"`
	// S3 secret key (auto-provisioned via ODF BucketClaim if not set)
	// +optional
	S3SecretKey string `json:"s3SecretKey,omitempty"`
	// S3 endpoint URL (auto-provisioned via ODF BucketClaim if not set)
	// +optional
	S3Endpoint string `json:"s3Endpoint,omitempty"`
}

type QuayDatabaseConfig struct {
	// Database hostname
	Host string `json:"host"`
	// Database name
	Name string `json:"name"`
	// Database port
	// +kubebuilder:validation:Minimum=1
	// +optional
	Port int32 `json:"port,omitempty"`
	// Database username
	// +optional
	Username string `json:"username,omitempty"`
	// Database password
	// +optional
	Password string `json:"password,omitempty"`
}

type RHTPAStorageConfig struct {
	// Storage backend: local (PVC) or s3 (ODF BucketClaim)
	// +kubebuilder:validation:Enum=local;s3
	// +kubebuilder:default="s3"
	Type string `json:"type"`
	// Storage size (for local type)
	// +optional
	Size string `json:"size,omitempty"`
	// S3 access key (auto-provisioned via ODF BucketClaim if not set)
	// +optional
	AccessKey string `json:"accessKey,omitempty"`
	// S3 secret key (auto-provisioned via ODF BucketClaim if not set)
	// +optional
	SecretKey string `json:"secretKey,omitempty"`
	// S3 bucket name (auto-provisioned via ODF BucketClaim if not set)
	// +optional
	Bucket string `json:"bucket,omitempty"`
	// S3 region
	// +optional
	Region string `json:"region,omitempty"`
}

type RHTPADatabaseConfig struct {
	// Database hostname
	Host string `json:"host"`
	// Database name
	Name string `json:"name"`
	// Database username
	Username string `json:"username"`
	// Database password
	Password string `json:"password"`
}

type RHTPAOIDCConfig struct {
	// OIDC issuer URL for RHTPA authentication
	// +optional
	Issuer string `json:"issuer,omitempty"`
	// OIDC client ID
	// +optional
	ClientID string `json:"clientId,omitempty"`
	// OIDC client secret
	// +optional
	ClientSecret string `json:"clientSecret,omitempty"`
}

type MirrorRegistryConfig struct {
	// Storage path for Quay data
	// +kubebuilder:default="/opt/quay"
	// +optional
	DataPath string `json:"dataPath,omitempty"`
	// Quay service port
	// +kubebuilder:default=8443
	// +kubebuilder:validation:Minimum=1
	// +optional
	Port int32 `json:"port,omitempty"`
}

type AirgappedConfig struct {
	// Whether this cluster serves as the management hub
	// +optional
	ManagementCluster bool `json:"managementCluster,omitempty"`
	// Target mirror registry URL for imported content
	// +optional
	MirrorRegistry string `json:"mirrorRegistry,omitempty"`
	// Enable cluster bootstrapping from mirrored content
	// +optional
	BootstrapEnabled bool `json:"bootstrapEnabled,omitempty"`
	// Filesystem path for physical media import (e.g. /mnt/physical-media)
	// +optional
	ImportPath string `json:"importPath,omitempty"`
	// Reference to a Secret containing registry pull/push credentials
	// +optional
	RegistryCredentials *corev1.LocalObjectReference `json:"registryCredentials,omitempty"`
	// Red Hat Trusted Artifact Signer configuration for airgapped signature verification
	// +optional
	RHTAS *AirgappedRHTASConfig `json:"rhtas,omitempty"`
	// Mirror registry deployment configuration (for operator-managed registry)
	// +optional
	MirrorRegistryConfig *MirrorRegistryConfig `json:"mirrorRegistryConfig,omitempty"`
}

type AirgappedRHTASConfig struct {
	// Reference to a Secret containing trusted root keys for airgapped signature verification
	// +optional
	TrustedRootKeys *corev1.LocalObjectReference `json:"trustedRootKeys,omitempty"`
}

type TLSConfig struct {
	// Route TLS termination type
	// +kubebuilder:validation:Enum=edge;passthrough;reencrypt
	// +kubebuilder:default="edge"
	// +optional
	Termination string `json:"termination,omitempty"`
}

type RouteConfig struct {
	// Custom hostname for the OpenShift Route
	// +optional
	Host string `json:"host,omitempty"`
	// TLS configuration for the Route
	// +optional
	TLS *TLSConfig `json:"tls,omitempty"`
}

type AirgapArchitectConfig struct {
	// Deploy the Airgap Architect web UI
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// Container image for the Architect frontend
	// +optional
	FrontendImage string `json:"frontendImage,omitempty"`
	// Container image for the Architect backend API
	// +optional
	BackendImage string `json:"backendImage,omitempty"`
	// OpenShift console plugin configuration for integrated UI
	// +optional
	ConsolePlugin *ConsolePluginConfig `json:"consolePlugin,omitempty"`
	// Number of replicas for the Architect deployment
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +optional
	Replicas int32 `json:"replicas,omitempty"`
	// OpenShift Route configuration for external access
	// +optional
	Route *RouteConfig `json:"route,omitempty"`
	// Reference to a Secret containing pull credentials for Architect images
	// +optional
	PullSecret *corev1.LocalObjectReference `json:"pullSecret,omitempty"`
	// Reference to a Secret containing a GitHub token for release lookups
	// +optional
	GitHubTokenSecret *corev1.LocalObjectReference `json:"githubTokenSecret,omitempty"`
}

type ConsolePluginConfig struct {
	// Enable the OpenShift console plugin for integrated management UI
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// Container image for the console plugin
	// +optional
	Image string `json:"image,omitempty"`
}

type GitOpsConfig struct {
	// Enable GitOps integration for declarative management
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// Git repository URL for storing platform configuration
	// +optional
	RepositoryURL string `json:"repositoryURL,omitempty"`
	// Git branch to use
	// +optional
	Branch string `json:"branch,omitempty"`
	// Path within the repository for platform configuration
	// +optional
	Path string `json:"path,omitempty"`
	// Reference to a Secret containing git credentials
	// +optional
	Credentials *corev1.LocalObjectReference `json:"credentials,omitempty"`
}

// DisconnectedPlatformSpec defines the desired state of DisconnectedPlatform
type DisconnectedPlatformSpec struct {
	// Platform operating mode: connected (internet access) or airgapped (disconnected)
	// +kubebuilder:validation:Enum=connected;airgapped
	Mode PlatformMode `json:"mode"`
	// Connected mode configuration for content collection and mirroring
	// +optional
	Connected *ConnectedConfig `json:"connected,omitempty"`
	// Airgapped mode configuration for importing and serving mirrored content
	// +optional
	Airgapped *AirgappedConfig `json:"airgapped,omitempty"`
	// Airgap Architect web UI configuration
	// +optional
	Architect *AirgapArchitectConfig `json:"architect,omitempty"`
	// GitOps integration configuration
	// +optional
	GitOps *GitOpsConfig `json:"gitOps,omitempty"`
}

type CollectionInfo struct {
	// Collection version identifier
	Version string `json:"version"`
	// Timestamp of the collection
	Timestamp metav1.Time `json:"timestamp"`
	// Total size of collected artifacts
	Size string `json:"size"`
	// Collection status
	Status string `json:"status"`
}

type ImportInfo struct {
	// Import version identifier
	Version string `json:"version"`
	// Timestamp of the import
	Timestamp metav1.Time `json:"timestamp"`
	// Import status
	Status string `json:"status"`
}

type ComponentStatus struct {
	// Component name
	Name string `json:"name"`
	// Component health status
	Status string `json:"status"`
	// Component URL (if externally accessible)
	// +optional
	URL string `json:"url,omitempty"`
	// Last health check timestamp
	// +optional
	LastCheck *metav1.Time `json:"lastCheck,omitempty"`
}

type PlatformPhase string

const (
	PlatformPhaseReady      PlatformPhase = "Ready"
	PlatformPhaseCollecting PlatformPhase = "Collecting"
	PlatformPhaseImporting  PlatformPhase = "Importing"
	PlatformPhaseError      PlatformPhase = "Error"
)

type DisconnectedPlatformStatus struct {
	// Current phase of the platform lifecycle
	// +optional
	Phase PlatformPhase `json:"phase,omitempty"`
	// Standard Kubernetes conditions
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// Most recent collection information
	// +optional
	LastCollection *CollectionInfo `json:"lastCollection,omitempty"`
	// History of past collections
	// +optional
	CollectionHistory []CollectionInfo `json:"collectionHistory,omitempty"`
	// Most recent import information
	// +optional
	LastImport *ImportInfo `json:"lastImport,omitempty"`
	// History of past imports
	// +optional
	ImportHistory []ImportInfo `json:"importHistory,omitempty"`
	// Status of managed components
	// +optional
	Components []ComponentStatus `json:"components,omitempty"`
	// RHTAS root key information for signature verification
	// +optional
	RHTASRootKeys *RHTASRootKeysInfo `json:"rhtasRootKeys,omitempty"`
}

type RHTASRootKeysInfo struct {
	// ConfigMap containing the root keys
	ConfigMap string `json:"configMap"`
	// Hash of the Fulcio root certificate
	FulcioRootHash string `json:"fulcioRootHash"`
	// Hash of the Rekor signing key
	RekorKeyHash string `json:"rekorKeyHash"`
	// Timestamp of last root key update
	LastUpdated metav1.Time `json:"lastUpdated"`
	// TUF repository URL for root key distribution
	// +optional
	TUFRepositoryURL string `json:"tufRepositoryUrl,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=disconnectedplatforms,shortName=dp,scope=Cluster

// DisconnectedPlatform is the top-level orchestrator for managing disconnected OpenShift environments
type DisconnectedPlatform struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DisconnectedPlatformSpec   `json:"spec,omitempty"`
	Status DisconnectedPlatformStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DisconnectedPlatformList contains a list of DisconnectedPlatform
type DisconnectedPlatformList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DisconnectedPlatform `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DisconnectedPlatform{}, &DisconnectedPlatformList{})
}

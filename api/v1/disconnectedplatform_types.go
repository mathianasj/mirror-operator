package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type PlatformMode string

const (
	PlatformModeConnected PlatformMode = "connected"
	PlatformModeAirgapped PlatformMode = "airgapped"
)

type TriggerType string

const (
	TriggerTypeScheduled TriggerType = "scheduled"
	TriggerTypeManual    TriggerType = "manual"
	TriggerTypeEvent     TriggerType = "event"
)

type ArtifactStorageConfig struct {
	StorageClass string `json:"storageClass,omitempty"`
	Size         string `json:"size"`
}

type ConnectedConfig struct {
	CollectionSchedule string                `json:"collectionSchedule"`
	MirrorRegistry     string                `json:"mirrorRegistry,omitempty"`
	ArtifactStorage    ArtifactStorageConfig `json:"artifactStorage"`
	TriggerTypes       []TriggerType         `json:"triggerTypes,omitempty"`
	Operators          *OperatorConfig       `json:"operators,omitempty"`
	RHTAS              *RHTASInstallerConfig `json:"rhtas,omitempty"`
	RHTPA              *RHTPAInstallerConfig `json:"rhtpa,omitempty"`
	Quay               *QuayInstallerConfig  `json:"quay,omitempty"`
}

type OLMSubscriptionConfig struct {
	Disabled         bool   `json:"disabled,omitempty"`
	Channel          string `json:"channel,omitempty"`
	ApprovalStrategy string `json:"approvalStrategy,omitempty"`
	CatalogSource    string `json:"catalogSource,omitempty"`
	CatalogSourceNS  string `json:"catalogSourceNamespace,omitempty"`
	StartingCSV      string `json:"startingCSV,omitempty"`
}

type OperatorConfig struct {
	OpenShiftPipelines *OLMSubscriptionConfig `json:"openshiftPipelines,omitempty"`
	Keycloak           *OLMSubscriptionConfig `json:"keycloak,omitempty"`
	RHTAS              *OLMSubscriptionConfig `json:"rhtas,omitempty"`
	RHTPA              *OLMSubscriptionConfig `json:"rhtpa,omitempty"`
	QuayOperator       *OLMSubscriptionConfig `json:"quayOperator,omitempty"`
}

type RHTASInstallerConfig struct {
	OIDC            *RHTASOIDCConfig             `json:"oidc,omitempty"`
	Database        *RHTASDatabaseConfig         `json:"database,omitempty"`
	TrustedRootKeys *corev1.LocalObjectReference `json:"trustedRootKeys,omitempty"`
}

type RHTASOIDCConfig struct {
	Issuer       string                 `json:"issuer,omitempty"`
	ClientID     string                 `json:"clientId,omitempty"`
	ClientSecret string                 `json:"clientSecret,omitempty"`
	Type         string                 `json:"type,omitempty"`
	Managed      *ManagedKeycloakConfig `json:"managed,omitempty"`
}

type ManagedKeycloakConfig struct {
	Enabled       bool                         `json:"enabled"`
	Realm         string                       `json:"realm,omitempty"`
	AdminUser     string                       `json:"adminUser,omitempty"`
	AdminPassword string                       `json:"adminPassword,omitempty"`
	TLSSecret     *corev1.LocalObjectReference `json:"tlsSecret,omitempty"`
	CertIssuer    *CertIssuerReference         `json:"certIssuer,omitempty"`
}

type CertIssuerReference struct {
	Name string `json:"name"`
	Kind string `json:"kind,omitempty"`
}

type RHTASDatabaseConfig struct {
	Host     string `json:"host"`
	Name     string `json:"name"`
	Port     int32  `json:"port,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

type RHTPAInstallerConfig struct {
	Storage  *RHTPAStorageConfig  `json:"storage,omitempty"`
	Database *RHTPADatabaseConfig `json:"database,omitempty"`
	OIDC     *RHTPAOIDCConfig     `json:"oidc,omitempty"`
}

type QuayInstallerConfig struct {
	// Managed Quay instance for intermediate registry
	Managed *ManagedQuayConfig `json:"managed,omitempty"`
	// External Quay configuration if not using managed instance
	ExternalURL string `json:"externalURL,omitempty"`
}

type ManagedQuayConfig struct {
	Enabled          bool                         `json:"enabled"`
	OrganizationName string                       `json:"organizationName,omitempty"`
	Storage          *QuayStorageConfig           `json:"storage,omitempty"`
	Database         *QuayDatabaseConfig          `json:"database,omitempty"`
	AdminUser        string                       `json:"adminUser,omitempty"`
	AdminPassword    string                       `json:"adminPassword,omitempty"`
	TLSSecret        *corev1.LocalObjectReference `json:"tlsSecret,omitempty"`
	CertIssuer       *CertIssuerReference         `json:"certIssuer,omitempty"`
	Clair            *ClairConfig                 `json:"clair,omitempty"`
}

type ClairConfig struct {
	// UseRedHatVEXOnly configures Clair to use only Red Hat VEX data for vulnerability scanning
	UseRedHatVEXOnly bool `json:"useRedHatVEXOnly,omitempty"`
}

type QuayStorageConfig struct {
	Type         string `json:"type"` // filesystem, s3, etc.
	Size         string `json:"size,omitempty"`
	StorageClass string `json:"storageClass,omitempty"`
	// S3-specific config
	S3Bucket    string `json:"s3Bucket,omitempty"`
	S3AccessKey string `json:"s3AccessKey,omitempty"`
	S3SecretKey string `json:"s3SecretKey,omitempty"`
	S3Endpoint  string `json:"s3Endpoint,omitempty"`
}

type QuayDatabaseConfig struct {
	Host     string `json:"host"`
	Name     string `json:"name"`
	Port     int32  `json:"port,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

type RHTPAStorageConfig struct {
	Type      string `json:"type"`
	Size      string `json:"size,omitempty"`
	AccessKey string `json:"accessKey,omitempty"`
	SecretKey string `json:"secretKey,omitempty"`
	Bucket    string `json:"bucket,omitempty"`
	Region    string `json:"region,omitempty"`
}

type RHTPADatabaseConfig struct {
	Host     string `json:"host"`
	Name     string `json:"name"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type RHTPAOIDCConfig struct {
	Issuer       string `json:"issuer,omitempty"`
	ClientID     string `json:"clientId,omitempty"`
	ClientSecret string `json:"clientSecret,omitempty"`
}

type AirgappedConfig struct {
	ManagementCluster   bool                         `json:"managementCluster"`
	MirrorRegistry      string                       `json:"mirrorRegistry"`
	BootstrapEnabled    bool                         `json:"bootstrapEnabled"`
	ImportPath          string                       `json:"importPath,omitempty"`
	RegistryCredentials *corev1.LocalObjectReference `json:"registryCredentials,omitempty"`
	RHTAS               *AirgappedRHTASConfig        `json:"rhtas,omitempty"`
}

type AirgappedRHTASConfig struct {
	TrustedRootKeys *corev1.LocalObjectReference `json:"trustedRootKeys,omitempty"`
}

type TLSConfig struct {
	Termination string `json:"termination"`
}

type RouteConfig struct {
	Host string     `json:"host,omitempty"`
	TLS  *TLSConfig `json:"tls,omitempty"`
}

type AirgapArchitectConfig struct {
	Enabled           bool                         `json:"enabled"`
	FrontendImage     string                       `json:"frontendImage,omitempty"`
	BackendImage      string                       `json:"backendImage,omitempty"`
	Replicas          int32                        `json:"replicas,omitempty"`
	Route             *RouteConfig                 `json:"route,omitempty"`
	PullSecret        *corev1.LocalObjectReference `json:"pullSecret,omitempty"`
	GitHubTokenSecret *corev1.LocalObjectReference `json:"githubTokenSecret,omitempty"`
}

type GitOpsConfig struct {
	Enabled       bool                         `json:"enabled"`
	RepositoryURL string                       `json:"repositoryURL,omitempty"`
	Branch        string                       `json:"branch,omitempty"`
	Path          string                       `json:"path,omitempty"`
	Credentials   *corev1.LocalObjectReference `json:"credentials,omitempty"`
}

type DisconnectedPlatformSpec struct {
	Mode      PlatformMode           `json:"mode"`
	Connected *ConnectedConfig       `json:"connected,omitempty"`
	Airgapped *AirgappedConfig       `json:"airgapped,omitempty"`
	Architect *AirgapArchitectConfig `json:"architect,omitempty"`
	GitOps    *GitOpsConfig          `json:"gitOps,omitempty"`
}

type CollectionInfo struct {
	Version   string      `json:"version"`
	Timestamp metav1.Time `json:"timestamp"`
	Size      string      `json:"size"`
	Status    string      `json:"status"`
}

type ImportInfo struct {
	Version   string      `json:"version"`
	Timestamp metav1.Time `json:"timestamp"`
	Status    string      `json:"status"`
}

type ComponentStatus struct {
	Name      string       `json:"name"`
	Status    string       `json:"status"`
	URL       string       `json:"url,omitempty"`
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
	Phase             PlatformPhase      `json:"phase,omitempty"`
	Conditions        []metav1.Condition `json:"conditions,omitempty"`
	LastCollection    *CollectionInfo    `json:"lastCollection,omitempty"`
	CollectionHistory []CollectionInfo   `json:"collectionHistory,omitempty"`
	LastImport        *ImportInfo        `json:"lastImport,omitempty"`
	ImportHistory     []ImportInfo       `json:"importHistory,omitempty"`
	Components        []ComponentStatus  `json:"components,omitempty"`
	RHTASRootKeys     *RHTASRootKeysInfo `json:"rhtasRootKeys,omitempty"`
}

type RHTASRootKeysInfo struct {
	ConfigMap        string      `json:"configMap"`
	FulcioRootHash   string      `json:"fulcioRootHash"`
	RekorKeyHash     string      `json:"rekorKeyHash"`
	LastUpdated      metav1.Time `json:"lastUpdated"`
	TUFRepositoryURL string      `json:"tufRepositoryUrl,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=disconnectedplatforms,shortName=dp,scope=Cluster

type DisconnectedPlatform struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DisconnectedPlatformSpec   `json:"spec,omitempty"`
	Status DisconnectedPlatformStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type DisconnectedPlatformList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DisconnectedPlatform `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DisconnectedPlatform{}, &DisconnectedPlatformList{})
}

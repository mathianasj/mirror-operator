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
	MirrorRegistry     string                `json:"mirrorRegistry"`
	ArtifactStorage    ArtifactStorageConfig `json:"artifactStorage"`
	TriggerTypes       []TriggerType         `json:"triggerTypes,omitempty"`
	Operators          *OperatorConfig       `json:"operators,omitempty"`
	RHTPA              *RHTPAInstallerConfig `json:"rhtpa,omitempty"`
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
	RHTAS              *OLMSubscriptionConfig `json:"rhtas,omitempty"`
	RHTPA              *OLMSubscriptionConfig `json:"rhtpa,omitempty"`
}

type RHTPAInstallerConfig struct {
	Storage  *RHTPAStorageConfig  `json:"storage,omitempty"`
	Database *RHTPADatabaseConfig `json:"database,omitempty"`
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

type AirgappedConfig struct {
	ManagementCluster   bool                         `json:"managementCluster"`
	MirrorRegistry      string                       `json:"mirrorRegistry"`
	BootstrapEnabled    bool                         `json:"bootstrapEnabled"`
	ImportPath          string                       `json:"importPath,omitempty"`
	RegistryCredentials *corev1.LocalObjectReference `json:"registryCredentials,omitempty"`
}

type TLSConfig struct {
	Termination string `json:"termination"`
}

type RouteConfig struct {
	Host string     `json:"host,omitempty"`
	TLS  *TLSConfig `json:"tls,omitempty"`
}

type AirgapArchitectConfig struct {
	Enabled       bool                         `json:"enabled"`
	FrontendImage string                       `json:"frontendImage,omitempty"`
	BackendImage  string                       `json:"backendImage,omitempty"`
	Replicas      int32                        `json:"replicas,omitempty"`
	Route         *RouteConfig                 `json:"route,omitempty"`
	PullSecret    *corev1.LocalObjectReference `json:"pullSecret,omitempty"`
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

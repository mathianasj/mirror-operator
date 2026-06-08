package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type S3Config struct {
	Bucket    string                      `json:"bucket"`
	Region    string                      `json:"region"`
	Endpoint  string                      `json:"endpoint,omitempty"`
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
}

type BundleOutput struct {
	PVC      string    `json:"pvc,omitempty"`
	Filename string    `json:"filename,omitempty"`
	S3       *S3Config `json:"s3,omitempty"`
}

type ArtifactOutput struct {
	Output *BundleOutput `json:"output,omitempty"`
}

type CosignSigningConfig struct {
	// Static key-based signing (legacy)
	KeySecretRef      *corev1.LocalObjectReference `json:"keySecretRef,omitempty"`
	PasswordSecretRef *corev1.LocalObjectReference `json:"passwordSecretRef,omitempty"`

	// Keyless signing with Fulcio
	Keyless *KeylessSigningConfig `json:"keyless,omitempty"`
}

type KeylessSigningConfig struct {
	// Fulcio URL for certificate issuance
	FulcioURL string `json:"fulcioURL"`

	// Rekor URL for transparency log
	RekorURL string `json:"rekorURL"`

	// TUF URL for root of trust distribution
	// +optional
	TUFURL string `json:"tufURL,omitempty"`

	// OIDC issuer URL
	OIDCIssuer string `json:"oidcIssuer"`

	// OIDC client credentials
	OIDCClientID     string                       `json:"oidcClientID"`
	OIDCClientSecret *corev1.LocalObjectReference `json:"oidcClientSecret,omitempty"`
}

type CollectionPipelineSpec struct {
	ImageSetConfig string               `json:"imageSetConfig"`
	TriggerType    TriggerType          `json:"triggerType,omitempty"`
	Incremental    bool                 `json:"incremental,omitempty"`
	BaseVersion    string               `json:"baseVersion,omitempty"`
	Storage        ArtifactOutput       `json:"storage,omitempty"`
	Signing        *CosignSigningConfig `json:"signing,omitempty"`
}

type CollectionPhase string

const (
	CollectionPhasePending    CollectionPhase = "Pending"
	CollectionPhaseCollecting CollectionPhase = "Collecting"
	CollectionPhaseComplete   CollectionPhase = "Complete"
	CollectionPhaseFailed     CollectionPhase = "Failed"
)

type CollectionPipelineStatus struct {
	Conditions      []metav1.Condition `json:"conditions,omitempty"`
	Phase           string             `json:"phase,omitempty"`
	Version         string             `json:"version,omitempty"`
	PipelineRunRef  string             `json:"pipelineRunRef,omitempty"`
	ConfigMapRef    string             `json:"configMapRef,omitempty"`
	SbomUploaded    bool               `json:"sbomUploaded,omitempty"`
	SbomUploaderRef string             `json:"sbomUploaderRef,omitempty"`
	StartTime       *metav1.Time       `json:"startTime,omitempty"`
	CompletionTime  *metav1.Time       `json:"completionTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=collectionpipelines,shortName=cp

type CollectionPipeline struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CollectionPipelineSpec   `json:"spec,omitempty"`
	Status CollectionPipelineStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type CollectionPipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CollectionPipeline `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CollectionPipeline{}, &CollectionPipelineList{})
}

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type BundleSource struct {
	PVC      string `json:"pvc"`
	Filename string `json:"filename"`
}

type RegistryConfig struct {
	URL         string `json:"url"`
	CAConfigMap string `json:"caConfigMap,omitempty"`
}

type PublishConfig struct {
	CatalogSource            bool `json:"catalogSource"`
	ImageContentSourcePolicy bool `json:"imageContentSourcePolicy"`
}

type EnterpriseContractPolicy struct {
	ConfigMapRef corev1.LocalObjectReference `json:"configMapRef"`
}

type CosignVerificationConfig struct {
	PublicKeySecretRef       *corev1.LocalObjectReference `json:"publicKeySecretRef,omitempty"`
	EnterpriseContractPolicy *EnterpriseContractPolicy    `json:"enterpriseContractPolicy,omitempty"`
}

type MirrorImportSpec struct {
	ImageSetConfig    string                    `json:"imageSetConfig"`
	Bundle            BundleSource              `json:"bundle"`
	TargetRegistry    RegistryConfig            `json:"targetRegistry"`
	Publish           PublishConfig             `json:"publish"`
	CollectionVersion string                    `json:"collectionVersion,omitempty"`
	Verify            *CosignVerificationConfig `json:"verify,omitempty"`
}

type MirrorImportStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	Phase      string             `json:"phase,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=mirrorimports,shortName=mi

type MirrorImport struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MirrorImportSpec   `json:"spec,omitempty"`
	Status MirrorImportStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type MirrorImportList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MirrorImport `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MirrorImport{}, &MirrorImportList{})
}

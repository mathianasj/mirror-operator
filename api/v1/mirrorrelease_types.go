// Deprecated: Use CollectionPipeline instead.
// MirrorRelease is kept for backward compatibility with existing CRs.
// New deployments should use CollectionPipeline which provides the same
// functionality with additional fields for incremental collections,
// trigger types, and version tracking.
package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type MirrorReleaseSpec struct {
	ImageSetConfig string        `json:"imageSetConfig"`
	Output         *BundleOutput `json:"output,omitempty"`
}

type MirrorReleaseStatus struct {
	Conditions     []metav1.Condition `json:"conditions,omitempty"`
	Phase          string             `json:"phase,omitempty"`
	PipelineRunRef string             `json:"pipelineRunRef,omitempty"`
	ConfigMapRef   string             `json:"configMapRef,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=mirrorreleases,shortName=mr

type MirrorRelease struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MirrorReleaseSpec   `json:"spec,omitempty"`
	Status MirrorReleaseStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type MirrorReleaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MirrorRelease `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MirrorRelease{}, &MirrorReleaseList{})
}

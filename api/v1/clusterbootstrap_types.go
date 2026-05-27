package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type NetworkConfig struct {
	ClusterNetwork string `json:"clusterNetwork,omitempty"`
	ServiceNetwork string `json:"serviceNetwork,omitempty"`
	NetworkType    string `json:"networkType,omitempty"`
}

type NodeResources struct {
	CPU    int32 `json:"cpu,omitempty"`
	Memory int32 `json:"memory,omitempty"`
	Disk   int32 `json:"disk,omitempty"`
}

type NodePoolConfig struct {
	Replicas  int32          `json:"replicas"`
	Resources *NodeResources `json:"resources,omitempty"`
}

type OperatorInstall struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
	Channel   string `json:"channel,omitempty"`
}

type PostInstallConfig struct {
	Operators []OperatorInstall `json:"operators,omitempty"`
}

type ClusterBootstrapSpec struct {
	Version        string                       `json:"version"`
	Platform       string                       `json:"platform"`
	InstallConfig  corev1.LocalObjectReference  `json:"installConfig"`
	MirrorRegistry string                       `json:"mirrorRegistry"`
	PullSecret     corev1.LocalObjectReference  `json:"pullSecret"`
	TrustBundle    *corev1.LocalObjectReference `json:"trustBundle,omitempty"`
	Network        *NetworkConfig               `json:"network,omitempty"`
	ControlPlane   *NodePoolConfig              `json:"controlPlane,omitempty"`
	Compute        *NodePoolConfig              `json:"compute,omitempty"`
	PostInstall    *PostInstallConfig           `json:"postInstall,omitempty"`
}

type BootstrapPhase string

const (
	BootstrapPhasePending    BootstrapPhase = "Pending"
	BootstrapPhaseValidating BootstrapPhase = "Validating"
	BootstrapPhaseInstalling BootstrapPhase = "Installing"
	BootstrapPhaseComplete   BootstrapPhase = "Complete"
	BootstrapPhaseFailed     BootstrapPhase = "Failed"
)

type ClusterBootstrapStatus struct {
	Phase          BootstrapPhase               `json:"phase,omitempty"`
	Conditions     []metav1.Condition           `json:"conditions,omitempty"`
	Kubeconfig     *corev1.LocalObjectReference `json:"kubeconfig,omitempty"`
	ConsoleURL     string                       `json:"consoleUrl,omitempty"`
	StartTime      *metav1.Time                 `json:"startTime,omitempty"`
	CompletionTime *metav1.Time                 `json:"completionTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=clusterbootstraps,shortName=cb

type ClusterBootstrap struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterBootstrapSpec   `json:"spec,omitempty"`
	Status ClusterBootstrapStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type ClusterBootstrapList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterBootstrap `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClusterBootstrap{}, &ClusterBootstrapList{})
}

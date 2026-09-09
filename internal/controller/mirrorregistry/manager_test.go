package mirrorregistry

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

func newFakeClient(objs ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = mirrorv1.AddToScheme(scheme)
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

func newPlatform() *mirrorv1.DisconnectedPlatform {
	return &mirrorv1.DisconnectedPlatform{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-platform",
			Namespace: "mirror-operator-system",
		},
		Spec: mirrorv1.DisconnectedPlatformSpec{
			Mode: mirrorv1.PlatformModeAirgapped,
			Airgapped: &mirrorv1.AirgappedConfig{
				MirrorRegistry: "registry.example.com:5000",
			},
		},
	}
}

func newMasterNode(name, ip string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{masterPoolLabel: ""},
		},
		Status: corev1.NodeStatus{
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: ip},
			},
		},
	}
}

func TestGetMasterNodes(t *testing.T) {
	master1 := newMasterNode("master-0", "10.0.0.1")
	master2 := newMasterNode("master-1", "10.0.0.2")
	worker := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "worker-0",
			Labels: map[string]string{"node-role.kubernetes.io/worker": ""},
		},
	}

	c := newFakeClient(master1, master2, worker)
	mgr := &Manager{Client: c, Scheme: c.Scheme()}

	nodes, err := mgr.getMasterNodes(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 master nodes, got %d", len(nodes))
	}
}

func TestGetNodeInternalAddress(t *testing.T) {
	tests := []struct {
		name     string
		node     corev1.Node
		expected string
	}{
		{
			name: "internal IP",
			node: corev1.Node{
				Status: corev1.NodeStatus{
					Addresses: []corev1.NodeAddress{
						{Type: corev1.NodeInternalIP, Address: "10.0.0.1"},
						{Type: corev1.NodeExternalIP, Address: "1.2.3.4"},
					},
				},
			},
			expected: "10.0.0.1",
		},
		{
			name: "internal DNS fallback",
			node: corev1.Node{
				Status: corev1.NodeStatus{
					Addresses: []corev1.NodeAddress{
						{Type: corev1.NodeInternalDNS, Address: "master-0.internal"},
					},
				},
			},
			expected: "master-0.internal",
		},
		{
			name: "no internal address",
			node: corev1.Node{
				Status: corev1.NodeStatus{
					Addresses: []corev1.NodeAddress{
						{Type: corev1.NodeExternalIP, Address: "1.2.3.4"},
					},
				},
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getNodeInternalAddress(tt.node)
			if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestBuildSystemdUnit(t *testing.T) {
	unit := buildSystemdUnit("/opt/mirror-registry", 5000, "docker.io/library/registry:2")
	if unit == "" {
		t.Fatal("expected non-empty systemd unit")
	}
	if !contains(unit, "Before=kubelet.service crio.service") {
		t.Error("systemd unit should start before kubelet and crio")
	}
	if !contains(unit, "/opt/mirror-registry:/var/lib/registry:z") {
		t.Error("systemd unit should mount data path")
	}
	if !contains(unit, "REGISTRY_HTTP_ADDR=0.0.0.0:5000") {
		t.Error("systemd unit should set registry port")
	}
	if !contains(unit, "docker.io/library/registry:2") {
		t.Error("systemd unit should use specified image")
	}
}

func TestReconcileNoMasterNodes(t *testing.T) {
	c := newFakeClient()
	mgr := &Manager{Client: c, Scheme: c.Scheme()}
	platform := newPlatform()

	err := mgr.Reconcile(context.Background(), platform)
	if err == nil {
		t.Fatal("expected error when no master nodes exist")
	}
	if !contains(err.Error(), "no master nodes found") {
		t.Errorf("expected 'no master nodes found' error, got: %v", err)
	}
}

func TestReconcileCreatesMachineConfig(t *testing.T) {
	master1 := newMasterNode("master-0", "10.0.0.1")
	master2 := newMasterNode("master-1", "10.0.0.2")
	c := newFakeClient(master1, master2)
	mgr := &Manager{Client: c, Scheme: c.Scheme()}
	platform := newPlatform()

	err := mgr.ensureMachineConfig(context.Background(), platform, "/opt/mirror-registry", 5000, "docker.io/library/registry:2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mc := &unstructured.Unstructured{}
	mc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "machineconfiguration.openshift.io",
		Version: "v1",
		Kind:    "MachineConfig",
	})
	err = c.Get(context.Background(), client.ObjectKey{Name: machineConfigName}, mc)
	if err != nil {
		t.Fatalf("MachineConfig not created: %v", err)
	}

	labels := mc.GetLabels()
	if labels["machineconfiguration.openshift.io/role"] != "master" {
		t.Error("MachineConfig should target master pool")
	}
}

func TestReconcileDefaultConfig(t *testing.T) {
	master1 := newMasterNode("master-0", "10.0.0.1")
	master2 := newMasterNode("master-1", "10.0.0.2")
	c := newFakeClient(master1, master2)
	mgr := &Manager{Client: c, Scheme: c.Scheme()}

	platform := newPlatform()
	platform.Spec.Airgapped.MirrorRegistryConfig = nil

	err := mgr.Reconcile(context.Background(), platform)
	if err != nil {
		t.Fatalf("unexpected error with nil config: %v", err)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

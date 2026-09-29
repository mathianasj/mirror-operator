package mirrorregistry

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

func TestGenerateCA(t *testing.T) {
	certPEM, keyPEM, err := generateCA()
	if err != nil {
		t.Fatalf("generateCA() error: %v", err)
	}

	if !strings.Contains(certPEM, "BEGIN CERTIFICATE") {
		t.Error("expected PEM-encoded certificate")
	}
	if !strings.Contains(keyPEM, "BEGIN EC PRIVATE KEY") {
		t.Error("expected PEM-encoded EC private key")
	}

	block, _ := pem.Decode([]byte(certPEM))
	if block == nil {
		t.Fatal("failed to decode cert PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("failed to parse certificate: %v", err)
	}

	if !cert.IsCA {
		t.Error("expected CA certificate")
	}
	if cert.Subject.CommonName != "Mirror Registry CA" {
		t.Errorf("expected CN 'Mirror Registry CA', got %q", cert.Subject.CommonName)
	}
	if cert.MaxPathLen != 0 {
		t.Errorf("expected MaxPathLen 0, got %d", cert.MaxPathLen)
	}
}

func TestIsNodeReady(t *testing.T) {
	tests := []struct {
		name     string
		node     corev1.Node
		expected bool
	}{
		{
			name: "ready node",
			node: corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			},
			expected: true,
		},
		{
			name: "not ready node",
			node: corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionFalse},
					},
				},
			},
			expected: false,
		},
		{
			name: "unknown ready status",
			node: corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionUnknown},
					},
				},
			},
			expected: false,
		},
		{
			name: "no ready condition",
			node: corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse},
					},
				},
			},
			expected: false,
		},
		{
			name:     "no conditions at all",
			node:     corev1.Node{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isNodeReady(tt.node); got != tt.expected {
				t.Errorf("isNodeReady() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestGetNodeInternalAddress(t *testing.T) {
	tests := []struct {
		name     string
		node     corev1.Node
		expected string
	}{
		{
			name: "has internal IP",
			node: corev1.Node{
				Status: corev1.NodeStatus{
					Addresses: []corev1.NodeAddress{
						{Type: corev1.NodeExternalIP, Address: "203.0.113.1"},
						{Type: corev1.NodeInternalIP, Address: "10.0.0.5"},
					},
				},
			},
			expected: "10.0.0.5",
		},
		{
			name: "only internal DNS",
			node: corev1.Node{
				Status: corev1.NodeStatus{
					Addresses: []corev1.NodeAddress{
						{Type: corev1.NodeInternalDNS, Address: "node1.internal"},
					},
				},
			},
			expected: "node1.internal",
		},
		{
			name: "prefers IP over DNS",
			node: corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node1"},
				Status: corev1.NodeStatus{
					Addresses: []corev1.NodeAddress{
						{Type: corev1.NodeInternalDNS, Address: "node1.internal"},
						{Type: corev1.NodeInternalIP, Address: "10.0.0.5"},
					},
				},
			},
			expected: "10.0.0.5",
		},
		{
			name:     "no addresses",
			node:     corev1.Node{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getNodeInternalAddress(tt.node); got != tt.expected {
				t.Errorf("getNodeInternalAddress() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = mirrorv1.AddToScheme(s)
	return s
}

func TestGetRegistryAddress(t *testing.T) {
	masterNode := func(name, ip string) *corev1.Node {
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   name,
				Labels: map[string]string{"node-role.kubernetes.io/master": ""},
			},
			Status: corev1.NodeStatus{
				Addresses: []corev1.NodeAddress{
					{Type: corev1.NodeInternalIP, Address: ip},
				},
			},
		}
	}

	tests := []struct {
		name       string
		nodes      []*corev1.Node
		config     *mirrorv1.MirrorRegistryConfig
		wantAddr   string
		wantErrMsg string
	}{
		{
			name:     "default port with single master",
			nodes:    []*corev1.Node{masterNode("master-0", "10.0.0.5")},
			config:   &mirrorv1.MirrorRegistryConfig{},
			wantAddr: "10.0.0.5:8443",
		},
		{
			name:     "custom port",
			nodes:    []*corev1.Node{masterNode("master-0", "10.0.0.5")},
			config:   &mirrorv1.MirrorRegistryConfig{Port: 9443},
			wantAddr: "10.0.0.5:9443",
		},
		{
			name:     "multiple masters returns first",
			nodes:    []*corev1.Node{masterNode("master-0", "10.0.0.5"), masterNode("master-1", "10.0.0.6")},
			config:   &mirrorv1.MirrorRegistryConfig{},
			wantAddr: "10.0.0.5:8443",
		},
		{
			name:       "no master nodes",
			nodes:      nil,
			config:     &mirrorv1.MirrorRegistryConfig{},
			wantErrMsg: "no master nodes found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := newTestScheme()
			var objs []runtime.Object
			for _, n := range tt.nodes {
				objs = append(objs, n)
			}
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()

			platform := &mirrorv1.DisconnectedPlatform{
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistryConfig: tt.config,
					},
				},
			}

			mgr := &Manager{Client: fakeClient, Scheme: scheme}
			got, err := mgr.GetRegistryAddress(context.Background(), platform)

			if tt.wantErrMsg != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErrMsg)
				}
				if !strings.Contains(err.Error(), tt.wantErrMsg) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.wantErrMsg)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.wantAddr {
				t.Errorf("GetRegistryAddress() = %q, want %q", got, tt.wantAddr)
			}
		})
	}
}

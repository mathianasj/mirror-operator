package mirrorregistry

import (
	"crypto/x509"
	"encoding/pem"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

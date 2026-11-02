package mirrorregistry

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

func testPlatform(config *mirrorv1.MirrorRegistryConfig) *mirrorv1.DisconnectedPlatform {
	return &mirrorv1.DisconnectedPlatform{
		ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
		Spec: mirrorv1.DisconnectedPlatformSpec{
			Mode: mirrorv1.PlatformModeAirgapped,
			Airgapped: &mirrorv1.AirgappedConfig{
				MirrorRegistryConfig: config,
			},
		},
	}
}

func testMasterNode(name, ip string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{masterPoolLabel: ""},
		},
		Status: corev1.NodeStatus{
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: ip},
			},
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			},
		},
	}
}

func TestResolveConfig(t *testing.T) {
	tests := []struct {
		name      string
		spec      *mirrorv1.MirrorRegistryConfig
		wantPort  int32
		wantPath  string
		wantQuay  string
		wantRedis string
		wantPause string
	}{
		{
			name:      "all defaults when spec is nil",
			spec:      nil,
			wantPort:  8443,
			wantPath:  "/opt/quay",
			wantQuay:  defaultQuayImage,
			wantRedis: defaultRedisImage,
			wantPause: defaultPauseImage,
		},
		{
			name:      "all defaults when spec is empty",
			spec:      &mirrorv1.MirrorRegistryConfig{},
			wantPort:  8443,
			wantPath:  "/opt/quay",
			wantQuay:  defaultQuayImage,
			wantRedis: defaultRedisImage,
			wantPause: defaultPauseImage,
		},
		{
			name: "custom port and path",
			spec: &mirrorv1.MirrorRegistryConfig{
				Port:     9443,
				DataPath: "/data/registry",
			},
			wantPort:  9443,
			wantPath:  "/data/registry",
			wantQuay:  defaultQuayImage,
			wantRedis: defaultRedisImage,
			wantPause: defaultPauseImage,
		},
		{
			name: "custom images",
			spec: &mirrorv1.MirrorRegistryConfig{
				QuayImage:  "my-quay:latest",
				RedisImage: "my-redis:latest",
				PauseImage: "my-pause:latest",
			},
			wantPort:  8443,
			wantPath:  "/opt/quay",
			wantQuay:  "my-quay:latest",
			wantRedis: "my-redis:latest",
			wantPause: "my-pause:latest",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := newTestScheme()
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
			m := &Manager{Client: fakeClient, Scheme: scheme}
			platform := testPlatform(tt.spec)

			cfg, err := m.resolveConfig(context.Background(), platform)
			if err != nil {
				t.Fatalf("resolveConfig() error: %v", err)
			}
			if cfg.Port != tt.wantPort {
				t.Errorf("Port = %d, want %d", cfg.Port, tt.wantPort)
			}
			if cfg.DataPath != tt.wantPath {
				t.Errorf("DataPath = %q, want %q", cfg.DataPath, tt.wantPath)
			}
			if cfg.QuayImage != tt.wantQuay {
				t.Errorf("QuayImage = %q, want %q", cfg.QuayImage, tt.wantQuay)
			}
			if cfg.RedisImage != tt.wantRedis {
				t.Errorf("RedisImage = %q, want %q", cfg.RedisImage, tt.wantRedis)
			}
			if cfg.PauseImage != tt.wantPause {
				t.Errorf("PauseImage = %q, want %q", cfg.PauseImage, tt.wantPause)
			}
		})
	}
}

func TestResolvePullSecret(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme()

	t.Run("uses explicit imagePullSecret from spec", func(t *testing.T) {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "my-pull-secret", Namespace: "test-ns"},
			Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		spec := &mirrorv1.MirrorRegistryConfig{
			ImagePullSecret: &corev1.LocalObjectReference{Name: "my-pull-secret"},
		}
		platform := &mirrorv1.DisconnectedPlatform{
			ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "test-ns"},
			Spec: mirrorv1.DisconnectedPlatformSpec{
				Mode:      mirrorv1.PlatformModeAirgapped,
				Airgapped: &mirrorv1.AirgappedConfig{MirrorRegistryConfig: spec},
			},
		}

		got := m.resolvePullSecret(ctx, platform, spec)
		if got != `{"auths":{}}` {
			t.Errorf("expected explicit pull secret, got %q", got)
		}
	})

	t.Run("uses auth.json key from explicit secret", func(t *testing.T) {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "auth-secret", Namespace: "test-ns"},
			Data:       map[string][]byte{"auth.json": []byte(`{"auths":{"reg":{}}}`)},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		spec := &mirrorv1.MirrorRegistryConfig{
			ImagePullSecret: &corev1.LocalObjectReference{Name: "auth-secret"},
		}
		platform := &mirrorv1.DisconnectedPlatform{
			ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "test-ns"},
			Spec: mirrorv1.DisconnectedPlatformSpec{
				Mode:      mirrorv1.PlatformModeAirgapped,
				Airgapped: &mirrorv1.AirgappedConfig{MirrorRegistryConfig: spec},
			},
		}

		got := m.resolvePullSecret(ctx, platform, spec)
		if got != `{"auths":{"reg":{}}}` {
			t.Errorf("expected auth.json data, got %q", got)
		}
	})

	t.Run("falls back to cluster pull-secret", func(t *testing.T) {
		clusterSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "openshift-config"},
			Data:       map[string][]byte{".dockerconfigjson": []byte(`{"cluster":"auth"}`)},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(clusterSecret).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		spec := &mirrorv1.MirrorRegistryConfig{}
		platform := testPlatform(spec)

		got := m.resolvePullSecret(ctx, platform, spec)
		if got != `{"cluster":"auth"}` {
			t.Errorf("expected cluster pull-secret, got %q", got)
		}
	})

	t.Run("returns empty when no secrets found", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		spec := &mirrorv1.MirrorRegistryConfig{}
		platform := testPlatform(spec)

		got := m.resolvePullSecret(ctx, platform, spec)
		if got != "" {
			t.Errorf("expected empty string, got %q", got)
		}
	})

	t.Run("falls back to cluster when explicit secret not found", func(t *testing.T) {
		clusterSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "openshift-config"},
			Data:       map[string][]byte{".dockerconfigjson": []byte(`{"fallback":"yes"}`)},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(clusterSecret).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		spec := &mirrorv1.MirrorRegistryConfig{
			ImagePullSecret: &corev1.LocalObjectReference{Name: "nonexistent"},
		}
		platform := &mirrorv1.DisconnectedPlatform{
			ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "test-ns"},
			Spec: mirrorv1.DisconnectedPlatformSpec{
				Mode:      mirrorv1.PlatformModeAirgapped,
				Airgapped: &mirrorv1.AirgappedConfig{MirrorRegistryConfig: spec},
			},
		}

		got := m.resolvePullSecret(ctx, platform, spec)
		if got != `{"fallback":"yes"}` {
			t.Errorf("expected fallback to cluster pull-secret, got %q", got)
		}
	})
}

func TestGetMasterNodes(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme()

	t.Run("returns master nodes", func(t *testing.T) {
		n1 := testMasterNode("master-0", "10.0.0.1")
		n2 := testMasterNode("master-1", "10.0.0.2")
		worker := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "worker-0",
				Labels: map[string]string{"node-role.kubernetes.io/worker": ""},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(n1, n2, worker).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		nodes, err := m.getMasterNodes(ctx)
		if err != nil {
			t.Fatalf("getMasterNodes() error: %v", err)
		}
		if len(nodes) != 2 {
			t.Errorf("expected 2 master nodes, got %d", len(nodes))
		}
	})

	t.Run("returns empty when no masters", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		nodes, err := m.getMasterNodes(ctx)
		if err != nil {
			t.Fatalf("getMasterNodes() error: %v", err)
		}
		if len(nodes) != 0 {
			t.Errorf("expected 0 nodes, got %d", len(nodes))
		}
	})
}

func TestEnsureCA(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme()

	t.Run("creates CA secret when missing", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}
		cfg := &Config{}

		if err := m.ensureCA(ctx, cfg); err != nil {
			t.Fatalf("ensureCA() error: %v", err)
		}

		if cfg.CACertPEM == "" {
			t.Error("expected CACertPEM to be populated")
		}
		if cfg.CAKeyPEM == "" {
			t.Error("expected CAKeyPEM to be populated")
		}
		if !strings.Contains(cfg.CACertPEM, "BEGIN CERTIFICATE") {
			t.Error("CACertPEM should contain PEM certificate")
		}

		secret := &corev1.Secret{}
		if err := fakeClient.Get(ctx, client.ObjectKey{Name: CASecretName, Namespace: operatorNamespace}, secret); err != nil {
			t.Fatalf("expected CA secret to be created: %v", err)
		}
		if string(secret.Data["ca.crt"]) != cfg.CACertPEM {
			t.Error("secret ca.crt doesn't match config")
		}
	})

	t.Run("reads existing CA secret", func(t *testing.T) {
		existingSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: CASecretName, Namespace: operatorNamespace},
			Data: map[string][]byte{
				"ca.crt": []byte("existing-cert"),
				"ca.key": []byte("existing-key"),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existingSecret).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}
		cfg := &Config{}

		if err := m.ensureCA(ctx, cfg); err != nil {
			t.Fatalf("ensureCA() error: %v", err)
		}

		if cfg.CACertPEM != "existing-cert" {
			t.Errorf("expected existing cert, got %q", cfg.CACertPEM)
		}
		if cfg.CAKeyPEM != "existing-key" {
			t.Errorf("expected existing key, got %q", cfg.CAKeyPEM)
		}
	})
}

func TestEnsureTrustConfigMap(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme()

	t.Run("creates ConfigMap when missing", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		nodes := []corev1.Node{*testMasterNode("master-0", "10.0.0.5")}
		cfg := &Config{Port: 8443, CACertPEM: "test-ca-cert"}

		if err := m.ensureTrustConfigMap(ctx, nodes, cfg); err != nil {
			t.Fatalf("ensureTrustConfigMap() error: %v", err)
		}

		cm := &corev1.ConfigMap{}
		if err := fakeClient.Get(ctx, client.ObjectKey{Name: TrustConfigMapName, Namespace: "openshift-config"}, cm); err != nil {
			t.Fatalf("expected ConfigMap to be created: %v", err)
		}
		key := "10.0.0.5..8443"
		if cm.Data[key] != "test-ca-cert" {
			t.Errorf("expected cert for %q, got %q", key, cm.Data[key])
		}
	})

	t.Run("updates existing ConfigMap", func(t *testing.T) {
		existing := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: TrustConfigMapName, Namespace: "openshift-config"},
			Data:       map[string]string{"old-key": "old-cert"},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		nodes := []corev1.Node{*testMasterNode("master-0", "10.0.0.5")}
		cfg := &Config{Port: 9443, CACertPEM: "new-ca-cert"}

		if err := m.ensureTrustConfigMap(ctx, nodes, cfg); err != nil {
			t.Fatalf("ensureTrustConfigMap() error: %v", err)
		}

		cm := &corev1.ConfigMap{}
		if err := fakeClient.Get(ctx, client.ObjectKey{Name: TrustConfigMapName, Namespace: "openshift-config"}, cm); err != nil {
			t.Fatal(err)
		}
		if _, ok := cm.Data["old-key"]; ok {
			t.Error("expected old key to be replaced")
		}
		key := "10.0.0.5..9443"
		if cm.Data[key] != "new-ca-cert" {
			t.Errorf("expected new cert for %q", key)
		}
	})

	t.Run("handles multiple nodes", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		nodes := []corev1.Node{
			*testMasterNode("master-0", "10.0.0.5"),
			*testMasterNode("master-1", "10.0.0.6"),
		}
		cfg := &Config{Port: 8443, CACertPEM: "ca-cert"}

		if err := m.ensureTrustConfigMap(ctx, nodes, cfg); err != nil {
			t.Fatalf("error: %v", err)
		}

		cm := &corev1.ConfigMap{}
		fakeClient.Get(ctx, client.ObjectKey{Name: TrustConfigMapName, Namespace: "openshift-config"}, cm)
		if len(cm.Data) != 2 {
			t.Errorf("expected 2 entries, got %d", len(cm.Data))
		}
	})

	t.Run("skips nodes without addresses", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		noAddr := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "no-addr"}}
		nodes := []corev1.Node{*testMasterNode("master-0", "10.0.0.5"), noAddr}
		cfg := &Config{Port: 8443, CACertPEM: "ca-cert"}

		if err := m.ensureTrustConfigMap(ctx, nodes, cfg); err != nil {
			t.Fatal(err)
		}

		cm := &corev1.ConfigMap{}
		fakeClient.Get(ctx, client.ObjectKey{Name: TrustConfigMapName, Namespace: "openshift-config"}, cm)
		if len(cm.Data) != 1 {
			t.Errorf("expected 1 entry (skipping node without address), got %d", len(cm.Data))
		}
	})
}

func TestEnsureImageConfig(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme()

	t.Run("updates image config when trust not set", func(t *testing.T) {
		imageConfig := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "config.openshift.io/v1",
				"kind":       "Image",
				"metadata":   map[string]interface{}{"name": "cluster"},
				"spec":       map[string]interface{}{},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(imageConfig).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		if err := m.ensureImageConfig(ctx); err != nil {
			t.Fatalf("ensureImageConfig() error: %v", err)
		}

		updated := &unstructured.Unstructured{}
		updated.SetGroupVersionKind(schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "Image"})
		if err := fakeClient.Get(ctx, client.ObjectKey{Name: "cluster"}, updated); err != nil {
			t.Fatal(err)
		}
		name, _, _ := unstructured.NestedString(updated.Object, "spec", "additionalTrustedCA", "name")
		if name != TrustConfigMapName {
			t.Errorf("expected trust ConfigMap name %q, got %q", TrustConfigMapName, name)
		}
	})

	t.Run("no-op when trust already set", func(t *testing.T) {
		imageConfig := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "config.openshift.io/v1",
				"kind":       "Image",
				"metadata":   map[string]interface{}{"name": "cluster"},
				"spec": map[string]interface{}{
					"additionalTrustedCA": map[string]interface{}{
						"name": TrustConfigMapName,
					},
				},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(imageConfig).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		if err := m.ensureImageConfig(ctx); err != nil {
			t.Fatalf("ensureImageConfig() error: %v", err)
		}
	})
}

func TestEnsureMachineConfig(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme()

	t.Run("creates MachineConfig when missing", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}
		cfg := &Config{
			DataPath:   "/opt/quay",
			Port:       8443,
			QuayImage:  defaultQuayImage,
			RedisImage: defaultRedisImage,
			PauseImage: defaultPauseImage,
			CACertPEM:  "test-cert",
			CAKeyPEM:   "test-key",
		}

		if err := m.ensureMachineConfig(ctx, cfg); err != nil {
			t.Fatalf("ensureMachineConfig() error: %v", err)
		}

		mc := &unstructured.Unstructured{}
		mc.SetGroupVersionKind(schema.GroupVersionKind{
			Group: "machineconfiguration.openshift.io", Version: "v1", Kind: "MachineConfig",
		})
		if err := fakeClient.Get(ctx, client.ObjectKey{Name: MachineConfigName}, mc); err != nil {
			t.Fatalf("expected MachineConfig to be created: %v", err)
		}

		labels := mc.GetLabels()
		if labels["machineconfiguration.openshift.io/role"] != "master" {
			t.Error("expected master role label")
		}

		version, _, _ := unstructured.NestedString(mc.Object, "spec", "config", "ignition", "version")
		if version != "3.2.0" {
			t.Errorf("expected ignition version 3.2.0, got %q", version)
		}
	})

	t.Run("updates existing MachineConfig", func(t *testing.T) {
		existing := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "machineconfiguration.openshift.io/v1",
				"kind":       "MachineConfig",
				"metadata":   map[string]interface{}{"name": MachineConfigName},
				"spec":       map[string]interface{}{"old": "data"},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}
		cfg := &Config{
			DataPath:   "/opt/quay",
			Port:       8443,
			QuayImage:  defaultQuayImage,
			RedisImage: defaultRedisImage,
			PauseImage: defaultPauseImage,
			CACertPEM:  "test-cert",
			CAKeyPEM:   "test-key",
		}

		if err := m.ensureMachineConfig(ctx, cfg); err != nil {
			t.Fatalf("ensureMachineConfig() error: %v", err)
		}

		mc := &unstructured.Unstructured{}
		mc.SetGroupVersionKind(schema.GroupVersionKind{
			Group: "machineconfiguration.openshift.io", Version: "v1", Kind: "MachineConfig",
		})
		fakeClient.Get(ctx, client.ObjectKey{Name: MachineConfigName}, mc)

		version, _, _ := unstructured.NestedString(mc.Object, "spec", "config", "ignition", "version")
		if version != "3.2.0" {
			t.Errorf("expected updated ignition config, got version %q", version)
		}
	})
}

func TestEnsureIDMS(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme()

	t.Run("creates IDMS with ready nodes", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		nodes := []corev1.Node{*testMasterNode("master-0", "10.0.0.5")}
		cfg := &Config{Port: 8443}

		if err := m.ensureIDMS(ctx, nodes, cfg); err != nil {
			t.Fatalf("ensureIDMS() error: %v", err)
		}

		idms := &unstructured.Unstructured{}
		idms.SetGroupVersionKind(schema.GroupVersionKind{
			Group: "config.openshift.io", Version: "v1", Kind: "ImageDigestMirrorSet",
		})
		if err := fakeClient.Get(ctx, client.ObjectKey{Name: IDMSName}, idms); err != nil {
			t.Fatalf("expected IDMS to be created: %v", err)
		}

		mirrors, _, _ := unstructured.NestedSlice(idms.Object, "spec", "imageDigestMirrors")
		if len(mirrors) != 1 {
			t.Fatalf("expected 1 mirror entry, got %d", len(mirrors))
		}
		entry := mirrors[0].(map[string]interface{})
		source := entry["source"].(string)
		if source != "10.0.0.5:8443" {
			t.Errorf("expected source 10.0.0.5:8443, got %q", source)
		}
	})

	t.Run("excludes not-ready nodes from mirrors", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		readyNode := *testMasterNode("master-0", "10.0.0.5")
		notReadyNode := corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "master-1",
				Labels: map[string]string{masterPoolLabel: ""},
			},
			Status: corev1.NodeStatus{
				Addresses: []corev1.NodeAddress{
					{Type: corev1.NodeInternalIP, Address: "10.0.0.6"},
				},
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionFalse},
				},
			},
		}
		nodes := []corev1.Node{readyNode, notReadyNode}
		cfg := &Config{Port: 8443}

		if err := m.ensureIDMS(ctx, nodes, cfg); err != nil {
			t.Fatalf("ensureIDMS() error: %v", err)
		}

		idms := &unstructured.Unstructured{}
		idms.SetGroupVersionKind(schema.GroupVersionKind{
			Group: "config.openshift.io", Version: "v1", Kind: "ImageDigestMirrorSet",
		})
		fakeClient.Get(ctx, client.ObjectKey{Name: IDMSName}, idms)

		mirrors, _, _ := unstructured.NestedSlice(idms.Object, "spec", "imageDigestMirrors")
		entry := mirrors[0].(map[string]interface{})
		mirrorList := entry["mirrors"].([]interface{})
		if len(mirrorList) != 1 {
			t.Errorf("expected 1 ready mirror, got %d", len(mirrorList))
		}
		if mirrorList[0].(string) != "10.0.0.5:8443" {
			t.Errorf("expected ready node mirror, got %v", mirrorList[0])
		}
	})

	t.Run("updates existing IDMS", func(t *testing.T) {
		existing := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "config.openshift.io/v1",
				"kind":       "ImageDigestMirrorSet",
				"metadata":   map[string]interface{}{"name": IDMSName},
				"spec":       map[string]interface{}{"old": "data"},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		nodes := []corev1.Node{*testMasterNode("master-0", "10.0.0.5")}
		cfg := &Config{Port: 8443}

		if err := m.ensureIDMS(ctx, nodes, cfg); err != nil {
			t.Fatalf("ensureIDMS() error: %v", err)
		}
	})
}

func TestReconcile(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme()

	t.Run("succeeds with master nodes and CA", func(t *testing.T) {
		node := testMasterNode("master-0", "10.0.0.5")
		imageConfig := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "config.openshift.io/v1",
				"kind":       "Image",
				"metadata":   map[string]interface{}{"name": "cluster"},
				"spec":       map[string]interface{}{},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node, imageConfig).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		platform := testPlatform(&mirrorv1.MirrorRegistryConfig{})
		err := m.Reconcile(ctx, platform)
		if err != nil {
			t.Fatalf("Reconcile() error: %v", err)
		}

		// Verify CA secret was created
		secret := &corev1.Secret{}
		if err := fakeClient.Get(ctx, client.ObjectKey{Name: CASecretName, Namespace: operatorNamespace}, secret); err != nil {
			t.Error("expected CA secret to be created")
		}

		// Verify trust ConfigMap was created
		cm := &corev1.ConfigMap{}
		if err := fakeClient.Get(ctx, client.ObjectKey{Name: TrustConfigMapName, Namespace: "openshift-config"}, cm); err != nil {
			t.Error("expected trust ConfigMap to be created")
		}

		// Verify MachineConfig was created
		mc := &unstructured.Unstructured{}
		mc.SetGroupVersionKind(schema.GroupVersionKind{
			Group: "machineconfiguration.openshift.io", Version: "v1", Kind: "MachineConfig",
		})
		if err := fakeClient.Get(ctx, client.ObjectKey{Name: MachineConfigName}, mc); err != nil {
			t.Error("expected MachineConfig to be created")
		}

		// Verify IDMS was created
		idms := &unstructured.Unstructured{}
		idms.SetGroupVersionKind(schema.GroupVersionKind{
			Group: "config.openshift.io", Version: "v1", Kind: "ImageDigestMirrorSet",
		})
		if err := fakeClient.Get(ctx, client.ObjectKey{Name: IDMSName}, idms); err != nil {
			t.Error("expected IDMS to be created")
		}
	})

	t.Run("fails when no master nodes", func(t *testing.T) {
		imageConfig := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "config.openshift.io/v1",
				"kind":       "Image",
				"metadata":   map[string]interface{}{"name": "cluster"},
				"spec":       map[string]interface{}{},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(imageConfig).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		platform := testPlatform(&mirrorv1.MirrorRegistryConfig{})
		err := m.Reconcile(ctx, platform)
		if err == nil {
			t.Fatal("expected error when no master nodes")
		}
		if !strings.Contains(err.Error(), "no master nodes") {
			t.Errorf("expected 'no master nodes' error, got: %v", err)
		}
	})

	t.Run("idempotent on second call", func(t *testing.T) {
		node := testMasterNode("master-0", "10.0.0.5")
		imageConfig := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "config.openshift.io/v1",
				"kind":       "Image",
				"metadata":   map[string]interface{}{"name": "cluster"},
				"spec":       map[string]interface{}{},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node, imageConfig).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		platform := testPlatform(&mirrorv1.MirrorRegistryConfig{})
		if err := m.Reconcile(ctx, platform); err != nil {
			t.Fatalf("first Reconcile() error: %v", err)
		}
		if err := m.Reconcile(ctx, platform); err != nil {
			t.Fatalf("second Reconcile() should be idempotent: %v", err)
		}
	})

	t.Run("uses existing CA on second call", func(t *testing.T) {
		node := testMasterNode("master-0", "10.0.0.5")
		imageConfig := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "config.openshift.io/v1",
				"kind":       "Image",
				"metadata":   map[string]interface{}{"name": "cluster"},
				"spec":       map[string]interface{}{},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node, imageConfig).Build()
		m := &Manager{Client: fakeClient, Scheme: scheme}

		platform := testPlatform(&mirrorv1.MirrorRegistryConfig{})
		m.Reconcile(ctx, platform)

		// Read the CA that was generated
		secret := &corev1.Secret{}
		fakeClient.Get(ctx, client.ObjectKey{Name: CASecretName, Namespace: operatorNamespace}, secret)
		originalCert := string(secret.Data["ca.crt"])

		// Second reconcile should reuse the same CA
		m.Reconcile(ctx, platform)
		fakeClient.Get(ctx, client.ObjectKey{Name: CASecretName, Namespace: operatorNamespace}, secret)
		if string(secret.Data["ca.crt"]) != originalCert {
			t.Error("expected CA to be reused on second reconcile")
		}
	})
}

// Suppress unused import warnings
var _ = fmt.Sprintf

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

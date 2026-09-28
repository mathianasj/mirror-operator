package mirrorregistry

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

const (
	MachineConfigName  = "99-master-mirror-registry"
	IDMSName           = "mirror-registry"
	CASecretName       = "mirror-registry-ca"
	TrustConfigMapName = "mirror-registry-ca-bundle"
	masterPoolLabel    = "node-role.kubernetes.io/master"
	operatorNamespace  = "mirror-operator-system"

	defaultQuayImage  = "quay.io/mathianasj/mirror-registry/quay-rhel8:latest"
	defaultRedisImage = "quay.io/mathianasj/mirror-registry/redis-6:latest"
	defaultPauseImage = "registry.access.redhat.com/ubi9/pause:latest"
	defaultDataPath   = "/opt/quay"
	defaultPort       = int32(8443)
)

type Config struct {
	DataPath       string
	Port           int32
	QuayImage      string
	RedisImage     string
	PauseImage     string
	PullSecretJSON string
	CACertPEM      string
	CAKeyPEM       string
}

type Manager struct {
	Client client.Client
	Scheme *runtime.Scheme
}

func (m *Manager) Reconcile(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)

	cfg, err := m.resolveConfig(ctx, platform)
	if err != nil {
		return fmt.Errorf("resolving mirror registry config: %w", err)
	}

	if err := m.ensureCA(ctx, cfg); err != nil {
		return fmt.Errorf("ensuring CA secret: %w", err)
	}

	masterNodes, err := m.getMasterNodes(ctx)
	if err != nil {
		return fmt.Errorf("listing master nodes: %w", err)
	}
	if len(masterNodes) == 0 {
		return fmt.Errorf("no master nodes found")
	}

	logger.Info("Reconciling mirror registry MachineConfig",
		"masterNodes", len(masterNodes), "port", cfg.Port, "dataPath", cfg.DataPath)

	if err := m.ensureMachineConfig(ctx, cfg); err != nil {
		return fmt.Errorf("ensuring MachineConfig: %w", err)
	}

	if err := m.ensureTrustConfigMap(ctx, masterNodes, cfg); err != nil {
		return fmt.Errorf("ensuring trust ConfigMap: %w", err)
	}

	if err := m.ensureImageConfig(ctx); err != nil {
		return fmt.Errorf("ensuring image config trust: %w", err)
	}

	if err := m.ensureIDMS(ctx, masterNodes, cfg); err != nil {
		return fmt.Errorf("ensuring ImageDigestMirrorSet: %w", err)
	}

	return nil
}

func (m *Manager) resolveConfig(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) (*Config, error) {
	spec := platform.Spec.Airgapped.MirrorRegistryConfig
	if spec == nil {
		spec = &mirrorv1.MirrorRegistryConfig{}
	}

	cfg := &Config{
		DataPath:   defaultDataPath,
		Port:       defaultPort,
		QuayImage:  defaultQuayImage,
		RedisImage: defaultRedisImage,
		PauseImage: defaultPauseImage,
	}

	if spec.DataPath != "" {
		cfg.DataPath = spec.DataPath
	}
	if spec.Port != 0 {
		cfg.Port = spec.Port
	}
	if spec.QuayImage != "" {
		cfg.QuayImage = spec.QuayImage
	}
	if spec.RedisImage != "" {
		cfg.RedisImage = spec.RedisImage
	}
	if spec.PauseImage != "" {
		cfg.PauseImage = spec.PauseImage
	}

	cfg.PullSecretJSON = m.resolvePullSecret(ctx, platform, spec)

	return cfg, nil
}

func (m *Manager) getMasterNodes(ctx context.Context) ([]corev1.Node, error) {
	nodeList := &corev1.NodeList{}
	if err := m.Client.List(ctx, nodeList, client.MatchingLabels{masterPoolLabel: ""}); err != nil {
		return nil, err
	}
	return nodeList.Items, nil
}

func (m *Manager) ensureCA(ctx context.Context, cfg *Config) error {
	logger := log.FromContext(ctx)

	secret := &corev1.Secret{}
	err := m.Client.Get(ctx, client.ObjectKey{Name: CASecretName, Namespace: operatorNamespace}, secret)
	if err == nil {
		cfg.CACertPEM = string(secret.Data["ca.crt"])
		cfg.CAKeyPEM = string(secret.Data["ca.key"])
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	certPEM, keyPEM, err := generateCA()
	if err != nil {
		return fmt.Errorf("generating CA: %w", err)
	}

	secret = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CASecretName,
			Namespace: operatorNamespace,
		},
		Data: map[string][]byte{
			"ca.crt": []byte(certPEM),
			"ca.key": []byte(keyPEM),
		},
	}
	if err := m.Client.Create(ctx, secret); err != nil {
		return err
	}

	logger.Info("Created mirror registry CA secret")
	cfg.CACertPEM = certPEM
	cfg.CAKeyPEM = keyPEM
	return nil
}

func generateCA() (certPEM, keyPEM string, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", err
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return "", "", err
	}

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   "Mirror Registry CA",
			Organization: []string{"mirror-operator"},
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            0,
		MaxPathLenZero:        true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return "", "", err
	}

	certBlock := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return "", "", err
	}
	keyBlock := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return string(certBlock), string(keyBlock), nil
}

func (m *Manager) ensureTrustConfigMap(ctx context.Context, masterNodes []corev1.Node, cfg *Config) error {
	logger := log.FromContext(ctx)

	data := make(map[string]string)
	for _, node := range masterNodes {
		addr := getNodeInternalAddress(node)
		if addr == "" {
			continue
		}
		key := strings.ReplaceAll(fmt.Sprintf("%s..%d", addr, cfg.Port), ":", "..")
		data[key] = cfg.CACertPEM
	}

	cm := &corev1.ConfigMap{}
	err := m.Client.Get(ctx, client.ObjectKey{Name: TrustConfigMapName, Namespace: "openshift-config"}, cm)
	if apierrors.IsNotFound(err) {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      TrustConfigMapName,
				Namespace: "openshift-config",
			},
			Data: data,
		}
		logger.Info("Creating mirror registry trust ConfigMap in openshift-config", "entries", len(data))
		return m.Client.Create(ctx, cm)
	}
	if err != nil {
		return err
	}

	cm.Data = data
	logger.Info("Updating mirror registry trust ConfigMap in openshift-config", "entries", len(data))
	return m.Client.Update(ctx, cm)
}

func (m *Manager) ensureImageConfig(ctx context.Context) error {
	logger := log.FromContext(ctx)

	imageConfig := &unstructured.Unstructured{}
	imageConfig.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "Image",
	})

	if err := m.Client.Get(ctx, client.ObjectKey{Name: "cluster"}, imageConfig); err != nil {
		return fmt.Errorf("getting image.config.openshift.io/cluster: %w", err)
	}

	existing, _, _ := unstructured.NestedString(imageConfig.Object, "spec", "additionalTrustedCA", "name")
	if existing == TrustConfigMapName {
		return nil
	}

	if err := unstructured.SetNestedField(imageConfig.Object, TrustConfigMapName, "spec", "additionalTrustedCA", "name"); err != nil {
		return err
	}

	logger.Info("Updating image.config.openshift.io/cluster with mirror registry CA trust", "configMap", TrustConfigMapName)
	return m.Client.Update(ctx, imageConfig)
}

func (m *Manager) ensureMachineConfig(ctx context.Context, cfg *Config) error {
	logger := log.FromContext(ctx)

	mc := &unstructured.Unstructured{}
	mc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "machineconfiguration.openshift.io",
		Version: "v1",
		Kind:    "MachineConfig",
	})
	mc.SetName(MachineConfigName)
	mc.SetLabels(map[string]string{
		"machineconfiguration.openshift.io/role": "master",
	})

	files := buildIgnitionFiles(cfg)
	units := buildSystemdUnits(cfg)

	mcSpec := map[string]interface{}{
		"config": map[string]interface{}{
			"ignition": map[string]interface{}{
				"version": "3.2.0",
			},
			"storage": map[string]interface{}{
				"files": files,
			},
			"systemd": map[string]interface{}{
				"units": units,
			},
		},
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(mc.GroupVersionKind())
	err := m.Client.Get(ctx, client.ObjectKey{Name: MachineConfigName}, existing)
	if apierrors.IsNotFound(err) {
		unstructured.SetNestedField(mc.Object, mcSpec, "spec")
		logger.Info("Creating MachineConfig for mirror registry")
		return m.Client.Create(ctx, mc)
	}
	if err != nil {
		return err
	}

	existing.SetLabels(mc.GetLabels())
	unstructured.SetNestedField(existing.Object, mcSpec, "spec")
	logger.Info("Updating MachineConfig for mirror registry")
	return m.Client.Update(ctx, existing)
}

func (m *Manager) ensureIDMS(ctx context.Context, masterNodes []corev1.Node, cfg *Config) error {
	logger := log.FromContext(ctx)

	var readyMirrors []interface{}
	readyCount := 0
	notReadyCount := 0
	for _, node := range masterNodes {
		addr := getNodeInternalAddress(node)
		if addr == "" {
			continue
		}
		if !isNodeReady(node) {
			logger.Info("Excluding NotReady node from IDMS mirrors", "node", node.Name, "address", addr)
			notReadyCount++
			continue
		}
		readyMirrors = append(readyMirrors, fmt.Sprintf("%s:%d", addr, cfg.Port))
		readyCount++
	}

	firstAddr := getNodeInternalAddress(masterNodes[0])
	if firstAddr == "" {
		return fmt.Errorf("no master nodes with internal addresses found")
	}
	sourceRegistry := fmt.Sprintf("%s:%d", firstAddr, cfg.Port)

	idms := &unstructured.Unstructured{}
	idms.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "ImageDigestMirrorSet",
	})
	idms.SetName(IDMSName)

	idmsSpec := map[string]interface{}{
		"imageDigestMirrors": []interface{}{
			map[string]interface{}{
				"source":  sourceRegistry,
				"mirrors": readyMirrors,
			},
		},
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(idms.GroupVersionKind())
	err := m.Client.Get(ctx, client.ObjectKey{Name: IDMSName}, existing)
	if apierrors.IsNotFound(err) {
		unstructured.SetNestedField(idms.Object, idmsSpec, "spec")
		logger.Info("Creating ImageDigestMirrorSet for mirror registry",
			"readyMirrors", readyCount, "notReadyNodes", notReadyCount, "source", sourceRegistry)
		return m.Client.Create(ctx, idms)
	}
	if err != nil {
		return err
	}

	unstructured.SetNestedField(existing.Object, idmsSpec, "spec")
	logger.Info("Updating ImageDigestMirrorSet for mirror registry",
		"readyMirrors", readyCount, "notReadyNodes", notReadyCount, "source", sourceRegistry)
	return m.Client.Update(ctx, existing)
}

func isNodeReady(node corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (m *Manager) resolvePullSecret(ctx context.Context, platform *mirrorv1.DisconnectedPlatform, spec *mirrorv1.MirrorRegistryConfig) string {
	logger := log.FromContext(ctx)

	// 1. Explicit imagePullSecret from the CRD spec
	if spec.ImagePullSecret != nil {
		secret := &corev1.Secret{}
		key := client.ObjectKey{
			Name:      spec.ImagePullSecret.Name,
			Namespace: platform.Namespace,
		}
		if err := m.Client.Get(ctx, key, secret); err != nil {
			logger.Error(err, "failed to read explicit imagePullSecret, falling back to cluster pull-secret")
		} else {
			if data, ok := secret.Data[".dockerconfigjson"]; ok {
				return string(data)
			}
			if data, ok := secret.Data["auth.json"]; ok {
				return string(data)
			}
		}
	}

	// 2. Fall back to cluster pull-secret from openshift-config
	clusterSecret := &corev1.Secret{}
	key := client.ObjectKey{Name: "pull-secret", Namespace: "openshift-config"}
	if err := m.Client.Get(ctx, key, clusterSecret); err != nil {
		logger.Info("Could not read cluster pull-secret from openshift-config", "error", err)
		return ""
	}
	if data, ok := clusterSecret.Data[".dockerconfigjson"]; ok {
		logger.Info("Using cluster pull-secret from openshift-config for mirror registry nodes")
		return string(data)
	}

	return ""
}

func getNodeInternalAddress(node corev1.Node) string {
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP {
			return addr.Address
		}
	}
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalDNS {
			return addr.Address
		}
	}
	return ""
}

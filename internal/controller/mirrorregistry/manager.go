package mirrorregistry

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

const (
	machineConfigName = "99-master-mirror-registry"
	registryUnitName  = "mirror-registry.service"
	idmsName          = "mirror-registry"
	masterPoolLabel   = "node-role.kubernetes.io/master"
)

type Manager struct {
	Client client.Client
	Scheme *runtime.Scheme
}

func (m *Manager) Reconcile(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)

	config := platform.Spec.Airgapped.MirrorRegistryConfig
	if config == nil {
		config = &mirrorv1.MirrorRegistryConfig{}
	}

	dataPath := "/opt/mirror-registry"
	if config.DataPath != "" {
		dataPath = config.DataPath
	}

	port := int32(5000)
	if config.Port != 0 {
		port = config.Port
	}

	image := "docker.io/library/registry:2"
	if config.Image != "" {
		image = config.Image
	}

	masterNodes, err := m.getMasterNodes(ctx)
	if err != nil {
		return fmt.Errorf("listing master nodes: %w", err)
	}

	if len(masterNodes) == 0 {
		return fmt.Errorf("no master nodes found")
	}

	logger.Info("Reconciling mirror registry MachineConfig", "masterNodes", len(masterNodes), "port", port)

	if err := m.ensureMachineConfig(ctx, platform, dataPath, port, image); err != nil {
		return fmt.Errorf("ensuring MachineConfig: %w", err)
	}

	if err := m.ensureIDMS(ctx, platform, masterNodes, port); err != nil {
		return fmt.Errorf("ensuring ImageDigestMirrorSet: %w", err)
	}

	return nil
}

func (m *Manager) getMasterNodes(ctx context.Context) ([]corev1.Node, error) {
	nodeList := &corev1.NodeList{}
	if err := m.Client.List(ctx, nodeList, client.MatchingLabels{masterPoolLabel: ""}); err != nil {
		return nil, err
	}
	return nodeList.Items, nil
}

func (m *Manager) ensureMachineConfig(ctx context.Context, platform *mirrorv1.DisconnectedPlatform, dataPath string, port int32, image string) error {
	logger := log.FromContext(ctx)

	unitContent := buildSystemdUnit(dataPath, port, image)

	mc := &unstructured.Unstructured{}
	mc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "machineconfiguration.openshift.io",
		Version: "v1",
		Kind:    "MachineConfig",
	})
	mc.SetName(machineConfigName)

	mcSpec := map[string]interface{}{
		"config": map[string]interface{}{
			"ignition": map[string]interface{}{
				"version": "3.2.0",
			},
			"storage": map[string]interface{}{
				"directories": []interface{}{
					map[string]interface{}{
						"path": dataPath,
						"mode": int64(0755),
					},
				},
				"files": []interface{}{
					map[string]interface{}{
						"path":      "/etc/containers/registries.conf.d/mirror-registry.conf",
						"mode":      int64(0644),
						"overwrite": true,
						"contents": map[string]interface{}{
							"source": "data:text/plain;base64," + base64.StdEncoding.EncodeToString(
								[]byte(fmt.Sprintf("[[registry]]\nlocation = \"localhost:%d\"\ninsecure = true\n", port)),
							),
						},
					},
				},
			},
			"systemd": map[string]interface{}{
				"units": []interface{}{
					map[string]interface{}{
						"name":     registryUnitName,
						"enabled":  true,
						"contents": unitContent,
					},
				},
			},
		},
		"osImageURL": "",
	}

	labels := map[string]string{
		"machineconfiguration.openshift.io/role": "master",
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(mc.GroupVersionKind())
	err := m.Client.Get(ctx, client.ObjectKey{Name: machineConfigName}, existing)
	if apierrors.IsNotFound(err) {
		mc.SetLabels(labels)
		unstructured.SetNestedField(mc.Object, mcSpec, "spec")
		logger.Info("Creating MachineConfig for mirror registry")
		return m.Client.Create(ctx, mc)
	}
	if err != nil {
		return err
	}

	existing.SetLabels(labels)
	unstructured.SetNestedField(existing.Object, mcSpec, "spec")
	logger.Info("Updating MachineConfig for mirror registry")
	return m.Client.Update(ctx, existing)
}

func buildSystemdUnit(dataPath string, port int32, image string) string {
	return fmt.Sprintf(`[Unit]
Description=Mirror Registry (podman)
After=network-online.target
Wants=network-online.target
Before=kubelet.service crio.service

[Service]
Type=simple
ExecStartPre=-/usr/bin/podman stop mirror-registry
ExecStartPre=-/usr/bin/podman rm mirror-registry
ExecStart=/usr/bin/podman run \
  --name mirror-registry \
  --net host \
  -v %s:/var/lib/registry:z \
  -e REGISTRY_HTTP_ADDR=0.0.0.0:%d \
  %s
ExecStop=/usr/bin/podman stop mirror-registry
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
`, dataPath, port, image)
}

func (m *Manager) ensureIDMS(ctx context.Context, platform *mirrorv1.DisconnectedPlatform, masterNodes []corev1.Node, port int32) error {
	logger := log.FromContext(ctx)

	var mirrors []interface{}
	for _, node := range masterNodes {
		nodeAddr := getNodeInternalAddress(node)
		if nodeAddr == "" {
			continue
		}
		mirrors = append(mirrors, map[string]interface{}{
			"mirror": fmt.Sprintf("%s:%d", nodeAddr, port),
		})
	}

	if len(mirrors) == 0 {
		return fmt.Errorf("no master nodes with internal addresses found")
	}

	mirrorRegistry := platform.Spec.Airgapped.MirrorRegistry
	if mirrorRegistry == "" {
		nodeAddr := getNodeInternalAddress(masterNodes[0])
		mirrorRegistry = fmt.Sprintf("%s:%d", nodeAddr, port)
	}

	registryScope := mirrorRegistry
	if idx := strings.Index(registryScope, "/"); idx > 0 {
		registryScope = registryScope[:idx]
	}

	idms := &unstructured.Unstructured{}
	idms.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "ImageDigestMirrorSet",
	})
	idms.SetName(idmsName)

	idmsSpec := map[string]interface{}{
		"imageDigestMirrors": []interface{}{
			map[string]interface{}{
				"source":  registryScope,
				"mirrors": mirrors,
			},
		},
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(idms.GroupVersionKind())
	err := m.Client.Get(ctx, client.ObjectKey{Name: idmsName}, existing)
	if apierrors.IsNotFound(err) {
		unstructured.SetNestedField(idms.Object, idmsSpec, "spec")
		logger.Info("Creating ImageDigestMirrorSet for mirror registry failover", "mirrors", len(mirrors))
		return m.Client.Create(ctx, idms)
	}
	if err != nil {
		return err
	}

	unstructured.SetNestedField(existing.Object, idmsSpec, "spec")
	logger.Info("Updating ImageDigestMirrorSet for mirror registry failover", "mirrors", len(mirrors))
	return m.Client.Update(ctx, existing)
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

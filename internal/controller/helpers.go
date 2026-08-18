package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func (r *DisconnectedPlatformReconciler) isSingleNodeOpenShift(ctx context.Context) bool {
	logger := log.FromContext(ctx)

	infra := &unstructured.Unstructured{}
	infra.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "Infrastructure",
	})
	infra.SetName("cluster")

	if err := r.Get(ctx, client.ObjectKeyFromObject(infra), infra); err != nil {
		logger.V(1).Info("Could not read Infrastructure CR for SNO detection, assuming multi-node", "error", err)
		return false
	}

	topology, _, _ := unstructured.NestedString(infra.Object, "status", "controlPlaneTopology")
	return topology == "SingleReplica"
}

func (r *DisconnectedPlatformReconciler) resolveQuayReplicaOverride(ctx context.Context, explicit *int32) *int32 {
	if explicit != nil {
		return explicit
	}
	if r.isSingleNodeOpenShift(ctx) {
		one := int32(1)
		return &one
	}
	return nil
}

func buildQuayComponents(replicaOverride *int32, objectStorageManaged bool) []interface{} {
	hpaManaged := true
	if replicaOverride != nil {
		hpaManaged = false
	}

	makeComponent := func(kind string, managed bool) map[string]interface{} {
		c := map[string]interface{}{"kind": kind, "managed": managed}
		if replicaOverride != nil && (kind == "quay" || kind == "clair" || kind == "mirror") {
			c["overrides"] = map[string]interface{}{
				"replicas": int64(*replicaOverride),
			}
		}
		return c
	}

	return []interface{}{
		makeComponent("clair", true),
		makeComponent("postgres", true),
		makeComponent("objectstorage", objectStorageManaged),
		makeComponent("redis", true),
		map[string]interface{}{"kind": "horizontalpodautoscaler", "managed": hpaManaged},
		makeComponent("route", true),
		makeComponent("mirror", true),
		makeComponent("tls", true),
		makeComponent("quay", true),
	}
}

func (r *DisconnectedPlatformReconciler) getClusterVersion(ctx context.Context) string {
	logger := log.FromContext(ctx)

	cv := &unstructured.Unstructured{}
	cv.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "ClusterVersion",
	})
	cv.SetName("version")

	if err := r.Get(ctx, client.ObjectKeyFromObject(cv), cv); err != nil {
		logger.V(1).Info("Could not read ClusterVersion", "error", err)
		return ""
	}

	version, _, _ := unstructured.NestedString(cv.Object, "status", "desired", "version")
	return version
}

func (r *DisconnectedPlatformReconciler) getClusterProxy(ctx context.Context) (httpProxy, httpsProxy, noProxy string) {
	logger := log.FromContext(ctx)

	proxy := &unstructured.Unstructured{}
	proxy.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "Proxy",
	})
	proxy.SetName("cluster")

	if err := r.Get(ctx, client.ObjectKeyFromObject(proxy), proxy); err != nil {
		logger.V(1).Info("Could not read Proxy CR", "error", err)
		return "", "", ""
	}

	httpProxy, _, _ = unstructured.NestedString(proxy.Object, "spec", "httpProxy")
	httpsProxy, _, _ = unstructured.NestedString(proxy.Object, "spec", "httpsProxy")
	noProxy, _, _ = unstructured.NestedString(proxy.Object, "spec", "noProxy")
	return
}

func (r *DisconnectedPlatformReconciler) getClusterSSHKey(ctx context.Context) string {
	logger := log.FromContext(ctx)

	mc := &unstructured.Unstructured{}
	mc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "machineconfiguration.openshift.io",
		Version: "v1",
		Kind:    "MachineConfig",
	})
	mc.SetName("99-master-ssh")

	if err := r.Get(ctx, client.ObjectKeyFromObject(mc), mc); err != nil {
		logger.V(1).Info("No 99-master-ssh MachineConfig found, skipping SSH key injection", "error", err)
		return ""
	}

	users, found, _ := unstructured.NestedSlice(mc.Object, "spec", "config", "passwd", "users")
	if !found || len(users) == 0 {
		return ""
	}
	user, ok := users[0].(map[string]interface{})
	if !ok {
		return ""
	}
	keys, found, _ := unstructured.NestedStringSlice(user, "sshAuthorizedKeys")
	if !found || len(keys) == 0 {
		return ""
	}
	return keys[0]
}

func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func removeString(slice []string, s string) []string {
	var out []string
	for _, item := range slice {
		if item != s {
			out = append(out, item)
		}
	}
	return out
}

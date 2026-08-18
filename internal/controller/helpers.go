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

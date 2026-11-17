package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

var _ = Describe("ensureUpdateService ordering guard (issue #135)", func() {
	var (
		ctx    context.Context
		scheme *runtime.Scheme
	)

	BeforeEach(func() {
		ctx = context.Background()
		scheme = runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
		Expect(mirrorv1.AddToScheme(scheme)).To(Succeed())
	})

	// quayRegistryWithHostname builds an unstructured QuayRegistry whose
	// status.registryEndpoint is already set so getQuayHostname returns a valid value.
	quayRegistryWithHostname := func(hostname string) *unstructured.Unstructured {
		qr := &unstructured.Unstructured{}
		qr.SetGroupVersionKind(quayRegistryGVK())
		qr.SetName("mirror-operator-quay")
		qr.SetNamespace(architectNamespace)
		_ = unstructured.SetNestedField(qr.Object, hostname, "status", "registryEndpoint")
		return qr
	}

	updateServiceExists := func(c client.Client) bool {
		us := &unstructured.Unstructured{}
		us.SetGroupVersionKind(updateServiceGVK())
		err := c.Get(ctx, types.NamespacedName{
			Name:      "update-service-oc-mirror",
			Namespace: "openshift-update-service",
		}, us)
		return err == nil
	}

	It("does NOT create UpdateService when no collection has completed", func() {
		qr := quayRegistryWithHostname("quay.example.com")

		platform := &mirrorv1.DisconnectedPlatform{
			ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
			Spec: mirrorv1.DisconnectedPlatformSpec{
				Mode:      mirrorv1.PlatformModeConnected,
				Connected: &mirrorv1.ConnectedConfig{},
			},
			// Status.LastCollection is nil — no collection has completed yet.
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(qr, platform).Build()
		r := &DisconnectedPlatformReconciler{Client: c, Scheme: scheme}

		err := r.ensureUpdateService(ctx, platform)
		Expect(err).NotTo(HaveOccurred())
		Expect(updateServiceExists(c)).To(BeFalse(),
			"UpdateService must not be created when graph-data has not yet been mirrored (no completed collection)")
	})

	It("creates UpdateService when at least one collection has completed", func() {
		qr := quayRegistryWithHostname("quay.example.com")

		now := metav1.Now()
		platform := &mirrorv1.DisconnectedPlatform{
			ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
			Spec: mirrorv1.DisconnectedPlatformSpec{
				Mode:      mirrorv1.PlatformModeConnected,
				Connected: &mirrorv1.ConnectedConfig{},
			},
			Status: mirrorv1.DisconnectedPlatformStatus{
				LastCollection: &mirrorv1.CollectionInfo{
					Version:   "4.14.5",
					Timestamp: now,
					Status:    "Complete",
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(qr, platform).Build()
		r := &DisconnectedPlatformReconciler{Client: c, Scheme: scheme}

		err := r.ensureUpdateService(ctx, platform)
		Expect(err).NotTo(HaveOccurred())
		Expect(updateServiceExists(c)).To(BeTrue(),
			"UpdateService must be created once graph-data has been mirrored (collection completed)")
	})
})

// quayRegistryGVK returns the GVK for QuayRegistry.
func quayRegistryGVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: "quay.redhat.com", Version: "v1", Kind: "QuayRegistry"}
}

// updateServiceGVK returns the GVK for UpdateService.
func updateServiceGVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{
		Group:   "updateservice.operator.openshift.io",
		Version: "v1",
		Kind:    "UpdateService",
	}
}

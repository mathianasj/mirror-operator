package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

var _ = Describe("Ready condition guard (issue #129)", func() {
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

	// Helper: build an airgapped platform with the finalizer pre-set so the
	// first reconcile reaches the main logic (not just the finalizer-add path).
	newAirgappedPlatform := func(name string) *mirrorv1.DisconnectedPlatform {
		return &mirrorv1.DisconnectedPlatform{
			ObjectMeta: metav1.ObjectMeta{
				Name:       name,
				Finalizers: []string{platformFinalizer},
			},
			Spec: mirrorv1.DisconnectedPlatformSpec{
				Mode: mirrorv1.PlatformModeAirgapped,
				Airgapped: &mirrorv1.AirgappedConfig{
					ImportPath: "/tmp/bundle",
				},
			},
		}
	}

	readyCondition := func(platform *mirrorv1.DisconnectedPlatform) *metav1.Condition {
		for i := range platform.Status.Conditions {
			if platform.Status.Conditions[i].Type == "Ready" {
				return &platform.Status.Conditions[i]
			}
		}
		return nil
	}

	It("does not set Ready=True when airgapped subscriptions are still pending", func() {
		platform := newAirgappedPlatform("test-airgapped-ready")
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(platform).
			WithStatusSubresource(platform).
			Build()

		r := &DisconnectedPlatformReconciler{Client: c, Scheme: scheme}
		req := ctrl.Request{NamespacedName: types.NamespacedName{Name: platform.Name}}

		// Reconcile — airgapped subscriptions will be pending (no PackageManifests),
		// so airgappedRequeue=true. Ready=True must NOT be set.
		_, err := r.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		// Re-fetch to get the status written by r.Status().Update(...)
		updated := &mirrorv1.DisconnectedPlatform{}
		Expect(c.Get(ctx, req.NamespacedName, updated)).To(Succeed())

		cond := readyCondition(updated)
		if cond != nil {
			Expect(cond.Status).NotTo(Equal(metav1.ConditionTrue),
				"Ready must not be True when airgapped reconciliation has pending work; got Reason=%s Message=%s",
				cond.Reason, cond.Message)
		}
		// A nil condition (not set at all) is also acceptable — just not True.
	})

	It("requeueues after 30s when airgapped subscriptions are still pending", func() {
		platform := newAirgappedPlatform("test-airgapped-requeue")
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(platform).
			WithStatusSubresource(platform).
			Build()

		r := &DisconnectedPlatformReconciler{Client: c, Scheme: scheme}
		req := ctrl.Request{NamespacedName: types.NamespacedName{Name: platform.Name}}

		result, err := r.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(BeNumerically(">", 0),
			"reconcile must requeue when airgapped subscriptions are pending")
	})
})

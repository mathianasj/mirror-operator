package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

var _ = Describe("ensureSubscription owner reference (issue #132)", func() {
	var (
		ctx      context.Context
		scheme   *runtime.Scheme
		platform *mirrorv1.DisconnectedPlatform
	)

	BeforeEach(func() {
		ctx = context.Background()
		scheme = runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
		Expect(mirrorv1.AddToScheme(scheme)).To(Succeed())

		platform = &mirrorv1.DisconnectedPlatform{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-platform",
				UID:  types.UID("test-uid-1234"),
			},
		}
	})

	It("sets an owner reference on a newly created Subscription", func() {
		op := operatorDef{
			name:      "quay-operator",
			pkg:       "quay-operator",
			ns:        "openshift-operators",
			catalog:   "redhat-operators",
			catalogNS: "openshift-marketplace",
			channel:   "stable-3.13",
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(platform).Build()
		r := &DisconnectedPlatformReconciler{Client: c, Scheme: scheme}

		Expect(r.ensureSubscription(ctx, platform, op, nil)).To(Succeed())

		sub := &unstructured.Unstructured{}
		sub.SetGroupVersionKind(subscriptionGVK)
		Expect(c.Get(ctx, client.ObjectKey{Name: "mirror-operator-quay-operator", Namespace: "openshift-operators"}, sub)).To(Succeed())

		refs := sub.GetOwnerReferences()
		Expect(refs).NotTo(BeEmpty(),
			"Subscription must have an owner reference so it is garbage-collected when DisconnectedPlatform is deleted")
		Expect(refs[0].Name).To(Equal("test-platform"),
			"owner reference must point to the DisconnectedPlatform")
		Expect(refs[0].UID).To(Equal(types.UID("test-uid-1234")),
			"owner reference UID must match the DisconnectedPlatform UID")
	})

	It("does not error when the Subscription already exists", func() {
		op := operatorDef{
			name:      "quay-operator",
			pkg:       "quay-operator",
			ns:        "openshift-operators",
			catalog:   "redhat-operators",
			catalogNS: "openshift-marketplace",
			channel:   "stable-3.13",
		}

		// Pre-create the subscription
		existingSub := &unstructured.Unstructured{}
		existingSub.SetGroupVersionKind(subscriptionGVK)
		existingSub.SetName("mirror-operator-quay-operator")
		existingSub.SetNamespace("openshift-operators")

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(platform, existingSub).Build()
		r := &DisconnectedPlatformReconciler{Client: c, Scheme: scheme}

		Expect(r.ensureSubscription(ctx, platform, op, nil)).To(Succeed())
	})
})

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

var _ = Describe("reconcileQuayComponentImage (issue #137)", func() {
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

	newQuaySub := func(existingImage string) *unstructured.Unstructured {
		sub := &unstructured.Unstructured{}
		sub.SetGroupVersionKind(subscriptionGVK)
		sub.SetName("mirror-operator-quay-operator")
		sub.SetNamespace("openshift-operators")
		if existingImage != "" {
			_ = unstructured.SetNestedSlice(sub.Object, []interface{}{
				map[string]interface{}{"name": "RELATED_IMAGE_COMPONENT_QUAY", "value": existingImage},
			}, "spec", "config", "env")
		}
		return sub
	}

	getEnv := func(c client.Client, name string) string {
		sub := &unstructured.Unstructured{}
		sub.SetGroupVersionKind(subscriptionGVK)
		_ = c.Get(ctx, client.ObjectKey{
			Name:      "mirror-operator-quay-operator",
			Namespace: "openshift-operators",
		}, sub)
		envs, _, _ := unstructured.NestedSlice(sub.Object, "spec", "config", "env")
		for _, e := range envs {
			em, ok := e.(map[string]interface{})
			if ok && em["name"] == name {
				v, _ := em["value"].(string)
				return v
			}
		}
		return ""
	}

	It("sets RELATED_IMAGE_COMPONENT_QUAY on the Quay Subscription", func() {
		sub := newQuaySub("")
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sub).Build()
		r := &DisconnectedPlatformReconciler{Client: c, Scheme: scheme}

		Expect(r.reconcileQuayComponentImage(ctx, "quay.io/my/quay:v3.11.0")).To(Succeed())

		Expect(getEnv(c, "RELATED_IMAGE_COMPONENT_QUAY")).To(Equal("quay.io/my/quay:v3.11.0"))
	})

	It("updates RELATED_IMAGE_COMPONENT_QUAY when the value changes", func() {
		sub := newQuaySub("quay.io/old/quay:v3.10.0")
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sub).Build()
		r := &DisconnectedPlatformReconciler{Client: c, Scheme: scheme}

		Expect(r.reconcileQuayComponentImage(ctx, "quay.io/new/quay:v3.11.0")).To(Succeed())

		Expect(getEnv(c, "RELATED_IMAGE_COMPONENT_QUAY")).To(Equal("quay.io/new/quay:v3.11.0"))
	})

	It("is a no-op when the Subscription does not exist yet", func() {
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &DisconnectedPlatformReconciler{Client: c, Scheme: scheme}

		Expect(r.reconcileQuayComponentImage(ctx, "quay.io/my/quay:v3.11.0")).To(Succeed())
	})

	It("is a no-op when the value already matches", func() {
		img := "quay.io/my/quay:v3.11.0"
		sub := newQuaySub(img)
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sub).Build()
		r := &DisconnectedPlatformReconciler{Client: c, Scheme: scheme}

		Expect(r.reconcileQuayComponentImage(ctx, img)).To(Succeed())
		// Value should still match (no spurious update)
		Expect(getEnv(c, "RELATED_IMAGE_COMPONENT_QUAY")).To(Equal(img))
	})
})

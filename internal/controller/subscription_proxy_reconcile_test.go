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

var _ = Describe("ensureSubscription proxy reconciliation (issue #128)", func() {
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

	newSub := func(name, ns, proxyVal string) *unstructured.Unstructured {
		sub := &unstructured.Unstructured{}
		sub.SetGroupVersionKind(subscriptionGVK)
		sub.SetName(name)
		sub.SetNamespace(ns)
		_ = unstructured.SetNestedField(sub.Object, "some-pkg", "spec", "name")
		if proxyVal != "" {
			_ = unstructured.SetNestedSlice(sub.Object, []interface{}{
				map[string]interface{}{"name": "HTTP_PROXY", "value": proxyVal},
				map[string]interface{}{"name": "HTTPS_PROXY", "value": proxyVal},
			}, "spec", "config", "env")
		}
		return sub
	}

	getEnv := func(sub *unstructured.Unstructured, key string) string {
		envs, _, _ := unstructured.NestedSlice(sub.Object, "spec", "config", "env")
		for _, e := range envs {
			em, ok := e.(map[string]interface{})
			if ok && em["name"] == key {
				v, _ := em["value"].(string)
				return v
			}
		}
		return ""
	}

	It("updates proxy env vars on an existing subscription when the cluster proxy changes", func() {
		oldProxy := "http://old-proxy:3128"
		newProxy := "http://new-proxy:8080"

		sub := newSub("mirror-operator-sub", "openshift-operators", oldProxy)
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sub).Build()
		r := &DisconnectedPlatformReconciler{Client: c, Scheme: scheme}

		Expect(r.reconcileSubscriptionProxy(ctx, sub, newProxy, newProxy, "")).To(Succeed())

		updated := &unstructured.Unstructured{}
		updated.SetGroupVersionKind(subscriptionGVK)
		Expect(c.Get(ctx, client.ObjectKeyFromObject(sub), updated)).To(Succeed())

		Expect(getEnv(updated, "HTTP_PROXY")).To(Equal(newProxy),
			"HTTP_PROXY must be updated to new value on existing subscription")
		Expect(getEnv(updated, "HTTPS_PROXY")).To(Equal(newProxy),
			"HTTPS_PROXY must be updated to new value on existing subscription")
	})

	It("removes proxy env vars when cluster proxy is cleared", func() {
		sub := newSub("mirror-operator-sub-clear", "openshift-operators", "http://old:3128")
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sub).Build()
		r := &DisconnectedPlatformReconciler{Client: c, Scheme: scheme}

		Expect(r.reconcileSubscriptionProxy(ctx, sub, "", "", "")).To(Succeed())

		updated := &unstructured.Unstructured{}
		updated.SetGroupVersionKind(subscriptionGVK)
		Expect(c.Get(ctx, client.ObjectKeyFromObject(sub), updated)).To(Succeed())

		Expect(getEnv(updated, "HTTP_PROXY")).To(BeEmpty(),
			"HTTP_PROXY must be removed when cluster proxy is cleared")
		Expect(getEnv(updated, "HTTPS_PROXY")).To(BeEmpty(),
			"HTTPS_PROXY must be removed when cluster proxy is cleared")
	})

	It("is a no-op when proxy vars already match", func() {
		proxy := "http://proxy:3128"
		sub := newSub("mirror-operator-sub-noop", "openshift-operators", proxy)
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sub).Build()
		r := &DisconnectedPlatformReconciler{Client: c, Scheme: scheme}

		// Same proxy — should not trigger an Update (no error, same state)
		Expect(r.reconcileSubscriptionProxy(ctx, sub, proxy, proxy, "")).To(Succeed())

		// Verify state unchanged
		updated := &unstructured.Unstructured{}
		updated.SetGroupVersionKind(subscriptionGVK)
		Expect(c.Get(ctx, client.ObjectKeyFromObject(sub), updated)).To(Succeed())
		Expect(getEnv(updated, "HTTP_PROXY")).To(Equal(proxy))
	})
})

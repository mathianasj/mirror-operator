package controller

import (
	"context"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

var _ = Describe("RHTAS health checker error propagation (issue #134)", func() {
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

	// secretMessage extracts the HealthCheckPassed condition message from a Securesign object.
	secretMessage := func(obj *unstructured.Unstructured) string {
		conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
		for _, c := range conditions {
			if m, ok := c.(map[string]interface{}); ok {
				if m["type"] == "HealthCheckPassed" {
					msg, _ := m["message"].(string)
					return msg
				}
			}
		}
		return ""
	}

	It("surfaces the actual error when validateEmailVerifiedClaim fails for a non-claim reason", func() {
		// Build a Securesign object.
		securesign := &unstructured.Unstructured{}
		securesign.SetGroupVersionKind(securesignGVK)
		securesign.SetName("mirror-operator-securesign")
		securesign.SetNamespace(architectNamespace)

		// Provide the Keycloak client secret with a non-empty clientSecret so the
		// function proceeds past the early-exit guards.
		kcSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mirror-operator-keycloak-client-secret",
				Namespace: architectNamespace,
			},
			Data: map[string][]byte{
				"clientSecret": []byte("test-secret-value"),
			},
		}

		// Build a platform with RHTAS OIDC Managed enabled so validateEmailVerifiedClaim
		// proceeds past the platform-spec guards and attempts the Ingress lookup.
		platform := &mirrorv1.DisconnectedPlatform{
			ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
			Spec: mirrorv1.DisconnectedPlatformSpec{
				Mode: mirrorv1.PlatformModeConnected,
				Connected: &mirrorv1.ConnectedConfig{
					RHTAS: &mirrorv1.RHTASInstallerConfig{
						OIDC: &mirrorv1.RHTASOIDCConfig{
							Managed: &mirrorv1.ManagedKeycloakConfig{
								Enabled: true,
								Realm:   "trusted-artifact-signer",
							},
						},
					},
				},
			},
		}

		// The Ingress "cluster" is intentionally absent; validateEmailVerifiedClaim will
		// return a "not found" error when it tries to get it. That error is NOT about
		// the email_verified claim — it must be surfaced verbatim, not swallowed behind
		// the generic "OIDC token missing email_verified claim or claim is false" message.
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(securesign, kcSecret, platform).
			WithStatusSubresource(securesign).
			Build()

		r := &RHTASHealthCheckReconciler{Client: c}
		req := ctrl.Request{NamespacedName: types.NamespacedName{
			Name:      "mirror-operator-securesign",
			Namespace: architectNamespace,
		}}

		_, err := r.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		// Re-fetch the Securesign to read the status condition written by the reconciler.
		updated := &unstructured.Unstructured{}
		updated.SetGroupVersionKind(securesignGVK)
		Expect(c.Get(ctx, types.NamespacedName{
			Name:      "mirror-operator-securesign",
			Namespace: architectNamespace,
		}, updated)).To(Succeed())

		msg := secretMessage(updated)
		Expect(msg).NotTo(BeEmpty(), "HealthCheckPassed condition must be set")

		Expect(strings.Contains(msg, "OIDC token missing email_verified claim or claim is false")).To(BeFalse(),
			"error message must not use the generic email_verified phrase when the real error is a network/TLS/lookup failure; got: %q", msg)

		// The actual error (Ingress not found) must appear in the message so operators
		// can diagnose the root cause.
		Expect(strings.Contains(msg, "OIDC") || strings.Contains(msg, "failed") || strings.Contains(msg, "not found")).To(BeTrue(),
			"error message must contain diagnostic information about the actual failure; got: %q", msg)
	})
})

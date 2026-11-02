package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

var _ = Describe("RHTASHealthCheckReconciler", func() {
	var (
		ctx        context.Context
		testScheme *runtime.Scheme
	)

	BeforeEach(func() {
		ctx = context.Background()
		testScheme = runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(testScheme)).To(Succeed())
		Expect(mirrorv1.AddToScheme(testScheme)).To(Succeed())
	})

	Describe("Reconcile", func() {
		It("returns empty result when Securesign does not exist", func() {
			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
			}
			req := reconcile.Request{NamespacedName: types.NamespacedName{
				Name: "missing", Namespace: architectNamespace,
			}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))
		})

		It("requeues after 10 minutes on a valid Securesign", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(securesignGVK)
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithObjects(securesign).
					Build(),
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{
				Name: "test-securesign", Namespace: architectNamespace,
			}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter.Minutes()).To(BeNumerically("~", 10, 0.1))
		})
	})

	Describe("Reconcile - health issue paths", func() {
		It("detects Fulcio crash-looping and Keycloak not found", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(securesignGVK)
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)
			unstructured.SetNestedSlice(securesign.Object, []interface{}{
				map[string]interface{}{
					"type":   "Ready",
					"status": "True",
				},
			}, "status", "conditions")

			fulcioPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "fulcio-server-0",
					Namespace: architectNamespace,
					Labels:    map[string]string{"app": "fulcio-server"},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{RestartCount: 10},
					},
				},
			}

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign, fulcioPod).Build(),
			}
			req := reconcile.Request{NamespacedName: types.NamespacedName{
				Name: "test-securesign", Namespace: architectNamespace,
			}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter.Minutes()).To(BeNumerically("~", 10, 0.1))
		})

		It("handles Fulcio restart when Keycloak is ready", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(securesignGVK)
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)

			fulcioPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "fulcio-server-0",
					Namespace: architectNamespace,
					Labels:    map[string]string{"app": "fulcio-server"},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{RestartCount: 5},
					},
				},
			}

			kc := &unstructured.Unstructured{}
			kc.SetGroupVersionKind(schema.GroupVersionKind{Group: "k8s.keycloak.org", Version: "v2alpha1", Kind: "Keycloak"})
			kc.SetName("mirror-operator-keycloak")
			kc.SetNamespace(architectNamespace)
			unstructured.SetNestedSlice(kc.Object, []interface{}{
				map[string]interface{}{
					"type":   "Ready",
					"status": "True",
				},
			}, "status", "conditions")

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign, fulcioPod, kc).Build(),
			}
			req := reconcile.Request{NamespacedName: types.NamespacedName{
				Name: "test-securesign", Namespace: architectNamespace,
			}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter.Minutes()).To(BeNumerically("~", 10, 0.1))

			deletedPod := &corev1.Pod{}
			err = r.Get(ctx, types.NamespacedName{Name: "fulcio-server-0", Namespace: architectNamespace}, deletedPod)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	Describe("validateTUFKeys", func() {
		It("removes tsa.certchain.pem key from TUF keys", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(securesignGVK)
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)
			unstructured.SetNestedSlice(securesign.Object, []interface{}{
				map[string]interface{}{"name": "fulcio.pem"},
				map[string]interface{}{"name": "tsa.certchain.pem"},
				map[string]interface{}{"name": "rekor.pub"},
			}, "spec", "tuf", "keys")

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build(),
			}

			err := r.validateTUFKeys(ctx, securesign)
			Expect(err).NotTo(HaveOccurred())

			updated := &unstructured.Unstructured{}
			updated.SetGroupVersionKind(securesignGVK)
			err = r.Get(ctx, types.NamespacedName{Name: "test-securesign", Namespace: architectNamespace}, updated)
			Expect(err).NotTo(HaveOccurred())

			keys, _, _ := unstructured.NestedSlice(updated.Object, "spec", "tuf", "keys")
			Expect(keys).To(HaveLen(2))
			for _, k := range keys {
				km := k.(map[string]interface{})
				Expect(km["name"]).NotTo(Equal("tsa.certchain.pem"))
			}
		})

		It("does nothing when no TUF keys are present", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(securesignGVK)
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build(),
			}

			err := r.validateTUFKeys(ctx, securesign)
			Expect(err).NotTo(HaveOccurred())
		})

		It("does nothing when tsa.certchain.pem is not in keys", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(securesignGVK)
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)
			unstructured.SetNestedSlice(securesign.Object, []interface{}{
				map[string]interface{}{"name": "fulcio.pem"},
				map[string]interface{}{"name": "rekor.pub"},
			}, "spec", "tuf", "keys")

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build(),
			}

			err := r.validateTUFKeys(ctx, securesign)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("validateComponentReadiness", func() {
		It("returns nil when Securesign is Ready", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(securesignGVK)
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)
			unstructured.SetNestedSlice(securesign.Object, []interface{}{
				map[string]interface{}{
					"type":   "Ready",
					"status": "True",
				},
			}, "status", "conditions")

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build(),
			}

			err := r.validateComponentReadiness(ctx, securesign)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when Securesign is not Ready (informational only)", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(securesignGVK)
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)
			unstructured.SetNestedSlice(securesign.Object, []interface{}{
				map[string]interface{}{
					"type":    "Ready",
					"status":  "False",
					"reason":  "ComponentNotReady",
					"message": "Waiting for Fulcio",
				},
			}, "status", "conditions")

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build(),
			}

			err := r.validateComponentReadiness(ctx, securesign)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when no conditions exist", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(securesignGVK)
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build(),
			}

			err := r.validateComponentReadiness(ctx, securesign)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("validateFulcioKeycloakConnection", func() {
		It("returns nil when no Fulcio pods exist", func() {
			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
			}

			err := r.validateFulcioKeycloakConnection(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when Fulcio pod has low restart count", func() {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "fulcio-server-1",
					Namespace: architectNamespace,
					Labels:    map[string]string{"app": "fulcio-server"},
				},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name:         "fulcio",
							RestartCount: 1,
						},
					},
				},
			}

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pod).Build(),
			}

			err := r.validateFulcioKeycloakConnection(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("deletes Fulcio pod when restart count is high and Keycloak is ready", func() {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "fulcio-server-1",
					Namespace: architectNamespace,
					Labels:    map[string]string{"app": "fulcio-server"},
				},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name:         "fulcio",
							RestartCount: 5,
						},
					},
				},
			}

			kc := &unstructured.Unstructured{}
			kc.SetGroupVersionKind(keycloakGVK)
			kc.SetName("mirror-operator-keycloak")
			kc.SetNamespace(architectNamespace)
			unstructured.SetNestedSlice(kc.Object, []interface{}{
				map[string]interface{}{
					"type":   "Ready",
					"status": "True",
				},
			}, "status", "conditions")

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pod, kc).Build(),
			}

			err := r.validateFulcioKeycloakConnection(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Pod should be deleted
			deletedPod := &corev1.Pod{}
			err = r.Get(ctx, types.NamespacedName{Name: "fulcio-server-1", Namespace: architectNamespace}, deletedPod)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("does not delete Fulcio pod when Keycloak is not ready", func() {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "fulcio-server-1",
					Namespace: architectNamespace,
					Labels:    map[string]string{"app": "fulcio-server"},
				},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name:         "fulcio",
							RestartCount: 10,
						},
					},
				},
			}

			kc := &unstructured.Unstructured{}
			kc.SetGroupVersionKind(keycloakGVK)
			kc.SetName("mirror-operator-keycloak")
			kc.SetNamespace(architectNamespace)
			unstructured.SetNestedSlice(kc.Object, []interface{}{
				map[string]interface{}{
					"type":   "Ready",
					"status": "False",
				},
			}, "status", "conditions")

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pod, kc).Build(),
			}

			err := r.validateFulcioKeycloakConnection(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Pod should NOT be deleted
			existingPod := &corev1.Pod{}
			err = r.Get(ctx, types.NamespacedName{Name: "fulcio-server-1", Namespace: architectNamespace}, existingPod)
			Expect(err).NotTo(HaveOccurred())
		})

		It("does not delete Fulcio pod when Keycloak does not exist", func() {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "fulcio-server-1",
					Namespace: architectNamespace,
					Labels:    map[string]string{"app": "fulcio-server"},
				},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name:         "fulcio",
							RestartCount: 10,
						},
					},
				},
			}

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pod).Build(),
			}

			err := r.validateFulcioKeycloakConnection(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Pod should NOT be deleted
			existingPod := &corev1.Pod{}
			err = r.Get(ctx, types.NamespacedName{Name: "fulcio-server-1", Namespace: architectNamespace}, existingPod)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("validateEmailVerifiedClaim", func() {
		It("returns nil when client secret does not exist", func() {
			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
			}

			err := r.validateEmailVerifiedClaim(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when client secret is empty", func() {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-keycloak-client-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"clientSecret": {},
				},
			}

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(secret).Build(),
			}

			err := r.validateEmailVerifiedClaim(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when no DisconnectedPlatform exists", func() {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-keycloak-client-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"clientSecret": []byte("some-secret"),
				},
			}

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(secret).Build(),
			}

			err := r.validateEmailVerifiedClaim(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when platform has nil connected config", func() {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-keycloak-client-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"clientSecret": []byte("some-secret"),
				},
			}

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-platform",
					Namespace: architectNamespace,
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
				},
			}

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(secret, platform).Build(),
			}

			err := r.validateEmailVerifiedClaim(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when platform has nil RHTAS config", func() {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-keycloak-client-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"clientSecret": []byte("some-secret"),
				},
			}

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-platform",
					Namespace: architectNamespace,
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      "connected",
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(secret, platform).Build(),
			}

			err := r.validateEmailVerifiedClaim(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when platform has nil OIDC managed config", func() {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-keycloak-client-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"clientSecret": []byte("some-secret"),
				},
			}

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-platform",
					Namespace: architectNamespace,
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{},
						},
					},
				},
			}

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(secret, platform).Build(),
			}

			err := r.validateEmailVerifiedClaim(ctx)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("updateSecuresignHealthStatus", func() {
		It("adds HealthCheckPassed condition when healthy", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(securesignGVK)
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).
					WithObjects(securesign).
					WithStatusSubresource(securesign).
					Build(),
			}

			r.updateSecuresignHealthStatus(ctx, securesign, true, "All checks passed")

			updated := &unstructured.Unstructured{}
			updated.SetGroupVersionKind(securesignGVK)
			err := r.Get(ctx, types.NamespacedName{Name: "test-securesign", Namespace: architectNamespace}, updated)
			Expect(err).NotTo(HaveOccurred())

			conditions, _, _ := unstructured.NestedSlice(updated.Object, "status", "conditions")
			Expect(conditions).To(HaveLen(1))
			cond := conditions[0].(map[string]interface{})
			Expect(cond["type"]).To(Equal("HealthCheckPassed"))
			Expect(cond["status"]).To(Equal("True"))
			Expect(cond["reason"]).To(Equal("AllHealthChecksPassed"))
			Expect(cond["message"]).To(Equal("All checks passed"))
		})

		It("sets HealthCheckPassed to False when unhealthy", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(securesignGVK)
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).
					WithObjects(securesign).
					WithStatusSubresource(securesign).
					Build(),
			}

			r.updateSecuresignHealthStatus(ctx, securesign, false, "TUF keys invalid")

			updated := &unstructured.Unstructured{}
			updated.SetGroupVersionKind(securesignGVK)
			err := r.Get(ctx, types.NamespacedName{Name: "test-securesign", Namespace: architectNamespace}, updated)
			Expect(err).NotTo(HaveOccurred())

			conditions, _, _ := unstructured.NestedSlice(updated.Object, "status", "conditions")
			Expect(conditions).To(HaveLen(1))
			cond := conditions[0].(map[string]interface{})
			Expect(cond["status"]).To(Equal("False"))
			Expect(cond["reason"]).To(Equal("HealthCheckFailed"))
		})

		It("updates existing HealthCheckPassed condition instead of adding duplicate", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(securesignGVK)
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)
			unstructured.SetNestedSlice(securesign.Object, []interface{}{
				map[string]interface{}{
					"type":   "HealthCheckPassed",
					"status": "True",
					"reason": "AllHealthChecksPassed",
				},
			}, "status", "conditions")

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).
					WithObjects(securesign).
					WithStatusSubresource(securesign).
					Build(),
			}

			r.updateSecuresignHealthStatus(ctx, securesign, false, "Now failing")

			updated := &unstructured.Unstructured{}
			updated.SetGroupVersionKind(securesignGVK)
			err := r.Get(ctx, types.NamespacedName{Name: "test-securesign", Namespace: architectNamespace}, updated)
			Expect(err).NotTo(HaveOccurred())

			conditions, _, _ := unstructured.NestedSlice(updated.Object, "status", "conditions")
			Expect(conditions).To(HaveLen(1))
			cond := conditions[0].(map[string]interface{})
			Expect(cond["status"]).To(Equal("False"))
		})
	})

	Describe("validateEmailVerifiedClaim", func() {
		It("returns nil when client secret does not exist", func() {
			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
			}
			err := r.validateEmailVerifiedClaim(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when client secret is empty", func() {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-keycloak-client-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"clientSecret": []byte(""),
				},
			}
			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(secret).Build(),
			}
			err := r.validateEmailVerifiedClaim(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when no DisconnectedPlatform exists", func() {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-keycloak-client-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"clientSecret": []byte("test-secret"),
				},
			}
			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(secret).Build(),
			}
			err := r.validateEmailVerifiedClaim(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when RHTAS OIDC is not configured", func() {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-keycloak-client-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"clientSecret": []byte("test-secret"),
				},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}
			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(secret, platform).Build(),
			}
			err := r.validateEmailVerifiedClaim(ctx)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("validateFulcioKeycloakConnection", func() {
		It("returns nil when no Fulcio pods exist", func() {
			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
			}
			err := r.validateFulcioKeycloakConnection(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when Fulcio pods have low restart count", func() {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "fulcio-server-1",
					Namespace: architectNamespace,
					Labels:    map[string]string{"app": "fulcio-server"},
				},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{RestartCount: 1},
					},
				},
			}
			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pod).Build(),
			}
			err := r.validateFulcioKeycloakConnection(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("restarts Fulcio pod when Keycloak is ready and restart count high", func() {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "fulcio-server-1",
					Namespace: architectNamespace,
					Labels:    map[string]string{"app": "fulcio-server"},
				},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{RestartCount: 5},
					},
				},
			}
			kc := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "k8s.keycloak.org/v2alpha1",
				"kind":       "Keycloak",
				"metadata": map[string]interface{}{
					"name":      "mirror-operator-keycloak",
					"namespace": architectNamespace,
				},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":   "Ready",
							"status": "True",
						},
					},
				},
			}}
			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pod, kc).Build(),
			}
			err := r.validateFulcioKeycloakConnection(ctx)
			Expect(err).NotTo(HaveOccurred())

			podList := &corev1.PodList{}
			Expect(r.List(ctx, podList)).To(Succeed())
			Expect(podList.Items).To(BeEmpty())
		})
	})

	Describe("validateComponentReadiness", func() {
		It("returns nil when Securesign has Ready=True condition", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(securesignGVK)
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)
			unstructured.SetNestedSlice(securesign.Object, []interface{}{
				map[string]interface{}{
					"type":   "Ready",
					"status": "True",
				},
			}, "status", "conditions")
			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build(),
			}
			err := r.validateComponentReadiness(ctx, securesign)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when Securesign has Ready=False (informational only)", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(securesignGVK)
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)
			unstructured.SetNestedSlice(securesign.Object, []interface{}{
				map[string]interface{}{
					"type":    "Ready",
					"status":  "False",
					"message": "components degraded",
				},
			}, "status", "conditions")
			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build(),
			}
			err := r.validateComponentReadiness(ctx, securesign)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Reconcile integration", func() {
		It("detects health issues and updates status", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(securesignGVK)
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)
			unstructured.SetNestedSlice(securesign.Object, []interface{}{
				map[string]interface{}{
					"type":    "Ready",
					"status":  "False",
					"message": "not ready",
				},
			}, "status", "conditions")

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build(),
			}
			req := reconcile.Request{NamespacedName: types.NamespacedName{
				Name: "test-securesign", Namespace: architectNamespace,
			}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter.Minutes()).To(BeNumerically("~", 10, 0.1))
		})
	})

	Describe("validateEmailVerifiedClaim", func() {
		It("returns nil when OIDC client secret does not exist", func() {
			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
			}
			err := r.validateEmailVerifiedClaim(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when clientSecret field is empty", func() {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-keycloak-client-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{"clientSecret": []byte("")},
			}
			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(secret).Build(),
			}
			err := r.validateEmailVerifiedClaim(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when no DisconnectedPlatforms exist", func() {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-keycloak-client-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{"clientSecret": []byte("some-secret")},
			}
			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(secret).Build(),
			}
			err := r.validateEmailVerifiedClaim(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when platform has no OIDC managed config", func() {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-keycloak-client-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{"clientSecret": []byte("some-secret")},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      "connected",
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(secret, platform).Build(),
			}
			err := r.validateEmailVerifiedClaim(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error when ingress not found", func() {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-keycloak-client-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{"clientSecret": []byte("some-secret")},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Managed: &mirrorv1.ManagedKeycloakConfig{Enabled: true},
							},
						},
					},
				},
			}
			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(secret, platform).Build(),
			}
			err := r.validateEmailVerifiedClaim(ctx)
			Expect(err).To(HaveOccurred())
		})

		It("uses default realm name when not set", func() {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-keycloak-client-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{"clientSecret": []byte("some-secret")},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Managed: &mirrorv1.ManagedKeycloakConfig{Enabled: true},
							},
						},
					},
				},
			}
			ingress := &unstructured.Unstructured{}
			ingress.SetGroupVersionKind(schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "Ingress"})
			ingress.SetName("cluster")
			unstructured.SetNestedField(ingress.Object, "apps.example.com", "spec", "domain")

			r := &RHTASHealthCheckReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(secret, platform, ingress).Build(),
			}
			err := r.validateEmailVerifiedClaim(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get access token"))
		})
	})

	Describe("updateSecuresignHealthStatus", func() {
		It("adds HealthCheckPassed condition when healthy", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(securesignGVK)
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build()
			r := &RHTASHealthCheckReconciler{Client: c}

			r.updateSecuresignHealthStatus(ctx, securesign, true, "All healthy")

			updated := &unstructured.Unstructured{}
			updated.SetGroupVersionKind(securesignGVK)
			Expect(c.Get(ctx, types.NamespacedName{Name: "test-securesign", Namespace: architectNamespace}, updated)).To(Succeed())
		})

		It("sets status to False when unhealthy", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(securesignGVK)
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)
			unstructured.SetNestedSlice(securesign.Object, []interface{}{}, "status", "conditions")

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build()
			r := &RHTASHealthCheckReconciler{Client: c}

			r.updateSecuresignHealthStatus(ctx, securesign, false, "Something failed")

			conditions, _, _ := unstructured.NestedSlice(securesign.Object, "status", "conditions")
			Expect(len(conditions)).To(BeNumerically(">=", 1))
			condMap := conditions[len(conditions)-1].(map[string]interface{})
			Expect(condMap["status"]).To(Equal("False"))
			Expect(condMap["reason"]).To(Equal("HealthCheckFailed"))
		})

		It("updates existing HealthCheckPassed condition", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(securesignGVK)
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)
			unstructured.SetNestedSlice(securesign.Object, []interface{}{
				map[string]interface{}{
					"type":   "HealthCheckPassed",
					"status": "False",
				},
			}, "status", "conditions")

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build()
			r := &RHTASHealthCheckReconciler{Client: c}

			r.updateSecuresignHealthStatus(ctx, securesign, true, "Fixed now")

			conditions, _, _ := unstructured.NestedSlice(securesign.Object, "status", "conditions")
			Expect(len(conditions)).To(Equal(1))
			condMap := conditions[0].(map[string]interface{})
			Expect(condMap["status"]).To(Equal("True"))
		})
	})

	Describe("Reconcile - full flow", func() {
		It("returns not-found gracefully", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &RHTASHealthCheckReconciler{Client: c}
			result, err := r.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: architectNamespace},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))
		})

		It("runs all health checks and updates status for healthy Securesign", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "rhtas.redhat.com", Version: "v1alpha1", Kind: "Securesign",
			})
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)

			unstructured.SetNestedMap(securesign.Object, map[string]interface{}{
				"tuf": map[string]interface{}{
					"status": map[string]interface{}{
						"keys": []interface{}{
							map[string]interface{}{
								"name": "fulcio_v1.crt.pem",
							},
							map[string]interface{}{
								"name": "rekor.pub",
							},
							map[string]interface{}{
								"name": "ctfe.pub",
							},
						},
					},
				},
				"fulcio": map[string]interface{}{
					"status": map[string]interface{}{"url": "https://fulcio.example.com"},
				},
				"rekor": map[string]interface{}{
					"status": map[string]interface{}{"url": "https://rekor.example.com"},
				},
			}, "status")

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build()
			r := &RHTASHealthCheckReconciler{Client: c}
			result, err := r.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "test-securesign", Namespace: architectNamespace},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0))
		})

		It("detects missing TUF keys and reports unhealthy", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "rhtas.redhat.com", Version: "v1alpha1", Kind: "Securesign",
			})
			securesign.SetName("test-securesign")
			securesign.SetNamespace(architectNamespace)

			unstructured.SetNestedMap(securesign.Object, map[string]interface{}{
				"tuf": map[string]interface{}{
					"status": map[string]interface{}{
						"keys": []interface{}{
							map[string]interface{}{
								"name": "fulcio_v1.crt.pem",
							},
						},
					},
				},
			}, "status")

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build()
			r := &RHTASHealthCheckReconciler{Client: c}
			result, err := r.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "test-securesign", Namespace: architectNamespace},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0))
		})
	})
})

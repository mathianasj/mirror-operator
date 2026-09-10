package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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
})

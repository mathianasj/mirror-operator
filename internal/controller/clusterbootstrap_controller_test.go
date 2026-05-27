package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

var _ = Describe("ClusterBootstrapReconciler", func() {
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
		It("handles a ClusterBootstrap that does not exist", func() {
			r := &ClusterBootstrapReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "missing", Namespace: "default"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))
		})

		It("adds finalizer on first reconcile", func() {
			bootstrap := &mirrorv1.ClusterBootstrap{
				ObjectMeta: metav1.ObjectMeta{Name: "test-bootstrap", Namespace: "default"},
				Spec: mirrorv1.ClusterBootstrapSpec{
					Version:  "v2026.05.26.001",
					Platform: "vsphere",
					InstallConfig: corev1.LocalObjectReference{
						Name: "install-config",
					},
					MirrorRegistry: "quay.internal:8443/mirror",
					PullSecret: corev1.LocalObjectReference{
						Name: "pull-secret",
					},
				},
			}

			r := &ClusterBootstrapReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(bootstrap).Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-bootstrap", Namespace: "default"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			updated := &mirrorv1.ClusterBootstrap{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-bootstrap", Namespace: "default"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Finalizers).To(ContainElement(bootstrapFinalizer))
		})

		It("sets phase to Pending after finalizer", func() {
			bootstrap := &mirrorv1.ClusterBootstrap{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-bootstrap",
					Namespace:  "default",
					Finalizers: []string{bootstrapFinalizer},
				},
				Spec: mirrorv1.ClusterBootstrapSpec{
					Version:  "v2026.05.26.001",
					Platform: "vsphere",
					InstallConfig: corev1.LocalObjectReference{
						Name: "install-config",
					},
					MirrorRegistry: "quay.internal:8443/mirror",
					PullSecret: corev1.LocalObjectReference{
						Name: "pull-secret",
					},
				},
			}

			r := &ClusterBootstrapReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.ClusterBootstrap{}).
					WithObjects(bootstrap).
					Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-bootstrap", Namespace: "default"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			updated := &mirrorv1.ClusterBootstrap{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-bootstrap", Namespace: "default"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.Phase).To(Equal(mirrorv1.BootstrapPhasePending))
			Expect(result).To(Equal(reconcile.Result{}))
		})

		It("removes finalizer on deletion so object can be garbage collected", func() {
			now := metav1.Now()
			bootstrap := &mirrorv1.ClusterBootstrap{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-bootstrap",
					Namespace:         "default",
					Finalizers:        []string{bootstrapFinalizer},
					DeletionTimestamp: &now,
				},
			}

			r := &ClusterBootstrapReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(bootstrap).Build(),
				Scheme: testScheme,
			}

			_, err := r.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "test-bootstrap", Namespace: "default"},
			})
			Expect(err).NotTo(HaveOccurred())

			By("object is deleted after finalizer is removed")
			updated := &mirrorv1.ClusterBootstrap{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-bootstrap", Namespace: "default"}, updated)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})
})

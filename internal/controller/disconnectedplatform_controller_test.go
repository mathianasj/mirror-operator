package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

var _ = Describe("DisconnectedPlatformReconciler", func() {
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
		It("handles a DisconnectedPlatform that does not exist", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "missing", Namespace: "default"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))
		})

		It("adds finalizer on first reconcile", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			updated := &mirrorv1.DisconnectedPlatform{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Finalizers).To(ContainElement(platformFinalizer))
		})

		It("sets phase to Ready after finalizer", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Finalizers: []string{platformFinalizer},
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform).
					Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			updated := &mirrorv1.DisconnectedPlatform{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.Phase).To(Equal(mirrorv1.PlatformPhaseReady))
			Expect(result).To(Equal(reconcile.Result{}))
		})

		It("removes finalizer on deletion so object can be garbage collected", func() {
			now := metav1.Now()
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-platform",
					Finalizers:        []string{platformFinalizer},
					DeletionTimestamp: &now,
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			_, err := r.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "test-platform"},
			})
			Expect(err).NotTo(HaveOccurred())

			By("object is deleted after finalizer is removed")
			updated := &mirrorv1.DisconnectedPlatform{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	Describe("collectionVersionComplete", func() {
		It("returns true for Complete", func() {
			Expect(collectionVersionComplete("Complete")).To(BeTrue())
		})

		It("returns true for Succeeded", func() {
			Expect(collectionVersionComplete("Succeeded")).To(BeTrue())
		})

		It("returns false for other phases", func() {
			Expect(collectionVersionComplete("Pending")).To(BeFalse())
			Expect(collectionVersionComplete("Failed")).To(BeFalse())
			Expect(collectionVersionComplete("Collecting")).To(BeFalse())
			Expect(collectionVersionComplete("")).To(BeFalse())
		})
	})

	Describe("getOperatorOverrides", func() {
		It("returns nil when no operator config", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			Expect(getOperatorOverrides(platform)).To(BeNil())
		})

		It("returns overrides when configured", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Connected: &mirrorv1.ConnectedConfig{
						Operators: &mirrorv1.OperatorConfig{
							OpenShiftPipelines: &mirrorv1.OLMSubscriptionConfig{
								Channel: "pipelines-1.16",
							},
							RHTAS: &mirrorv1.OLMSubscriptionConfig{
								Disabled: true,
							},
						},
					},
				},
			}
			overrides := getOperatorOverrides(platform)
			Expect(overrides).To(HaveLen(2))
			Expect(overrides["openshift-pipelines"].Channel).To(Equal("pipelines-1.16"))
			Expect(overrides["trusted-artifact-signer"].Disabled).To(BeTrue())
		})

		It("returns nil when Connected is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{}
			Expect(getOperatorOverrides(platform)).To(BeNil())
		})
	})

	Describe("connected mode subscriptions", func() {
		It("creates OLM subscriptions for all operators", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Finalizers: []string{platformFinalizer},
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform).
					Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			updated := &mirrorv1.DisconnectedPlatform{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.Phase).To(Equal(mirrorv1.PlatformPhaseReady))

			// Verify components include operator statuses
			components := updated.Status.Components
			names := make(map[string]string)
			for _, c := range components {
				names[c.Name] = c.Status
			}
			Expect(names).To(HaveKey("openshift-pipelines"))
			Expect(names).To(HaveKey("trusted-artifact-signer"))
			Expect(names).To(HaveKey("trusted-profile-analyzer"))
			Expect(names).To(HaveKey("disconnected-platform"))
		})

		It("skips disabled operators", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Finalizers: []string{platformFinalizer},
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						Operators: &mirrorv1.OperatorConfig{
							RHTAS: &mirrorv1.OLMSubscriptionConfig{
								Disabled: true,
							},
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform).
					Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			updated := &mirrorv1.DisconnectedPlatform{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.Phase).To(Equal(mirrorv1.PlatformPhaseReady))

			names := make(map[string]string)
			for _, c := range updated.Status.Components {
				names[c.Name] = c.Status
			}
			Expect(names["trusted-artifact-signer"]).To(Equal("Disabled"))
			Expect(names).To(HaveKey("openshift-pipelines"))
			Expect(names).To(HaveKey("trusted-profile-analyzer"))
		})
	})

	Describe("aggregation", func() {
		It("aggregates collection history from completed CollectionPipeline resources", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Finalizers: []string{platformFinalizer},
				},
			}
			pipeline := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pipeline",
					Namespace: "default",
				},
				Status: mirrorv1.CollectionPipelineStatus{
					Version: "v2025.01.15.001-manual",
					Phase:   "Complete",
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform, pipeline).
					Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			updated := &mirrorv1.DisconnectedPlatform{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.CollectionHistory).To(HaveLen(1))
			Expect(updated.Status.CollectionHistory[0].Version).To(Equal("v2025.01.15.001-manual"))
			Expect(updated.Status.LastCollection).NotTo(BeNil())
			Expect(updated.Status.LastCollection.Version).To(Equal("v2025.01.15.001-manual"))
		})

		It("skips in-flight pipelines in collection history", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Finalizers: []string{platformFinalizer},
				},
			}
			pipeline := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pipeline",
					Namespace: "default",
				},
				Status: mirrorv1.CollectionPipelineStatus{
					Version: "v2025.01.15.001-manual",
					Phase:   "Collecting",
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform, pipeline).
					Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform"}}
			_, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			updated := &mirrorv1.DisconnectedPlatform{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.CollectionHistory).To(BeEmpty())
		})

		It("aggregates import history from completed MirrorImport resources", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Finalizers: []string{platformFinalizer},
				},
			}
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-import",
					Namespace: "default",
				},
				Spec: mirrorv1.MirrorImportSpec{
					CollectionVersion: "v2025.01.15.001-manual",
				},
				Status: mirrorv1.MirrorImportStatus{
					Phase: "Complete",
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform, importCR).
					Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			updated := &mirrorv1.DisconnectedPlatform{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.ImportHistory).To(HaveLen(1))
			Expect(updated.Status.ImportHistory[0].Version).To(Equal("v2025.01.15.001-manual"))
			Expect(updated.Status.LastImport).NotTo(BeNil())
			Expect(updated.Status.LastImport.Version).To(Equal("v2025.01.15.001-manual"))
		})

		It("skips in-flight imports in import history", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Finalizers: []string{platformFinalizer},
				},
			}
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-import",
					Namespace: "default",
				},
				Spec: mirrorv1.MirrorImportSpec{
					CollectionVersion: "v2025.01.15.001-manual",
				},
				Status: mirrorv1.MirrorImportStatus{
					Phase: "Importing",
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform, importCR).
					Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform"}}
			_, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			updated := &mirrorv1.DisconnectedPlatform{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.ImportHistory).To(BeEmpty())
		})
	})
})

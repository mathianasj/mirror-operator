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
	"sigs.k8s.io/controller-runtime/pkg/client"
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

	Describe("architect reconciliation", func() {
		It("creates frontend and backend deployments when architect is enabled", func() {
			replicas := int32(1)
			pullSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "openshift-config"},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Finalizers: []string{platformFinalizer},
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Architect: &mirrorv1.AirgapArchitectConfig{
						Enabled:       true,
						Replicas:      replicas,
						FrontendImage: "quay.io/mathianasj/openshift-airgap-architect-frontend:latest",
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform, pullSecret).
					Build(),
				Scheme:                 testScheme,
				ArchitectFrontendImage: "quay.io/mathianasj/openshift-airgap-architect-frontend:latest",
				ArchitectBackendImage:  "quay.io/mathianasj/openshift-airgap-architect-backend:latest",
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			updated := &mirrorv1.DisconnectedPlatform{}
			_ = r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)

			names := make(map[string]string)
			for _, c := range updated.Status.Components {
				names[c.Name] = c.Status
			}
			Expect(names).To(HaveKey("airgap-architect-frontend"))
			Expect(names).To(HaveKey("airgap-architect-backend"))
			Expect(names["airgap-architect-frontend"]).To(Equal("Running"))
			Expect(names["airgap-architect-backend"]).To(Equal("Running"))
		})

		It("creates route when route config is provided", func() {
			pullSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "openshift-config"},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Finalizers: []string{platformFinalizer},
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Architect: &mirrorv1.AirgapArchitectConfig{
						Enabled:       true,
						FrontendImage: "quay.io/mathianasj/openshift-airgap-architect-frontend:latest",
						Route: &mirrorv1.RouteConfig{
							Host: "architect.apps.example.com",
							TLS: &mirrorv1.TLSConfig{
								Termination: "edge",
							},
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform, pullSecret).
					Build(),
				Scheme:                 testScheme,
				ArchitectFrontendImage: "quay.io/mathianasj/openshift-airgap-architect-frontend:latest",
				ArchitectBackendImage:  "quay.io/mathianasj/openshift-airgap-architect-backend:latest",
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			updated := &mirrorv1.DisconnectedPlatform{}
			_ = r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)

			names := make(map[string]string)
			for _, c := range updated.Status.Components {
				names[c.Name] = c.Status
			}
			Expect(names).To(HaveKey("airgap-architect-frontend"))
			Expect(names).To(HaveKey("airgap-architect-backend"))
		})

		It("does not create architect resources when architect is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Finalizers: []string{platformFinalizer},
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform).
					Build(),
				Scheme:                 testScheme,
				ArchitectFrontendImage: "quay.io/mathianasj/openshift-airgap-architect-frontend:latest",
				ArchitectBackendImage:  "quay.io/mathianasj/openshift-airgap-architect-backend:latest",
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			updated := &mirrorv1.DisconnectedPlatform{}
			_ = r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)

			for _, c := range updated.Status.Components {
				Expect(c.Name).NotTo(Equal("airgap-architect-frontend"))
				Expect(c.Name).NotTo(Equal("airgap-architect-backend"))
			}
		})

		It("deletes all architect resources when disabled after being enabled", func() {
			backendDepName := "mo-test-platform-airgap-architect-backend"
			frontendDepName := "mo-test-platform-airgap-architect-frontend"
			backendSvcName := backendDepName
			frontendSvcName := frontendDepName

			backendDep := &unstructured.Unstructured{}
			backendDep.SetGroupVersionKind(deploymentGVK)
			backendDep.SetName(backendDepName)
			backendDep.SetNamespace(architectNamespace)

			frontendDep := &unstructured.Unstructured{}
			frontendDep.SetGroupVersionKind(deploymentGVK)
			frontendDep.SetName(frontendDepName)
			frontendDep.SetNamespace(architectNamespace)

			backendSvc := &unstructured.Unstructured{}
			backendSvc.SetGroupVersionKind(serviceGVK)
			backendSvc.SetName(backendSvcName)
			backendSvc.SetNamespace(architectNamespace)

			frontendSvc := &unstructured.Unstructured{}
			frontendSvc.SetGroupVersionKind(serviceGVK)
			frontendSvc.SetName(frontendSvcName)
			frontendSvc.SetNamespace(architectNamespace)

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Finalizers: []string{platformFinalizer},
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      mirrorv1.PlatformModeAirgapped,
					Architect: &mirrorv1.AirgapArchitectConfig{Enabled: false},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform, backendDep, frontendDep, backendSvc, frontendSvc).
					Build(),
				Scheme:                 testScheme,
				ArchitectFrontendImage: "quay.io/mathianasj/openshift-airgap-architect-frontend:latest",
				ArchitectBackendImage:  "quay.io/mathianasj/openshift-airgap-architect-backend:latest",
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform"}}
			_, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			checkDeleted := func(name string) {
				obj := &unstructured.Unstructured{}
				obj.SetGroupVersionKind(deploymentGVK)
				obj.SetName(name)
				obj.SetNamespace(architectNamespace)
				err := r.Get(ctx, client.ObjectKeyFromObject(obj), obj)
				Expect(apierrors.IsNotFound(err)).To(BeTrue())
			}
			checkDeleted(backendDepName)
			checkDeleted(frontendDepName)

			checkDeletedSvc := func(name string) {
				obj := &unstructured.Unstructured{}
				obj.SetGroupVersionKind(serviceGVK)
				obj.SetName(name)
				obj.SetNamespace(architectNamespace)
				err := r.Get(ctx, client.ObjectKeyFromObject(obj), obj)
				Expect(apierrors.IsNotFound(err)).To(BeTrue())
			}
			checkDeletedSvc(backendSvcName)
			checkDeletedSvc(frontendSvcName)
		})

		It("deletes architect resources on finalizer cleanup", func() {
			now := metav1.Now()
			backendDepName := "mo-test-platform-airgap-architect-backend"

			backendDep := &unstructured.Unstructured{}
			backendDep.SetGroupVersionKind(deploymentGVK)
			backendDep.SetName(backendDepName)
			backendDep.SetNamespace(architectNamespace)

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-platform",
					Finalizers:        []string{platformFinalizer},
					DeletionTimestamp: &now,
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Architect: &mirrorv1.AirgapArchitectConfig{
						Enabled: true,
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform, backendDep).
					Build(),
				Scheme:                 testScheme,
				ArchitectFrontendImage: "quay.io/mathianasj/openshift-airgap-architect-frontend:latest",
				ArchitectBackendImage:  "quay.io/mathianasj/openshift-airgap-architect-backend:latest",
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform"}}
			_, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			obj := &unstructured.Unstructured{}
			obj.SetGroupVersionKind(deploymentGVK)
			obj.SetName(backendDepName)
			obj.SetNamespace(architectNamespace)
			err = r.Get(ctx, client.ObjectKeyFromObject(obj), obj)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			updated := &mirrorv1.DisconnectedPlatform{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
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

	Describe("cluster CA bundle", func() {
		It("creates the cluster-ca-bundle ConfigMap with injection label", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			err := r.ensureClusterCABundle(ctx)
			Expect(err).NotTo(HaveOccurred())

			cm := &corev1.ConfigMap{}
			err = r.Get(ctx, types.NamespacedName{Name: clusterCABundleName, Namespace: architectNamespace}, cm)
			Expect(err).NotTo(HaveOccurred())
			Expect(cm.Labels).To(HaveKeyWithValue("config.openshift.io/inject-ca-bundle", "true"))
		})

		It("does not error when ConfigMap already exists with correct label", func() {
			existing := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterCABundleName,
					Namespace: architectNamespace,
					Labels: map[string]string{
						"config.openshift.io/inject-ca-bundle": "true",
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}

			err := r.ensureClusterCABundle(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("includes CA volume and env in backend deployment", func() {
			container := makeBackendContainerBuilder("", "connected", nil)("test-backend", "test-image:latest", map[string]string{})
			mounts := container["volumeMounts"].([]interface{})
			foundMount := false
			for _, m := range mounts {
				mount := m.(map[string]interface{})
				if mount["name"] == clusterCAVolumeName {
					foundMount = true
					Expect(mount["mountPath"]).To(Equal(clusterCAMountPath))
					Expect(mount["readOnly"]).To(BeTrue())
				}
			}
			Expect(foundMount).To(BeTrue(), "expected cluster-ca-bundle volume mount in backend container")

			envVars := container["env"].([]interface{})
			foundEnv := false
			for _, e := range envVars {
				env := e.(map[string]interface{})
				if env["name"] == "NODE_EXTRA_CA_CERTS" {
					foundEnv = true
					Expect(env["value"]).To(Equal(clusterCAFilePath))
				}
			}
			Expect(foundEnv).To(BeTrue(), "expected NODE_EXTRA_CA_CERTS env in backend container")
		})

		It("includes CA volume and env in frontend deployment", func() {
			container := frontendContainer("test-frontend", "test-image:latest", map[string]string{"app.kubernetes.io/component": "frontend"}, "", "")
			mounts := container["volumeMounts"].([]interface{})
			foundMount := false
			for _, m := range mounts {
				mount := m.(map[string]interface{})
				if mount["name"] == clusterCAVolumeName {
					foundMount = true
					Expect(mount["mountPath"]).To(Equal(clusterCAMountPath))
					Expect(mount["readOnly"]).To(BeTrue())
				}
			}
			Expect(foundMount).To(BeTrue(), "expected cluster-ca-bundle volume mount in frontend container")

			envVars := container["env"].([]interface{})
			foundEnv := false
			for _, e := range envVars {
				env := e.(map[string]interface{})
				if env["name"] == "NODE_EXTRA_CA_CERTS" {
					foundEnv = true
					Expect(env["value"]).To(Equal(clusterCAFilePath))
				}
			}
			Expect(foundEnv).To(BeTrue(), "expected NODE_EXTRA_CA_CERTS env in frontend container")
		})

		It("includes CA volume in deployment spec for backend and frontend components", func() {
			for _, component := range []string{"backend", "frontend"} {
				spec := architectDeploymentSpec("test", "test:latest", 1, map[string]string{"app.kubernetes.io/component": component}, "pull-secret", "openshift-config", func(name, image string, labels map[string]string) map[string]interface{} {
					return map[string]interface{}{"name": "test"}
				})
				volumes, _, _ := unstructured.NestedSlice(map[string]interface{}{"spec": spec}, "spec", "template", "spec", "volumes")
				foundVol := false
				for _, v := range volumes {
					vol := v.(map[string]interface{})
					if vol["name"] == clusterCAVolumeName {
						foundVol = true
						cmSource := vol["configMap"].(map[string]interface{})
						Expect(cmSource["name"]).To(Equal(clusterCABundleName))
						Expect(cmSource["optional"]).To(BeTrue())
					}
				}
				Expect(foundVol).To(BeTrue(), "expected cluster-ca-bundle volume in %s deployment spec", component)
			}
		})
	})

	Describe("injectCABundleIntoTasks", func() {
		It("adds workspace and env to all tasks", func() {
			tasks := []map[string]interface{}{
				{
					"name": "task1",
					"taskSpec": map[string]interface{}{
						"steps": []map[string]interface{}{
							{"name": "step1", "image": "test:latest"},
						},
					},
					"workspaces": []map[string]interface{}{
						{"name": "output"},
					},
				},
				{
					"name": "task2",
					"taskSpec": map[string]interface{}{
						"steps": []map[string]interface{}{
							{
								"name":  "step2",
								"image": "test:latest",
								"env": []map[string]interface{}{
									{"name": "EXISTING_VAR", "value": "val"},
								},
							},
						},
					},
					"workspaces": []map[string]interface{}{
						{"name": "config"},
					},
				},
			}

			result := injectCABundleIntoTasks(tasks)
			Expect(result).To(HaveLen(2))

			caPath := "/workspace/cluster-ca-bundle/" + clusterCABundleKey
			for _, task := range result {
				ws := task["workspaces"].([]map[string]interface{})
				lastWs := ws[len(ws)-1]
				Expect(lastWs["name"]).To(Equal("cluster-ca-bundle"))

				taskSpec := task["taskSpec"].(map[string]interface{})
				steps := taskSpec["steps"].([]map[string]interface{})
				for _, step := range steps {
					env := step["env"].([]map[string]interface{})
					envNames := make(map[string]string)
					for _, e := range env {
						envNames[e["name"].(string)] = e["value"].(string)
					}
					Expect(envNames).To(HaveKeyWithValue("SSL_CERT_FILE", caPath))
					Expect(envNames).To(HaveKeyWithValue("AWS_CA_BUNDLE", caPath))
					Expect(envNames).To(HaveKeyWithValue("REQUESTS_CA_BUNDLE", caPath))
				}
			}

			// Verify existing env is preserved in task2
			task2Spec := result[1]["taskSpec"].(map[string]interface{})
			task2Steps := task2Spec["steps"].([]map[string]interface{})
			task2Env := task2Steps[0]["env"].([]map[string]interface{})
			Expect(task2Env).To(HaveLen(4))
			Expect(task2Env[0]["name"]).To(Equal("EXISTING_VAR"))
		})
	})

	Describe("Quay CA injection", func() {
		It("injects CA bundle into config bundle secret when ConfigMap exists", func() {
			caCM := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterCABundleName,
					Namespace: architectNamespace,
				},
				Data: map[string]string{
					clusterCABundleKey: "-----BEGIN CERTIFICATE-----\ntest-ca-data\n-----END CERTIFICATE-----",
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(caCM).Build(),
				Scheme: testScheme,
			}

			quayRegistry := &unstructured.Unstructured{}
			quayRegistry.SetGroupVersionKind(schema.GroupVersionKind{Group: "quay.redhat.com", Version: "v1", Kind: "QuayRegistry"})
			quayRegistry.SetName("mirror-operator-quay")
			quayRegistry.SetNamespace(architectNamespace)

			err := r.ensureQuayCAInConfigBundle(ctx, quayRegistry)
			Expect(err).NotTo(HaveOccurred())

			secret := &corev1.Secret{}
			err = r.Get(ctx, types.NamespacedName{Name: "mirror-operator-quay-config-bundle", Namespace: architectNamespace}, secret)
			Expect(err).NotTo(HaveOccurred())
			Expect(secret.Data).To(HaveKey("extra_ca_cert_cluster-ca.crt"))
			Expect(string(secret.Data["extra_ca_cert_cluster-ca.crt"])).To(ContainSubstring("test-ca-data"))
		})

		It("skips silently when CA ConfigMap does not exist", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			quayRegistry := &unstructured.Unstructured{}
			quayRegistry.SetGroupVersionKind(schema.GroupVersionKind{Group: "quay.redhat.com", Version: "v1", Kind: "QuayRegistry"})
			quayRegistry.SetName("mirror-operator-quay")
			quayRegistry.SetNamespace(architectNamespace)

			err := r.ensureQuayCAInConfigBundle(ctx, quayRegistry)
			Expect(err).NotTo(HaveOccurred())
		})

		It("updates existing config bundle secret with CA data", func() {
			caCM := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterCABundleName,
					Namespace: architectNamespace,
				},
				Data: map[string]string{
					clusterCABundleKey: "new-ca-data",
				},
			}
			existingSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-quay-config-bundle",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"config.yaml": []byte("existing-config"),
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(caCM, existingSecret).Build(),
				Scheme: testScheme,
			}

			quayRegistry := &unstructured.Unstructured{}
			quayRegistry.SetGroupVersionKind(schema.GroupVersionKind{Group: "quay.redhat.com", Version: "v1", Kind: "QuayRegistry"})
			quayRegistry.SetName("mirror-operator-quay")
			quayRegistry.SetNamespace(architectNamespace)

			err := r.ensureQuayCAInConfigBundle(ctx, quayRegistry)
			Expect(err).NotTo(HaveOccurred())

			secret := &corev1.Secret{}
			err = r.Get(ctx, types.NamespacedName{Name: "mirror-operator-quay-config-bundle", Namespace: architectNamespace}, secret)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(secret.Data["extra_ca_cert_cluster-ca.crt"])).To(Equal("new-ca-data"))
			Expect(string(secret.Data["config.yaml"])).To(Equal("existing-config"))
		})
	})

	Describe("Keycloak CA injection", func() {
		It("includes unsupported.podTemplate with CA volume in kcSpec", func() {
			kcSpec := map[string]interface{}{
				"instances": int64(1),
				"hostname": map[string]interface{}{
					"hostname": "keycloak.example.com",
				},
				"http": map[string]interface{}{
					"tlsSecret": "test-tls",
				},
				"additionalOptions": []map[string]interface{}{
					{"name": "KEYCLOAK_ADMIN", "value": "admin"},
					{"name": "spi-truststore-file-file", "value": clusterCAFilePath},
					{"name": "spi-truststore-file-type", "value": "pem"},
				},
				"unsupported": map[string]interface{}{
					"podTemplate": map[string]interface{}{
						"spec": map[string]interface{}{
							"containers": []map[string]interface{}{
								{
									"volumeMounts": []map[string]interface{}{
										{
											"name":      clusterCAVolumeName,
											"mountPath": clusterCAMountPath,
											"readOnly":  true,
										},
									},
								},
							},
							"volumes": []map[string]interface{}{
								{
									"name": clusterCAVolumeName,
									"configMap": map[string]interface{}{
										"name":     clusterCABundleName,
										"optional": true,
									},
								},
							},
						},
					},
				},
			}

			// Verify unsupported.podTemplate structure
			unsupported, ok := kcSpec["unsupported"].(map[string]interface{})
			Expect(ok).To(BeTrue())
			podTemplate, ok := unsupported["podTemplate"].(map[string]interface{})
			Expect(ok).To(BeTrue())
			spec, ok := podTemplate["spec"].(map[string]interface{})
			Expect(ok).To(BeTrue())

			volumes := spec["volumes"].([]map[string]interface{})
			Expect(volumes).To(HaveLen(1))
			Expect(volumes[0]["name"]).To(Equal(clusterCAVolumeName))
			cm := volumes[0]["configMap"].(map[string]interface{})
			Expect(cm["name"]).To(Equal(clusterCABundleName))

			containers := spec["containers"].([]map[string]interface{})
			Expect(containers).To(HaveLen(1))
			mounts := containers[0]["volumeMounts"].([]map[string]interface{})
			Expect(mounts).To(HaveLen(1))
			Expect(mounts[0]["name"]).To(Equal(clusterCAVolumeName))
			Expect(mounts[0]["mountPath"]).To(Equal(clusterCAMountPath))

			// Verify SPI truststore in additionalOptions
			opts := kcSpec["additionalOptions"].([]map[string]interface{})
			foundFile := false
			foundType := false
			for _, opt := range opts {
				if opt["name"] == "spi-truststore-file-file" {
					foundFile = true
					Expect(opt["value"]).To(Equal(clusterCAFilePath))
				}
				if opt["name"] == "spi-truststore-file-type" {
					foundType = true
					Expect(opt["value"]).To(Equal("pem"))
				}
			}
			Expect(foundFile).To(BeTrue(), "expected spi-truststore-file-file in additionalOptions")
			Expect(foundType).To(BeTrue(), "expected spi-truststore-file-type in additionalOptions")
		})
	})
})

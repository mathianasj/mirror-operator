package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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

	Describe("isCRDNotFoundError", func() {
		It("returns true for no-matches error", func() {
			err := fmt.Errorf("no matches for kind \"Foo\" in version \"bar/v1\"")
			Expect(isCRDNotFoundError(err)).To(BeTrue())
		})

		It("returns false for other errors", func() {
			err := fmt.Errorf("connection refused")
			Expect(isCRDNotFoundError(err)).To(BeFalse())
		})
	})

	Describe("extractPSKKey", func() {
		It("extracts key from config string", func() {
			config := "some: value\nkey: my-secret-key\nother: data"
			Expect(extractPSKKey(config)).To(Equal("my-secret-key"))
		})

		It("returns empty string when key is absent", func() {
			config := "some: value\nother: data"
			Expect(extractPSKKey(config)).To(BeEmpty())
		})

		It("handles empty config", func() {
			Expect(extractPSKKey("")).To(BeEmpty())
		})
	})

	Describe("extractValue", func() {
		It("extracts value for a given key", func() {
			config := "host: localhost\nport: 5432\nname: mydb"
			Expect(extractValue(config, "port")).To(Equal("5432"))
		})

		It("returns empty string when key is absent", func() {
			config := "host: localhost"
			Expect(extractValue(config, "port")).To(BeEmpty())
		})

		It("handles key at the end of string", func() {
			config := "host: localhost"
			Expect(extractValue(config, "host")).To(Equal("localhost"))
		})
	})

	Describe("hashString", func() {
		It("returns a 16-character hex string", func() {
			result := hashString("test-input")
			Expect(result).To(HaveLen(16))
		})

		It("is deterministic", func() {
			Expect(hashString("hello")).To(Equal(hashString("hello")))
		})

		It("produces different hashes for different inputs", func() {
			Expect(hashString("hello")).NotTo(Equal(hashString("world")))
		})
	})

	Describe("generateRandomString", func() {
		It("returns string of requested length", func() {
			result := generateRandomString(10)
			Expect(result).To(HaveLen(10))
		})

		It("returns empty string for zero length", func() {
			Expect(generateRandomString(0)).To(BeEmpty())
		})

		It("is deterministic (uses index-based charset)", func() {
			a := generateRandomString(5)
			b := generateRandomString(5)
			Expect(a).To(Equal(b))
		})
	})

	Describe("architectResourceName", func() {
		It("generates name with mo prefix", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "my-platform"},
			}
			name := architectResourceName(platform, "backend")
			Expect(name).To(Equal("mo-my-platform-backend"))
		})

		It("stays within 63 character limit", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "this-is-a-very-long-platform-name-that-exceeds-reasonable-limits-for-k8s"},
			}
			name := architectResourceName(platform, "airgap-architect-frontend")
			Expect(len(name)).To(BeNumerically("<=", 63))
			Expect(name).To(HavePrefix("mo-"))
			Expect(name).To(HaveSuffix("-airgap-architect-frontend"))
		})

		It("uses hash for long platform names", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "extremely-long-name-that-will-cause-truncation-issues-in-kubernetes"},
			}
			name := architectResourceName(platform, "backend")
			Expect(name).To(HavePrefix("mo-"))
			Expect(name).NotTo(ContainSubstring("extremely-long"))
		})
	})

	Describe("architectComponentLabels", func() {
		It("returns standard labels for a component", func() {
			labels := architectComponentLabels("backend")
			Expect(labels["app.kubernetes.io/name"]).To(Equal("airgap-architect-backend"))
			Expect(labels["app.kubernetes.io/component"]).To(Equal("backend"))
			Expect(labels["app.kubernetes.io/part-of"]).To(Equal("mirror-operator"))
			Expect(labels["app.kubernetes.io/managed-by"]).To(Equal("mirror-operator"))
		})

		It("creates different labels for different components", func() {
			backend := architectComponentLabels("backend")
			frontend := architectComponentLabels("frontend")
			Expect(backend["app.kubernetes.io/name"]).NotTo(Equal(frontend["app.kubernetes.io/name"]))
		})
	})

	Describe("setOwnerReference", func() {
		It("sets owner reference on unstructured object", func() {
			obj := &unstructured.Unstructured{}
			obj.SetGroupVersionKind(deploymentGVK)
			obj.SetName("test-dep")

			owner := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					UID:        "uid-123",
					Finalizers: []string{platformFinalizer},
				},
				TypeMeta: metav1.TypeMeta{
					APIVersion: "mirror.mirror.mathianasj.github.com/v1",
					Kind:       "DisconnectedPlatform",
				},
			}

			setOwnerReference(obj, owner)
			refs := obj.GetOwnerReferences()
			Expect(refs).To(HaveLen(1))
			Expect(refs[0].Name).To(Equal("test-platform"))
			Expect(refs[0].UID).To(Equal(types.UID("uid-123")))
			Expect(*refs[0].Controller).To(BeTrue())
		})
	})

	Describe("getPullSecretReference", func() {
		It("returns default when cfg is nil", func() {
			name, ns := getPullSecretReference(nil)
			Expect(name).To(Equal(defaultPullSecretName))
			Expect(ns).To(Equal(defaultPullSecretNS))
		})

		It("returns default when pullSecret is nil", func() {
			cfg := &mirrorv1.AirgapArchitectConfig{Enabled: true}
			name, ns := getPullSecretReference(cfg)
			Expect(name).To(Equal(defaultPullSecretName))
			Expect(ns).To(Equal(defaultPullSecretNS))
		})

		It("returns custom pull secret when configured", func() {
			cfg := &mirrorv1.AirgapArchitectConfig{
				PullSecret: &corev1.LocalObjectReference{Name: "custom-secret"},
			}
			name, ns := getPullSecretReference(cfg)
			Expect(name).To(Equal("custom-secret"))
			Expect(ns).To(Equal(architectNamespace))
		})
	})

	Describe("taskRunSucceeded", func() {
		It("returns true when Succeeded condition is True", func() {
			u := &unstructured.Unstructured{}
			unstructured.SetNestedSlice(u.Object, []interface{}{
				map[string]interface{}{
					"type":   "Succeeded",
					"status": "True",
				},
			}, "status", "conditions")
			Expect(taskRunSucceeded(u)).To(BeTrue())
		})

		It("returns false when Succeeded condition is False", func() {
			u := &unstructured.Unstructured{}
			unstructured.SetNestedSlice(u.Object, []interface{}{
				map[string]interface{}{
					"type":   "Succeeded",
					"status": "False",
				},
			}, "status", "conditions")
			Expect(taskRunSucceeded(u)).To(BeFalse())
		})

		It("returns false when no conditions", func() {
			u := &unstructured.Unstructured{Object: map[string]interface{}{}}
			Expect(taskRunSucceeded(u)).To(BeFalse())
		})

		It("returns false when Succeeded condition is missing", func() {
			u := &unstructured.Unstructured{}
			unstructured.SetNestedSlice(u.Object, []interface{}{
				map[string]interface{}{
					"type":   "Running",
					"status": "True",
				},
			}, "status", "conditions")
			Expect(taskRunSucceeded(u)).To(BeFalse())
		})
	})

	Describe("buildRHTPAImportersMap", func() {
		It("always includes redhat-csaf", func() {
			m := buildRHTPAImportersMap(nil)
			Expect(m).To(HaveKey("redhat-csaf"))
		})

		It("includes redhat-sboms when configured", func() {
			m := buildRHTPAImportersMap(&mirrorv1.RHTPAImportersConfig{
				RedHatSBOMs: true,
			})
			Expect(m).To(HaveKey("redhat-csaf"))
			Expect(m).To(HaveKey("redhat-sboms"))
		})

		It("includes cve when configured", func() {
			m := buildRHTPAImportersMap(&mirrorv1.RHTPAImportersConfig{
				CVE: true,
			})
			Expect(m).To(HaveKey("cve"))
		})

		It("includes osv-github when configured", func() {
			m := buildRHTPAImportersMap(&mirrorv1.RHTPAImportersConfig{
				OSVGitHub: true,
			})
			Expect(m).To(HaveKey("osv-github"))
		})

		It("includes all importers when all flags are set", func() {
			m := buildRHTPAImportersMap(&mirrorv1.RHTPAImportersConfig{
				RedHatSBOMs: true,
				CVE:         true,
				OSVGitHub:   true,
			})
			Expect(m).To(HaveLen(4))
		})
	})

	Describe("setErrorCondition", func() {
		It("adds Ready=False condition when no conditions exist", func() {
			platform := &mirrorv1.DisconnectedPlatform{}
			r := &DisconnectedPlatformReconciler{}

			r.setErrorCondition(platform, "TestError", "something broke")

			Expect(platform.Status.Conditions).To(HaveLen(1))
			Expect(platform.Status.Conditions[0].Type).To(Equal("Ready"))
			Expect(platform.Status.Conditions[0].Status).To(Equal(metav1.ConditionFalse))
			Expect(platform.Status.Conditions[0].Reason).To(Equal("TestError"))
			Expect(platform.Status.Conditions[0].Message).To(Equal("something broke"))
		})

		It("updates existing Ready condition", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Status: mirrorv1.DisconnectedPlatformStatus{
					Conditions: []metav1.Condition{
						{
							Type:   "Ready",
							Status: metav1.ConditionTrue,
							Reason: "AllGood",
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{}

			r.setErrorCondition(platform, "SomeError", "now broken")

			Expect(platform.Status.Conditions).To(HaveLen(1))
			Expect(platform.Status.Conditions[0].Status).To(Equal(metav1.ConditionFalse))
			Expect(platform.Status.Conditions[0].Reason).To(Equal("SomeError"))
		})

		It("is no-op when condition already matches", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Status: mirrorv1.DisconnectedPlatformStatus{
					Conditions: []metav1.Condition{
						{
							Type:    "Ready",
							Status:  metav1.ConditionFalse,
							Reason:  "TestError",
							Message: "same message",
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{}

			r.setErrorCondition(platform, "TestError", "same message")
			Expect(platform.Status.Conditions).To(HaveLen(1))
		})
	})

	Describe("setReadyCondition", func() {
		It("adds Ready=True condition when no conditions exist", func() {
			platform := &mirrorv1.DisconnectedPlatform{}
			r := &DisconnectedPlatformReconciler{}

			r.setReadyCondition(platform, "AllGood", "everything working")

			Expect(platform.Status.Conditions).To(HaveLen(1))
			Expect(platform.Status.Conditions[0].Type).To(Equal("Ready"))
			Expect(platform.Status.Conditions[0].Status).To(Equal(metav1.ConditionTrue))
			Expect(platform.Status.Conditions[0].Reason).To(Equal("AllGood"))
		})

		It("updates existing Ready condition from False to True", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Status: mirrorv1.DisconnectedPlatformStatus{
					Conditions: []metav1.Condition{
						{
							Type:   "Ready",
							Status: metav1.ConditionFalse,
							Reason: "SomeError",
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{}

			r.setReadyCondition(platform, "Recovered", "all good now")
			Expect(platform.Status.Conditions[0].Status).To(Equal(metav1.ConditionTrue))
		})

		It("is no-op when condition already matches", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Status: mirrorv1.DisconnectedPlatformStatus{
					Conditions: []metav1.Condition{
						{
							Type:    "Ready",
							Status:  metav1.ConditionTrue,
							Reason:  "AllGood",
							Message: "same",
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{}

			r.setReadyCondition(platform, "AllGood", "same")
			Expect(platform.Status.Conditions).To(HaveLen(1))
		})
	})

	Describe("updateDegradedCondition", func() {
		It("adds Degraded condition", func() {
			platform := &mirrorv1.DisconnectedPlatform{}
			r := &DisconnectedPlatformReconciler{}

			r.updateDegradedCondition(ctx, platform, "HealthCheckFailed", "component not ready")

			Expect(platform.Status.Conditions).To(HaveLen(1))
			Expect(platform.Status.Conditions[0].Type).To(Equal("Degraded"))
			Expect(platform.Status.Conditions[0].Status).To(Equal(metav1.ConditionTrue))
			Expect(platform.Status.Conditions[0].Reason).To(Equal("HealthCheckFailed"))
			Expect(platform.Status.Phase).To(Equal(mirrorv1.PlatformPhase("Degraded")))
		})

		It("updates existing Degraded condition with new reason", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Status: mirrorv1.DisconnectedPlatformStatus{
					Conditions: []metav1.Condition{
						{
							Type:   "Degraded",
							Status: metav1.ConditionTrue,
							Reason: "OldReason",
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{}

			r.updateDegradedCondition(ctx, platform, "NewReason", "new issue")

			Expect(platform.Status.Conditions).To(HaveLen(1))
			Expect(platform.Status.Conditions[0].Reason).To(Equal("NewReason"))
		})

		It("does not override Error phase", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Status: mirrorv1.DisconnectedPlatformStatus{
					Phase: mirrorv1.PlatformPhaseError,
				},
			}
			r := &DisconnectedPlatformReconciler{}

			r.updateDegradedCondition(ctx, platform, "HealthCheckFailed", "issue")
			Expect(platform.Status.Phase).To(Equal(mirrorv1.PlatformPhaseError))
		})
	})

	Describe("clearDegradedCondition", func() {
		It("sets Degraded condition to False for matching reason", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Status: mirrorv1.DisconnectedPlatformStatus{
					Conditions: []metav1.Condition{
						{
							Type:   "Degraded",
							Status: metav1.ConditionTrue,
							Reason: "HealthCheckFailed",
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{}

			r.clearDegradedCondition(ctx, platform, "HealthCheckFailed")

			Expect(platform.Status.Conditions[0].Status).To(Equal(metav1.ConditionFalse))
			Expect(platform.Status.Conditions[0].Message).To(Equal("Health check passed"))
		})

		It("does not affect conditions with different reason", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Status: mirrorv1.DisconnectedPlatformStatus{
					Conditions: []metav1.Condition{
						{
							Type:   "Degraded",
							Status: metav1.ConditionTrue,
							Reason: "OtherReason",
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{}

			r.clearDegradedCondition(ctx, platform, "HealthCheckFailed")
			Expect(platform.Status.Conditions[0].Status).To(Equal(metav1.ConditionTrue))
		})
	})

	Describe("architectBackendDeployment", func() {
		It("creates a Deployment with correct structure", func() {
			labels := architectComponentLabels("backend")
			dep := architectBackendDeployment("test-backend", "quay.io/test/backend:v1", 2, labels, "pull-secret", "openshift-config", backendContainer)

			Expect(dep.GetName()).To(Equal("test-backend"))
			Expect(dep.GetNamespace()).To(Equal(architectNamespace))
			Expect(dep.GroupVersionKind()).To(Equal(deploymentGVK))

			replicas, _, _ := unstructured.NestedInt64(dep.Object, "spec", "replicas")
			Expect(replicas).To(Equal(int64(2)))

			containers, _, _ := unstructured.NestedSlice(dep.Object, "spec", "template", "spec", "containers")
			Expect(containers).To(HaveLen(1))
			container := containers[0].(map[string]interface{})
			Expect(container["image"]).To(Equal("quay.io/test/backend:v1"))
		})
	})

	Describe("architectFrontendDeployment", func() {
		It("creates a frontend Deployment with API URL env vars", func() {
			labels := architectComponentLabels("frontend")
			dep := architectFrontendDeployment("test-frontend", "quay.io/test/frontend:v1", 1, labels, "backend.apps.example.com", "frontend.apps.example.com")

			Expect(dep.GetName()).To(Equal("test-frontend"))
			Expect(dep.GetNamespace()).To(Equal(architectNamespace))

			containers, _, _ := unstructured.NestedSlice(dep.Object, "spec", "template", "spec", "containers")
			Expect(containers).To(HaveLen(1))

			container := containers[0].(map[string]interface{})
			envList := container["env"].([]interface{})
			envMap := map[string]string{}
			for _, e := range envList {
				em := e.(map[string]interface{})
				envMap[em["name"].(string)] = em["value"].(string)
			}
			Expect(envMap).To(HaveKey("VITE_API_URL"))
			Expect(envMap["VITE_API_URL"]).To(ContainSubstring("backend.apps.example.com"))
		})
	})

	Describe("architectService", func() {
		It("creates a Service with correct ports and selector", func() {
			labels := map[string]string{"app": "test"}
			svc := architectService("test-svc", 4000, labels, false)

			Expect(svc.GetName()).To(Equal("test-svc"))
			Expect(svc.GetNamespace()).To(Equal(architectNamespace))
			Expect(svc.GroupVersionKind()).To(Equal(serviceGVK))

			ports, _, _ := unstructured.NestedSlice(svc.Object, "spec", "ports")
			Expect(ports).To(HaveLen(1))
			port := ports[0].(map[string]interface{})
			Expect(port["port"]).To(Equal(int64(4000)))
			Expect(port["name"]).To(Equal("http"))
		})

		It("uses https port name when TLS is enabled", func() {
			labels := map[string]string{"app": "test"}
			svc := architectService("test-svc", 4000, labels, true)

			ports, _, _ := unstructured.NestedSlice(svc.Object, "spec", "ports")
			port := ports[0].(map[string]interface{})
			Expect(port["name"]).To(Equal("https"))
		})

		It("adds serving cert annotation when TLS is enabled", func() {
			labels := map[string]string{"app": "test"}
			svc := architectService("test-svc", 4000, labels, true)

			annotations := svc.GetAnnotations()
			Expect(annotations).To(HaveKey("service.beta.openshift.io/serving-cert-secret-name"))
			Expect(annotations["service.beta.openshift.io/serving-cert-secret-name"]).To(Equal("test-svc-cert"))
		})
	})

	Describe("architectRoute", func() {
		It("creates a Route with default edge TLS", func() {
			route := architectRoute("test-route", nil, "test-frontend-svc")

			Expect(route.GetName()).To(Equal("test-route"))
			Expect(route.GetNamespace()).To(Equal(architectNamespace))

			toName, _, _ := unstructured.NestedString(route.Object, "spec", "to", "name")
			Expect(toName).To(Equal("test-frontend-svc"))

			termination, _, _ := unstructured.NestedString(route.Object, "spec", "tls", "termination")
			Expect(termination).To(Equal("edge"))
		})

		It("uses custom host and TLS termination when configured", func() {
			routeCfg := &mirrorv1.RouteConfig{
				Host: "custom.example.com",
				TLS: &mirrorv1.TLSConfig{
					Termination: "passthrough",
				},
			}
			route := architectRoute("test-route", routeCfg, "test-frontend-svc")

			host, _, _ := unstructured.NestedString(route.Object, "spec", "host")
			Expect(host).To(Equal("custom.example.com"))

			termination, _, _ := unstructured.NestedString(route.Object, "spec", "tls", "termination")
			Expect(termination).To(Equal("passthrough"))
		})

		It("sets insecure edge termination policy to Redirect", func() {
			route := architectRoute("test-route", nil, "test-svc")

			policy, _, _ := unstructured.NestedString(route.Object, "spec", "tls", "insecureEdgeTerminationPolicy")
			Expect(policy).To(Equal("Redirect"))
		})
	})

	Describe("deploymentsEqual", func() {
		It("returns true for identical deployment specs", func() {
			spec := map[string]interface{}{
				"replicas": int64(2),
				"template": map[string]interface{}{
					"spec": map[string]interface{}{
						"containers": []interface{}{
							map[string]interface{}{
								"image": "test:v1",
								"env":   []interface{}{},
							},
						},
					},
				},
			}
			equal, reason := deploymentsEqual(spec, spec)
			Expect(equal).To(BeTrue())
			Expect(reason).To(BeEmpty())
		})

		It("detects replica differences", func() {
			existing := map[string]interface{}{"replicas": int64(1)}
			desired := map[string]interface{}{"replicas": int64(3)}
			equal, reason := deploymentsEqual(existing, desired)
			Expect(equal).To(BeFalse())
			Expect(reason).To(Equal("replicas differ"))
		})

		It("detects image differences", func() {
			existing := map[string]interface{}{
				"replicas": int64(1),
				"template": map[string]interface{}{
					"spec": map[string]interface{}{
						"containers": []interface{}{
							map[string]interface{}{"image": "old:v1"},
						},
					},
				},
			}
			desired := map[string]interface{}{
				"replicas": int64(1),
				"template": map[string]interface{}{
					"spec": map[string]interface{}{
						"containers": []interface{}{
							map[string]interface{}{"image": "new:v2"},
						},
					},
				},
			}
			equal, reason := deploymentsEqual(existing, desired)
			Expect(equal).To(BeFalse())
			Expect(reason).To(ContainSubstring("image differs"))
		})

		It("detects container count differences", func() {
			existing := map[string]interface{}{
				"replicas": int64(1),
				"template": map[string]interface{}{
					"spec": map[string]interface{}{
						"containers": []interface{}{
							map[string]interface{}{"image": "a:v1"},
						},
					},
				},
			}
			desired := map[string]interface{}{
				"replicas": int64(1),
				"template": map[string]interface{}{
					"spec": map[string]interface{}{
						"containers": []interface{}{
							map[string]interface{}{"image": "a:v1"},
							map[string]interface{}{"image": "b:v1"},
						},
					},
				},
			}
			equal, reason := deploymentsEqual(existing, desired)
			Expect(equal).To(BeFalse())
			Expect(reason).To(Equal("container count differs"))
		})
	})

	Describe("ensureNamespace", func() {
		It("creates namespace when it does not exist", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			err := r.ensureNamespace(ctx, "test-ns")
			Expect(err).NotTo(HaveOccurred())

			ns := &corev1.Namespace{}
			err = r.Get(ctx, client.ObjectKey{Name: "test-ns"}, ns)
			Expect(err).NotTo(HaveOccurred())
		})

		It("is idempotent — second call succeeds without error", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			Expect(r.ensureNamespace(ctx, "test-ns")).To(Succeed())
			Expect(r.ensureNamespace(ctx, "test-ns")).To(Succeed())
		})

		It("skips openshift-operators namespace", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			err := r.ensureNamespace(ctx, "openshift-operators")
			Expect(err).NotTo(HaveOccurred())

			ns := &corev1.Namespace{}
			err = r.Get(ctx, client.ObjectKey{Name: "openshift-operators"}, ns)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	Describe("ensurePullSecret", func() {
		It("copies pull secret from source namespace to operator namespace", func() {
			sourceSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "openshift-config"},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: architectNamespace},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sourceSecret, ns).Build(),
				Scheme: testScheme,
			}

			err := r.ensurePullSecret(ctx, "pull-secret", "openshift-config")
			Expect(err).NotTo(HaveOccurred())

			target := &corev1.Secret{}
			err = r.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: architectNamespace}, target)
			Expect(err).NotTo(HaveOccurred())
			Expect(target.Data).To(HaveKey(".dockerconfigjson"))
		})

		It("returns nil when source namespace is the operator namespace", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			err := r.ensurePullSecret(ctx, "pull-secret", architectNamespace)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error when source secret does not exist", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			err := r.ensurePullSecret(ctx, "pull-secret", "openshift-config")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get pull secret"))
		})
	})

	Describe("mergePullSecrets", func() {
		It("merges source auths into existing", func() {
			existing := []byte(`{"auths":{"reg1.io":{"auth":"existing"}}}`)
			source := []byte(`{"auths":{"reg2.io":{"auth":"new"}}}`)

			r := &DisconnectedPlatformReconciler{}
			merged, changed, err := r.mergePullSecrets(existing, source)
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(BeTrue())

			var config map[string]interface{}
			Expect(json.Unmarshal(merged, &config)).To(Succeed())
			auths := config["auths"].(map[string]interface{})
			Expect(auths).To(HaveKey("reg1.io"))
			Expect(auths).To(HaveKey("reg2.io"))
		})

		It("returns changed=false when source has same entries", func() {
			data := []byte(`{"auths":{"reg1.io":{"auth":"same"}}}`)

			r := &DisconnectedPlatformReconciler{}
			_, changed, err := r.mergePullSecrets(data, data)
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(BeFalse())
		})

		It("preserves Quay credentials from existing", func() {
			existing := []byte(`{"auths":{"mirror-operator-quay.apps.local":{"auth":"quay-cred"},"reg1.io":{"auth":"old"}}}`)
			source := []byte(`{"auths":{"reg1.io":{"auth":"new"}}}`)

			r := &DisconnectedPlatformReconciler{}
			merged, _, err := r.mergePullSecrets(existing, source)
			Expect(err).NotTo(HaveOccurred())

			var config map[string]interface{}
			Expect(json.Unmarshal(merged, &config)).To(Succeed())
			auths := config["auths"].(map[string]interface{})
			Expect(auths).To(HaveKey("mirror-operator-quay.apps.local"))
		})

		It("returns error for invalid JSON", func() {
			r := &DisconnectedPlatformReconciler{}
			_, _, err := r.mergePullSecrets([]byte("invalid"), []byte(`{"auths":{}}`))
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("consolePluginDeployment", func() {
		It("creates a console plugin Deployment with correct container name", func() {
			labels := architectComponentLabels("plugin")
			dep := consolePluginDeployment("test-plugin", "quay.io/test/plugin:v1", 1, labels, "pull-secret", "openshift-config")

			Expect(dep.GetName()).To(Equal("test-plugin"))
			Expect(dep.GetNamespace()).To(Equal(architectNamespace))

			containers, _, _ := unstructured.NestedSlice(dep.Object, "spec", "template", "spec", "containers")
			Expect(containers).To(HaveLen(1))
			container := containers[0].(map[string]interface{})
			Expect(container["name"]).To(Equal("plugin"))
			Expect(container["image"]).To(Equal("quay.io/test/plugin:v1"))
		})
	})

	Describe("generateRandomPassword", func() {
		It("returns password of requested length", func() {
			password := generateRandomPassword(20)
			Expect(password).To(HaveLen(20))
		})

		It("contains only alphanumeric characters", func() {
			password := generateRandomPassword(100)
			Expect(password).To(MatchRegexp("^[a-zA-Z0-9]+$"))
		})
	})

	// Suppress unused import warnings from the errors import
	Describe("error handling", func() {
		It("uses errors.Is for comparison", func() {
			err := fmt.Errorf("wrapped: %w", errors.New("inner"))
			Expect(errors.Is(err, err)).To(BeTrue())
		})
	})
})

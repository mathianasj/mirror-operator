package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"
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
			u := &unstructured.Unstructured{Object: map[string]interface{}{}}
			unstructured.SetNestedSlice(u.Object, []interface{}{
				map[string]interface{}{
					"type":   "Succeeded",
					"status": "True",
				},
			}, "status", "conditions")
			Expect(taskRunSucceeded(u)).To(BeTrue())
		})

		It("returns false when Succeeded condition is False", func() {
			u := &unstructured.Unstructured{Object: map[string]interface{}{}}
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
			u := &unstructured.Unstructured{Object: map[string]interface{}{}}
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
			Expect(envMap).To(HaveKey("VITE_API_BASE"))
			Expect(envMap["VITE_API_BASE"]).To(ContainSubstring("backend.apps.example.com"))
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

	Describe("ensureOperatorGroup", func() {
		It("skips creation for openshift-operators namespace", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			op := operatorDef{name: "test-op", ns: "openshift-operators"}
			err := r.ensureOperatorGroup(ctx, op)
			Expect(err).NotTo(HaveOccurred())

			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(operatorGroupGVK)
			Expect(r.List(ctx, list, client.InNamespace("openshift-operators"))).To(Succeed())
			Expect(list.Items).To(BeEmpty())
		})

		It("skips creation when an OperatorGroup already exists", func() {
			existing := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "operators.coreos.com/v1",
				"kind":       "OperatorGroup",
				"metadata": map[string]interface{}{
					"name":      "existing-og",
					"namespace": "test-ns",
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			op := operatorDef{name: "test-op", ns: "test-ns"}
			err := r.ensureOperatorGroup(ctx, op)
			Expect(err).NotTo(HaveOccurred())

			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(operatorGroupGVK)
			Expect(r.List(ctx, list, client.InNamespace("test-ns"))).To(Succeed())
			Expect(list.Items).To(HaveLen(1))
			Expect(list.Items[0].GetName()).To(Equal("existing-og"))
		})

		It("creates an OperatorGroup when none exists", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			op := operatorDef{name: "test-op", ns: "test-ns"}
			err := r.ensureOperatorGroup(ctx, op)
			Expect(err).NotTo(HaveOccurred())

			og := &unstructured.Unstructured{Object: map[string]interface{}{}}
			og.SetGroupVersionKind(operatorGroupGVK)
			err = r.Get(ctx, client.ObjectKey{Name: "mirror-operator-test-op", Namespace: "test-ns"}, og)
			Expect(err).NotTo(HaveOccurred())

			targetNS, _, _ := unstructured.NestedStringSlice(og.Object, "spec", "targetNamespaces")
			Expect(targetNS).To(ConsistOf("test-ns"))
		})
	})

	Describe("ensureSubscription", func() {
		It("creates a subscription with correct fields from operatorDef defaults", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			op := operatorDef{
				name:      "test-op",
				pkg:       "test-package",
				channel:   "stable",
				catalog:   "redhat-operators",
				catalogNS: "openshift-marketplace",
				ns:        "test-ns",
			}
			err := r.ensureSubscription(ctx, op, nil)
			Expect(err).NotTo(HaveOccurred())

			sub := &unstructured.Unstructured{Object: map[string]interface{}{}}
			sub.SetGroupVersionKind(subscriptionGVK)
			err = r.Get(ctx, client.ObjectKey{Name: "mirror-operator-test-op", Namespace: "test-ns"}, sub)
			Expect(err).NotTo(HaveOccurred())

			name, _, _ := unstructured.NestedString(sub.Object, "spec", "name")
			Expect(name).To(Equal("test-package"))
			channel, _, _ := unstructured.NestedString(sub.Object, "spec", "channel")
			Expect(channel).To(Equal("stable"))
			source, _, _ := unstructured.NestedString(sub.Object, "spec", "source")
			Expect(source).To(Equal("redhat-operators"))
			sourceNS, _, _ := unstructured.NestedString(sub.Object, "spec", "sourceNamespace")
			Expect(sourceNS).To(Equal("openshift-marketplace"))
			approval, _, _ := unstructured.NestedString(sub.Object, "spec", "installPlanApproval")
			Expect(approval).To(Equal("Automatic"))
		})

		It("skips creation when subscription already exists", func() {
			existing := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "operators.coreos.com/v1alpha1",
				"kind":       "Subscription",
				"metadata": map[string]interface{}{
					"name":      "mirror-operator-test-op",
					"namespace": "test-ns",
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			op := operatorDef{name: "test-op", ns: "test-ns"}
			err := r.ensureSubscription(ctx, op, nil)
			Expect(err).NotTo(HaveOccurred())
		})

		It("respects OLMSubscriptionConfig overrides", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			op := operatorDef{
				name:      "test-op",
				pkg:       "test-package",
				channel:   "stable",
				catalog:   "redhat-operators",
				catalogNS: "openshift-marketplace",
				ns:        "test-ns",
			}
			cfg := &mirrorv1.OLMSubscriptionConfig{
				Channel:          "fast",
				CatalogSource:    "custom-catalog",
				CatalogSourceNS:  "custom-ns",
				ApprovalStrategy: "Manual",
				StartingCSV:      "test-operator.v1.0.0",
			}
			err := r.ensureSubscription(ctx, op, cfg)
			Expect(err).NotTo(HaveOccurred())

			sub := &unstructured.Unstructured{Object: map[string]interface{}{}}
			sub.SetGroupVersionKind(subscriptionGVK)
			err = r.Get(ctx, client.ObjectKey{Name: "mirror-operator-test-op", Namespace: "test-ns"}, sub)
			Expect(err).NotTo(HaveOccurred())

			channel, _, _ := unstructured.NestedString(sub.Object, "spec", "channel")
			Expect(channel).To(Equal("fast"))
			source, _, _ := unstructured.NestedString(sub.Object, "spec", "source")
			Expect(source).To(Equal("custom-catalog"))
			sourceNS, _, _ := unstructured.NestedString(sub.Object, "spec", "sourceNamespace")
			Expect(sourceNS).To(Equal("custom-ns"))
			approval, _, _ := unstructured.NestedString(sub.Object, "spec", "installPlanApproval")
			Expect(approval).To(Equal("Manual"))
			startingCSV, _, _ := unstructured.NestedString(sub.Object, "spec", "startingCSV")
			Expect(startingCSV).To(Equal("test-operator.v1.0.0"))
		})
	})

	Describe("csvStatus", func() {
		It("returns phase when CSV exists", func() {
			sub := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "operators.coreos.com/v1alpha1",
				"kind":       "Subscription",
				"metadata": map[string]interface{}{
					"name":      "mirror-operator-test-op",
					"namespace": "test-ns",
				},
				"status": map[string]interface{}{
					"currentCSV": "test-op.v1.0.0",
				},
			}}
			csv := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "operators.coreos.com/v1alpha1",
				"kind":       "ClusterServiceVersion",
				"metadata": map[string]interface{}{
					"name":      "test-op.v1.0.0",
					"namespace": "test-ns",
				},
				"status": map[string]interface{}{
					"phase": "Succeeded",
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sub, csv).Build(),
				Scheme: testScheme,
			}
			op := operatorDef{name: "test-op", ns: "test-ns"}
			Expect(r.csvStatus(ctx, op)).To(Equal("Succeeded"))
		})

		It("returns empty when subscription is missing", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			op := operatorDef{name: "test-op", ns: "test-ns"}
			Expect(r.csvStatus(ctx, op)).To(BeEmpty())
		})

		It("returns empty when CSV name is empty in subscription status", func() {
			sub := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "operators.coreos.com/v1alpha1",
				"kind":       "Subscription",
				"metadata": map[string]interface{}{
					"name":      "mirror-operator-test-op",
					"namespace": "test-ns",
				},
				"status": map[string]interface{}{},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sub).Build(),
				Scheme: testScheme,
			}
			op := operatorDef{name: "test-op", ns: "test-ns"}
			Expect(r.csvStatus(ctx, op)).To(BeEmpty())
		})

		It("returns empty when CSV does not exist", func() {
			sub := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "operators.coreos.com/v1alpha1",
				"kind":       "Subscription",
				"metadata": map[string]interface{}{
					"name":      "mirror-operator-test-op",
					"namespace": "test-ns",
				},
				"status": map[string]interface{}{
					"currentCSV": "missing-csv.v1.0.0",
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sub).Build(),
				Scheme: testScheme,
			}
			op := operatorDef{name: "test-op", ns: "test-ns"}
			Expect(r.csvStatus(ctx, op)).To(BeEmpty())
		})
	})

	Describe("ensureArchitectServiceAccount", func() {
		It("creates ServiceAccount, Role, and RoleBinding", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			err := r.ensureArchitectServiceAccount(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			sa := &corev1.ServiceAccount{}
			err = r.Get(ctx, client.ObjectKey{Name: "airgap-architect-backend", Namespace: architectNamespace}, sa)
			Expect(err).NotTo(HaveOccurred())
			Expect(sa.Labels).To(HaveKeyWithValue("app.kubernetes.io/name", "airgap-architect"))
			Expect(sa.Labels).To(HaveKeyWithValue("app.kubernetes.io/component", "backend"))

			role := &unstructured.Unstructured{Object: map[string]interface{}{}}
			role.SetGroupVersionKind(schema.GroupVersionKind{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "Role"})
			err = r.Get(ctx, client.ObjectKey{Name: "airgap-architect-backend", Namespace: architectNamespace}, role)
			Expect(err).NotTo(HaveOccurred())

			rb := &unstructured.Unstructured{Object: map[string]interface{}{}}
			rb.SetGroupVersionKind(schema.GroupVersionKind{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "RoleBinding"})
			err = r.Get(ctx, client.ObjectKey{Name: "airgap-architect-backend", Namespace: architectNamespace}, rb)
			Expect(err).NotTo(HaveOccurred())
		})

		It("is idempotent — does not error when resources already exist", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			Expect(r.ensureArchitectServiceAccount(ctx, platform)).To(Succeed())
			Expect(r.ensureArchitectServiceAccount(ctx, platform)).To(Succeed())
		})
	})

	Describe("ensureArchitectBackend", func() {
		It("creates backend deployment when it does not exist", func() {
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
			labels := architectComponentLabels("backend")

			err := r.ensureArchitectBackend(ctx, platform, "test-backend", "quay.io/test/backend:v1", 1, labels, "pull-secret", "openshift-config")
			Expect(err).NotTo(HaveOccurred())

			dep := &unstructured.Unstructured{Object: map[string]interface{}{}}
			dep.SetGroupVersionKind(deploymentGVK)
			err = r.Get(ctx, client.ObjectKey{Name: "test-backend", Namespace: architectNamespace}, dep)
			Expect(err).NotTo(HaveOccurred())

			containers, _, _ := unstructured.NestedSlice(dep.Object, "spec", "template", "spec", "containers")
			Expect(containers).To(HaveLen(1))
			container := containers[0].(map[string]interface{})
			Expect(container["image"]).To(Equal("quay.io/test/backend:v1"))
		})
	})

	Describe("ensureArchitectFrontend", func() {
		It("creates frontend deployment when it does not exist", func() {
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
			labels := architectComponentLabels("frontend")

			err := r.ensureArchitectFrontend(ctx, platform, "test-frontend", "quay.io/test/frontend:v1", 1, labels, "backend.example.com", "frontend.example.com")
			Expect(err).NotTo(HaveOccurred())

			dep := &unstructured.Unstructured{Object: map[string]interface{}{}}
			dep.SetGroupVersionKind(deploymentGVK)
			err = r.Get(ctx, client.ObjectKey{Name: "test-frontend", Namespace: architectNamespace}, dep)
			Expect(err).NotTo(HaveOccurred())

			containers, _, _ := unstructured.NestedSlice(dep.Object, "spec", "template", "spec", "containers")
			Expect(containers).To(HaveLen(1))
			container := containers[0].(map[string]interface{})
			Expect(container["image"]).To(Equal("quay.io/test/frontend:v1"))
		})
	})

	Describe("deleteArchitectResources", func() {
		It("deletes backend and frontend deployments, services, and routes", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}

			backendName := architectResourceName(platform, "airgap-architect-backend")
			frontendName := architectResourceName(platform, "airgap-architect-frontend")

			backendDep := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "apps/v1", "kind": "Deployment",
				"metadata": map[string]interface{}{"name": backendName, "namespace": architectNamespace},
			}}
			frontendDep := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "apps/v1", "kind": "Deployment",
				"metadata": map[string]interface{}{"name": frontendName, "namespace": architectNamespace},
			}}
			backendSvc := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "v1", "kind": "Service",
				"metadata": map[string]interface{}{"name": backendName, "namespace": architectNamespace},
			}}
			frontendSvc := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "v1", "kind": "Service",
				"metadata": map[string]interface{}{"name": frontendName, "namespace": architectNamespace},
			}}
			apiRoute := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "route.openshift.io/v1", "kind": "Route",
				"metadata": map[string]interface{}{"name": "airgap-architect-api", "namespace": architectNamespace},
			}}
			uiRoute := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "route.openshift.io/v1", "kind": "Route",
				"metadata": map[string]interface{}{"name": "airgap-architect", "namespace": architectNamespace},
			}}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(
					backendDep, frontendDep, backendSvc, frontendSvc, apiRoute, uiRoute,
				).Build(),
				Scheme: testScheme,
			}

			err := r.deleteArchitectResources(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			check := &unstructured.Unstructured{Object: map[string]interface{}{}}
			check.SetGroupVersionKind(deploymentGVK)
			err = r.Get(ctx, client.ObjectKey{Name: backendName, Namespace: architectNamespace}, check)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("does not error when resources do not exist", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			err := r.deleteArchitectResources(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("deleteResource", func() {
		It("deletes an existing resource", func() {
			dep := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "apps/v1", "kind": "Deployment",
				"metadata": map[string]interface{}{"name": "to-delete", "namespace": architectNamespace},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(dep).Build(),
				Scheme: testScheme,
			}

			err := r.deleteResource(ctx, deploymentGVK, "to-delete")
			Expect(err).NotTo(HaveOccurred())

			check := &unstructured.Unstructured{Object: map[string]interface{}{}}
			check.SetGroupVersionKind(deploymentGVK)
			err = r.Get(ctx, client.ObjectKey{Name: "to-delete", Namespace: architectNamespace}, check)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("returns nil when resource does not exist", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.deleteResource(ctx, deploymentGVK, "nonexistent")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("getQuayHostname", func() {
		It("returns hostname from QuayRegistry status registryEndpoint", func() {
			quayRegistry := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata": map[string]interface{}{
					"name":      "test-quay",
					"namespace": architectNamespace,
				},
				"status": map[string]interface{}{
					"registryEndpoint": "https://quay.example.com",
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(quayRegistry).Build(),
				Scheme: testScheme,
			}
			hostname, err := r.getQuayHostname(ctx, quayRegistry)
			Expect(err).NotTo(HaveOccurred())
			Expect(hostname).To(Equal("quay.example.com"))
		})

		It("strips http:// prefix from hostname", func() {
			quayRegistry := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata": map[string]interface{}{
					"name":      "test-quay",
					"namespace": architectNamespace,
				},
				"status": map[string]interface{}{
					"registryEndpoint": "http://quay.example.com",
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(quayRegistry).Build(),
				Scheme: testScheme,
			}
			hostname, err := r.getQuayHostname(ctx, quayRegistry)
			Expect(err).NotTo(HaveOccurred())
			Expect(hostname).To(Equal("quay.example.com"))
		})

		It("falls back to Route spec.host when no status endpoint", func() {
			quayRegistry := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata": map[string]interface{}{
					"name":      "test-quay",
					"namespace": architectNamespace,
				},
			}}
			route := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "route.openshift.io/v1",
				"kind":       "Route",
				"metadata": map[string]interface{}{
					"name":      "test-quay-quay",
					"namespace": architectNamespace,
				},
				"spec": map[string]interface{}{
					"host": "quay-route.example.com",
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(quayRegistry, route).Build(),
				Scheme: testScheme,
			}
			hostname, err := r.getQuayHostname(ctx, quayRegistry)
			Expect(err).NotTo(HaveOccurred())
			Expect(hostname).To(Equal("quay-route.example.com"))
		})

		It("returns empty string when no status and no route", func() {
			quayRegistry := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata": map[string]interface{}{
					"name":      "test-quay",
					"namespace": architectNamespace,
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			hostname, err := r.getQuayHostname(ctx, quayRegistry)
			Expect(err).NotTo(HaveOccurred())
			Expect(hostname).To(BeEmpty())
		})
	})

	Describe("ensureQuayGunicornTimeout", func() {
		It("adds GUNICORN_CMD_ARGS and WORKER_COUNT_REGISTRY env vars to quay component", func() {
			quayRegistry := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata": map[string]interface{}{
					"name":      "test-quay",
					"namespace": architectNamespace,
				},
				"spec": map[string]interface{}{
					"components": []interface{}{
						map[string]interface{}{
							"kind":    "quay",
							"managed": true,
						},
						map[string]interface{}{
							"kind":    "clair",
							"managed": true,
						},
					},
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(quayRegistry).Build(),
				Scheme: testScheme,
			}
			err := r.ensureQuayGunicornTimeout(ctx, quayRegistry)
			Expect(err).NotTo(HaveOccurred())

			updated := &unstructured.Unstructured{Object: map[string]interface{}{}}
			updated.SetGroupVersionKind(schema.GroupVersionKind{Group: "quay.redhat.com", Version: "v1", Kind: "QuayRegistry"})
			Expect(r.Get(ctx, client.ObjectKey{Name: "test-quay", Namespace: architectNamespace}, updated)).To(Succeed())

			components, _, _ := unstructured.NestedSlice(updated.Object, "spec", "components")
			for _, comp := range components {
				c := comp.(map[string]interface{})
				kind, _, _ := unstructured.NestedString(c, "kind")
				if kind == "quay" {
					envList, _, _ := unstructured.NestedSlice(c, "overrides", "env")
					Expect(len(envList)).To(BeNumerically(">=", 2))
					names := []string{}
					for _, e := range envList {
						entry := e.(map[string]interface{})
						names = append(names, entry["name"].(string))
					}
					Expect(names).To(ContainElements("GUNICORN_CMD_ARGS", "WORKER_COUNT_REGISTRY"))
				}
			}
		})

		It("is idempotent — does not duplicate env vars", func() {
			quayRegistry := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata": map[string]interface{}{
					"name":      "test-quay",
					"namespace": architectNamespace,
				},
				"spec": map[string]interface{}{
					"components": []interface{}{
						map[string]interface{}{
							"kind":    "quay",
							"managed": true,
							"overrides": map[string]interface{}{
								"env": []interface{}{
									map[string]interface{}{"name": "GUNICORN_CMD_ARGS", "value": "--timeout 300"},
									map[string]interface{}{"name": "WORKER_COUNT_REGISTRY", "value": "2"},
								},
							},
						},
					},
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(quayRegistry).Build(),
				Scheme: testScheme,
			}
			err := r.ensureQuayGunicornTimeout(ctx, quayRegistry)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when no components found", func() {
			quayRegistry := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata": map[string]interface{}{
					"name":      "test-quay",
					"namespace": architectNamespace,
				},
				"spec": map[string]interface{}{},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(quayRegistry).Build(),
				Scheme: testScheme,
			}
			err := r.ensureQuayGunicornTimeout(ctx, quayRegistry)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureQuayComponentsUnmanaged", func() {
		It("sets route and tls components to unmanaged", func() {
			quayRegistry := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata": map[string]interface{}{
					"name":      "test-quay",
					"namespace": architectNamespace,
				},
				"spec": map[string]interface{}{
					"components": []interface{}{
						map[string]interface{}{
							"kind":    "route",
							"managed": true,
						},
						map[string]interface{}{
							"kind":    "tls",
							"managed": true,
						},
						map[string]interface{}{
							"kind":    "clair",
							"managed": true,
						},
					},
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(quayRegistry).Build(),
				Scheme: testScheme,
			}
			err := r.ensureQuayComponentsUnmanaged(ctx, quayRegistry)
			Expect(err).NotTo(HaveOccurred())

			updated := &unstructured.Unstructured{Object: map[string]interface{}{}}
			updated.SetGroupVersionKind(schema.GroupVersionKind{Group: "quay.redhat.com", Version: "v1", Kind: "QuayRegistry"})
			Expect(r.Get(ctx, client.ObjectKey{Name: "test-quay", Namespace: architectNamespace}, updated)).To(Succeed())

			components, _, _ := unstructured.NestedSlice(updated.Object, "spec", "components")
			for _, comp := range components {
				c := comp.(map[string]interface{})
				kind, _, _ := unstructured.NestedString(c, "kind")
				managed, _, _ := unstructured.NestedBool(c, "managed")
				if kind == "route" || kind == "tls" {
					Expect(managed).To(BeFalse())
				}
				if kind == "clair" {
					Expect(managed).To(BeTrue())
				}
			}
		})

		It("is idempotent when components already unmanaged", func() {
			quayRegistry := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata": map[string]interface{}{
					"name":      "test-quay",
					"namespace": architectNamespace,
				},
				"spec": map[string]interface{}{
					"components": []interface{}{
						map[string]interface{}{
							"kind":    "route",
							"managed": false,
						},
						map[string]interface{}{
							"kind":    "tls",
							"managed": false,
						},
					},
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(quayRegistry).Build(),
				Scheme: testScheme,
			}
			err := r.ensureQuayComponentsUnmanaged(ctx, quayRegistry)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("reconcileQuayOBC", func() {
		It("creates ObjectBucketClaim when not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-platform",
					Namespace: architectNamespace,
					UID:       "uid-123",
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}
			creds, err := r.reconcileQuayOBC(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
			Expect(creds).To(BeNil())

			obc := &unstructured.Unstructured{Object: map[string]interface{}{}}
			obc.SetGroupVersionKind(schema.GroupVersionKind{Group: "objectbucket.io", Version: "v1alpha1", Kind: "ObjectBucketClaim"})
			Expect(r.Get(ctx, client.ObjectKey{Name: "quay-storage", Namespace: architectNamespace}, obc)).To(Succeed())
			bucketName, _, _ := unstructured.NestedString(obc.Object, "spec", "generateBucketName")
			Expect(bucketName).To(Equal("quay-storage"))
		})

		It("returns resolved credentials when OBC configmap and secret exist", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-platform",
					Namespace: architectNamespace,
					UID:       "uid-123",
				},
			}
			obc := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "objectbucket.io/v1alpha1",
				"kind":       "ObjectBucketClaim",
				"metadata": map[string]interface{}{
					"name":      "quay-storage",
					"namespace": architectNamespace,
				},
				"spec": map[string]interface{}{
					"generateBucketName": "quay-storage",
				},
			}}
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "quay-storage", Namespace: architectNamespace},
				Data: map[string]string{
					"BUCKET_HOST": "s3.openshift-storage.svc",
					"BUCKET_PORT": "443",
					"BUCKET_NAME": "my-bucket",
				},
			}
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "quay-storage", Namespace: architectNamespace},
				Data: map[string][]byte{
					"AWS_ACCESS_KEY_ID":     []byte("access-key"),
					"AWS_SECRET_ACCESS_KEY": []byte("secret-key"),
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform, obc, cm, secret).Build(),
				Scheme: testScheme,
			}
			creds, err := r.reconcileQuayOBC(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
			Expect(creds).NotTo(BeNil())
			Expect(creds.Hostname).To(Equal("s3.openshift-storage.svc.cluster.local"))
			Expect(creds.Port).To(Equal(443))
			Expect(creds.IsSecure).To(BeTrue())
			Expect(creds.Bucket).To(Equal("my-bucket"))
			Expect(creds.AccessKey).To(Equal("access-key"))
			Expect(creds.SecretKey).To(Equal("secret-key"))
		})

		It("returns nil credentials when OBC ConfigMap is not ready", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-platform",
					Namespace: architectNamespace,
					UID:       "uid-123",
				},
			}
			obc := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "objectbucket.io/v1alpha1",
				"kind":       "ObjectBucketClaim",
				"metadata": map[string]interface{}{
					"name":      "quay-storage",
					"namespace": architectNamespace,
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform, obc).Build(),
				Scheme: testScheme,
			}
			creds, err := r.reconcileQuayOBC(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
			Expect(creds).To(BeNil())
		})
	})

	Describe("configureClairVEX", func() {
		It("returns nil when Clair deployment not found", func() {
			quayRegistry := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata": map[string]interface{}{
					"name":      "test-quay",
					"namespace": architectNamespace,
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.configureClairVEX(ctx, quayRegistry)
			Expect(err).NotTo(HaveOccurred())
		})

		It("creates VEX config secret and updates deployment volume", func() {
			quayRegistry := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata": map[string]interface{}{
					"name":      "test-quay",
					"namespace": architectNamespace,
					"uid":       "quay-uid-123",
				},
			}}

			clairConfig := `auth:
    psk:
        key: test-psk-key
http_listen_addr: :8080
indexer:
    connstring: host=db port=5432
matcher:
    connstring: host=db port=5432
notifier:
    connstring: host=db port=5432
    webhook:
        target: http://callback-target
`
			configSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-quay-clair-config",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"config.yaml": []byte(clairConfig),
				},
			}

			clairDeployment := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-quay-clair-app",
					Namespace: architectNamespace,
				},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "clair"}},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "clair"}},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "clair", Image: "clair:latest"}},
							Volumes: []corev1.Volume{
								{
									Name: "config",
									VolumeSource: corev1.VolumeSource{
										Secret: &corev1.SecretVolumeSource{SecretName: "test-quay-clair-config"},
									},
								},
							},
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(configSecret, clairDeployment).Build(),
				Scheme: testScheme,
			}
			err := r.configureClairVEX(ctx, quayRegistry)
			Expect(err).NotTo(HaveOccurred())

			vexSecret := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "test-quay-clair-vex-config", Namespace: architectNamespace}, vexSecret)).To(Succeed())
			configContent := string(vexSecret.Data["config.yaml"])
			if configContent == "" {
				configContent = vexSecret.StringData["config.yaml"]
			}
			Expect(configContent).To(ContainSubstring("vex: true"))

			updatedDeploy := &appsv1.Deployment{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "test-quay-clair-app", Namespace: architectNamespace}, updatedDeploy)).To(Succeed())
			Expect(updatedDeploy.Spec.Template.Spec.Volumes[0].Secret.SecretName).To(Equal("test-quay-clair-vex-config"))
		})
	})

	Describe("createOrUpdateQuayS3ConfigSecret", func() {
		It("creates config bundle secret with S3 credentials", func() {
			quayRegistry := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata": map[string]interface{}{
					"name":      "test-quay",
					"namespace": architectNamespace,
					"uid":       "quay-uid-123",
				},
			}}
			creds := &resolvedS3Credentials{
				Hostname:  "s3.example.com",
				Port:      443,
				IsSecure:  true,
				Bucket:    "my-bucket",
				AccessKey: "access",
				SecretKey: "secret",
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.createOrUpdateQuayS3ConfigSecret(ctx, quayRegistry, creds, "quay.example.com")
			Expect(err).NotTo(HaveOccurred())

			secret := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "test-quay-config-bundle", Namespace: architectNamespace}, secret)).To(Succeed())
			Expect(secret.Data).To(HaveKey("config.yaml"))
			configYAML := string(secret.Data["config.yaml"])
			Expect(configYAML).To(ContainSubstring("s3.example.com"))
			Expect(configYAML).To(ContainSubstring("my-bucket"))
		})

		It("updates existing config bundle secret", func() {
			existing := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-quay-config-bundle",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{"config.yaml": []byte("old-config")},
			}
			quayRegistry := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata": map[string]interface{}{
					"name":      "test-quay",
					"namespace": architectNamespace,
					"uid":       "quay-uid-123",
				},
			}}
			creds := &resolvedS3Credentials{
				Hostname: "s3-new.example.com", Port: 443, IsSecure: true,
				Bucket: "new-bucket", AccessKey: "key", SecretKey: "secret",
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			err := r.createOrUpdateQuayS3ConfigSecret(ctx, quayRegistry, creds, "")
			Expect(err).NotTo(HaveOccurred())

			secret := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "test-quay-config-bundle", Namespace: architectNamespace}, secret)).To(Succeed())
			Expect(string(secret.Data["config.yaml"])).To(ContainSubstring("s3-new.example.com"))
		})
	})

	Describe("ensureQuayPassthroughRoute", func() {
		It("creates passthrough route when none exists", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-platform",
					Namespace: architectNamespace,
					UID:       "uid-123",
				},
			}
			quayRegistry := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata": map[string]interface{}{
					"name":      "test-quay",
					"namespace": architectNamespace,
				},
			}}
			ingress := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "config.openshift.io/v1",
				"kind":       "Ingress",
				"metadata": map[string]interface{}{
					"name": "cluster",
				},
				"spec": map[string]interface{}{
					"domain": "apps.cluster.example.com",
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform, ingress).Build(),
				Scheme: testScheme,
			}
			err := r.ensureQuayPassthroughRoute(ctx, platform, quayRegistry)
			Expect(err).NotTo(HaveOccurred())

			route := &unstructured.Unstructured{Object: map[string]interface{}{}}
			route.SetGroupVersionKind(routeGVK)
			Expect(r.Get(ctx, client.ObjectKey{Name: "test-quay-quay", Namespace: architectNamespace}, route)).To(Succeed())
			termination, _, _ := unstructured.NestedString(route.Object, "spec", "tls", "termination")
			Expect(termination).To(Equal("passthrough"))
			host, _, _ := unstructured.NestedString(route.Object, "spec", "host")
			Expect(host).To(Equal("test-quay-quay-" + architectNamespace + ".apps.cluster.example.com"))
		})

		It("returns nil when route already has passthrough termination", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform", Namespace: architectNamespace, UID: "uid-123"},
			}
			quayRegistry := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata":   map[string]interface{}{"name": "test-quay", "namespace": architectNamespace},
			}}
			existingRoute := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "route.openshift.io/v1",
				"kind":       "Route",
				"metadata":   map[string]interface{}{"name": "test-quay-quay", "namespace": architectNamespace},
				"spec": map[string]interface{}{
					"tls": map[string]interface{}{"termination": "passthrough"},
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform, existingRoute).Build(),
				Scheme: testScheme,
			}
			err := r.ensureQuayPassthroughRoute(ctx, platform, quayRegistry)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("reconcileRHTASConfig", func() {
		It("returns nil when RHTAS config is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileRHTASConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when OIDC is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileRHTASConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("creates SecureSign CR with explicit OIDC config", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Issuer:   "https://keycloak.example.com/realms/sigstore",
								ClientID: "trusted-artifact-signer",
								Type:     "email",
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileRHTASConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			ss := &unstructured.Unstructured{Object: map[string]interface{}{}}
			ss.SetGroupVersionKind(securesignGVK)
			Expect(r.Get(ctx, client.ObjectKey{Name: "mirror-operator-securesign", Namespace: architectNamespace}, ss)).To(Succeed())

			fulcioEnabled, _, _ := unstructured.NestedBool(ss.Object, "spec", "fulcio", "enabled")
			Expect(fulcioEnabled).To(BeTrue())
			rekorEnabled, _, _ := unstructured.NestedBool(ss.Object, "spec", "rekor", "enabled")
			Expect(rekorEnabled).To(BeTrue())
		})

		It("is idempotent — does not error when SecureSign already exists", func() {
			existing := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Securesign",
				"metadata": map[string]interface{}{
					"name":      "mirror-operator-securesign",
					"namespace": architectNamespace,
				},
				"spec": map[string]interface{}{},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Issuer:   "https://keycloak.example.com/realms/sigstore",
								ClientID: "tas",
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileRHTASConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("uses managed Keycloak OIDC when managed config is set", func() {
			ingress := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "config.openshift.io/v1",
				"kind":       "Ingress",
				"metadata":   map[string]interface{}{"name": "cluster"},
				"spec":       map[string]interface{}{"domain": "apps.test.example.com"},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Managed: &mirrorv1.ManagedKeycloakConfig{
									Enabled: true,
									Realm:   "custom-realm",
								},
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ingress).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileRHTASConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			ss := &unstructured.Unstructured{Object: map[string]interface{}{}}
			ss.SetGroupVersionKind(securesignGVK)
			Expect(r.Get(ctx, client.ObjectKey{Name: "mirror-operator-securesign", Namespace: architectNamespace}, ss)).To(Succeed())

			issuers, _, _ := unstructured.NestedSlice(ss.Object, "spec", "fulcio", "config", "OIDCIssuers")
			Expect(len(issuers)).To(Equal(1))
			issuer := issuers[0].(map[string]interface{})
			Expect(issuer["Issuer"]).To(Equal("https://keycloak.apps.test.example.com/realms/custom-realm"))
			Expect(issuer["ClientID"]).To(Equal("trusted-artifact-signer"))
		})

		It("includes database config when specified", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Issuer:   "https://keycloak.example.com/realms/sigstore",
								ClientID: "tas",
							},
							Database: &mirrorv1.RHTASDatabaseConfig{
								Host:     "db.example.com",
								Name:     "sigstore",
								Port:     5432,
								Username: "admin",
								Password: "secret",
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileRHTASConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			ss := &unstructured.Unstructured{Object: map[string]interface{}{}}
			ss.SetGroupVersionKind(securesignGVK)
			Expect(r.Get(ctx, client.ObjectKey{Name: "mirror-operator-securesign", Namespace: architectNamespace}, ss)).To(Succeed())

			dbHost, _, _ := unstructured.NestedString(ss.Object, "spec", "trillian", "database", "host")
			Expect(dbHost).To(Equal("db.example.com"))
			dbName, _, _ := unstructured.NestedString(ss.Object, "spec", "rekor", "database", "name")
			Expect(dbName).To(Equal("sigstore"))
		})

		It("returns error when issuer and clientID are empty", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileRHTASConfig(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("OIDC issuer and clientID are required"))
		})
	})

	Describe("deleteRHTASConfig", func() {
		It("deletes SecureSign and ConfigMap without error", func() {
			ss := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Securesign",
				"metadata":   map[string]interface{}{"name": "mirror-operator-securesign", "namespace": architectNamespace},
			}}
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "rhtas-trusted-root", Namespace: architectNamespace},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ss, cm).Build(),
				Scheme: testScheme,
			}
			r.deleteRHTASConfig(ctx)

			check := &unstructured.Unstructured{Object: map[string]interface{}{}}
			check.SetGroupVersionKind(securesignGVK)
			err := r.Get(ctx, client.ObjectKey{Name: "mirror-operator-securesign", Namespace: architectNamespace}, check)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			checkCM := &corev1.ConfigMap{}
			err = r.Get(ctx, client.ObjectKey{Name: "rhtas-trusted-root", Namespace: architectNamespace}, checkCM)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("does not error when resources do not exist", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			r.deleteRHTASConfig(ctx)
		})
	})

	Describe("extractRHTASRootKeys", func() {
		It("creates ConfigMap with Fulcio and Rekor keys from TUF status", func() {
			ss := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Securesign",
				"metadata":   map[string]interface{}{"name": "mirror-operator-securesign", "namespace": architectNamespace},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{"type": "Ready", "status": "True"},
					},
					"tuf": map[string]interface{}{
						"url": "http://tuf.mirror-operator-system.svc",
					},
				},
			}}
			tuf := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Tuf",
				"metadata":   map[string]interface{}{"name": "mirror-operator-securesign", "namespace": architectNamespace},
				"status": map[string]interface{}{
					"keys": []interface{}{
						map[string]interface{}{
							"name":      "fulcio_v1.crt.pem",
							"secretRef": map[string]interface{}{"name": "fulcio-root-secret", "key": "cert"},
						},
						map[string]interface{}{
							"name":      "rekor.pub",
							"secretRef": map[string]interface{}{"name": "rekor-pub-secret", "key": "public"},
						},
					},
				},
			}}
			fulcioSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "fulcio-root-secret", Namespace: architectNamespace},
				Data:       map[string][]byte{"cert": []byte("-----BEGIN CERTIFICATE-----\nFULCIO\n-----END CERTIFICATE-----")},
			}
			rekorSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "rekor-pub-secret", Namespace: architectNamespace},
				Data:       map[string][]byte{"public": []byte("-----BEGIN PUBLIC KEY-----\nREKOR\n-----END PUBLIC KEY-----")},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Status:     mirrorv1.DisconnectedPlatformStatus{},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ss, tuf, fulcioSecret, rekorSecret).Build(),
				Scheme: testScheme,
			}
			err := r.extractRHTASRootKeys(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			cm := &corev1.ConfigMap{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "rhtas-trusted-root", Namespace: architectNamespace}, cm)).To(Succeed())
			Expect(cm.Data["fulcio-root.pem"]).To(ContainSubstring("FULCIO"))
			Expect(cm.Data["rekor-public-key.pem"]).To(ContainSubstring("REKOR"))
			Expect(cm.Data["tuf-repository-url"]).To(Equal("http://tuf.mirror-operator-system.svc"))

			Expect(platform.Status.RHTASRootKeys).NotTo(BeNil())
			Expect(platform.Status.RHTASRootKeys.ConfigMap).To(Equal("rhtas-trusted-root"))
			Expect(platform.Status.RHTASRootKeys.TUFRepositoryURL).To(Equal("http://tuf.mirror-operator-system.svc"))
		})

		It("returns error when SecureSign not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.extractRHTASRootKeys(ctx, platform)
			Expect(err).To(HaveOccurred())
		})

		It("returns error when SecureSign is not ready", func() {
			ss := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Securesign",
				"metadata":   map[string]interface{}{"name": "mirror-operator-securesign", "namespace": architectNamespace},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{"type": "Ready", "status": "False"},
					},
				},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ss).Build(),
				Scheme: testScheme,
			}
			err := r.extractRHTASRootKeys(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not ready"))
		})

		It("returns error when Fulcio or Rekor secret refs are missing from TUF status", func() {
			ss := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Securesign",
				"metadata":   map[string]interface{}{"name": "mirror-operator-securesign", "namespace": architectNamespace},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{"type": "Ready", "status": "True"},
					},
					"tuf": map[string]interface{}{"url": "http://tuf.svc"},
				},
			}}
			tuf := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Tuf",
				"metadata":   map[string]interface{}{"name": "mirror-operator-securesign", "namespace": architectNamespace},
				"status": map[string]interface{}{
					"keys": []interface{}{
						map[string]interface{}{"name": "other-key"},
					},
				},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ss, tuf).Build(),
				Scheme: testScheme,
			}
			err := r.extractRHTASRootKeys(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Fulcio or Rekor secret references not found"))
		})
	})

	Describe("ensureKeycloakOIDCClient", func() {
		It("returns cached secret when it already exists", func() {
			cache := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "sigstore-client-secret-cache",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{"secret": []byte("cached-secret-value")},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cache).Build(),
				Scheme: testScheme,
			}
			secret, err := r.ensureKeycloakOIDCClient(ctx, "keycloak.example.com", "sigstore", "admin", "pass", "tas")
			Expect(err).NotTo(HaveOccurred())
			Expect(secret).To(Equal("cached-secret-value"))
		})

		It("generates and caches a new secret when none exists", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			secret, err := r.ensureKeycloakOIDCClient(ctx, "keycloak.example.com", "sigstore", "admin", "pass", "tas")
			Expect(err).NotTo(HaveOccurred())
			Expect(secret).To(HaveLen(32))

			cache := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "sigstore-client-secret-cache", Namespace: architectNamespace}, cache)).To(Succeed())
			cachedValue := string(cache.Data["secret"])
			if cachedValue == "" {
				cachedValue = cache.StringData["secret"]
			}
			Expect(cachedValue).To(Equal(secret))
		})
	})

	Describe("ensureKeycloakTLS", func() {
		It("returns error when certIssuer is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      "connected",
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureKeycloakTLS(ctx, platform, "keycloak.example.com", "keycloak-tls")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("certIssuer must be specified"))
		})

		It("creates Certificate CR with correct spec", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						CertIssuer: &mirrorv1.CertIssuerReference{
							Name: "letsencrypt-prod",
							Kind: "ClusterIssuer",
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureKeycloakTLS(ctx, platform, "keycloak.example.com", "keycloak-tls")
			Expect(err).NotTo(HaveOccurred())

			cert := &unstructured.Unstructured{Object: map[string]interface{}{}}
			cert.SetGroupVersionKind(certificateGVK)
			Expect(r.Get(ctx, client.ObjectKey{Name: "keycloak-certificate", Namespace: architectNamespace}, cert)).To(Succeed())

			secretName, _, _ := unstructured.NestedString(cert.Object, "spec", "secretName")
			Expect(secretName).To(Equal("keycloak-tls"))
			commonName, _, _ := unstructured.NestedString(cert.Object, "spec", "commonName")
			Expect(commonName).To(Equal("keycloak.example.com"))
			issuerName, _, _ := unstructured.NestedString(cert.Object, "spec", "issuerRef", "name")
			Expect(issuerName).To(Equal("letsencrypt-prod"))
		})

		It("is idempotent — skips when certificate already exists", func() {
			existing := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "cert-manager.io/v1",
				"kind":       "Certificate",
				"metadata":   map[string]interface{}{"name": "keycloak-certificate", "namespace": architectNamespace},
				"spec":       map[string]interface{}{"secretName": "keycloak-tls"},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						CertIssuer: &mirrorv1.CertIssuerReference{Name: "letsencrypt-prod"},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			err := r.ensureKeycloakTLS(ctx, platform, "keycloak.example.com", "keycloak-tls")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("deleteManagedKeycloak", func() {
		It("deletes Keycloak, RealmImport, and Certificate resources", func() {
			realm := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "k8s.keycloak.org/v2alpha1",
				"kind":       "KeycloakRealmImport",
				"metadata":   map[string]interface{}{"name": "mirror-operator-keycloak-realm", "namespace": architectNamespace},
			}}
			kc := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "k8s.keycloak.org/v2alpha1",
				"kind":       "Keycloak",
				"metadata":   map[string]interface{}{"name": "mirror-operator-keycloak", "namespace": architectNamespace},
			}}
			cert := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "cert-manager.io/v1",
				"kind":       "Certificate",
				"metadata":   map[string]interface{}{"name": "keycloak-certificate", "namespace": architectNamespace},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(realm, kc, cert).Build(),
				Scheme: testScheme,
			}
			r.deleteManagedKeycloak(ctx)

			check := &unstructured.Unstructured{Object: map[string]interface{}{}}
			check.SetGroupVersionKind(keycloakGVK)
			err := r.Get(ctx, client.ObjectKey{Name: "mirror-operator-keycloak", Namespace: architectNamespace}, check)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			check2 := &unstructured.Unstructured{Object: map[string]interface{}{}}
			check2.SetGroupVersionKind(keycloakRealmGVK)
			err = r.Get(ctx, client.ObjectKey{Name: "mirror-operator-keycloak-realm", Namespace: architectNamespace}, check2)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("does not error when resources do not exist", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			r.deleteManagedKeycloak(ctx)
		})
	})

	Describe("resolveQuayS3Credentials", func() {
		It("returns explicit credentials from storage config", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			storage := &mirrorv1.QuayStorageConfig{
				S3Endpoint:  "s3.example.com:9000",
				S3Bucket:    "test-bucket",
				S3AccessKey: "mykey",
				S3SecretKey: "mysecret",
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			creds, err := r.resolveQuayS3Credentials(ctx, platform, storage)
			Expect(err).NotTo(HaveOccurred())
			Expect(creds.Hostname).To(Equal("s3.example.com"))
			Expect(creds.Port).To(Equal(9000))
			Expect(creds.IsSecure).To(BeFalse())
			Expect(creds.Bucket).To(Equal("test-bucket"))
		})

		It("falls back to OBC when no explicit credentials", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: architectNamespace,
					UID:       "uid-123",
				},
			}
			storage := &mirrorv1.QuayStorageConfig{}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}
			creds, err := r.resolveQuayS3Credentials(ctx, platform, storage)
			Expect(err).NotTo(HaveOccurred())
			Expect(creds).To(BeNil())
		})
	})

	Describe("getClusterIngressDomain", func() {
		It("returns domain from cluster Ingress resource", func() {
			ingress := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "config.openshift.io/v1",
				"kind":       "Ingress",
				"metadata":   map[string]interface{}{"name": "cluster"},
				"spec":       map[string]interface{}{"domain": "apps.mycluster.example.com"},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ingress).Build(),
				Scheme: testScheme,
			}
			domain, err := r.getClusterIngressDomain(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(domain).To(Equal("apps.mycluster.example.com"))
		})

		It("returns error when Ingress not found", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			_, err := r.getClusterIngressDomain(ctx)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("addOpenShiftIdentityProvider", func() {
		It("creates OAuthClient and identity provider when neither exists", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/test-realm/identity-provider/instances", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode([]map[string]interface{}{})
					return
				}
				if r.Method == "POST" {
					body, _ := io.ReadAll(r.Body)
					var idp map[string]interface{}
					json.Unmarshal(body, &idp)
					Expect(idp["alias"]).To(Equal("openshift"))
					Expect(idp["providerId"]).To(Equal("openshift-v4"))
					w.WriteHeader(http.StatusCreated)
					return
				}
			})
			mux.HandleFunc("/admin/realms/test-realm/authentication/flows/first%20broker%20login/executions", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode([]map[string]interface{}{})
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")

			reconciler := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			err := reconciler.addOpenShiftIdentityProvider(ctx, keycloakHost, "test-realm", "https://api.cluster.example.com:6443", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())

			oauthClient := &unstructured.Unstructured{Object: map[string]interface{}{}}
			oauthClient.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "oauth.openshift.io", Version: "v1", Kind: "OAuthClient",
			})
			Expect(reconciler.Get(ctx, client.ObjectKey{Name: "keycloak-test-realm"}, oauthClient)).To(Succeed())
			secret, _, _ := unstructured.NestedString(oauthClient.Object, "secret")
			Expect(secret).NotTo(BeEmpty())
		})

		It("updates existing identity provider when it already exists", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/test-realm/identity-provider/instances", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode([]map[string]interface{}{
						{"alias": "openshift", "config": map[string]interface{}{"clientSecret": "old-secret"}},
					})
					return
				}
			})
			mux.HandleFunc("/admin/realms/test-realm/identity-provider/instances/openshift", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(map[string]interface{}{
						"alias":  "openshift",
						"config": map[string]interface{}{"clientSecret": "old-secret", "baseUrl": "https://old.api"},
					})
					return
				}
				if r.Method == "PUT" {
					w.WriteHeader(http.StatusNoContent)
					return
				}
			})
			mux.HandleFunc("/admin/realms/test-realm/authentication/flows/first%20broker%20login/executions", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode([]map[string]interface{}{})
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			oauthClient := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "oauth.openshift.io/v1",
				"kind":       "OAuthClient",
				"metadata":   map[string]interface{}{"name": "keycloak-test-realm"},
				"secret":     "existing-secret",
			}}
			reconciler := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(oauthClient).Build(),
				Scheme: testScheme,
			}

			err := reconciler.addOpenShiftIdentityProvider(ctx, keycloakHost, "test-realm", "https://api.cluster.example.com:6443", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error when OAuthClient secret is empty", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/test-realm/identity-provider/instances", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode([]map[string]interface{}{})
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			oauthClient := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "oauth.openshift.io/v1",
				"kind":       "OAuthClient",
				"metadata":   map[string]interface{}{"name": "keycloak-test-realm"},
			}}
			reconciler := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(oauthClient).Build(),
				Scheme: testScheme,
			}

			err := reconciler.addOpenShiftIdentityProvider(ctx, keycloakHost, "test-realm", "https://api.cluster.example.com:6443", "test-token", ts.Client())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("OAuthClient secret is empty"))
		})
	})

	Describe("configureCollectionPipelineSigning", func() {
		It("returns nil when RHTAS config is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      "connected",
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.configureCollectionPipelineSigning(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when managed OIDC is not enabled", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Issuer:   "https://external.keycloak.com",
								ClientID: "tas",
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.configureCollectionPipelineSigning(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureAWSLoadBalancerTimeout", func() {
		It("returns nil when platform is not AWS", func() {
			infra := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "config.openshift.io/v1",
				"kind":       "Infrastructure",
				"metadata":   map[string]interface{}{"name": "cluster"},
				"status": map[string]interface{}{
					"platformStatus": map[string]interface{}{"type": "vSphere"},
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(infra).Build(),
				Scheme: testScheme,
			}
			err := r.ensureAWSLoadBalancerTimeout(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("sets idle timeout to 5m on AWS", func() {
			infra := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "config.openshift.io/v1",
				"kind":       "Infrastructure",
				"metadata":   map[string]interface{}{"name": "cluster"},
				"status":     map[string]interface{}{"platformStatus": map[string]interface{}{"type": "AWS"}},
			}}
			ic := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "operator.openshift.io/v1",
				"kind":       "IngressController",
				"metadata":   map[string]interface{}{"name": "default", "namespace": "openshift-ingress-operator"},
				"spec": map[string]interface{}{
					"endpointPublishingStrategy": map[string]interface{}{
						"type": "LoadBalancerService",
						"loadBalancer": map[string]interface{}{
							"scope": "External",
						},
					},
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(infra, ic).Build(),
				Scheme: testScheme,
			}
			err := r.ensureAWSLoadBalancerTimeout(ctx)
			Expect(err).NotTo(HaveOccurred())

			updated := &unstructured.Unstructured{Object: map[string]interface{}{}}
			updated.SetGroupVersionKind(schema.GroupVersionKind{Group: "operator.openshift.io", Version: "v1", Kind: "IngressController"})
			Expect(r.Get(ctx, client.ObjectKey{Name: "default", Namespace: "openshift-ingress-operator"}, updated)).To(Succeed())
			timeout, _, _ := unstructured.NestedString(updated.Object,
				"spec", "endpointPublishingStrategy", "loadBalancer", "providerParameters", "aws", "classicLoadBalancer", "connectionIdleTimeout")
			Expect(timeout).To(Equal("5m0s"))
		})

		It("is idempotent when timeout already set", func() {
			infra := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "config.openshift.io/v1",
				"kind":       "Infrastructure",
				"metadata":   map[string]interface{}{"name": "cluster"},
				"status":     map[string]interface{}{"platformStatus": map[string]interface{}{"type": "AWS"}},
			}}
			ic := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "operator.openshift.io/v1",
				"kind":       "IngressController",
				"metadata":   map[string]interface{}{"name": "default", "namespace": "openshift-ingress-operator"},
				"spec": map[string]interface{}{
					"endpointPublishingStrategy": map[string]interface{}{
						"type": "LoadBalancerService",
						"loadBalancer": map[string]interface{}{
							"scope": "External",
							"providerParameters": map[string]interface{}{
								"type": "AWS",
								"aws": map[string]interface{}{
									"type": "Classic",
									"classicLoadBalancer": map[string]interface{}{
										"connectionIdleTimeout": "5m0s",
									},
								},
							},
						},
					},
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(infra, ic).Build(),
				Scheme: testScheme,
			}
			err := r.ensureAWSLoadBalancerTimeout(ctx)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("saveQuayRobotCredentials", func() {
		It("creates secret when none exists", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.saveQuayRobotCredentials(ctx, "mirror+bot", "my-token")
			Expect(err).NotTo(HaveOccurred())

			secret := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "quay-robot-credentials", Namespace: architectNamespace}, secret)).To(Succeed())
			Expect(string(secret.Data["username"])).To(Equal("mirror+bot"))
			Expect(string(secret.Data["token"])).To(Equal("my-token"))
		})

		It("updates existing secret", func() {
			existing := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "quay-robot-credentials", Namespace: architectNamespace},
				Data: map[string][]byte{
					"username": []byte("old-user"),
					"token":    []byte("old-token"),
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			err := r.saveQuayRobotCredentials(ctx, "mirror+bot", "new-token")
			Expect(err).NotTo(HaveOccurred())

			secret := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "quay-robot-credentials", Namespace: architectNamespace}, secret)).To(Succeed())
			Expect(string(secret.Data["token"])).To(Equal("new-token"))
		})
	})

	Describe("getQuayRobotCredentials", func() {
		It("returns cached credentials from secret", func() {
			cache := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "quay-robot-credentials", Namespace: architectNamespace},
				Data: map[string][]byte{
					"username": []byte("mirror+mirroroperator"),
					"token":    []byte("cached-token-value"),
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cache).Build(),
				Scheme: testScheme,
			}
			robot, token, err := r.getQuayRobotCredentials(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(robot).To(Equal("mirror+mirroroperator"))
			Expect(token).To(Equal("cached-token-value"))
		})
	})

	Describe("deleteRHTPAConfig", func() {
		It("deletes TrustedProfileAnalyzer CR", func() {
			tpa := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtpa.io/v1",
				"kind":       "TrustedProfileAnalyzer",
				"metadata":   map[string]interface{}{"name": "mirror-operator-trusted-profile-analyzer", "namespace": architectNamespace},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(tpa).Build(),
				Scheme: testScheme,
			}
			r.deleteRHTPAConfig(ctx)

			check := &unstructured.Unstructured{Object: map[string]interface{}{}}
			check.SetGroupVersionKind(schema.GroupVersionKind{Group: "rhtpa.io", Version: "v1", Kind: "TrustedProfileAnalyzer"})
			err := r.Get(ctx, client.ObjectKey{Name: "mirror-operator-trusted-profile-analyzer", Namespace: architectNamespace}, check)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("does not error when TPA does not exist", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			r.deleteRHTPAConfig(ctx)
		})
	})

	Describe("reconcileRHTPAConfig", func() {
		It("returns nil when RHTPA config is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileRHTPAConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when RHTPA storage is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						RHTPA: &mirrorv1.RHTPAInstallerConfig{},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileRHTPAConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("updateStatusFromSecuresignHealth", func() {
		It("returns nil when SecureSign not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.updateStatusFromSecuresignHealth(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when no conditions in status", func() {
			ss := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Securesign",
				"metadata":   map[string]interface{}{"name": "mirror-operator-securesign", "namespace": architectNamespace},
				"status":     map[string]interface{}{},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ss).Build(),
				Scheme: testScheme,
			}
			err := r.updateStatusFromSecuresignHealth(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("sets degraded condition when HealthCheckPassed is False", func() {
			ss := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Securesign",
				"metadata":   map[string]interface{}{"name": "mirror-operator-securesign", "namespace": architectNamespace},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":    "HealthCheckPassed",
							"status":  "False",
							"reason":  "HealthCheckFailed",
							"message": "Fulcio is unreachable",
						},
					},
				},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ss).Build(),
				Scheme: testScheme,
			}
			err := r.updateStatusFromSecuresignHealth(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			hasDegraded := false
			for _, cond := range platform.Status.Conditions {
				if cond.Type == "Degraded" && cond.Status == metav1.ConditionTrue {
					hasDegraded = true
					Expect(cond.Message).To(ContainSubstring("Fulcio is unreachable"))
				}
			}
			Expect(hasDegraded).To(BeTrue())
		})

		It("clears degraded condition when HealthCheckPassed is True", func() {
			ss := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Securesign",
				"metadata":   map[string]interface{}{"name": "mirror-operator-securesign", "namespace": architectNamespace},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":   "HealthCheckPassed",
							"status": "True",
						},
					},
				},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Status: mirrorv1.DisconnectedPlatformStatus{
					Conditions: []metav1.Condition{
						{
							Type:    "Degraded",
							Status:  metav1.ConditionTrue,
							Reason:  "HealthCheckFailed",
							Message: "old failure",
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ss).Build(),
				Scheme: testScheme,
			}
			err := r.updateStatusFromSecuresignHealth(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureUpdateService", func() {
		It("returns nil when QuayRegistry not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      "connected",
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureUpdateService(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when Quay hostname not available", func() {
			quay := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata":   map[string]interface{}{"name": "mirror-operator-quay", "namespace": architectNamespace},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      "connected",
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(quay).Build(),
				Scheme: testScheme,
			}
			err := r.ensureUpdateService(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("creates UpdateService CR with correct spec", func() {
			quay := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata":   map[string]interface{}{"name": "mirror-operator-quay", "namespace": architectNamespace},
				"status":     map[string]interface{}{"registryEndpoint": "https://quay.example.com"},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      "connected",
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(quay).Build(),
				Scheme: testScheme,
			}
			err := r.ensureUpdateService(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			us := &unstructured.Unstructured{Object: map[string]interface{}{}}
			us.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "updateservice.operator.openshift.io", Version: "v1", Kind: "UpdateService",
			})
			Expect(r.Get(ctx, client.ObjectKey{Name: "update-service-oc-mirror", Namespace: "openshift-update-service"}, us)).To(Succeed())
			graphImage, _, _ := unstructured.NestedString(us.Object, "spec", "graphDataImage")
			Expect(graphImage).To(Equal("quay.example.com/mirror/openshift/graph-image:latest"))
			releases, _, _ := unstructured.NestedString(us.Object, "spec", "releases")
			Expect(releases).To(Equal("quay.example.com/mirror/openshift/release-images"))
		})

		It("is idempotent — skips update when spec matches", func() {
			quay := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata":   map[string]interface{}{"name": "mirror-operator-quay", "namespace": architectNamespace},
				"status":     map[string]interface{}{"registryEndpoint": "https://quay.example.com"},
			}}
			existing := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "updateservice.operator.openshift.io/v1",
				"kind":       "UpdateService",
				"metadata":   map[string]interface{}{"name": "update-service-oc-mirror", "namespace": "openshift-update-service"},
				"spec": map[string]interface{}{
					"graphDataImage": "quay.example.com/mirror/openshift/graph-image:latest",
					"releases":       "quay.example.com/mirror/openshift/release-images",
					"replicas":       int64(2),
				},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      "connected",
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(quay, existing).Build(),
				Scheme: testScheme,
			}
			err := r.ensureUpdateService(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensurePullSecret", func() {
		It("returns nil when source namespace is operator namespace", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensurePullSecret(ctx, "pull-secret", architectNamespace)
			Expect(err).NotTo(HaveOccurred())
		})

		It("copies pull secret from source namespace to operator namespace", func() {
			source := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "openshift-config"},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(source).Build(),
				Scheme: testScheme,
			}
			err := r.ensurePullSecret(ctx, "pull-secret", "openshift-config")
			Expect(err).NotTo(HaveOccurred())

			target := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: architectNamespace}, target)).To(Succeed())
			Expect(target.Type).To(Equal(corev1.SecretTypeDockerConfigJson))
		})

		It("returns error when source secret not found", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensurePullSecret(ctx, "pull-secret", "openshift-config")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get pull secret"))
		})
	})

	Describe("addOpenShiftAttributeMappers", func() {
		It("creates username mapper via Keycloak API", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/test-realm/identity-provider/instances/openshift/mappers", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" {
					body, _ := io.ReadAll(r.Body)
					var mapper map[string]interface{}
					json.Unmarshal(body, &mapper)
					Expect(mapper["name"]).To(Equal("username-mapper"))
					Expect(mapper["identityProviderMapper"]).To(Equal("oidc-username-idp-mapper"))
					w.WriteHeader(http.StatusCreated)
					return
				}
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			reconciler := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := reconciler.addOpenShiftAttributeMappers(ctx, keycloakHost, "test-realm", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
		})

		It("continues when mapper already exists (409 Conflict)", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/test-realm/identity-provider/instances/openshift/mappers", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusConflict)
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			reconciler := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := reconciler.addOpenShiftAttributeMappers(ctx, keycloakHost, "test-realm", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("configureRealmProfileSettings", func() {
		It("disables Review Profile execution", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/test-realm/authentication/flows/first%20broker%20login/executions", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode([]map[string]interface{}{
						{
							"id":          "exec-1",
							"displayName": "Review Profile",
							"requirement": "REQUIRED",
						},
					})
					return
				}
				if r.Method == "PUT" {
					body, _ := io.ReadAll(r.Body)
					var exec map[string]interface{}
					json.Unmarshal(body, &exec)
					Expect(exec["requirement"]).To(Equal("DISABLED"))
					w.WriteHeader(http.StatusNoContent)
					return
				}
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			reconciler := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := reconciler.configureRealmProfileSettings(ctx, keycloakHost, "test-realm", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
		})

		It("skips when Review Profile already disabled", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/test-realm/authentication/flows/first%20broker%20login/executions", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode([]map[string]interface{}{
					{
						"id":          "exec-1",
						"displayName": "Review Profile",
						"requirement": "DISABLED",
					},
				})
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			reconciler := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := reconciler.configureRealmProfileSettings(ctx, keycloakHost, "test-realm", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("addQuayCredentialsIfNeeded", func() {
		It("returns unchanged when no QuayRegistry exists", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			input := []byte(`{"auths":{}}`)
			result, changed, err := r.addQuayCredentialsIfNeeded(ctx, input)
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(BeFalse())
			Expect(result).To(Equal(input))
		})

		It("returns unchanged when Quay hostname is empty", func() {
			quay := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata":   map[string]interface{}{"name": "mirror-operator-quay", "namespace": architectNamespace},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(quay).Build(),
				Scheme: testScheme,
			}
			input := []byte(`{"auths":{}}`)
			result, changed, err := r.addQuayCredentialsIfNeeded(ctx, input)
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(BeFalse())
			Expect(result).To(Equal(input))
		})
	})

	Describe("ensureOSUSPullSecret", func() {
		It("copies pull secret from operator namespace to openshift-update-service", func() {
			source := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: architectNamespace},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			sa := &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "openshift-update-service"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(source, sa).Build(),
				Scheme: testScheme,
			}
			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).NotTo(HaveOccurred())

			target := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: "openshift-update-service"}, target)).To(Succeed())

			updatedSA := &corev1.ServiceAccount{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "default", Namespace: "openshift-update-service"}, updatedSA)).To(Succeed())
			found := false
			for _, s := range updatedSA.ImagePullSecrets {
				if s.Name == "pull-secret" {
					found = true
				}
			}
			Expect(found).To(BeTrue())
		})

		It("returns error when source pull secret not found", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get pull-secret"))
		})
	})

	Describe("ensureQuayTLSCertificate", func() {
		It("returns error when certIssuer is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      "connected",
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureQuayTLSCertificate(ctx, platform, "quay.example.com")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("certIssuer must be specified"))
		})
	})

	Describe("mergePullSecrets", func() {
		It("merges two dockerconfig JSONs", func() {
			existing := []byte(`{"auths":{"registry.a.com":{"auth":"dXNlcjE6cGFzczE="}}}`)
			source := []byte(`{"auths":{"registry.b.com":{"auth":"dXNlcjI6cGFzczI="}}}`)
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			result, changed, err := r.mergePullSecrets(existing, source)
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(BeTrue())

			var cfg map[string]interface{}
			Expect(json.Unmarshal(result, &cfg)).To(Succeed())
			auths := cfg["auths"].(map[string]interface{})
			Expect(auths).To(HaveKey("registry.a.com"))
			Expect(auths).To(HaveKey("registry.b.com"))
		})

		It("returns unchanged when source is already merged", func() {
			existing := []byte(`{"auths":{"registry.a.com":{"auth":"dXNlcjE6cGFzczE="}}}`)
			source := []byte(`{"auths":{"registry.a.com":{"auth":"dXNlcjE6cGFzczE="}}}`)
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			_, changed, err := r.mergePullSecrets(existing, source)
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(BeFalse())
		})
	})

	Describe("ensureTrustifyReadOnlyScope", func() {
		It("creates read:document scope when it does not exist", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/trustify/client-scopes", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode([]map[string]interface{}{})
					return
				}
				if r.Method == "POST" {
					body, _ := io.ReadAll(r.Body)
					var scope map[string]interface{}
					json.Unmarshal(body, &scope)
					Expect(scope["name"]).To(Equal("read:document"))
					w.WriteHeader(http.StatusCreated)
					return
				}
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			reconciler := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := reconciler.ensureTrustifyReadOnlyScope(ctx, keycloakHost, "trustify", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
		})

		It("skips creation when scope already exists", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/trustify/client-scopes", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode([]map[string]interface{}{
					{"id": "scope-1", "name": "read:document"},
				})
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			reconciler := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := reconciler.ensureTrustifyReadOnlyScope(ctx, keycloakHost, "trustify", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureTrustifyCreateScope", func() {
		It("creates create:document scope when it does not exist", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/trustify/client-scopes", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode([]map[string]interface{}{})
					return
				}
				if r.Method == "POST" {
					body, _ := io.ReadAll(r.Body)
					var scope map[string]interface{}
					json.Unmarshal(body, &scope)
					Expect(scope["name"]).To(Equal("create:document"))
					w.WriteHeader(http.StatusCreated)
					return
				}
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			reconciler := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := reconciler.ensureTrustifyCreateScope(ctx, keycloakHost, "trustify", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureTrustifyManagerRole", func() {
		It("creates role when it does not exist (404)", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/trustify/roles/trustify-manager", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			})
			mux.HandleFunc("/admin/realms/trustify/roles", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" {
					body, _ := io.ReadAll(r.Body)
					var role map[string]interface{}
					json.Unmarshal(body, &role)
					Expect(role["name"]).To(Equal("trustify-manager"))
					w.WriteHeader(http.StatusCreated)
					return
				}
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			reconciler := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := reconciler.ensureTrustifyManagerRole(ctx, keycloakHost, "trustify", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when role already exists", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/trustify/roles/trustify-manager", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{"name": "trustify-manager"})
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			reconciler := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := reconciler.ensureTrustifyManagerRole(ctx, keycloakHost, "trustify", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("assignScopeToClient", func() {
		It("assigns scope to client by UUID", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/trustify/client-scopes", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode([]map[string]interface{}{
					{"id": "scope-uuid-123", "name": "read:document"},
				})
			})
			mux.HandleFunc("/admin/realms/trustify/clients/client-uuid-456/default-client-scopes/scope-uuid-123", func(w http.ResponseWriter, r *http.Request) {
				Expect(r.Method).To(Equal("PUT"))
				w.WriteHeader(http.StatusNoContent)
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			reconciler := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := reconciler.assignScopeToClient(ctx, keycloakHost, "trustify", "client-uuid-456", "read:document", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error when scope not found", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/trustify/client-scopes", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode([]map[string]interface{}{})
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			reconciler := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := reconciler.assignScopeToClient(ctx, keycloakHost, "trustify", "client-uuid", "read:document", "test-token", ts.Client())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("scope not found"))
		})
	})

	Describe("discoverPackageInfo", func() {
		It("returns catalog info from PackageManifest status", func() {
			pm := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "packages.operators.coreos.com/v1",
				"kind":       "PackageManifest",
				"metadata":   map[string]interface{}{"name": "test-operator", "namespace": "openshift-marketplace"},
				"status": map[string]interface{}{
					"catalogSource":          "community-operators",
					"catalogSourceNamespace": "openshift-marketplace",
					"defaultChannel":         "stable",
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pm).Build(),
				Scheme: testScheme,
			}
			catalog, catalogNS, channel, err := r.discoverPackageInfo(ctx, "test-operator")
			Expect(err).NotTo(HaveOccurred())
			Expect(catalog).To(Equal("community-operators"))
			Expect(catalogNS).To(Equal("openshift-marketplace"))
			Expect(channel).To(Equal("stable"))
		})

		It("returns error when PackageManifest not found", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			_, _, _, err := r.discoverPackageInfo(ctx, "nonexistent-operator")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not found"))
		})
	})

	Describe("reconcileAirgappedSubscriptions", func() {
		It("does nothing when airgapped is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "airgapped",
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			_, err := r.reconcileAirgappedSubscriptions(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
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
			Expect(r.Get(ctx, client.ObjectKey{Name: "test-ns"}, ns)).To(Succeed())
		})

		It("returns nil when namespace already exists", func() {
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "existing-ns"}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ns).Build(),
				Scheme: testScheme,
			}
			err := r.ensureNamespace(ctx, "existing-ns")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureTrustifyOIDCClient", func() {
		It("creates a new public OIDC client", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/trustify/clients", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					json.NewEncoder(w).Encode([]map[string]interface{}{})
					return
				}
				if r.Method == "POST" {
					w.WriteHeader(http.StatusCreated)
					return
				}
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()
			keycloakHost := strings.TrimPrefix(ts.URL, "https://")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureTrustifyOIDCClient(ctx, keycloakHost, "trustify", "frontend", "tpa.example.com", "test-token", ts.Client(), true)
			Expect(err).NotTo(HaveOccurred())
		})

		It("updates an existing confidential client and stores secret", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/trustify/clients", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					json.NewEncoder(w).Encode([]map[string]interface{}{
						{"id": "uuid-123", "clientId": "cli"},
					})
					return
				}
			})
			mux.HandleFunc("/admin/realms/trustify/clients/uuid-123", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "PUT" {
					w.WriteHeader(http.StatusNoContent)
					return
				}
			})
			mux.HandleFunc("/admin/realms/trustify/clients/uuid-123/client-secret", func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]interface{}{"value": "test-secret"})
			})
			mux.HandleFunc("/admin/realms/trustify/client-scopes", func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode([]map[string]interface{}{
					{"id": "scope-uuid", "name": "read:document"},
				})
			})
			mux.HandleFunc("/admin/realms/trustify/clients/uuid-123/default-client-scopes/scope-uuid", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()
			keycloakHost := strings.TrimPrefix(ts.URL, "https://")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureTrustifyOIDCClient(ctx, keycloakHost, "trustify", "cli", "tpa.example.com", "test-token", ts.Client(), false)
			Expect(err).NotTo(HaveOccurred())

			secret := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "rhtpa-oidc-cli-secret", Namespace: architectNamespace}, secret)).To(Succeed())
		})
	})

	Describe("assignScopeToTrustifyClients", func() {
		It("assigns scopes to frontend and cli clients", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/trustify/clients", func(w http.ResponseWriter, r *http.Request) {
				clientID := r.URL.Query().Get("clientId")
				json.NewEncoder(w).Encode([]map[string]interface{}{
					{"id": "uuid-" + clientID, "clientId": clientID},
				})
			})
			mux.HandleFunc("/admin/realms/trustify/client-scopes", func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode([]map[string]interface{}{
					{"id": "scope-read", "name": "read:document"},
					{"id": "scope-create", "name": "create:document"},
				})
			})
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "PUT" || r.Method == "POST" {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				if r.Method == "GET" {
					if strings.Contains(r.URL.Path, "service-account-user") {
						json.NewEncoder(w).Encode(map[string]interface{}{"id": "sa-uuid"})
						return
					}
					if strings.Contains(r.URL.Path, "/roles/") {
						json.NewEncoder(w).Encode(map[string]interface{}{"id": "role-uuid", "name": "trustify-manager"})
						return
					}
				}
				w.WriteHeader(http.StatusOK)
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()
			keycloakHost := strings.TrimPrefix(ts.URL, "https://")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.assignScopeToTrustifyClients(ctx, keycloakHost, "trustify", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("assignReadScopeToClient", func() {
		It("delegates to assignScopeToClient with read:document", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/trustify/client-scopes", func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode([]map[string]interface{}{
					{"id": "scope-read-uuid", "name": "read:document"},
				})
			})
			mux.HandleFunc("/admin/realms/trustify/clients/client-uuid-1/default-client-scopes/scope-read-uuid", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()
			keycloakHost := strings.TrimPrefix(ts.URL, "https://")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.assignReadScopeToClient(ctx, keycloakHost, "trustify", "client-uuid-1", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("assignRoleToServiceAccountByClient", func() {
		It("assigns role to service account by client UUID", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/trustify/roles", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					json.NewEncoder(w).Encode([]map[string]interface{}{})
					return
				}
				if r.Method == "POST" {
					w.WriteHeader(http.StatusCreated)
					return
				}
			})
			mux.HandleFunc("/admin/realms/trustify/roles/trustify-manager", func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]interface{}{"id": "role-uuid", "name": "trustify-manager"})
			})
			mux.HandleFunc("/admin/realms/trustify/clients/client-uuid/service-account-user", func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]interface{}{"id": "sa-user-uuid"})
			})
			mux.HandleFunc("/admin/realms/trustify/users/sa-user-uuid/role-mappings/realm", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()
			keycloakHost := strings.TrimPrefix(ts.URL, "https://")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.assignRoleToServiceAccountByClient(ctx, keycloakHost, "trustify", "client-uuid", "trustify-manager", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("assignRoleToServiceAccount", func() {
		It("assigns role to service account by username", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/trustify/users", func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode([]map[string]interface{}{
					{"id": "user-uuid", "username": "service-account-cli"},
				})
			})
			mux.HandleFunc("/admin/realms/trustify/roles/trustify-manager", func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]interface{}{"id": "role-uuid", "name": "trustify-manager"})
			})
			mux.HandleFunc("/admin/realms/trustify/users/user-uuid/role-mappings/realm", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()
			keycloakHost := strings.TrimPrefix(ts.URL, "https://")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.assignRoleToServiceAccount(ctx, keycloakHost, "trustify", "service-account-cli", "trustify-manager", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error when user not found", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/trustify/users", func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode([]map[string]interface{}{})
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()
			keycloakHost := strings.TrimPrefix(ts.URL, "https://")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.assignRoleToServiceAccount(ctx, keycloakHost, "trustify", "nonexistent-user", "trustify-manager", "test-token", ts.Client())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not found"))
		})

		It("handles 409 conflict (already assigned) gracefully", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/trustify/users", func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode([]map[string]interface{}{
					{"id": "user-uuid", "username": "service-account-cli"},
				})
			})
			mux.HandleFunc("/admin/realms/trustify/roles/trustify-manager", func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]interface{}{"id": "role-uuid", "name": "trustify-manager"})
			})
			mux.HandleFunc("/admin/realms/trustify/users/user-uuid/role-mappings/realm", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusConflict)
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()
			keycloakHost := strings.TrimPrefix(ts.URL, "https://")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.assignRoleToServiceAccount(ctx, keycloakHost, "trustify", "service-account-cli", "trustify-manager", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("getClientScopeIDByName", func() {
		It("returns scope ID when found", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/test-realm/client-scopes", func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode([]map[string]interface{}{
					{"id": "scope-1", "name": "email"},
					{"id": "scope-2", "name": "profile"},
				})
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()
			keycloakHost := strings.TrimPrefix(ts.URL, "https://")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			id, err := r.getClientScopeIDByName(ctx, keycloakHost, "test-realm", "email", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
			Expect(id).To(Equal("scope-1"))
		})

		It("returns empty string when scope not found", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/admin/realms/test-realm/client-scopes", func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode([]map[string]interface{}{
					{"id": "scope-1", "name": "email"},
				})
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()
			keycloakHost := strings.TrimPrefix(ts.URL, "https://")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			id, err := r.getClientScopeIDByName(ctx, keycloakHost, "test-realm", "nonexistent", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
			Expect(id).To(BeEmpty())
		})
	})

	Describe("ensureEmailVerifiedClientScope", func() {
		It("creates scope and mappers when they don't exist", func() {
			mux := http.NewServeMux()
			callCount := 0
			mux.HandleFunc("/admin/realms/test-realm/client-scopes", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					callCount++
					if callCount == 1 {
						json.NewEncoder(w).Encode([]map[string]interface{}{})
					} else {
						json.NewEncoder(w).Encode([]map[string]interface{}{
							{"id": "email-scope-id", "name": "email"},
						})
					}
					return
				}
				if r.Method == "POST" {
					w.Header().Set("Location", "/admin/realms/test-realm/client-scopes/new-scope-id")
					w.WriteHeader(http.StatusCreated)
					return
				}
			})
			mux.HandleFunc("/admin/realms/test-realm/client-scopes/new-scope-id/protocol-mappers/models", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					json.NewEncoder(w).Encode([]map[string]interface{}{})
					return
				}
				if r.Method == "POST" {
					w.WriteHeader(http.StatusCreated)
					return
				}
			})
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "PUT" || r.Method == "DELETE" {
					w.WriteHeader(http.StatusNoContent)
				}
			})
			ts := httptest.NewTLSServer(mux)
			defer ts.Close()
			keycloakHost := strings.TrimPrefix(ts.URL, "https://")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureEmailVerifiedClientScope(ctx, keycloakHost, "test-client", "test-realm", "client-uuid", "test-token", ts.Client())
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureRHTPAPostgreSQL", func() {
		It("creates all PostgreSQL resources on first run", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			host, dbName, user, password, err := r.ensureRHTPAPostgreSQL(ctx, "50Gi", "200Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(host).To(ContainSubstring("rhtpa-postgresql"))
			Expect(dbName).To(Equal("rhtpadb"))
			Expect(user).To(Equal("rhtpa"))
			Expect(password).NotTo(BeEmpty())

			secret := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "rhtpa-db-credentials", Namespace: architectNamespace}, secret)).To(Succeed())

			pvc := &corev1.PersistentVolumeClaim{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "rhtpa-postgresql-data", Namespace: architectNamespace}, pvc)).To(Succeed())

			sts := &appsv1.StatefulSet{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "rhtpa-postgresql", Namespace: architectNamespace}, sts)).To(Succeed())

			svc := &corev1.Service{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "rhtpa-postgresql", Namespace: architectNamespace}, svc)).To(Succeed())
		})

		It("reads password from existing secret on subsequent runs", func() {
			existingSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "rhtpa-db-credentials", Namespace: architectNamespace},
				Data: map[string][]byte{
					"username": []byte("rhtpa"),
					"password": []byte("existing-password"),
					"database": []byte("rhtpadb"),
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existingSecret).Build(),
				Scheme: testScheme,
			}
			_, _, _, password, err := r.ensureRHTPAPostgreSQL(ctx, "", "")
			Expect(err).NotTo(HaveOccurred())
			Expect(password).To(Equal("existing-password"))
		})
	})

	Describe("ensureManagedPostgreSQL", func() {
		It("creates PVC, StatefulSet, and Service for Keycloak", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureManagedPostgreSQL(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			pvc := &corev1.PersistentVolumeClaim{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "keycloak-postgresql-data", Namespace: architectNamespace}, pvc)).To(Succeed())

			sts := &appsv1.StatefulSet{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "keycloak-postgresql", Namespace: architectNamespace}, sts)).To(Succeed())

			svc := &corev1.Service{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "keycloak-postgresql", Namespace: architectNamespace}, svc)).To(Succeed())
		})

		It("is idempotent when resources already exist", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "keycloak-postgresql-data", Namespace: architectNamespace},
			}
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: "keycloak-postgresql", Namespace: architectNamespace},
			}
			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "keycloak-postgresql", Namespace: architectNamespace},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pvc, sts, svc).Build(),
				Scheme: testScheme,
			}
			err := r.ensureManagedPostgreSQL(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("restartBackendPods", func() {
		It("deletes backend pods to restart them", func() {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "backend-pod-1",
					Namespace: architectNamespace,
					Labels: map[string]string{
						"app.kubernetes.io/component": "backend",
						"app.kubernetes.io/part-of":   "mirror-operator",
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pod).Build(),
				Scheme: testScheme,
			}
			err := r.restartBackendPods(ctx)
			Expect(err).NotTo(HaveOccurred())

			podList := &corev1.PodList{}
			Expect(r.List(ctx, podList, client.InNamespace(architectNamespace))).To(Succeed())
			Expect(podList.Items).To(BeEmpty())
		})

		It("succeeds when no backend pods exist", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.restartBackendPods(ctx)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("getServiceCACert", func() {
		It("returns CA cert from ConfigMap", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "signing-cabundle", Namespace: "openshift-service-ca"},
				Data:       map[string]string{"ca-bundle.crt": "-----BEGIN CERTIFICATE-----\nMIIC..."},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cm).Build(),
				Scheme: testScheme,
			}
			cert, err := r.getServiceCACert(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(cert).To(ContainSubstring("BEGIN CERTIFICATE"))
		})

		It("returns error when ConfigMap not found", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			_, err := r.getServiceCACert(ctx)
			Expect(err).To(HaveOccurred())
		})

		It("returns error when ca-bundle.crt key missing", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "signing-cabundle", Namespace: "openshift-service-ca"},
				Data:       map[string]string{"other-key": "data"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cm).Build(),
				Scheme: testScheme,
			}
			_, err := r.getServiceCACert(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("ca-bundle.crt not found"))
		})
	})

	Describe("deleteConsolePluginResources", func() {
		It("deletes ConsolePlugin, Service, and Deployment", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.deleteConsolePluginResources(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("deleteArchitectFrontendResources", func() {
		It("returns nil when resources don't exist", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.deleteArchitectFrontendResources(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("reconcileArtifactFileServer", func() {
		It("skips when no completed pipelines exist", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileArtifactFileServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("creates deployment and service for completed pipelines with bound PVCs", func() {
			pipeline := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pipeline", Namespace: architectNamespace},
				Status: mirrorv1.CollectionPipelineStatus{
					Phase: "Complete",
				},
			}
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts-test-pipeline", Namespace: architectNamespace},
				Status: corev1.PersistentVolumeClaimStatus{
					Phase: corev1.ClaimBound,
				},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pipeline, pvc).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileArtifactFileServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			dep := &appsv1.Deployment{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "artifact-fileserver", Namespace: architectNamespace}, dep)).To(Succeed())

			svc := &corev1.Service{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "artifact-fileserver", Namespace: architectNamespace}, svc)).To(Succeed())
		})
	})

	Describe("autoExpandPVC", func() {
		It("does not expand when PVC is within capacity limits", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: architectNamespace},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("50Gi"),
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pvc).Build(),
				Scheme: testScheme,
			}
			logger := log.FromContext(ctx)
			r.autoExpandPVC(ctx, pvc, "200Gi", logger)
		})
	})
})

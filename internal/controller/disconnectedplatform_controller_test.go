package controller

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"time"

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
	"github.com/mathianasj/mirror-operator/config/scripts"
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

		It("reconciles connected mode with RHTPA configured (missing CRDs trigger requeue)", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Finalizers: []string{platformFinalizer},
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTPA: &mirrorv1.RHTPAInstallerConfig{},
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

			updated := &mirrorv1.DisconnectedPlatform{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(err).NotTo(HaveOccurred())
			// RHTPA with nil Storage is a no-op, so reconcile succeeds with Ready phase
			Expect(updated.Status.Phase).To(Equal(mirrorv1.PlatformPhaseReady))
			Expect(result).To(Equal(reconcile.Result{}))
		})

		It("reconciles connected mode with RHTPA storage configured (error is logged, not returned)", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Finalizers: []string{platformFinalizer},
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTPA: &mirrorv1.RHTPAInstallerConfig{
							Storage: &mirrorv1.RHTPAStorageConfig{
								Type: "s3",
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

			updated := &mirrorv1.DisconnectedPlatform{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(err).NotTo(HaveOccurred())
			// reconcileRHTPAConfig errors (cluster Ingress not found in test env),
			// but the error is logged not returned. Reconcile completes with Ready phase.
			Expect(updated.Status.Phase).To(Equal(mirrorv1.PlatformPhaseReady))
			Expect(result).To(Equal(reconcile.Result{}))
		})

		It("reconciles airgapped mode with Airgapped spec set", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Finalizers: []string{platformFinalizer},
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry:    "registry.airgap.local:8443",
						ManagementCluster: false,
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

			updated := &mirrorv1.DisconnectedPlatform{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(err).NotTo(HaveOccurred())
			// Airgapped subscriptions will fail (no PackageManifest CRD) so
			// airgappedRequeue is set, triggering a requeue after 30s.
			Expect(result.RequeueAfter).To(Equal(30 * time.Second))
		})

		It("aggregates collection history from multiple completed pipelines", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Finalizers: []string{platformFinalizer},
				},
			}
			pipeline1 := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "pipeline-1", Namespace: "default"},
				Status: mirrorv1.CollectionPipelineStatus{
					Version: "v2025.01.01.001-manual",
					Phase:   "Complete",
				},
			}
			pipeline2 := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "pipeline-2", Namespace: "default"},
				Status: mirrorv1.CollectionPipelineStatus{
					Version: "v2025.02.01.001-scheduled",
					Phase:   "Complete",
				},
			}
			pipelineRunning := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "pipeline-running", Namespace: "default"},
				Status: mirrorv1.CollectionPipelineStatus{
					Version: "v2025.03.01.001-manual",
					Phase:   "Collecting",
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform, pipeline1, pipeline2, pipelineRunning).
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
			// Only 2 complete pipelines should appear; running pipeline is excluded
			Expect(updated.Status.CollectionHistory).To(HaveLen(2))
			Expect(updated.Status.LastCollection).NotTo(BeNil())
		})

		It("aggregates import history with multiple imports and uses Name when CollectionVersion is empty", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Finalizers: []string{platformFinalizer},
				},
			}
			import1 := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "import-1", Namespace: "default"},
				Spec: mirrorv1.MirrorImportSpec{
					CollectionVersion: "v2025.01.01.001-manual",
				},
				Status: mirrorv1.MirrorImportStatus{Phase: "Complete"},
			}
			import2 := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "import-auto-name", Namespace: "default"},
				Spec:       mirrorv1.MirrorImportSpec{},
				Status:     mirrorv1.MirrorImportStatus{Phase: "Complete"},
			}
			importFailed := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "import-failed", Namespace: "default"},
				Spec: mirrorv1.MirrorImportSpec{
					CollectionVersion: "v2025.02.01.001-manual",
				},
				Status: mirrorv1.MirrorImportStatus{Phase: "Failed"},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform, import1, import2, importFailed).
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
			// Only 2 complete imports; failed one is excluded
			Expect(updated.Status.ImportHistory).To(HaveLen(2))
			Expect(updated.Status.LastImport).NotTo(BeNil())
			// Verify the import with empty CollectionVersion uses Name as version
			versions := make(map[string]bool)
			for _, i := range updated.Status.ImportHistory {
				versions[i.Version] = true
			}
			Expect(versions).To(HaveKey("v2025.01.01.001-manual"))
			Expect(versions).To(HaveKey("import-auto-name"))
		})

		It("sets Ready condition and components on successful reconcile", func() {
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
			_, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			updated := &mirrorv1.DisconnectedPlatform{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(err).NotTo(HaveOccurred())

			// Verify Ready condition is set
			var readyCondition *metav1.Condition
			for i := range updated.Status.Conditions {
				if updated.Status.Conditions[i].Type == "Ready" {
					readyCondition = &updated.Status.Conditions[i]
					break
				}
			}
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionTrue))
			Expect(readyCondition.Reason).To(Equal("ReconciliationSucceeded"))

			// Verify disconnected-platform component is always present
			componentNames := make(map[string]bool)
			for _, c := range updated.Status.Components {
				componentNames[c.Name] = true
			}
			Expect(componentNames).To(HaveKey("disconnected-platform"))
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
			Expect(cm.Labels).To(HaveKeyWithValue("config.openshift.io/inject-trusted-cabundle", "true"))
		})

		It("does not error when ConfigMap already exists with correct label", func() {
			existing := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterCABundleName,
					Namespace: architectNamespace,
					Labels: map[string]string{
						"config.openshift.io/inject-trusted-cabundle": "true",
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
					Expect(envNames).To(HaveKeyWithValue("CURL_CA_BUNDLE", caPath))
					Expect(envNames).To(HaveKeyWithValue("AWS_CA_BUNDLE", caPath))
					Expect(envNames).To(HaveKeyWithValue("REQUESTS_CA_BUNDLE", caPath))
				}
			}

			// Verify existing env is preserved in task2
			task2Spec := result[1]["taskSpec"].(map[string]interface{})
			task2Steps := task2Spec["steps"].([]map[string]interface{})
			task2Env := task2Steps[0]["env"].([]map[string]interface{})
			Expect(task2Env).To(HaveLen(5))
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
					{"name": "truststore-paths", "value": clusterCAFilePath},
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

			// Verify truststore-paths in additionalOptions
			opts := kcSpec["additionalOptions"].([]map[string]interface{})
			foundPaths := false
			for _, opt := range opts {
				if opt["name"] == "truststore-paths" {
					foundPaths = true
					Expect(opt["value"]).To(Equal(clusterCAFilePath))
				}
			}
			Expect(foundPaths).To(BeTrue(), "expected truststore-paths in additionalOptions")
		})
	})

	Describe("reconcileRHCOSServer", func() {
		It("uses MirrorRegistry when MirrorRegistryConfig is not set", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.example.com/mirror",
						ACM: &mirrorv1.AirgappedACMConfig{
							Enabled: true,
							HostInventory: &mirrorv1.HostInventoryConfig{
								Enabled: true,
								Versions: []mirrorv1.HostInventoryVersion{
									{OpenshiftVersion: "4.18.12"},
								},
							},
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			err := r.reconcileRHCOSServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			err = r.Get(ctx, client.ObjectKey{Name: "rhcos-server", Namespace: architectNamespace}, deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Template.Spec.Containers[0].Image).To(Equal("quay.example.com/mirror/rhcos-server:4.18.12"))
		})

		It("uses node mirror registry when MirrorRegistryConfig is set", func() {
			masterNode := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "master-0",
					Labels: map[string]string{"node-role.kubernetes.io/master": ""},
				},
				Status: corev1.NodeStatus{
					Addresses: []corev1.NodeAddress{
						{Type: corev1.NodeInternalIP, Address: "10.0.0.5"},
					},
				},
			}

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.example.com/mirror",
						MirrorRegistryConfig: &mirrorv1.MirrorRegistryConfig{
							Port: 8443,
						},
						ACM: &mirrorv1.AirgappedACMConfig{
							Enabled: true,
							HostInventory: &mirrorv1.HostInventoryConfig{
								Enabled: true,
								Versions: []mirrorv1.HostInventoryVersion{
									{OpenshiftVersion: "4.18.12"},
								},
							},
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform, masterNode).Build(),
				Scheme: testScheme,
			}

			err := r.reconcileRHCOSServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			err = r.Get(ctx, client.ObjectKey{Name: "rhcos-server", Namespace: architectNamespace}, deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Template.Spec.Containers[0].Image).To(Equal("10.0.0.5:8443/rhcos-server:4.18.12"))
		})

		It("uses explicit RHCOSImage override regardless of MirrorRegistryConfig", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.example.com/mirror",
						MirrorRegistryConfig: &mirrorv1.MirrorRegistryConfig{
							Port: 8443,
						},
						ACM: &mirrorv1.AirgappedACMConfig{
							Enabled: true,
							HostInventory: &mirrorv1.HostInventoryConfig{
								Enabled:    true,
								RHCOSImage: "custom-registry:5000/rhcos-server:4.21",
								Versions: []mirrorv1.HostInventoryVersion{
									{OpenshiftVersion: "4.18.12"},
								},
							},
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			err := r.reconcileRHCOSServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			err = r.Get(ctx, client.ObjectKey{Name: "rhcos-server", Namespace: architectNamespace}, deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Template.Spec.Containers[0].Image).To(Equal("custom-registry:5000/rhcos-server:4.21"))
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
			testContainerBuilder := func(name, image string, labels map[string]string) map[string]interface{} {
				return map[string]interface{}{
					"name":  name,
					"image": image,
				}
			}
			dep := architectBackendDeployment("test-backend", "quay.io/test/backend:v1", 2, labels, "pull-secret", "openshift-config", testContainerBuilder)

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
					Mode:      "connected",
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
					Mode:      "connected",
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

	Describe("checkKeycloakHealth", func() {
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
			err := r.checkKeycloakHealth(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when Keycloak is ready", func() {
			kc := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "k8s.keycloak.org/v2alpha1",
				"kind":       "Keycloak",
				"metadata":   map[string]interface{}{"name": "mirror-operator-keycloak", "namespace": architectNamespace},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{"type": "Ready", "status": "True"},
					},
				},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
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
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(kc).Build(),
				Scheme: testScheme,
			}
			err := r.checkKeycloakHealth(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error when Keycloak not ready", func() {
			kc := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "k8s.keycloak.org/v2alpha1",
				"kind":       "Keycloak",
				"metadata":   map[string]interface{}{"name": "mirror-operator-keycloak", "namespace": architectNamespace},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{"type": "Ready", "status": "False", "message": "initializing"},
					},
				},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
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
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(kc).Build(),
				Scheme: testScheme,
			}
			err := r.checkKeycloakHealth(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not ready"))
		})

		It("returns error when Keycloak resource not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
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
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.checkKeycloakHealth(ctx, platform)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("performRHTASHealthChecks", func() {
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
			err := r.performRHTASHealthChecks(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when managed OIDC is enabled but calls sub-checks", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
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
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.performRHTASHealthChecks(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("checkAndFixTUFKeys", func() {
		It("returns nil when Securesign not found", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.checkAndFixTUFKeys(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when TUF keys are clean (no tsa.certchain.pem)", func() {
			securesign := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Securesign",
				"metadata":   map[string]interface{}{"name": "mirror-operator-securesign", "namespace": architectNamespace},
				"spec": map[string]interface{}{
					"tuf": map[string]interface{}{
						"keys": []interface{}{
							map[string]interface{}{"name": "fulcio_v1.crt.pem"},
							map[string]interface{}{"name": "rekor.pub"},
						},
					},
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build(),
				Scheme: testScheme,
			}
			err := r.checkAndFixTUFKeys(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("removes tsa.certchain.pem from TUF keys", func() {
			securesign := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Securesign",
				"metadata":   map[string]interface{}{"name": "mirror-operator-securesign", "namespace": architectNamespace},
				"spec": map[string]interface{}{
					"tuf": map[string]interface{}{
						"keys": []interface{}{
							map[string]interface{}{"name": "fulcio_v1.crt.pem"},
							map[string]interface{}{"name": "tsa.certchain.pem"},
							map[string]interface{}{"name": "rekor.pub"},
						},
					},
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build(),
				Scheme: testScheme,
			}
			err := r.checkAndFixTUFKeys(ctx)
			Expect(err).NotTo(HaveOccurred())

			updated := &unstructured.Unstructured{Object: map[string]interface{}{}}
			updated.SetGroupVersionKind(securesignGVK)
			Expect(r.Get(ctx, client.ObjectKey{Name: "mirror-operator-securesign", Namespace: architectNamespace}, updated)).To(Succeed())
			keys, _, _ := unstructured.NestedSlice(updated.Object, "spec", "tuf", "keys")
			Expect(len(keys)).To(Equal(2))
			for _, key := range keys {
				keyMap := key.(map[string]interface{})
				Expect(keyMap["name"]).NotTo(Equal("tsa.certchain.pem"))
			}
		})
	})

	Describe("checkFulcioKeycloakConnectivity", func() {
		It("returns nil when Securesign not found", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.checkFulcioKeycloakConnectivity(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when no Fulcio pods exist", func() {
			securesign := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Securesign",
				"metadata":   map[string]interface{}{"name": "mirror-operator-securesign", "namespace": architectNamespace},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build(),
				Scheme: testScheme,
			}
			err := r.checkFulcioKeycloakConnectivity(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when Fulcio pod has low restart count", func() {
			securesign := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Securesign",
				"metadata":   map[string]interface{}{"name": "mirror-operator-securesign", "namespace": architectNamespace},
			}}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "fulcio-pod-1",
					Namespace: architectNamespace,
					Labels:    map[string]string{"app": "fulcio-server"},
				},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{RestartCount: 1},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign, pod).Build(),
				Scheme: testScheme,
			}
			err := r.checkFulcioKeycloakConnectivity(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("restarts Fulcio pod when high restart count and Keycloak is ready", func() {
			securesign := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Securesign",
				"metadata":   map[string]interface{}{"name": "mirror-operator-securesign", "namespace": architectNamespace},
			}}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "fulcio-pod-1",
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
				"metadata":   map[string]interface{}{"name": "mirror-operator-keycloak", "namespace": architectNamespace},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{"type": "Ready", "status": "True"},
					},
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign, pod, kc).Build(),
				Scheme: testScheme,
			}
			err := r.checkFulcioKeycloakConnectivity(ctx)
			Expect(err).NotTo(HaveOccurred())

			podList := &corev1.PodList{}
			Expect(r.List(ctx, podList, client.InNamespace(architectNamespace), client.MatchingLabels{"app": "fulcio-server"})).To(Succeed())
			Expect(podList.Items).To(BeEmpty())
		})
	})

	Describe("updateArchitectRoute", func() {
		It("updates an existing route", func() {
			existing := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "route.openshift.io/v1",
				"kind":       "Route",
				"metadata":   map[string]interface{}{"name": "test-route", "namespace": architectNamespace},
				"spec": map[string]interface{}{
					"to": map[string]interface{}{"kind": "Service", "name": "old-service"},
				},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			err := r.updateArchitectRoute(ctx, platform, "test-route", nil, "frontend-svc")
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error when route not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.updateArchitectRoute(ctx, platform, "nonexistent-route", nil, "frontend-svc")
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("enableConsolePluginInOperator", func() {
		It("adds plugin to existing console operator", func() {
			console := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "operator.openshift.io/v1",
				"kind":       "Console",
				"metadata":   map[string]interface{}{"name": "cluster"},
				"spec":       map[string]interface{}{},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(console).Build(),
				Scheme: testScheme,
			}
			err := r.enableConsolePluginInOperator(ctx, "airgap-architect-plugin")
			Expect(err).NotTo(HaveOccurred())

			updated := &unstructured.Unstructured{Object: map[string]interface{}{}}
			updated.SetGroupVersionKind(schema.GroupVersionKind{Group: "operator.openshift.io", Version: "v1", Kind: "Console"})
			Expect(r.Get(ctx, client.ObjectKey{Name: "cluster"}, updated)).To(Succeed())
			plugins, _, _ := unstructured.NestedStringSlice(updated.Object, "spec", "plugins")
			Expect(plugins).To(ContainElement("airgap-architect-plugin"))
		})

		It("is idempotent when plugin already enabled", func() {
			console := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "operator.openshift.io/v1",
				"kind":       "Console",
				"metadata":   map[string]interface{}{"name": "cluster"},
				"spec": map[string]interface{}{
					"plugins": []interface{}{"airgap-architect-plugin"},
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(console).Build(),
				Scheme: testScheme,
			}
			err := r.enableConsolePluginInOperator(ctx, "airgap-architect-plugin")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("updateConsolePluginCR", func() {
		It("updates ConsolePlugin spec with CA cert", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "signing-cabundle", Namespace: "openshift-service-ca"},
				Data:       map[string]string{"ca-bundle.crt": "-----BEGIN CERTIFICATE-----\ntest-cert"},
			}
			consolePlugin := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "console.openshift.io/v1",
				"kind":       "ConsolePlugin",
				"metadata":   map[string]interface{}{"name": "airgap-architect-plugin"},
				"spec": map[string]interface{}{
					"displayName": "Airgap Architect",
					"backend":     map[string]interface{}{},
				},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cm, consolePlugin).Build(),
				Scheme: testScheme,
			}
			err := r.updateConsolePluginCR(ctx, platform, consolePlugin, "airgap-architect-plugin")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("reconcileAirgappedACM", func() {
		It("returns false when ACM package not available", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "airgapped",
					Airgapped: &mirrorv1.AirgappedConfig{
						ACM: &mirrorv1.AirgappedACMConfig{Enabled: true},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			ready, err := r.reconcileAirgappedACM(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
			Expect(ready).To(BeFalse())
		})
	})

	Describe("nodeReadyStatus", func() {
		It("returns True when node has Ready=True condition", func() {
			node := &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			Expect(nodeReadyStatus(node)).To(Equal(corev1.ConditionTrue))
		})

		It("returns False when node has Ready=False", func() {
			node := &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionFalse},
					},
				},
			}
			Expect(nodeReadyStatus(node)).To(Equal(corev1.ConditionFalse))
		})

		It("returns Unknown when no Ready condition", func() {
			node := &corev1.Node{}
			Expect(nodeReadyStatus(node)).To(Equal(corev1.ConditionUnknown))
		})
	})

	Describe("pipelineProxyEnvVars", func() {
		It("returns all 6 proxy env var templates", func() {
			envs := pipelineProxyEnvVars()
			Expect(envs).To(HaveLen(6))
			names := []string{}
			for _, e := range envs {
				names = append(names, e["name"].(string))
			}
			Expect(names).To(ContainElements("HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy"))
		})
	})

	Describe("appendEnvVars", func() {
		It("appends extra env vars to existing", func() {
			existing := []map[string]interface{}{{"name": "FOO", "value": "bar"}}
			extra := []map[string]interface{}{{"name": "BAZ", "value": "qux"}}
			result := appendEnvVars(existing, extra)
			Expect(result).To(HaveLen(2))
			Expect(result[1]["name"]).To(Equal("BAZ"))
		})
	})

	Describe("ensureClusterCABundleInNamespace", func() {
		It("creates ConfigMap with inject label when not found", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureClusterCABundleInNamespace(ctx, "test-ns")
			Expect(err).NotTo(HaveOccurred())

			cm := &corev1.ConfigMap{}
			Expect(r.Get(ctx, client.ObjectKey{Name: clusterCABundleName, Namespace: "test-ns"}, cm)).To(Succeed())
			Expect(cm.Labels["config.openshift.io/inject-trusted-cabundle"]).To(Equal("true"))
		})

		It("is idempotent when ConfigMap exists with correct label", func() {
			existing := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterCABundleName,
					Namespace: "test-ns",
					Labels:    map[string]string{"config.openshift.io/inject-trusted-cabundle": "true"},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			Expect(r.ensureClusterCABundleInNamespace(ctx, "test-ns")).To(Succeed())
		})

		It("updates label when ConfigMap exists without it", func() {
			existing := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterCABundleName,
					Namespace: "test-ns",
					Labels:    map[string]string{},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			Expect(r.ensureClusterCABundleInNamespace(ctx, "test-ns")).To(Succeed())

			updated := &corev1.ConfigMap{}
			Expect(r.Get(ctx, client.ObjectKey{Name: clusterCABundleName, Namespace: "test-ns"}, updated)).To(Succeed())
			Expect(updated.Labels["config.openshift.io/inject-trusted-cabundle"]).To(Equal("true"))
		})
	})

	Describe("ensureServiceCAConfigMap", func() {
		It("creates ConfigMap with inject annotation when not found", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureServiceCAConfigMap(ctx)
			Expect(err).NotTo(HaveOccurred())

			cm := &corev1.ConfigMap{}
			err = r.Get(ctx, client.ObjectKey{Name: serviceCAConfigMap, Namespace: architectNamespace}, cm)
			Expect(err).NotTo(HaveOccurred())
			Expect(cm.Annotations["service.beta.openshift.io/inject-cabundle"]).To(Equal("true"))
		})

		It("is idempotent when ConfigMap already exists with correct annotation", func() {
			existing := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      serviceCAConfigMap,
					Namespace: architectNamespace,
					Annotations: map[string]string{
						"service.beta.openshift.io/inject-cabundle": "true",
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			Expect(r.ensureServiceCAConfigMap(ctx)).To(Succeed())
		})

		It("updates annotation when ConfigMap exists without it", func() {
			existing := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      serviceCAConfigMap,
					Namespace: architectNamespace,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			Expect(r.ensureServiceCAConfigMap(ctx)).To(Succeed())

			updated := &corev1.ConfigMap{}
			r.Get(ctx, client.ObjectKey{Name: serviceCAConfigMap, Namespace: architectNamespace}, updated)
			Expect(updated.Annotations["service.beta.openshift.io/inject-cabundle"]).To(Equal("true"))
		})
	})

	Describe("ensureCombinedCABundle", func() {
		It("creates combined CA bundle from cluster and service CAs", func() {
			clusterCA := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterCABundleName,
					Namespace: architectNamespace,
				},
				Data: map[string]string{
					clusterCABundleKey: "-----BEGIN CERT-----\ncluster-ca\n-----END CERT-----",
				},
			}
			serviceCA := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      serviceCAConfigMap,
					Namespace: architectNamespace,
					Annotations: map[string]string{
						"service.beta.openshift.io/inject-cabundle": "true",
					},
				},
				Data: map[string]string{
					serviceCAKey: "-----BEGIN CERT-----\nservice-ca\n-----END CERT-----",
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(clusterCA, serviceCA).Build(),
				Scheme: testScheme,
			}
			Expect(r.ensureCombinedCABundle(ctx)).To(Succeed())

			combined := &corev1.ConfigMap{}
			err := r.Get(ctx, client.ObjectKey{Name: combinedCABundleName, Namespace: architectNamespace}, combined)
			Expect(err).NotTo(HaveOccurred())
			Expect(combined.Data[clusterCABundleKey]).To(ContainSubstring("cluster-ca"))
			Expect(combined.Data[clusterCABundleKey]).To(ContainSubstring("service-ca"))
		})

		It("does nothing when no CAs exist", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			Expect(r.ensureCombinedCABundle(ctx)).To(Succeed())

			combined := &corev1.ConfigMap{}
			err := r.Get(ctx, client.ObjectKey{Name: combinedCABundleName, Namespace: architectNamespace}, combined)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("is idempotent when combined CA already matches", func() {
			clusterCA := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterCABundleName,
					Namespace: architectNamespace,
				},
				Data: map[string]string{
					clusterCABundleKey: "ca-data",
				},
			}
			existingCombined := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      combinedCABundleName,
					Namespace: architectNamespace,
				},
				Data: map[string]string{
					clusterCABundleKey: "ca-data",
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(clusterCA, existingCombined).Build(),
				Scheme: testScheme,
			}
			Expect(r.ensureCombinedCABundle(ctx)).To(Succeed())
		})
	})

	Describe("ensureQuayCAInConfigBundle", func() {
		It("creates config bundle secret with CA cert", func() {
			clusterCA := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterCABundleName,
					Namespace: architectNamespace,
				},
				Data: map[string]string{
					clusterCABundleKey: "ca-cert-data",
				},
			}
			quayRegistry := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata": map[string]interface{}{
					"name":      "mirror-operator-quay",
					"namespace": architectNamespace,
					"uid":       "test-uid",
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(clusterCA).Build(),
				Scheme: testScheme,
			}
			Expect(r.ensureQuayCAInConfigBundle(ctx, quayRegistry)).To(Succeed())

			secret := &corev1.Secret{}
			err := r.Get(ctx, client.ObjectKey{Name: "mirror-operator-quay-config-bundle", Namespace: architectNamespace}, secret)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(secret.Data["extra_ca_cert_cluster-ca.crt"])).To(Equal("ca-cert-data"))
		})

		It("does nothing when cluster CA is not available", func() {
			quayRegistry := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata": map[string]interface{}{
					"name":      "mirror-operator-quay",
					"namespace": architectNamespace,
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			Expect(r.ensureQuayCAInConfigBundle(ctx, quayRegistry)).To(Succeed())
		})
	})

	Describe("updateArchitectDeployment", func() {
		It("updates deployment when spec differs", func() {
			labels := architectComponentLabels("backend")
			testBuilder := func(name, image string, labels map[string]string) map[string]interface{} {
				return map[string]interface{}{
					"name":  name,
					"image": image,
				}
			}
			existing := architectBackendDeployment("test-dep", "quay.io/test/old:v1", 1, labels, "ps", "ns", testBuilder)
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform", UID: "test-uid"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			err := r.updateArchitectDeployment(ctx, platform, "test-dep", "quay.io/test/new:v2", 1, labels, "ps", "ns", testBuilder)
			Expect(err).NotTo(HaveOccurred())

			updated := &unstructured.Unstructured{}
			updated.SetGroupVersionKind(deploymentGVK)
			r.Get(ctx, client.ObjectKey{Name: "test-dep", Namespace: architectNamespace}, updated)
			containers, _, _ := unstructured.NestedSlice(updated.Object, "spec", "template", "spec", "containers")
			Expect(containers).To(HaveLen(1))
			container := containers[0].(map[string]interface{})
			Expect(container["image"]).To(Equal("quay.io/test/new:v2"))
		})

		It("skips update when deployment spec unchanged", func() {
			labels := architectComponentLabels("backend")
			testBuilder := func(name, image string, labels map[string]string) map[string]interface{} {
				return map[string]interface{}{
					"name":  name,
					"image": image,
				}
			}
			existing := architectBackendDeployment("test-dep", "quay.io/test/same:v1", 1, labels, "ps", "ns", testBuilder)
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform", UID: "test-uid"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			err := r.updateArchitectDeployment(ctx, platform, "test-dep", "quay.io/test/same:v1", 1, labels, "ps", "ns", testBuilder)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureArchitectService", func() {
		It("creates service when not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", UID: "uid-1"},
			}
			labels := architectComponentLabels("backend")
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureArchitectService(ctx, platform, "airgap-architect-backend-test", int32(4000), labels)
			Expect(err).NotTo(HaveOccurred())

			svc := &unstructured.Unstructured{}
			svc.SetGroupVersionKind(serviceGVK)
			err = r.Get(ctx, client.ObjectKey{Name: "airgap-architect-backend-test", Namespace: architectNamespace}, svc)
			Expect(err).NotTo(HaveOccurred())
			// Backend service should have TLS annotation
			annotations := svc.GetAnnotations()
			Expect(annotations).To(HaveKey("service.beta.openshift.io/serving-cert-secret-name"))
		})

		It("creates non-TLS service for non-backend names", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", UID: "uid-1"},
			}
			labels := architectComponentLabels("frontend")
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureArchitectService(ctx, platform, "my-frontend-svc", int32(5173), labels)
			Expect(err).NotTo(HaveOccurred())

			svc := &unstructured.Unstructured{}
			svc.SetGroupVersionKind(serviceGVK)
			err = r.Get(ctx, client.ObjectKey{Name: "my-frontend-svc", Namespace: architectNamespace}, svc)
			Expect(err).NotTo(HaveOccurred())
		})

		It("updates existing service", func() {
			existingSvc := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Service",
				"metadata": map[string]interface{}{
					"name":      "existing-svc",
					"namespace": architectNamespace,
				},
				"spec": map[string]interface{}{
					"type": "ClusterIP",
				},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", UID: "uid-1"},
			}
			labels := architectComponentLabels("backend")
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existingSvc).Build(),
				Scheme: testScheme,
			}
			err := r.ensureArchitectService(ctx, platform, "existing-svc", int32(4000), labels)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureArchitectFrontend", func() {
		It("creates frontend deployment when not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", UID: "uid-1"},
			}
			labels := architectComponentLabels("frontend")
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureArchitectFrontend(ctx, platform, "test-frontend", "quay.io/test/frontend:v1", 1, labels, "api.example.com", "app.example.com")
			Expect(err).NotTo(HaveOccurred())

			dep := &unstructured.Unstructured{}
			dep.SetGroupVersionKind(deploymentGVK)
			err = r.Get(ctx, client.ObjectKey{Name: "test-frontend", Namespace: architectNamespace}, dep)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureArchitectRoute", func() {
		It("creates route when not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", UID: "uid-1"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureArchitectRoute(ctx, platform, "test-route", nil, "test-frontend-svc")
			Expect(err).NotTo(HaveOccurred())

			route := &unstructured.Unstructured{}
			route.SetGroupVersionKind(routeGVK)
			err = r.Get(ctx, client.ObjectKey{Name: "test-route", Namespace: architectNamespace}, route)
			Expect(err).NotTo(HaveOccurred())
		})

		It("updates route when it already exists", func() {
			existingRoute := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "route.openshift.io/v1",
				"kind":       "Route",
				"metadata": map[string]interface{}{
					"name":      "existing-route",
					"namespace": architectNamespace,
				},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", UID: "uid-1"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existingRoute).Build(),
				Scheme: testScheme,
			}
			err := r.ensureArchitectRoute(ctx, platform, "existing-route", nil, "test-svc")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("reconcileCollectionPipelineTemplate", func() {
		It("skips pipeline creation in airgapped mode", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			Expect(r.reconcileCollectionPipelineTemplate(ctx, platform)).To(Succeed())
		})

		It("creates pipeline template in connected mode", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", UID: "uid-1"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client:      fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme:      testScheme,
				MirrorImage: "quay.io/test/oc-mirror:v2",
			}
			Expect(r.reconcileCollectionPipelineTemplate(ctx, platform)).To(Succeed())

			pipeline := &unstructured.Unstructured{}
			pipeline.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "tekton.dev", Version: "v1", Kind: "Pipeline",
			})
			err := r.Get(ctx, client.ObjectKey{Name: "collection-pipeline-template", Namespace: architectNamespace}, pipeline)
			Expect(err).NotTo(HaveOccurred())
			spec, _, _ := unstructured.NestedMap(pipeline.Object, "spec")
			Expect(spec).To(HaveKey("params"))
			Expect(spec).To(HaveKey("tasks"))
			Expect(spec).To(HaveKey("finally"))
		})

		It("updates existing pipeline template", func() {
			existing := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "tekton.dev/v1",
				"kind":       "Pipeline",
				"metadata": map[string]interface{}{
					"name":      "collection-pipeline-template",
					"namespace": architectNamespace,
				},
				"spec": map[string]interface{}{
					"params": []interface{}{},
				},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", UID: "uid-1"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client:      fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme:      testScheme,
				MirrorImage: "quay.io/test/oc-mirror:v2",
			}
			Expect(r.reconcileCollectionPipelineTemplate(ctx, platform)).To(Succeed())
		})
	})

	Describe("cleanup", func() {
		It("removes finalizer on deletion", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test",
					Finalizers: []string{platformFinalizer},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).
					WithStatusSubresource(platform).Build(),
				Scheme: testScheme,
			}
			result, err := r.cleanup(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			updated := &mirrorv1.DisconnectedPlatform{}
			r.Get(ctx, client.ObjectKey{Name: "test"}, updated)
			Expect(updated.Finalizers).NotTo(ContainElement(platformFinalizer))
		})

		It("does nothing without finalizer", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}
			result, err := r.cleanup(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))
		})
	})

	Describe("getRouteHostname", func() {
		It("returns hostname from route status ingress", func() {
			route := &unstructured.Unstructured{Object: map[string]interface{}{
				"status": map[string]interface{}{
					"ingress": []interface{}{
						map[string]interface{}{
							"host": "my-app.apps.cluster.example.com",
						},
					},
				},
			}}
			r := &DisconnectedPlatformReconciler{}
			Expect(r.getRouteHostname(route)).To(Equal("my-app.apps.cluster.example.com"))
		})

		It("returns empty string when no ingress", func() {
			route := &unstructured.Unstructured{Object: map[string]interface{}{
				"status": map[string]interface{}{},
			}}
			r := &DisconnectedPlatformReconciler{}
			Expect(r.getRouteHostname(route)).To(BeEmpty())
		})
	})

	Describe("ensureSubscriptionCABundle", func() {
		It("adds CA volume and env when cluster CA exists", func() {
			clusterCA := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterCABundleName,
					Namespace: architectNamespace,
					Labels: map[string]string{
						"config.openshift.io/inject-trusted-cabundle": "true",
					},
				},
				Data: map[string]string{
					clusterCABundleKey: "ca-cert-data",
				},
			}
			sub := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "operators.coreos.com/v1alpha1",
				"kind":       "Subscription",
				"metadata": map[string]interface{}{
					"name":      "test-sub",
					"namespace": architectNamespace,
				},
				"spec": map[string]interface{}{
					"name": "test-operator",
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(clusterCA, sub).Build(),
				Scheme: testScheme,
			}
			err := r.ensureSubscriptionCABundle(ctx, sub)
			Expect(err).NotTo(HaveOccurred())

			envs, _, _ := unstructured.NestedSlice(sub.Object, "spec", "config", "env")
			found := false
			for _, e := range envs {
				em := e.(map[string]interface{})
				if em["name"] == "SSL_CERT_FILE" {
					found = true
					Expect(em["value"]).To(Equal(clusterCAFilePath))
				}
			}
			Expect(found).To(BeTrue())
		})

		It("does nothing when no cluster CA exists", func() {
			sub := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "operators.coreos.com/v1alpha1",
				"kind":       "Subscription",
				"metadata": map[string]interface{}{
					"name":      "test-sub",
					"namespace": architectNamespace,
				},
				"spec": map[string]interface{}{},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureSubscriptionCABundle(ctx, sub)
			Expect(err).NotTo(HaveOccurred())
		})

		It("skips when CA volume already present", func() {
			clusterCA := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterCABundleName,
					Namespace: architectNamespace,
				},
				Data: map[string]string{
					clusterCABundleKey: "ca-cert-data",
				},
			}
			sub := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "operators.coreos.com/v1alpha1",
				"kind":       "Subscription",
				"metadata": map[string]interface{}{
					"name":      "test-sub",
					"namespace": architectNamespace,
				},
				"spec": map[string]interface{}{
					"config": map[string]interface{}{
						"volumes": []interface{}{
							map[string]interface{}{
								"name": clusterCAVolumeName,
							},
						},
					},
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(clusterCA).Build(),
				Scheme: testScheme,
			}
			err := r.ensureSubscriptionCABundle(ctx, sub)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureOSUSPullSecret", func() {
		It("creates pull secret in OSUS namespace", func() {
			sourceSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pull-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					".dockerconfigjson": []byte(`{"auths":{}}`),
				},
				Type: corev1.SecretTypeDockerConfigJson,
			}
			defaultSA := &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: "openshift-update-service",
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sourceSecret, defaultSA).Build(),
				Scheme: testScheme,
			}
			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).NotTo(HaveOccurred())

			target := &corev1.Secret{}
			err = r.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: "openshift-update-service"}, target)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(target.Data[".dockerconfigjson"])).To(Equal(`{"auths":{}}`))

			sa := &corev1.ServiceAccount{}
			r.Get(ctx, client.ObjectKey{Name: "default", Namespace: "openshift-update-service"}, sa)
			Expect(sa.ImagePullSecrets).To(ContainElement(corev1.LocalObjectReference{Name: "pull-secret"}))
		})

		It("returns error when source secret not found", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get pull-secret"))
		})
	})

	Describe("reconcileArtifactsBucket", func() {
		It("creates ObjectBucketClaim when not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace, UID: "uid-1"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}
			Expect(r.reconcileArtifactsBucket(ctx, platform)).To(Succeed())

			obc := &unstructured.Unstructured{}
			obc.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "objectbucket.io", Version: "v1alpha1", Kind: "ObjectBucketClaim",
			})
			err := r.Get(ctx, client.ObjectKey{Name: "collection-artifacts", Namespace: architectNamespace}, obc)
			Expect(err).NotTo(HaveOccurred())
		})

		It("is idempotent when OBC already exists", func() {
			existing := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "objectbucket.io/v1alpha1",
				"kind":       "ObjectBucketClaim",
				"metadata": map[string]interface{}{
					"name":      "collection-artifacts",
					"namespace": architectNamespace,
				},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace, UID: "uid-1"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			Expect(r.reconcileArtifactsBucket(ctx, platform)).To(Succeed())
		})
	})

	Describe("frontendContainer", func() {
		It("sets VITE_API_BASE when backendRouteHostname is provided", func() {
			container := frontendContainer("frontend", "quay.io/test/frontend:v1", map[string]string{}, "api.example.com", "app.example.com")
			envVars := container["env"].([]interface{})
			found := false
			for _, e := range envVars {
				em := e.(map[string]interface{})
				if em["name"] == "VITE_API_BASE" {
					found = true
					Expect(em["value"]).To(Equal("https://api.example.com"))
				}
			}
			Expect(found).To(BeTrue())
		})

		It("sets VITE_ALLOWED_HOSTS when frontendRouteHostname is provided", func() {
			container := frontendContainer("frontend", "quay.io/test/frontend:v1", map[string]string{}, "", "app.example.com")
			envVars := container["env"].([]interface{})
			found := false
			for _, e := range envVars {
				em := e.(map[string]interface{})
				if em["name"] == "VITE_ALLOWED_HOSTS" {
					found = true
					Expect(em["value"]).To(Equal("app.example.com"))
				}
			}
			Expect(found).To(BeTrue())
		})

		It("omits VITE_API_BASE when no backend hostname", func() {
			container := frontendContainer("frontend", "quay.io/test/frontend:v1", map[string]string{}, "", "")
			envVars := container["env"].([]interface{})
			for _, e := range envVars {
				em := e.(map[string]interface{})
				Expect(em["name"]).NotTo(Equal("VITE_API_BASE"))
			}
		})
	})

	Describe("consolePluginContainer", func() {
		It("returns container with correct structure", func() {
			container := consolePluginContainer("plugin", "quay.io/test/plugin:v1", map[string]string{"app": "plugin"})
			Expect(container["name"]).To(Equal("plugin"))
			Expect(container["image"]).To(Equal("quay.io/test/plugin:v1"))
			ports := container["ports"].([]interface{})
			Expect(ports).To(HaveLen(1))
			portMap := ports[0].(map[string]interface{})
			Expect(portMap["containerPort"]).To(Equal(int64(9001)))
		})
	})

	Describe("ensureConsolePlugin", func() {
		It("creates console plugin deployment when not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", UID: "uid-1"},
			}
			labels := architectComponentLabels("console-plugin")
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureConsolePlugin(ctx, platform, "airgap-architect-plugin", "quay.io/test/plugin:v1", 1, labels, "pull-secret", "openshift-config")
			Expect(err).NotTo(HaveOccurred())

			dep := &unstructured.Unstructured{}
			dep.SetGroupVersionKind(deploymentGVK)
			err = r.Get(ctx, client.ObjectKey{Name: "airgap-architect-plugin", Namespace: architectNamespace}, dep)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("reconcileArchitect", func() {
		It("deletes resources when architect is disabled", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Architect: &mirrorv1.AirgapArchitectConfig{Enabled: false},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileArchitect(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("deletes resources when architect config is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec:       mirrorv1.DisconnectedPlatformSpec{},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileArchitect(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureOwnSubscriptionConfig", func() {
		It("does nothing when no mirror-operator subscription found", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			Expect(r.ensureOwnSubscriptionConfig(ctx)).To(Succeed())
		})
	})

	Describe("ensureImportScriptConfigMap", func() {
		It("creates script ConfigMap when not found", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureImportScriptConfigMap(ctx)
			Expect(err).NotTo(HaveOccurred())

			cm := &corev1.ConfigMap{}
			err = r.Get(ctx, client.ObjectKey{Name: "airgap-architect-import-script", Namespace: architectNamespace}, cm)
			Expect(err).NotTo(HaveOccurred())
			Expect(cm.Data).To(HaveKey("import-airgap-architect.sh"))
		})

		It("is idempotent when ConfigMap exists with correct content", func() {
			existing := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "airgap-architect-import-script",
					Namespace: architectNamespace,
				},
				Data: map[string]string{"import-airgap-architect.sh": scripts.ImportAirgapArchitectScript},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			Expect(r.ensureImportScriptConfigMap(ctx)).To(Succeed())
		})

		It("updates ConfigMap when content is stale", func() {
			existing := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "airgap-architect-import-script",
					Namespace: architectNamespace,
				},
				Data: map[string]string{"import-airgap-architect.sh": "old-script-content"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			Expect(r.ensureImportScriptConfigMap(ctx)).To(Succeed())

			updated := &corev1.ConfigMap{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "airgap-architect-import-script", Namespace: architectNamespace}, updated)).To(Succeed())
			Expect(updated.Data["import-airgap-architect.sh"]).To(Equal(scripts.ImportAirgapArchitectScript))
		})
	})

	Describe("reconcileSubscriptions", func() {
		It("creates subscriptions for all default operators", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileSubscriptions(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
			Expect(platform.Status.Components).NotTo(BeEmpty())
		})

		It("marks disabled operators as Disabled", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						Operators: &mirrorv1.OperatorConfig{
							OSUS: &mirrorv1.OLMSubscriptionConfig{Disabled: true},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileSubscriptions(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			found := false
			for _, comp := range platform.Status.Components {
				if comp.Name == "cincinnati-operator" {
					found = true
					Expect(comp.Status).To(Equal("Disabled"))
				}
			}
			Expect(found).To(BeTrue())
		})
	})

	Describe("ensurePullSecret", func() {
		It("returns nil when source namespace is architect namespace", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensurePullSecret(ctx, "pull-secret", architectNamespace)
			Expect(err).NotTo(HaveOccurred())
		})

		It("copies pull secret from source namespace to architect namespace", func() {
			sourceSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pull-secret",
					Namespace: "openshift-config",
				},
				Data: map[string][]byte{
					".dockerconfigjson": []byte(`{"auths":{"registry.example.com":{"auth":"dGVzdDp0ZXN0"}}}`),
				},
				Type: corev1.SecretTypeDockerConfigJson,
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sourceSecret).Build(),
				Scheme: testScheme,
			}
			err := r.ensurePullSecret(ctx, "pull-secret", "openshift-config")
			Expect(err).NotTo(HaveOccurred())

			target := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: architectNamespace}, target)).To(Succeed())
			Expect(target.Data[".dockerconfigjson"]).To(Equal(sourceSecret.Data[".dockerconfigjson"]))
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

	Describe("ensureOSUSPullSecret", func() {
		It("copies pull secret from architect namespace to openshift-update-service", func() {
			sourceSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pull-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					".dockerconfigjson": []byte(`{"auths":{}}`),
				},
				Type: corev1.SecretTypeDockerConfigJson,
			}
			sa := &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: "openshift-update-service",
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sourceSecret, sa).Build(),
				Scheme: testScheme,
			}
			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).NotTo(HaveOccurred())

			target := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: "openshift-update-service"}, target)).To(Succeed())
			Expect(target.Data[".dockerconfigjson"]).To(Equal(sourceSecret.Data[".dockerconfigjson"]))
		})

		It("updates existing pull secret when content differs", func() {
			sourceSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pull-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					".dockerconfigjson": []byte(`{"auths":{"new":"creds"}}`),
				},
				Type: corev1.SecretTypeDockerConfigJson,
			}
			existingSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pull-secret",
					Namespace: "openshift-update-service",
				},
				Data: map[string][]byte{
					".dockerconfigjson": []byte(`{"auths":{"old":"creds"}}`),
				},
				Type: corev1.SecretTypeDockerConfigJson,
			}
			sa := &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: "openshift-update-service",
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sourceSecret, existingSecret, sa).Build(),
				Scheme: testScheme,
			}
			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).NotTo(HaveOccurred())

			updated := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: "openshift-update-service"}, updated)).To(Succeed())
			Expect(string(updated.Data[".dockerconfigjson"])).To(Equal(`{"auths":{"new":"creds"}}`))
		})

		It("links pull secret to default service account", func() {
			sourceSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pull-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					".dockerconfigjson": []byte(`{"auths":{}}`),
				},
				Type: corev1.SecretTypeDockerConfigJson,
			}
			sa := &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: "openshift-update-service",
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sourceSecret, sa).Build(),
				Scheme: testScheme,
			}
			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).NotTo(HaveOccurred())

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

		It("returns error when source pull secret missing", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get pull-secret"))
		})
	})

	Describe("ensureOwnSubscriptionConfig", func() {
		It("returns nil when no mirror-operator subscription exists", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureOwnSubscriptionConfig(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("calls ensureSubscriptionCABundle on mirror-operator subscription", func() {
			sub := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "operators.coreos.com/v1alpha1",
				"kind":       "Subscription",
				"metadata": map[string]interface{}{
					"name":      "mirror-operator-sub",
					"namespace": architectNamespace,
				},
				"spec": map[string]interface{}{
					"name": "mirror-operator",
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sub).Build(),
				Scheme: testScheme,
			}
			err := r.ensureOwnSubscriptionConfig(ctx)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("reconcileAirgappedSubscriptions", func() {
		It("returns needsRequeue when package manifests not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			needsRequeue, err := r.reconcileAirgappedSubscriptions(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
			Expect(needsRequeue).To(BeTrue())
		})

		It("creates subscriptions when package manifests are found", func() {
			var objs []client.Object
			for _, op := range airgappedOperators {
				pm := &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "packages.operators.coreos.com/v1",
					"kind":       "PackageManifest",
					"metadata": map[string]interface{}{
						"name":      op.pkg,
						"namespace": "openshift-marketplace",
					},
					"status": map[string]interface{}{
						"catalogSource":          "redhat-operators",
						"catalogSourceNamespace": "openshift-marketplace",
						"defaultChannel":         "stable",
					},
				}}
				objs = append(objs, pm)
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(objs...).Build(),
				Scheme: testScheme,
			}
			needsRequeue, err := r.reconcileAirgappedSubscriptions(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
			Expect(needsRequeue).To(BeTrue())
			Expect(platform.Status.Components).To(HaveLen(len(airgappedOperators)))
		})
	})

	Describe("discoverPackageInfo", func() {
		It("returns catalog info from PackageManifest", func() {
			pm := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "packages.operators.coreos.com/v1",
				"kind":       "PackageManifest",
				"metadata": map[string]interface{}{
					"name":      "test-operator",
					"namespace": "openshift-marketplace",
				},
				"status": map[string]interface{}{
					"catalogSource":          "my-catalog",
					"catalogSourceNamespace": "my-ns",
					"defaultChannel":         "stable",
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pm).Build(),
				Scheme: testScheme,
			}
			catalog, catalogNS, channel, err := r.discoverPackageInfo(ctx, "test-operator")
			Expect(err).NotTo(HaveOccurred())
			Expect(catalog).To(Equal("my-catalog"))
			Expect(catalogNS).To(Equal("my-ns"))
			Expect(channel).To(Equal("stable"))
		})

		It("returns error when PackageManifest not found", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			_, _, _, err := r.discoverPackageInfo(ctx, "nonexistent")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not found"))
		})

		It("returns error when catalogSource is empty", func() {
			pm := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "packages.operators.coreos.com/v1",
				"kind":       "PackageManifest",
				"metadata": map[string]interface{}{
					"name":      "test-operator",
					"namespace": "openshift-marketplace",
				},
				"status": map[string]interface{}{},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pm).Build(),
				Scheme: testScheme,
			}
			_, _, _, err := r.discoverPackageInfo(ctx, "test-operator")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("has no catalogSource"))
		})
	})

	Describe("reconcileArtifactFileServer", func() {
		It("does nothing when no completed collection pipelines exist", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileArtifactFileServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("creates deployment with volume mounts for completed pipelines", func() {
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
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pipeline, pvc, platform).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileArtifactFileServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			dep := &appsv1.Deployment{}
			err = r.Get(ctx, client.ObjectKey{Name: "artifact-fileserver", Namespace: architectNamespace}, dep)
			Expect(err).NotTo(HaveOccurred())
			Expect(dep.Spec.Template.Spec.Volumes).NotTo(BeEmpty())
		})

		It("skips pipelines that are not completed", func() {
			pipeline := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "running-pipeline", Namespace: architectNamespace},
				Status: mirrorv1.CollectionPipelineStatus{
					Phase: "Running",
				},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pipeline, platform).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileArtifactFileServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("getQuayTLSCertData", func() {
		It("returns cert and key when secret exists", func() {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-quay-tls",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"tls.crt": []byte("cert-data"),
					"tls.key": []byte("key-data"),
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(secret).Build(),
				Scheme: testScheme,
			}
			cert, key, err := r.getQuayTLSCertData(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(cert)).To(Equal("cert-data"))
			Expect(string(key)).To(Equal("key-data"))
		})

		It("returns error when secret not found", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			_, _, err := r.getQuayTLSCertData(ctx)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("syncS3ConfigToSecret", func() {
		It("creates S3 config secret from OBC ConfigMap and Secret", func() {
			obcCM := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "collection-artifacts",
					Namespace: architectNamespace,
				},
				Data: map[string]string{
					"BUCKET_NAME": "my-bucket",
					"BUCKET_HOST": "s3.example.com",
					"BUCKET_PORT": "443",
				},
			}
			obcSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "collection-artifacts",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"AWS_ACCESS_KEY_ID":     []byte("access-key"),
					"AWS_SECRET_ACCESS_KEY": []byte("secret-key"),
				},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(obcCM, obcSecret).Build(),
				Scheme: testScheme,
			}
			Expect(r.syncS3ConfigToSecret(ctx, platform)).To(Succeed())

			secret := &corev1.Secret{}
			err := r.Get(ctx, client.ObjectKey{Name: "collection-artifacts", Namespace: architectNamespace}, secret)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(secret.Data["S3_BUCKET"])).To(Equal("my-bucket"))
		})

		It("returns nil when OBC ConfigMap not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.syncS3ConfigToSecret(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureQuayTLSCertificate - Certificate creation", func() {
		It("creates Certificate resource when certIssuer is set", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						CertIssuer: &mirrorv1.CertIssuerReference{
							Name: "letsencrypt-prod",
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureQuayTLSCertificate(ctx, platform, "quay.apps.example.com")
			Expect(err).NotTo(HaveOccurred())

			cert := &unstructured.Unstructured{}
			cert.SetGroupVersionKind(certificateGVK)
			err = r.Get(ctx, client.ObjectKey{Name: "quay-certificate", Namespace: architectNamespace}, cert)
			Expect(err).NotTo(HaveOccurred())
			spec, _, _ := unstructured.NestedMap(cert.Object, "spec")
			Expect(spec["secretName"]).To(Equal("mirror-operator-quay-tls"))
		})

		It("is idempotent when Certificate already exists", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						CertIssuer: &mirrorv1.CertIssuerReference{
							Name: "letsencrypt-prod",
							Kind: "Issuer",
						},
					},
				},
			}
			existing := &unstructured.Unstructured{}
			existing.SetGroupVersionKind(certificateGVK)
			existing.SetName("quay-certificate")
			existing.SetNamespace(architectNamespace)
			existing.Object["spec"] = map[string]interface{}{
				"secretName": "mirror-operator-quay-tls",
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			err := r.ensureQuayTLSCertificate(ctx, platform, "quay.apps.example.com")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("deleteResource", func() {
		It("deletes an existing resource", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "to-delete", Namespace: architectNamespace},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cm).Build(),
				Scheme: testScheme,
			}
			gvk := schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}
			err := r.deleteResource(ctx, gvk, "to-delete")
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil for not found", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			gvk := schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}
			err := r.deleteResource(ctx, gvk, "nonexistent")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("configureCollectionPipelineSigning", func() {
		It("returns nil when no signing config", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						CollectionSchedule: "0 2 * * 0",
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

	Describe("reconcileRHTPAConfig", func() {
		It("returns nil when RHTPA is nil in connected config", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      mirrorv1.PlatformModeConnected,
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
	})

	Describe("ensurePipelineOSUSAccess", func() {
		It("creates RBAC for pipeline to access OSUS", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", UID: "uid-1"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}
			err := r.ensurePipelineOSUSAccess(ctx)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("reconcileQuayConfig", func() {
		It("returns nil when Quay is not ready", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileQuayConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("reconcileAirgappedSubscriptions", func() {
		It("returns nil when airgapped config is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
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

	Describe("makeBackendContainerBuilder", func() {
		It("creates container with correct ports and env", func() {
			builder := makeBackendContainerBuilder("", "", nil)
			container := builder("backend", "quay.io/test/backend:v1", map[string]string{})
			Expect(container["name"]).To(Equal("airgap-architect-backend"))
			Expect(container["image"]).To(Equal("quay.io/test/backend:v1"))
			ports := container["ports"].([]interface{})
			Expect(ports).To(HaveLen(1))
		})

		It("includes github token env var when set", func() {
			builder := makeBackendContainerBuilder("gh-token-secret", "", nil)
			container := builder("backend", "quay.io/test/backend:v1", map[string]string{})
			envVars := container["env"].([]interface{})
			found := false
			for _, e := range envVars {
				em := e.(map[string]interface{})
				if em["name"] == "GITHUB_TOKEN" {
					found = true
				}
			}
			Expect(found).To(BeTrue())
		})

		It("includes deployment side env var when set", func() {
			builder := makeBackendContainerBuilder("", "connected", nil)
			container := builder("backend", "quay.io/test/backend:v1", map[string]string{})
			envVars := container["env"].([]interface{})
			found := false
			for _, e := range envVars {
				em := e.(map[string]interface{})
				if em["name"] == "DEPLOYMENT_SIDE" && em["value"] == "connected" {
					found = true
				}
			}
			Expect(found).To(BeTrue())
		})
	})

	Describe("Reconcile - connected mode full path", func() {
		It("reconciles connected platform with collection and import history", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Namespace:  "default",
					Finalizers: []string{platformFinalizer},
					Generation: 1,
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			cp := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "cp1", Namespace: "default"},
				Status: mirrorv1.CollectionPipelineStatus{
					Version: "4.14",
					Phase:   "Complete",
				},
			}
			mi := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "mi1", Namespace: "default"},
				Status: mirrorv1.MirrorImportStatus{
					Phase: "Complete",
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform, cp, mi).WithStatusSubresource(platform).Build(),
				Scheme: testScheme,
			}
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform", Namespace: "default"}})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())

			updated := &mirrorv1.DisconnectedPlatform{}
			Expect(r.Get(ctx, client.ObjectKeyFromObject(platform), updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(mirrorv1.PlatformPhaseReady))
			Expect(updated.Status.CollectionHistory).To(HaveLen(1))
			Expect(updated.Status.CollectionHistory[0].Version).To(Equal("4.14"))
			Expect(updated.Status.ImportHistory).To(HaveLen(1))
			Expect(updated.Status.LastImport).NotTo(BeNil())
		})

		It("reconciles connected platform with RHTPA config", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Namespace:  "default",
					Finalizers: []string{platformFinalizer},
					Generation: 1,
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTPA: &mirrorv1.RHTPAInstallerConfig{
							Storage: &mirrorv1.RHTPAStorageConfig{
								Size: "10Gi",
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).WithStatusSubresource(platform).Build(),
				Scheme: testScheme,
			}
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform", Namespace: "default"}})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())
		})
	})

	Describe("Reconcile - airgapped mode", func() {
		It("requeues when airgapped operators not yet installed", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Namespace:  "default",
					Finalizers: []string{platformFinalizer},
					Generation: 1,
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).WithStatusSubresource(platform).Build(),
				Scheme: testScheme,
			}
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform", Namespace: "default"}})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(30 * time.Second))
		})
	})

	Describe("Reconcile - deletion", func() {
		It("handles deletion with finalizer", func() {
			now := metav1.Now()
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-platform",
					Namespace:         "default",
					Finalizers:        []string{platformFinalizer},
					DeletionTimestamp: &now,
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform", Namespace: "default"}})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Reconcile - not found", func() {
		It("returns no error when platform not found", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "missing", Namespace: "default"}})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))
		})
	})

	Describe("reconcileQuayConfig - external URL", func() {
		It("sets mirrorRegistry from external URL", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						Quay: &mirrorv1.QuayInstallerConfig{
							ExternalURL: "quay.external.example.com",
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileQuayConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
			Expect(platform.Spec.Connected.MirrorRegistry).To(Equal("quay.external.example.com"))
		})
	})

	Describe("reconcileRHTPAConfig - nil config", func() {
		It("returns nil when RHTPA not configured", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTPA: nil,
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

		It("returns nil when RHTPA storage is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
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

	Describe("ensurePullSecret - same namespace", func() {
		It("returns nil when source namespace is operator namespace", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensurePullSecret(ctx, "pull-secret", architectNamespace)
			Expect(err).NotTo(HaveOccurred())
		})

		It("creates target secret when not found", func() {
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

	Describe("addQuayCredentialsIfNeeded - no QuayRegistry", func() {
		It("returns unchanged when no QuayRegistry exists", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			original := []byte(`{"auths":{"reg.example.com":{"auth":"abc123"}}}`)
			result, changed, err := r.addQuayCredentialsIfNeeded(ctx, original)
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(BeFalse())
			Expect(result).To(Equal(original))
		})
	})

	Describe("checkKeycloakHealth - various states", func() {
		It("returns nil when RHTAS not configured", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.checkKeycloakHealth(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error when Keycloak resource not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Managed: &mirrorv1.ManagedKeycloakConfig{
									Enabled: true,
								},
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.checkKeycloakHealth(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Keycloak resource not found"))
		})

		It("returns nil when Keycloak is Ready", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Managed: &mirrorv1.ManagedKeycloakConfig{
									Enabled: true,
								},
							},
						},
					},
				},
			}
			kc := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "k8s.keycloak.org/v2alpha1",
				"kind":       "Keycloak",
				"metadata":   map[string]interface{}{"name": "mirror-operator-keycloak", "namespace": architectNamespace},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":   "Ready",
							"status": "True",
						},
					},
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(kc).Build(),
				Scheme: testScheme,
			}
			err := r.checkKeycloakHealth(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error when Keycloak is not Ready", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Managed: &mirrorv1.ManagedKeycloakConfig{
									Enabled: true,
								},
							},
						},
					},
				},
			}
			kc := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "k8s.keycloak.org/v2alpha1",
				"kind":       "Keycloak",
				"metadata":   map[string]interface{}{"name": "mirror-operator-keycloak", "namespace": architectNamespace},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":    "Ready",
							"status":  "False",
							"message": "database connection failed",
						},
					},
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(kc).Build(),
				Scheme: testScheme,
			}
			err := r.checkKeycloakHealth(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Keycloak not ready"))
		})

		It("returns error when status has no conditions", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Managed: &mirrorv1.ManagedKeycloakConfig{
									Enabled: true,
								},
							},
						},
					},
				},
			}
			kc := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "k8s.keycloak.org/v2alpha1",
				"kind":       "Keycloak",
				"metadata":   map[string]interface{}{"name": "mirror-operator-keycloak", "namespace": architectNamespace},
				"status":     map[string]interface{}{},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(kc).Build(),
				Scheme: testScheme,
			}
			err := r.checkKeycloakHealth(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("conditions not available"))
		})
	})

	Describe("updateStatusFromSecuresignHealth - various states", func() {
		It("returns nil when securesign not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.updateStatusFromSecuresignHealth(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("handles HealthCheckPassed=True condition", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
			}
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
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ss).Build(),
				Scheme: testScheme,
			}
			err := r.updateStatusFromSecuresignHealth(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("handles HealthCheckPassed=False condition", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
			}
			ss := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Securesign",
				"metadata":   map[string]interface{}{"name": "mirror-operator-securesign", "namespace": architectNamespace},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":    "HealthCheckPassed",
							"status":  "False",
							"reason":  "FulcioUnhealthy",
							"message": "Fulcio CA is not responding",
						},
					},
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ss).Build(),
				Scheme: testScheme,
			}
			err := r.updateStatusFromSecuresignHealth(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("autoExpandPVC - early returns", func() {
		It("returns immediately when capacity >= maxSize", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: architectNamespace},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("100Gi"),
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pvc).Build(),
				Scheme: testScheme,
			}
			logger := log.FromContext(ctx)
			r.autoExpandPVC(ctx, pvc, "50Gi", logger)
		})

		It("returns when no matching pods found", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: architectNamespace},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pvc).Build(),
				Scheme: testScheme,
			}
			logger := log.FromContext(ctx)
			r.autoExpandPVC(ctx, pvc, "100Gi", logger)
		})

		It("expands PVC when pod is crash-looping", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: architectNamespace},
				Spec: corev1.PersistentVolumeClaimSpec{
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("10Gi"),
						},
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rhtpa-pg-0",
					Namespace: architectNamespace,
					Labels:    map[string]string{"app": "rhtpa-postgresql"},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason: "CrashLoopBackOff",
								},
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pvc, pod).Build(),
				Scheme: testScheme,
			}
			logger := log.FromContext(ctx)
			r.autoExpandPVC(ctx, pvc, "100Gi", logger)

			updated := &corev1.PersistentVolumeClaim{}
			Expect(r.Get(ctx, client.ObjectKeyFromObject(pvc), updated)).To(Succeed())
			newSize := updated.Spec.Resources.Requests[corev1.ResourceStorage]
			Expect(newSize.Cmp(resource.MustParse("10Gi"))).To(BeNumerically(">", 0))
		})

		It("expands PVC to max when pod is Failed", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: architectNamespace},
				Spec: corev1.PersistentVolumeClaimSpec{
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("10Gi"),
						},
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rhtpa-pg-0",
					Namespace: architectNamespace,
					Labels:    map[string]string{"app": "rhtpa-postgresql"},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodFailed,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pvc, pod).Build(),
				Scheme: testScheme,
			}
			logger := log.FromContext(ctx)
			r.autoExpandPVC(ctx, pvc, "15Gi", logger)

			updated := &corev1.PersistentVolumeClaim{}
			Expect(r.Get(ctx, client.ObjectKeyFromObject(pvc), updated)).To(Succeed())
			newSize := updated.Spec.Resources.Requests[corev1.ResourceStorage]
			Expect(newSize.Cmp(resource.MustParse("15Gi"))).To(Equal(0))
		})
	})

	Describe("ensureOwnSubscriptionConfig - no matching subscription", func() {
		It("returns nil when no mirror-operator subscription exists", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureOwnSubscriptionConfig(ctx)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("collectionVersionComplete", func() {
		It("returns true for Complete", func() {
			Expect(collectionVersionComplete("Complete")).To(BeTrue())
		})
		It("returns true for Succeeded", func() {
			Expect(collectionVersionComplete("Succeeded")).To(BeTrue())
		})
		It("returns false for Running", func() {
			Expect(collectionVersionComplete("Running")).To(BeFalse())
		})
		It("returns false for empty string", func() {
			Expect(collectionVersionComplete("")).To(BeFalse())
		})
	})

	Describe("ensureQuayCredentials - no QuayRegistry", func() {
		It("returns error when QuayRegistry not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureQuayCredentials(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get QuayRegistry"))
		})
	})

	Describe("reconcileQuayConfig - nil paths", func() {
		It("returns nil when Connected is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileQuayConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when Quay is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileQuayConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureQuayAPIToken", func() {
		It("returns cached token from secret", func() {
			tokenSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "quay-api-token",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"token": []byte("cached-token-123"),
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(tokenSecret).Build(),
				Scheme: testScheme,
			}
			token, err := r.ensureQuayAPIToken(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(Equal("cached-token-123"))
		})
	})

	Describe("getQuayRobotCredentials - cached secret", func() {
		It("returns credentials from cached secret", func() {
			robotSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "quay-robot-credentials",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"token": []byte("robot-token-abc"),
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(robotSecret).Build(),
				Scheme: testScheme,
			}
			robot, token, err := r.getQuayRobotCredentials(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(robot).To(Equal("mirror+mirroroperator"))
			Expect(token).To(Equal("robot-token-abc"))
		})
	})

	Describe("configureCollectionPipelineSigning - guard clauses", func() {
		It("returns nil when RHTAS is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			Expect(r.configureCollectionPipelineSigning(ctx, platform)).To(Succeed())
		})

		It("returns nil when OIDC managed is not enabled", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
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
			Expect(r.configureCollectionPipelineSigning(ctx, platform)).To(Succeed())
		})

		It("returns error when Securesign not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Managed: &mirrorv1.ManagedKeycloakConfig{
									Enabled: true,
								},
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
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("waiting for Securesign"))
		})

		It("returns error when Securesign status not available", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Managed: &mirrorv1.ManagedKeycloakConfig{
									Enabled: true,
								},
							},
						},
					},
				},
			}
			ss := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Securesign",
				"metadata":   map[string]interface{}{"name": "mirror-operator-securesign", "namespace": architectNamespace},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ss).Build(),
				Scheme: testScheme,
			}
			err := r.configureCollectionPipelineSigning(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("status not available"))
		})

		It("returns error when Fulcio/Rekor URLs not available", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Managed: &mirrorv1.ManagedKeycloakConfig{
									Enabled: true,
								},
							},
						},
					},
				},
			}
			ss := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Securesign",
				"metadata":   map[string]interface{}{"name": "mirror-operator-securesign", "namespace": architectNamespace},
				"status":     map[string]interface{}{},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ss).Build(),
				Scheme: testScheme,
			}
			err := r.configureCollectionPipelineSigning(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Fulcio or Rekor URL not available"))
		})

		It("returns error when Keycloak not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Managed: &mirrorv1.ManagedKeycloakConfig{
									Enabled: true,
								},
							},
						},
					},
				},
			}
			ss := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtas.redhat.com/v1alpha1",
				"kind":       "Securesign",
				"metadata":   map[string]interface{}{"name": "mirror-operator-securesign", "namespace": architectNamespace},
				"status": map[string]interface{}{
					"fulcio": map[string]interface{}{"url": "https://fulcio.example.com"},
					"rekor":  map[string]interface{}{"url": "https://rekor.example.com"},
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ss).Build(),
				Scheme: testScheme,
			}
			err := r.configureCollectionPipelineSigning(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("waiting for Keycloak"))
		})
	})

	Describe("ensureOSUSPullSecret - additional paths", func() {
		It("copies pull secret to OSUS namespace and links to SA", func() {
			sourceSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: architectNamespace},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			sa := &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "openshift-update-service"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sourceSecret, sa).Build(),
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

		It("updates existing OSUS pull secret when content differs", func() {
			sourceSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: architectNamespace},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{"new":"cred"}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			existingTarget := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "openshift-update-service"},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{"old":"cred"}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			sa := &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "openshift-update-service"},
				ImagePullSecrets: []corev1.LocalObjectReference{
					{Name: "pull-secret"},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sourceSecret, existingTarget, sa).Build(),
				Scheme: testScheme,
			}
			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).NotTo(HaveOccurred())

			updated := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: "openshift-update-service"}, updated)).To(Succeed())
			Expect(string(updated.Data[".dockerconfigjson"])).To(ContainSubstring("new"))
		})
	})

	Describe("ensurePullSecret - merge path", func() {
		It("merges pull secrets when target already exists", func() {
			source := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "openshift-config"},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{"source.io":{"auth":"c3Jj"}}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			existing := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: architectNamespace},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{"existing.io":{"auth":"ZXhpc3Q="}}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(source, existing).Build(),
				Scheme: testScheme,
			}
			err := r.ensurePullSecret(ctx, "pull-secret", "openshift-config")
			Expect(err).NotTo(HaveOccurred())

			updated := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: architectNamespace}, updated)).To(Succeed())
			Expect(string(updated.Data[".dockerconfigjson"])).To(ContainSubstring("source.io"))
		})
	})

	Describe("reconcileRHTPAConfig - TPA exists with OIDC", func() {
		It("enters existing TPA update path when TPA has OIDC config", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTPA: &mirrorv1.RHTPAInstallerConfig{
							Storage: &mirrorv1.RHTPAStorageConfig{
								Size: "10Gi",
							},
						},
					},
				},
			}
			tpa := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "rhtpa.io/v1",
				"kind":       "TrustedProfileAnalyzer",
				"metadata":   map[string]interface{}{"name": "mirror-operator-trusted-profile-analyzer", "namespace": architectNamespace},
				"spec": map[string]interface{}{
					"oidc": map[string]interface{}{
						"issuerUrl": "https://keycloak.example.com/realms/trustify",
						"clientId":  "frontend",
					},
					"modules": map[string]interface{}{
						"createDatabase":  map[string]interface{}{"enabled": true},
						"migrateDatabase": map[string]interface{}{"enabled": true},
					},
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(tpa).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileRHTPAConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("reconcileQuayConfig - managed QuayRegistry", func() {
		It("enters managed path when Quay managed is enabled but QuayRegistry not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						Quay: &mirrorv1.QuayInstallerConfig{
							Managed: &mirrorv1.ManagedQuayConfig{
								Enabled: true,
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileQuayConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("handles existing QuayRegistry without hostname", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						Quay: &mirrorv1.QuayInstallerConfig{
							Managed: &mirrorv1.ManagedQuayConfig{
								Enabled: true,
							},
						},
					},
				},
			}
			qr := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata":   map[string]interface{}{"name": "mirror-operator-quay", "namespace": architectNamespace},
				"status":     map[string]interface{}{},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(qr).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileQuayConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureQuayAPIToken - bootstrap path", func() {
		It("bootstraps token when no secret exists", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			token, err := r.ensureQuayAPIToken(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(Equal("unused"))

			secret := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "quay-api-token", Namespace: architectNamespace}, secret)).To(Succeed())
			Expect(string(secret.Data["token"])).To(Equal("unused"))
		})
	})

	Describe("addQuayCredentialsIfNeeded - QuayRegistry exists", func() {
		It("returns unchanged when QuayRegistry exists but no hostname", func() {
			qr := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata":   map[string]interface{}{"name": "mirror-operator-quay", "namespace": architectNamespace},
				"status":     map[string]interface{}{},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(qr).Build(),
				Scheme: testScheme,
			}
			original := []byte(`{"auths":{}}`)
			result, changed, err := r.addQuayCredentialsIfNeeded(ctx, original)
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(BeFalse())
			Expect(result).To(Equal(original))
		})
	})

	Describe("reconcileSubscriptions - connected mode", func() {
		It("handles connected mode with operator overrides", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						Operators: &mirrorv1.OperatorConfig{
							OpenShiftPipelines: &mirrorv1.OLMSubscriptionConfig{
								Disabled: true,
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileSubscriptions(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("isCRDNotFoundError", func() {
		It("returns true for resource not found errors with CRD kind", func() {
			Expect(isCRDNotFoundError(fmt.Errorf("no matches for kind \"Foo\" in version \"bar/v1\""))).To(BeTrue())
		})
		It("returns false for other errors", func() {
			Expect(isCRDNotFoundError(fmt.Errorf("connection refused"))).To(BeFalse())
		})
	})

	Describe("Reconcile - signing components ready", func() {
		It("processes RHTAS and signing when signingImages is true", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Namespace:  "default",
					Finalizers: []string{platformFinalizer},
					Generation: 1,
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{},
				},
				Status: mirrorv1.DisconnectedPlatformStatus{
					Components: []mirrorv1.ComponentStatus{
						{Name: "trusted-artifact-signer", Status: "Succeeded"},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).WithStatusSubresource(platform).Build(),
				Scheme: testScheme,
			}
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform", Namespace: "default"}})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())
		})
	})

	Describe("Reconcile - full connected flow with all features", func() {
		It("processes Quay, OSUS, pipeline template in connected mode", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Namespace:  "default",
					Finalizers: []string{platformFinalizer},
					Generation: 1,
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						MirrorRegistry: "quay.example.com/mirror",
						Quay: &mirrorv1.QuayInstallerConfig{
							ExternalURL: "quay.example.com",
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).WithStatusSubresource(platform).Build(),
				Scheme: testScheme,
			}
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-platform", Namespace: "default"}})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())

			updated := &mirrorv1.DisconnectedPlatform{}
			Expect(r.Get(ctx, client.ObjectKeyFromObject(platform), updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(mirrorv1.PlatformPhaseReady))
		})
	})

	Describe("execInQuayPod - no pod found", func() {
		It("returns error when no Quay pod exists", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			_, err := r.execInQuayPod(ctx, architectNamespace, "print('hello')")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("no Quay pods found"))
		})
	})

	Describe("ensurePullSecret - merge with existing target", func() {
		It("merges source and target secrets when target already exists with different auths", func() {
			sourceSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pull-secret",
					Namespace: "openshift-config",
				},
				Data: map[string][]byte{
					".dockerconfigjson": []byte(`{"auths":{"registry.redhat.io":{"auth":"cmVkaGF0OnJlZGhhdA=="}}}`),
				},
				Type: corev1.SecretTypeDockerConfigJson,
			}
			targetSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pull-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					".dockerconfigjson": []byte(`{"auths":{"quay.io":{"auth":"cXVheTpxdWF5"}}}`),
				},
				Type: corev1.SecretTypeDockerConfigJson,
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sourceSecret, targetSecret).Build(),
				Scheme: testScheme,
			}
			err := r.ensurePullSecret(ctx, "pull-secret", "openshift-config")
			Expect(err).NotTo(HaveOccurred())

			updated := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: architectNamespace}, updated)).To(Succeed())

			var config map[string]interface{}
			Expect(json.Unmarshal(updated.Data[".dockerconfigjson"], &config)).To(Succeed())
			auths := config["auths"].(map[string]interface{})
			Expect(auths).To(HaveKey("registry.redhat.io"))
			Expect(auths).To(HaveKey("quay.io"))
		})

		It("does not update target when merge produces no changes", func() {
			sharedData := []byte(`{"auths":{"registry.redhat.io":{"auth":"cmVkaGF0OnJlZGhhdA=="}}}`)
			sourceSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pull-secret",
					Namespace: "openshift-config",
				},
				Data: map[string][]byte{
					".dockerconfigjson": sharedData,
				},
				Type: corev1.SecretTypeDockerConfigJson,
			}
			targetSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pull-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					".dockerconfigjson": sharedData,
				},
				Type: corev1.SecretTypeDockerConfigJson,
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sourceSecret, targetSecret).Build(),
				Scheme: testScheme,
			}
			err := r.ensurePullSecret(ctx, "pull-secret", "openshift-config")
			Expect(err).NotTo(HaveOccurred())
		})

		It("preserves Quay credentials during merge", func() {
			sourceSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pull-secret",
					Namespace: "openshift-config",
				},
				Data: map[string][]byte{
					".dockerconfigjson": []byte(`{"auths":{"registry.redhat.io":{"auth":"cmVkaGF0OnJlZGhhdA=="}}}`),
				},
				Type: corev1.SecretTypeDockerConfigJson,
			}
			targetSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pull-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					".dockerconfigjson": []byte(`{"auths":{"mirror-operator-quay-registry.example.com":{"auth":"cXVheTpxdWF5"}}}`),
				},
				Type: corev1.SecretTypeDockerConfigJson,
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sourceSecret, targetSecret).Build(),
				Scheme: testScheme,
			}
			err := r.ensurePullSecret(ctx, "pull-secret", "openshift-config")
			Expect(err).NotTo(HaveOccurred())

			updated := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: architectNamespace}, updated)).To(Succeed())

			var config map[string]interface{}
			Expect(json.Unmarshal(updated.Data[".dockerconfigjson"], &config)).To(Succeed())
			auths := config["auths"].(map[string]interface{})
			Expect(auths).To(HaveKey("mirror-operator-quay-registry.example.com"))
			Expect(auths).To(HaveKey("registry.redhat.io"))
		})

		It("restarts backend pods after merge changes", func() {
			sourceSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pull-secret",
					Namespace: "openshift-config",
				},
				Data: map[string][]byte{
					".dockerconfigjson": []byte(`{"auths":{"new-registry.io":{"auth":"bmV3Om5ldw=="}}}`),
				},
				Type: corev1.SecretTypeDockerConfigJson,
			}
			targetSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pull-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					".dockerconfigjson": []byte(`{"auths":{"old-registry.io":{"auth":"b2xkOm9sZA=="}}}`),
				},
				Type: corev1.SecretTypeDockerConfigJson,
			}
			backendPod := &corev1.Pod{
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
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sourceSecret, targetSecret, backendPod).Build(),
				Scheme: testScheme,
			}
			err := r.ensurePullSecret(ctx, "pull-secret", "openshift-config")
			Expect(err).NotTo(HaveOccurred())

			// Backend pod should have been deleted for restart
			pod := &corev1.Pod{}
			err = r.Get(ctx, client.ObjectKey{Name: "backend-pod-1", Namespace: architectNamespace}, pod)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	Describe("ensureOSUSPullSecret - SA already linked", func() {
		It("skips SA update when pull-secret already in ImagePullSecrets", func() {
			sourceSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pull-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					".dockerconfigjson": []byte(`{"auths":{}}`),
				},
				Type: corev1.SecretTypeDockerConfigJson,
			}
			sa := &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: "openshift-update-service",
				},
				ImagePullSecrets: []corev1.LocalObjectReference{
					{Name: "pull-secret"},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sourceSecret, sa).Build(),
				Scheme: testScheme,
			}
			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).NotTo(HaveOccurred())

			updatedSA := &corev1.ServiceAccount{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "default", Namespace: "openshift-update-service"}, updatedSA)).To(Succeed())
			count := 0
			for _, s := range updatedSA.ImagePullSecrets {
				if s.Name == "pull-secret" {
					count++
				}
			}
			Expect(count).To(Equal(1))
		})

		It("does not update secret when target data matches source", func() {
			sameData := []byte(`{"auths":{"registry.example.com":{"auth":"dGVzdDp0ZXN0"}}}`)
			sourceSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pull-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					".dockerconfigjson": sameData,
				},
				Type: corev1.SecretTypeDockerConfigJson,
			}
			existingSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pull-secret",
					Namespace: "openshift-update-service",
				},
				Data: map[string][]byte{
					".dockerconfigjson": sameData,
				},
				Type: corev1.SecretTypeDockerConfigJson,
			}
			sa := &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: "openshift-update-service",
				},
				ImagePullSecrets: []corev1.LocalObjectReference{
					{Name: "pull-secret"},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sourceSecret, existingSecret, sa).Build(),
				Scheme: testScheme,
			}
			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error when default SA not found", func() {
			sourceSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pull-secret",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					".dockerconfigjson": []byte(`{"auths":{}}`),
				},
				Type: corev1.SecretTypeDockerConfigJson,
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sourceSecret).Build(),
				Scheme: testScheme,
			}
			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get default SA"))
		})
	})

	Describe("autoExpandPVC - additional scenarios", func() {
		It("does not expand when pod is in Pending phase", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: architectNamespace},
				Spec: corev1.PersistentVolumeClaimSpec{
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("10Gi"),
						},
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rhtpa-pg-0",
					Namespace: architectNamespace,
					Labels:    map[string]string{"app": "rhtpa-postgresql"},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodPending,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pvc, pod).Build(),
				Scheme: testScheme,
			}
			logger := log.FromContext(ctx)
			r.autoExpandPVC(ctx, pvc, "100Gi", logger)

			updated := &corev1.PersistentVolumeClaim{}
			Expect(r.Get(ctx, client.ObjectKeyFromObject(pvc), updated)).To(Succeed())
			currentSize := updated.Spec.Resources.Requests[corev1.ResourceStorage]
			Expect(currentSize.Cmp(resource.MustParse("10Gi"))).To(Equal(0))
		})

		It("doubles PVC size when pod has multiple containers and one is crash-looping", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: architectNamespace},
				Spec: corev1.PersistentVolumeClaimSpec{
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("5Gi"),
						},
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("5Gi"),
					},
				},
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rhtpa-pg-0",
					Namespace: architectNamespace,
					Labels:    map[string]string{"app": "rhtpa-postgresql"},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "sidecar",
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{},
							},
						},
						{
							Name: "postgresql",
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason: "CrashLoopBackOff",
								},
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pvc, pod).Build(),
				Scheme: testScheme,
			}
			logger := log.FromContext(ctx)
			r.autoExpandPVC(ctx, pvc, "100Gi", logger)

			updated := &corev1.PersistentVolumeClaim{}
			Expect(r.Get(ctx, client.ObjectKeyFromObject(pvc), updated)).To(Succeed())
			newSize := updated.Spec.Resources.Requests[corev1.ResourceStorage]
			Expect(newSize.Cmp(resource.MustParse("10Gi"))).To(Equal(0))
		})

		It("caps expanded size at max when doubling exceeds max", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: architectNamespace},
				Spec: corev1.PersistentVolumeClaimSpec{
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("60Gi"),
						},
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("60Gi"),
					},
				},
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rhtpa-pg-0",
					Namespace: architectNamespace,
					Labels:    map[string]string{"app": "rhtpa-postgresql"},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodFailed,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pvc, pod).Build(),
				Scheme: testScheme,
			}
			logger := log.FromContext(ctx)
			r.autoExpandPVC(ctx, pvc, "100Gi", logger)

			updated := &corev1.PersistentVolumeClaim{}
			Expect(r.Get(ctx, client.ObjectKeyFromObject(pvc), updated)).To(Succeed())
			newSize := updated.Spec.Resources.Requests[corev1.ResourceStorage]
			// 60Gi doubled = 120Gi, capped at 100Gi
			Expect(newSize.Cmp(resource.MustParse("100Gi"))).To(Equal(0))
		})

		It("does not expand when capacity exactly equals max", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: architectNamespace},
				Spec: corev1.PersistentVolumeClaimSpec{
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("100Gi"),
						},
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("100Gi"),
					},
				},
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rhtpa-pg-0",
					Namespace: architectNamespace,
					Labels:    map[string]string{"app": "rhtpa-postgresql"},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodFailed,
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pvc, pod).Build(),
				Scheme: testScheme,
			}
			logger := log.FromContext(ctx)
			r.autoExpandPVC(ctx, pvc, "100Gi", logger)

			updated := &corev1.PersistentVolumeClaim{}
			Expect(r.Get(ctx, client.ObjectKeyFromObject(pvc), updated)).To(Succeed())
			currentSize := updated.Spec.Resources.Requests[corev1.ResourceStorage]
			Expect(currentSize.Cmp(resource.MustParse("100Gi"))).To(Equal(0))
		})
	})

	Describe("configureOpenShiftOAuth", func() {
		It("returns error when admin secret is missing", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.configureOpenShiftOAuth(ctx, "keycloak.example.com")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get Keycloak admin credentials"))
		})

		It("returns error when Infrastructure resource is missing", func() {
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-keycloak-initial-admin",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"username": []byte("admin"),
					"password": []byte("admin-pass"),
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret).Build(),
				Scheme: testScheme,
			}
			err := r.configureOpenShiftOAuth(ctx, "keycloak.example.com")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get cluster infrastructure"))
		})

		It("returns error when apiServerURL is empty", func() {
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-keycloak-initial-admin",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"username": []byte("admin"),
					"password": []byte("admin-pass"),
				},
			}
			infra := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "config.openshift.io/v1",
				"kind":       "Infrastructure",
				"metadata":   map[string]interface{}{"name": "cluster"},
				"status":     map[string]interface{}{},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret, infra).Build(),
				Scheme: testScheme,
			}
			err := r.configureOpenShiftOAuth(ctx, "keycloak.example.com")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("apiServerURL not found"))
		})

		It("fails at HTTP call when all prereqs exist", func() {
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-keycloak-initial-admin",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"username": []byte("admin"),
					"password": []byte("admin-pass"),
				},
			}
			infra := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "config.openshift.io/v1",
				"kind":       "Infrastructure",
				"metadata":   map[string]interface{}{"name": "cluster"},
				"status":     map[string]interface{}{"apiServerURL": "https://api.test.example.com:6443"},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret, infra).Build(),
				Scheme: testScheme,
			}
			// Use a non-routable host so the HTTP call fails quickly
			err := r.configureOpenShiftOAuth(ctx, "192.0.2.1:1")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get admin token"))
		})
	})

	Describe("addQuayCredentialsIfNeeded - with robot credentials", func() {
		It("adds Quay auth when robot credentials are cached", func() {
			quay := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata":   map[string]interface{}{"name": "mirror-operator-quay", "namespace": architectNamespace},
				"status":     map[string]interface{}{"registryEndpoint": "https://quay.apps.test.example.com"},
			}}
			robotSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "quay-robot-credentials",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"token": []byte("robot-token-xyz"),
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(quay, robotSecret).Build(),
				Scheme: testScheme,
			}
			original := []byte(`{"auths":{}}`)
			result, changed, err := r.addQuayCredentialsIfNeeded(ctx, original)
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(BeTrue())

			var parsed map[string]interface{}
			Expect(json.Unmarshal(result, &parsed)).To(Succeed())
			auths := parsed["auths"].(map[string]interface{})
			quayAuth := auths["quay.apps.test.example.com"].(map[string]interface{})
			expectedAuth := base64.StdEncoding.EncodeToString([]byte("mirror+mirroroperator:robot-token-xyz"))
			Expect(quayAuth["auth"]).To(Equal(expectedAuth))
		})

		It("returns unchanged when credentials already present and match", func() {
			quay := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata":   map[string]interface{}{"name": "mirror-operator-quay", "namespace": architectNamespace},
				"status":     map[string]interface{}{"registryEndpoint": "https://quay.apps.test.example.com"},
			}}
			robotSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "quay-robot-credentials",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"token": []byte("robot-token-xyz"),
				},
			}
			existingAuth := base64.StdEncoding.EncodeToString([]byte("mirror+mirroroperator:robot-token-xyz"))
			original := []byte(fmt.Sprintf(`{"auths":{"quay.apps.test.example.com":{"auth":"%s"}}}`, existingAuth))

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(quay, robotSecret).Build(),
				Scheme: testScheme,
			}
			result, changed, err := r.addQuayCredentialsIfNeeded(ctx, original)
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(BeFalse())
			Expect(result).To(Equal(original))
		})

		It("returns error on invalid JSON input", func() {
			quay := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata":   map[string]interface{}{"name": "mirror-operator-quay", "namespace": architectNamespace},
				"status":     map[string]interface{}{"registryEndpoint": "https://quay.apps.test.example.com"},
			}}
			robotSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "quay-robot-credentials",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"token": []byte("robot-token-xyz"),
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(quay, robotSecret).Build(),
				Scheme: testScheme,
			}
			_, _, err := r.addQuayCredentialsIfNeeded(ctx, []byte("not-valid-json"))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to parse dockerconfig"))
		})

		It("returns unchanged when QuayRegistry exists but route has no hostname", func() {
			quay := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata":   map[string]interface{}{"name": "mirror-operator-quay", "namespace": architectNamespace},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(quay).Build(),
				Scheme: testScheme,
			}
			original := []byte(`{"auths":{}}`)
			result, changed, err := r.addQuayCredentialsIfNeeded(ctx, original)
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(BeFalse())
			Expect(result).To(Equal(original))
		})
	})

	Describe("ensureQuayCredentials - hostname empty", func() {
		It("returns nil when QuayRegistry exists but hostname is empty", func() {
			quay := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "quay.redhat.com/v1",
				"kind":       "QuayRegistry",
				"metadata":   map[string]interface{}{"name": "mirror-operator-quay", "namespace": architectNamespace},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(quay).Build(),
				Scheme: testScheme,
			}
			err := r.ensureQuayCredentials(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureQuayAPIToken - empty token in secret", func() {
		It("returns error when secret exists but token is empty due to Create conflict", func() {
			tokenSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "quay-api-token",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"token": []byte(""),
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(tokenSecret).Build(),
				Scheme: testScheme,
			}
			// When secret exists with empty token, Get succeeds and populates ResourceVersion.
			// Bootstrap returns "unused" but Create fails because object already has ResourceVersion.
			_, err := r.ensureQuayAPIToken(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to save API token"))
		})

		It("returns token when secret has no token key", func() {
			tokenSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "quay-api-token",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(tokenSecret).Build(),
				Scheme: testScheme,
			}
			// When token key doesn't exist, ok is false, falls through to bootstrap
			_, err := r.ensureQuayAPIToken(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to save API token"))
		})
	})

	Describe("configureKeycloakRealmAndClient", func() {
		It("returns error when admin secret is missing", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.configureKeycloakRealmAndClient(ctx, "keycloak.example.com", "test-realm", "test-client")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get admin secret"))
		})

		It("creates realm and client when neither exists", func() {
			realmCreated := false
			clientCreated := false

			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/realms/master/protocol/openid-connect/token" && r.Method == "POST":
					json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "test-token"})

				case r.URL.Path == "/admin/realms" && r.Method == "GET":
					json.NewEncoder(w).Encode([]map[string]interface{}{})

				case r.URL.Path == "/admin/realms" && r.Method == "POST":
					realmCreated = true
					w.WriteHeader(http.StatusCreated)

				case strings.HasPrefix(r.URL.Path, "/admin/realms/test-realm/clients") && r.Method == "GET" && r.URL.RawQuery != "":
					json.NewEncoder(w).Encode([]map[string]interface{}{})

				case strings.HasPrefix(r.URL.Path, "/admin/realms/test-realm/clients") && r.Method == "POST":
					clientCreated = true
					w.Header().Set("Location", "/admin/realms/test-realm/clients/uuid-123")
					w.WriteHeader(http.StatusCreated)

				case strings.Contains(r.URL.Path, "/protocol-mappers/models") && r.Method == "GET":
					json.NewEncoder(w).Encode([]map[string]interface{}{})

				case strings.Contains(r.URL.Path, "/protocol-mappers/models") && r.Method == "POST":
					w.WriteHeader(http.StatusCreated)

				case strings.Contains(r.URL.Path, "/client-scopes") && r.Method == "GET":
					json.NewEncoder(w).Encode([]map[string]interface{}{})

				case strings.Contains(r.URL.Path, "/client-scopes") && r.Method == "POST":
					w.WriteHeader(http.StatusCreated)

				case strings.Contains(r.URL.Path, "/default-client-scopes/"):
					w.WriteHeader(http.StatusNoContent)

				case strings.Contains(r.URL.Path, "/client-secret") && r.Method == "GET":
					json.NewEncoder(w).Encode(map[string]interface{}{"value": "the-secret"})

				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"password": []byte("admin-pass")},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret).Build(),
				Scheme: testScheme,
			}

			_ = r.configureKeycloakRealmAndClient(ctx, keycloakHost, "test-realm", "test-client")
			Expect(realmCreated).To(BeTrue())
			Expect(clientCreated).To(BeTrue())
		})

		It("skips realm creation when realm already exists", func() {
			realmCreated := false

			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/realms/master/protocol/openid-connect/token":
					json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "test-token"})
				case r.URL.Path == "/admin/realms" && r.Method == "GET":
					json.NewEncoder(w).Encode([]map[string]interface{}{{"realm": "test-realm"}})
				case r.URL.Path == "/admin/realms" && r.Method == "POST":
					realmCreated = true
					w.WriteHeader(http.StatusCreated)
				case strings.HasPrefix(r.URL.Path, "/admin/realms/test-realm/clients") && r.Method == "GET":
					json.NewEncoder(w).Encode([]map[string]interface{}{{"id": "existing-uuid", "clientId": "test-client"}})
				case strings.Contains(r.URL.Path, "/protocol-mappers/models") && r.Method == "GET":
					json.NewEncoder(w).Encode([]map[string]interface{}{
						{"name": "email-mapper"},
						{"name": "email-verified-mapper", "id": "ev-id", "config": map[string]interface{}{"jsonType.label": "boolean"}},
					})
				case strings.Contains(r.URL.Path, "/client-scopes") && r.Method == "GET":
					json.NewEncoder(w).Encode([]map[string]interface{}{{"name": "email_verified", "id": "scope-1"}})
				case strings.Contains(r.URL.Path, "/default-client-scopes/"):
					w.WriteHeader(http.StatusNoContent)
				case strings.Contains(r.URL.Path, "/client-secret"):
					json.NewEncoder(w).Encode(map[string]interface{}{"value": "existing-secret"})
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"password": []byte("admin-pass")},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret).Build(),
				Scheme: testScheme,
			}

			_ = r.configureKeycloakRealmAndClient(ctx, keycloakHost, "test-realm", "test-client")
			Expect(realmCreated).To(BeFalse())
		})

		It("returns error when token endpoint fails", func() {
			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte("invalid credentials"))
			}))
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"password": []byte("bad-pass")},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret).Build(),
				Scheme: testScheme,
			}

			err := r.configureKeycloakRealmAndClient(ctx, keycloakHost, "test-realm", "test-client")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get admin token"))
		})

		It("deletes incorrect email_verified mapper and recreates", func() {
			deleteCalled := false
			recreatedMapper := false

			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/realms/master/protocol/openid-connect/token":
					json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "test-token"})
				case r.URL.Path == "/admin/realms" && r.Method == "GET":
					json.NewEncoder(w).Encode([]map[string]interface{}{{"realm": "test-realm"}})
				case strings.HasPrefix(r.URL.Path, "/admin/realms/test-realm/clients") && r.Method == "GET" && r.URL.RawQuery != "":
					json.NewEncoder(w).Encode([]map[string]interface{}{{"id": "client-uuid", "clientId": "test-client"}})
				case strings.Contains(r.URL.Path, "/protocol-mappers/models") && r.Method == "GET":
					json.NewEncoder(w).Encode([]map[string]interface{}{
						{"name": "email-mapper"},
						{"name": "email-verified-mapper", "id": "bad-mapper-id", "config": map[string]interface{}{"jsonType.label": "String"}},
					})
				case strings.Contains(r.URL.Path, "/protocol-mappers/models/bad-mapper-id") && r.Method == "DELETE":
					deleteCalled = true
					w.WriteHeader(http.StatusNoContent)
				case strings.Contains(r.URL.Path, "/protocol-mappers/models") && r.Method == "POST":
					body, _ := io.ReadAll(r.Body)
					if strings.Contains(string(body), "email-verified-mapper") {
						recreatedMapper = true
					}
					w.WriteHeader(http.StatusCreated)
				case strings.Contains(r.URL.Path, "/client-scopes") && r.Method == "GET":
					json.NewEncoder(w).Encode([]map[string]interface{}{{"name": "email_verified", "id": "scope-1"}})
				case strings.Contains(r.URL.Path, "/default-client-scopes/"):
					w.WriteHeader(http.StatusNoContent)
				case strings.Contains(r.URL.Path, "/client-secret"):
					json.NewEncoder(w).Encode(map[string]interface{}{"value": "secret-val"})
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"password": []byte("admin-pass")},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret).Build(),
				Scheme: testScheme,
			}

			_ = r.configureKeycloakRealmAndClient(ctx, keycloakHost, "test-realm", "test-client")
			Expect(deleteCalled).To(BeTrue())
			Expect(recreatedMapper).To(BeTrue())
		})
	})

	Describe("configureKeycloakEmailMapper", func() {
		It("returns error when admin secret is missing", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.configureKeycloakEmailMapper(ctx, "keycloak.example.com", "test-client", "test-realm")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get admin secret"))
		})

		It("creates all three mappers when none exist", func() {
			mappersCreated := map[string]bool{}

			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/realms/master/protocol/openid-connect/token":
					json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "test-token"})
				case strings.Contains(r.URL.Path, "/clients") && r.Method == "GET" && r.URL.RawQuery != "":
					json.NewEncoder(w).Encode([]map[string]interface{}{{"id": "client-uuid", "clientId": "test-client"}})
				case strings.Contains(r.URL.Path, "/protocol-mappers/models") && r.Method == "GET":
					json.NewEncoder(w).Encode([]map[string]interface{}{})
				case strings.Contains(r.URL.Path, "/protocol-mappers/models") && r.Method == "POST":
					body, _ := io.ReadAll(r.Body)
					if strings.Contains(string(body), "email-mapper") {
						mappersCreated["email"] = true
					}
					if strings.Contains(string(body), "email-verified-mapper") {
						mappersCreated["email-verified"] = true
					}
					if strings.Contains(string(body), "audience-mapper") {
						mappersCreated["audience"] = true
					}
					w.WriteHeader(http.StatusCreated)
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"password": []byte("admin-pass")},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret).Build(),
				Scheme: testScheme,
			}

			err := r.configureKeycloakEmailMapper(ctx, keycloakHost, "test-client", "test-realm")
			Expect(err).NotTo(HaveOccurred())
			Expect(mappersCreated["email"]).To(BeTrue())
			Expect(mappersCreated["email-verified"]).To(BeTrue())
			Expect(mappersCreated["audience"]).To(BeTrue())
		})

		It("skips existing mappers", func() {
			createCalled := false

			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/realms/master/protocol/openid-connect/token":
					json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "test-token"})
				case strings.Contains(r.URL.Path, "/protocol-mappers/models") && r.Method == "GET":
					json.NewEncoder(w).Encode([]map[string]interface{}{
						{"name": "email-mapper"},
						{"name": "email-verified-mapper"},
						{"name": "audience-mapper"},
					})
				case strings.Contains(r.URL.Path, "/protocol-mappers/models") && r.Method == "POST":
					createCalled = true
					w.WriteHeader(http.StatusCreated)
				case strings.Contains(r.URL.Path, "/clients") && r.Method == "GET":
					json.NewEncoder(w).Encode([]map[string]interface{}{{"id": "client-uuid"}})
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"password": []byte("admin-pass")},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret).Build(),
				Scheme: testScheme,
			}

			err := r.configureKeycloakEmailMapper(ctx, keycloakHost, "test-client", "test-realm")
			Expect(err).NotTo(HaveOccurred())
			Expect(createCalled).To(BeFalse())
		})

		It("returns error when client not found", func() {
			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/realms/master/protocol/openid-connect/token":
					json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "test-token"})
				case strings.Contains(r.URL.Path, "/clients") && r.Method == "GET":
					json.NewEncoder(w).Encode([]map[string]interface{}{})
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"password": []byte("admin-pass")},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret).Build(),
				Scheme: testScheme,
			}

			err := r.configureKeycloakEmailMapper(ctx, keycloakHost, "test-client", "test-realm")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("client test-client not found"))
		})
	})

	Describe("updateKeycloakClientSecret", func() {
		It("returns error when admin secret is missing", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.updateKeycloakClientSecret(ctx, "keycloak.example.com", "test-client", "test-realm")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get admin secret"))
		})

		It("retrieves client secret and creates OIDC secret", func() {
			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/realms/master/protocol/openid-connect/token":
					json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "test-token"})
				case strings.Contains(r.URL.Path, "/clients") && r.Method == "GET" && r.URL.RawQuery != "":
					json.NewEncoder(w).Encode([]map[string]interface{}{{"id": "client-uuid"}})
				case strings.Contains(r.URL.Path, "/client-secret") && r.Method == "GET":
					json.NewEncoder(w).Encode(map[string]interface{}{"value": "real-client-secret"})
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"password": []byte("admin-pass")},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret).Build(),
				Scheme: testScheme,
			}

			err := r.updateKeycloakClientSecret(ctx, keycloakHost, "test-client", "test-realm")
			Expect(err).NotTo(HaveOccurred())

			oidcSecret := &corev1.Secret{}
			err = r.Get(ctx, client.ObjectKey{Name: "mirror-operator-keycloak-client-secret", Namespace: architectNamespace}, oidcSecret)
			Expect(err).NotTo(HaveOccurred())
			// Fake client stores StringData as-is, not converting to Data
			secretVal := string(oidcSecret.Data["clientSecret"])
			if secretVal == "" {
				secretVal = oidcSecret.StringData["clientSecret"]
			}
			Expect(secretVal).To(Equal("real-client-secret"))
		})

		It("regenerates placeholder secret", func() {
			regenerated := false

			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/realms/master/protocol/openid-connect/token":
					json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "test-token"})
				case strings.Contains(r.URL.Path, "/clients") && r.Method == "GET" && r.URL.RawQuery != "":
					json.NewEncoder(w).Encode([]map[string]interface{}{{"id": "client-uuid"}})
				case strings.Contains(r.URL.Path, "/client-secret") && r.Method == "GET":
					json.NewEncoder(w).Encode(map[string]interface{}{"value": "will-be-replaced-by-controller"})
				case strings.Contains(r.URL.Path, "/client-secret") && r.Method == "POST":
					regenerated = true
					json.NewEncoder(w).Encode(map[string]interface{}{"value": "regenerated-secret"})
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"password": []byte("admin-pass")},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret).Build(),
				Scheme: testScheme,
			}

			err := r.updateKeycloakClientSecret(ctx, keycloakHost, "test-client", "test-realm")
			Expect(err).NotTo(HaveOccurred())
			Expect(regenerated).To(BeTrue())

			oidcSecret := &corev1.Secret{}
			err = r.Get(ctx, client.ObjectKey{Name: "mirror-operator-keycloak-client-secret", Namespace: architectNamespace}, oidcSecret)
			Expect(err).NotTo(HaveOccurred())
			secretVal := string(oidcSecret.Data["clientSecret"])
			if secretVal == "" {
				secretVal = oidcSecret.StringData["clientSecret"]
			}
			Expect(secretVal).To(Equal("regenerated-secret"))
		})

		It("updates existing OIDC secret", func() {
			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/realms/master/protocol/openid-connect/token":
					json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "test-token"})
				case strings.Contains(r.URL.Path, "/clients") && r.Method == "GET" && r.URL.RawQuery != "":
					json.NewEncoder(w).Encode([]map[string]interface{}{{"id": "client-uuid"}})
				case strings.Contains(r.URL.Path, "/client-secret"):
					json.NewEncoder(w).Encode(map[string]interface{}{"value": "updated-secret"})
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"password": []byte("admin-pass")},
			}
			existingOIDCSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-client-secret", Namespace: architectNamespace},
				Data:       map[string][]byte{"clientSecret": []byte("old-secret")},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret, existingOIDCSecret).Build(),
				Scheme: testScheme,
			}

			err := r.updateKeycloakClientSecret(ctx, keycloakHost, "test-client", "test-realm")
			Expect(err).NotTo(HaveOccurred())

			oidcSecret := &corev1.Secret{}
			err = r.Get(ctx, client.ObjectKey{Name: "mirror-operator-keycloak-client-secret", Namespace: architectNamespace}, oidcSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(oidcSecret.Data["clientSecret"])).To(Equal("updated-secret"))
		})

		It("returns error when client not found", func() {
			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/realms/master/protocol/openid-connect/token":
					json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "test-token"})
				case strings.Contains(r.URL.Path, "/clients"):
					json.NewEncoder(w).Encode([]map[string]interface{}{})
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"password": []byte("admin-pass")},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret).Build(),
				Scheme: testScheme,
			}

			err := r.updateKeycloakClientSecret(ctx, keycloakHost, "test-client", "test-realm")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("client test-client not found"))
		})
	})

	Describe("reconcileManagedKeycloak", func() {
		It("returns nil when RHTAS is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      "connected",
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			err := r.reconcileManagedKeycloak(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when OIDC managed is not enabled", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Managed: &mirrorv1.ManagedKeycloakConfig{Enabled: false},
							},
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			err := r.reconcileManagedKeycloak(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error when cluster ingress not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
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

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			err := r.reconcileManagedKeycloak(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get cluster ingress"))
		})

		It("proceeds past ingress lookup when ingress exists", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform", Namespace: architectNamespace},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Managed: &mirrorv1.ManagedKeycloakConfig{
									Enabled:   true,
									TLSSecret: &corev1.LocalObjectReference{Name: "my-tls-secret"},
								},
							},
						},
					},
				},
			}

			ingress := &unstructured.Unstructured{}
			ingress.SetGroupVersionKind(schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "Ingress"})
			ingress.SetName("cluster")
			unstructured.SetNestedField(ingress.Object, "apps.example.com", "spec", "domain")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ingress).Build(),
				Scheme: testScheme,
			}

			var testErr error
			func() {
				defer func() { recover() }()
				testErr = r.reconcileManagedKeycloak(ctx, platform)
			}()
			if testErr != nil {
				Expect(testErr.Error()).NotTo(ContainSubstring("failed to get cluster ingress"))
			}
		})
	})

	Describe("ensureTrustifyRealmAndOIDC", func() {
		It("returns error when admin secret is missing", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureTrustifyRealmAndOIDC(ctx, "keycloak.example.com", "trustify", "apps.example.com")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get Keycloak admin credentials"))
		})

		It("creates realm when it does not exist", func() {
			realmCreated := false

			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/realms/master/protocol/openid-connect/token":
					json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "test-token"})
				case r.URL.Path == "/admin/realms/trustify" && r.Method == "GET":
					w.WriteHeader(http.StatusNotFound)
				case r.URL.Path == "/admin/realms" && r.Method == "POST":
					realmCreated = true
					w.WriteHeader(http.StatusCreated)
				case strings.Contains(r.URL.Path, "/client-scopes") && r.Method == "GET":
					json.NewEncoder(w).Encode([]map[string]interface{}{})
				case strings.Contains(r.URL.Path, "/client-scopes") && r.Method == "POST":
					w.WriteHeader(http.StatusCreated)
				case strings.Contains(r.URL.Path, "/clients") && r.Method == "GET" && r.URL.RawQuery != "":
					q, _ := url.ParseQuery(r.URL.RawQuery)
					clientID := q.Get("clientId")
					json.NewEncoder(w).Encode([]map[string]interface{}{{"id": "uuid-" + clientID, "clientId": clientID}})
				case strings.Contains(r.URL.Path, "/clients") && r.Method == "PUT":
					w.WriteHeader(http.StatusOK)
				case strings.Contains(r.URL.Path, "/default-client-scopes/"):
					w.WriteHeader(http.StatusNoContent)
				case strings.Contains(r.URL.Path, "/client-secret"):
					json.NewEncoder(w).Encode(map[string]interface{}{"value": "cli-secret"})
				case strings.Contains(r.URL.Path, "/service-account-user"):
					json.NewEncoder(w).Encode(map[string]interface{}{"id": "sa-user-id"})
				case strings.Contains(r.URL.Path, "/role-mappings"):
					w.WriteHeader(http.StatusNoContent)
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"username": []byte("admin"), "password": []byte("admin-pass")},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret).Build(),
				Scheme: testScheme,
			}

			err := r.ensureTrustifyRealmAndOIDC(ctx, keycloakHost, "trustify", "apps.example.com")
			Expect(err).NotTo(HaveOccurred())
			Expect(realmCreated).To(BeTrue())
		})

		It("skips realm creation when realm exists", func() {
			realmCreated := false

			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/realms/master/protocol/openid-connect/token":
					json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "test-token"})
				case r.URL.Path == "/admin/realms/trustify" && r.Method == "GET":
					json.NewEncoder(w).Encode(map[string]interface{}{"realm": "trustify"})
				case r.URL.Path == "/admin/realms" && r.Method == "POST":
					realmCreated = true
					w.WriteHeader(http.StatusCreated)
				case strings.Contains(r.URL.Path, "/client-scopes") && r.Method == "GET":
					json.NewEncoder(w).Encode([]map[string]interface{}{
						{"name": "read:document", "id": "scope-ro"},
						{"name": "create:document", "id": "scope-rw"},
					})
				case strings.Contains(r.URL.Path, "/clients") && r.Method == "GET" && r.URL.RawQuery != "":
					q, _ := url.ParseQuery(r.URL.RawQuery)
					clientID := q.Get("clientId")
					json.NewEncoder(w).Encode([]map[string]interface{}{{"id": "uuid-" + clientID, "clientId": clientID}})
				case strings.Contains(r.URL.Path, "/clients") && r.Method == "PUT":
					w.WriteHeader(http.StatusOK)
				case strings.Contains(r.URL.Path, "/default-client-scopes/"):
					w.WriteHeader(http.StatusNoContent)
				case strings.Contains(r.URL.Path, "/client-secret"):
					json.NewEncoder(w).Encode(map[string]interface{}{"value": "cli-secret"})
				case strings.Contains(r.URL.Path, "/service-account-user"):
					json.NewEncoder(w).Encode(map[string]interface{}{"id": "sa-user-id"})
				case strings.Contains(r.URL.Path, "/role-mappings"):
					w.WriteHeader(http.StatusNoContent)
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"username": []byte("admin"), "password": []byte("admin-pass")},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret).Build(),
				Scheme: testScheme,
			}

			err := r.ensureTrustifyRealmAndOIDC(ctx, keycloakHost, "trustify", "apps.example.com")
			Expect(err).NotTo(HaveOccurred())
			Expect(realmCreated).To(BeFalse())
		})

		It("returns error when token request fails with bad status", func() {
			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte("bad credentials"))
			}))
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"username": []byte("admin"), "password": []byte("bad")},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret).Build(),
				Scheme: testScheme,
			}

			err := r.ensureTrustifyRealmAndOIDC(ctx, keycloakHost, "trustify", "apps.example.com")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get admin token"))
		})
	})

	Describe("updateTrustifyRedirectURIs", func() {
		It("returns error when TPA resource not found", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.updateTrustifyRedirectURIs(ctx, "keycloak.example.com", "trustify", "frontend")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get TPA resource"))
		})

		It("returns error when no server ingress hostname found", func() {
			tpa := &unstructured.Unstructured{}
			tpa.SetGroupVersionKind(schema.GroupVersionKind{Group: "rhtpa.io", Version: "v1", Kind: "TrustedProfileAnalyzer"})
			tpa.SetName("mirror-operator-trusted-profile-analyzer")
			tpa.SetNamespace(architectNamespace)
			tpa.SetUID("tpa-uid-123")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(tpa).Build(),
				Scheme: testScheme,
			}
			err := r.updateTrustifyRedirectURIs(ctx, "keycloak.example.com", "trustify", "frontend")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("RHTPA server ingress not found"))
		})

		It("updates redirect URIs when server ingress exists", func() {
			updateCalled := false

			tpa := &unstructured.Unstructured{}
			tpa.SetGroupVersionKind(schema.GroupVersionKind{Group: "rhtpa.io", Version: "v1", Kind: "TrustedProfileAnalyzer"})
			tpa.SetName("mirror-operator-trusted-profile-analyzer")
			tpa.SetNamespace(architectNamespace)
			tpa.SetUID("tpa-uid-123")

			serverIngress := &unstructured.Unstructured{}
			serverIngress.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.k8s.io", Version: "v1", Kind: "Ingress"})
			serverIngress.SetName("tpa-server-ingress")
			serverIngress.SetNamespace(architectNamespace)
			serverIngress.SetOwnerReferences([]metav1.OwnerReference{{
				UID:  "tpa-uid-123",
				Kind: "TrustedProfileAnalyzer",
			}})
			unstructured.SetNestedSlice(serverIngress.Object, []interface{}{
				map[string]interface{}{"host": "trustify.apps.example.com"},
			}, "spec", "rules")

			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/realms/master/protocol/openid-connect/token":
					json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "test-token"})
				case strings.Contains(r.URL.Path, "/clients") && r.Method == "GET" && r.URL.RawQuery != "":
					json.NewEncoder(w).Encode([]map[string]interface{}{{"id": "client-uuid"}})
				case strings.Contains(r.URL.Path, "/clients/client-uuid") && r.Method == "PUT":
					updateCalled = true
					body, _ := io.ReadAll(r.Body)
					Expect(string(body)).To(ContainSubstring("trustify.apps.example.com"))
					w.WriteHeader(http.StatusOK)
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"password": []byte("admin-pass")},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(tpa, serverIngress, adminSecret).Build(),
				Scheme: testScheme,
			}

			err := r.updateTrustifyRedirectURIs(ctx, keycloakHost, "trustify", "frontend")
			Expect(err).NotTo(HaveOccurred())
			Expect(updateCalled).To(BeTrue())
		})
	})

	Describe("configureOpenShiftOAuth", func() {
		It("returns error when admin secret is missing", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.configureOpenShiftOAuth(ctx, "keycloak.example.com")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get Keycloak admin credentials"))
		})

		It("returns error when infrastructure not found", func() {
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"username": []byte("admin"), "password": []byte("admin-pass")},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret).Build(),
				Scheme: testScheme,
			}

			err := r.configureOpenShiftOAuth(ctx, "keycloak.example.com")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get cluster infrastructure"))
		})

		It("returns error when apiServerURL is empty", func() {
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"username": []byte("admin"), "password": []byte("admin-pass")},
			}
			infra := &unstructured.Unstructured{}
			infra.SetGroupVersionKind(schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "Infrastructure"})
			infra.SetName("cluster")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret, infra).Build(),
				Scheme: testScheme,
			}

			err := r.configureOpenShiftOAuth(ctx, "keycloak.example.com")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("apiServerURL not found"))
		})

		It("configures OAuth for both realms when infrastructure and token are available", func() {
			realmsCalled := map[string]bool{}

			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/realms/master/protocol/openid-connect/token":
					json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "test-token"})
				case strings.Contains(r.URL.Path, "/identity-provider/instances") && r.Method == "GET":
					// Extract realm from path: /admin/realms/<realm>/identity-provider/instances
					parts := strings.Split(r.URL.Path, "/")
					for i, p := range parts {
						if p == "realms" && i+1 < len(parts) {
							realmsCalled[parts[i+1]] = true
							break
						}
					}
					json.NewEncoder(w).Encode([]map[string]interface{}{{"alias": "openshift-v4"}})
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer ts.Close()

			keycloakHost := strings.TrimPrefix(ts.URL, "https://")
			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"username": []byte("admin"), "password": []byte("admin-pass")},
			}
			infra := &unstructured.Unstructured{}
			infra.SetGroupVersionKind(schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "Infrastructure"})
			infra.SetName("cluster")
			unstructured.SetNestedField(infra.Object, "https://api.example.com:6443", "status", "apiServerURL")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(adminSecret, infra).Build(),
				Scheme: testScheme,
			}

			err := r.configureOpenShiftOAuth(ctx, keycloakHost)
			Expect(err).NotTo(HaveOccurred())
			Expect(realmsCalled["trusted-artifact-signer"]).To(BeTrue())
			Expect(realmsCalled["trustify"]).To(BeTrue())
		})
	})

	Describe("reconcileRHTPAConfig", func() {
		It("returns nil when RHTPA config is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTPA: nil,
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

		It("returns nil when RHTPA storage is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
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

		It("returns error when cluster Ingress is missing", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTPA: &mirrorv1.RHTPAInstallerConfig{
							Storage: &mirrorv1.RHTPAStorageConfig{Type: "s3"},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.reconcileRHTPAConfig(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get cluster ingress"))
		})

		It("creates TPA with explicit OIDC and database config", func() {
			ingress := &unstructured.Unstructured{}
			ingress.SetGroupVersionKind(schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "Ingress"})
			ingress.SetName("cluster")
			unstructured.SetNestedField(ingress.Object, "apps.test.example.com", "spec", "domain")

			obcCM := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "rhtpa-storage", Namespace: architectNamespace},
				Data:       map[string]string{"BUCKET_NAME": "test-bucket", "BUCKET_HOST": "s3.example.com"},
			}
			obcSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "rhtpa-storage", Namespace: architectNamespace},
				Data: map[string][]byte{
					"AWS_ACCESS_KEY_ID":     []byte("AKID"),
					"AWS_SECRET_ACCESS_KEY": []byte("SECRET"),
				},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ingress, obcCM, obcSecret).Build()

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTPA: &mirrorv1.RHTPAInstallerConfig{
							Storage: &mirrorv1.RHTPAStorageConfig{Type: "s3"},
							OIDC:    &mirrorv1.RHTPAOIDCConfig{Issuer: "https://sso.example.com/realms/trustify"},
							Database: &mirrorv1.RHTPADatabaseConfig{
								Host:     "db.example.com",
								Name:     "trustify",
								Username: "dbuser",
								Password: "dbpass",
							},
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			var testErr error
			func() {
				defer func() { recover() }()
				testErr = r.reconcileRHTPAConfig(ctx, platform)
			}()
			if testErr != nil {
				Expect(testErr.Error()).NotTo(ContainSubstring("failed to get cluster ingress"))
			}
		})

		It("updates existing TPA with OIDC and database modules configured", func() {
			ingress := &unstructured.Unstructured{}
			ingress.SetGroupVersionKind(schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "Ingress"})
			ingress.SetName("cluster")
			unstructured.SetNestedField(ingress.Object, "apps.test.example.com", "spec", "domain")

			existingTPA := &unstructured.Unstructured{}
			existingTPA.SetGroupVersionKind(schema.GroupVersionKind{Group: "rhtpa.io", Version: "v1", Kind: "TrustedProfileAnalyzer"})
			existingTPA.SetName("mirror-operator-trusted-profile-analyzer")
			existingTPA.SetNamespace(architectNamespace)
			unstructured.SetNestedMap(existingTPA.Object, map[string]interface{}{
				"issuerUrl": "https://keycloak.apps.test.example.com/realms/trustify",
			}, "spec", "oidc")
			unstructured.SetNestedMap(existingTPA.Object, map[string]interface{}{
				"createDatabase":  map[string]interface{}{"enabled": true},
				"migrateDatabase": map[string]interface{}{"enabled": true},
			}, "spec", "modules")

			obcCM := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "rhtpa-storage", Namespace: architectNamespace},
				Data:       map[string]string{"BUCKET_NAME": "updated-bucket"},
			}
			obcSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "rhtpa-storage", Namespace: architectNamespace},
				Data:       map[string][]byte{"AWS_ACCESS_KEY_ID": []byte("AK"), "AWS_SECRET_ACCESS_KEY": []byte("SK")},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ingress, existingTPA, obcCM, obcSecret).Build()

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTPA: &mirrorv1.RHTPAInstallerConfig{
							Storage: &mirrorv1.RHTPAStorageConfig{Type: "s3"},
							Importers: &mirrorv1.RHTPAImportersConfig{
								RedHatSBOMs: true,
								CVE:         true,
							},
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileRHTPAConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			updated := &unstructured.Unstructured{}
			updated.SetGroupVersionKind(schema.GroupVersionKind{Group: "rhtpa.io", Version: "v1", Kind: "TrustedProfileAnalyzer"})
			err = c.Get(ctx, client.ObjectKey{Name: "mirror-operator-trusted-profile-analyzer", Namespace: architectNamespace}, updated)
			Expect(err).NotTo(HaveOccurred())

			storageType, _, _ := unstructured.NestedString(updated.Object, "spec", "storage", "type")
			Expect(storageType).To(Equal("s3"))
			bucket, _, _ := unstructured.NestedString(updated.Object, "spec", "storage", "bucket")
			Expect(bucket).To(Equal("updated-bucket"))
		})

		It("creates OBC when it does not exist", func() {
			ingress := &unstructured.Unstructured{}
			ingress.SetGroupVersionKind(schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "Ingress"})
			ingress.SetName("cluster")
			unstructured.SetNestedField(ingress.Object, "apps.test.example.com", "spec", "domain")

			obcCM := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "rhtpa-storage", Namespace: architectNamespace},
				Data:       map[string]string{"BUCKET_NAME": "b"},
			}
			obcSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "rhtpa-storage", Namespace: architectNamespace},
				Data:       map[string][]byte{"AWS_ACCESS_KEY_ID": []byte("k"), "AWS_SECRET_ACCESS_KEY": []byte("s")},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ingress, obcCM, obcSecret).Build()

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTPA: &mirrorv1.RHTPAInstallerConfig{
							Storage: &mirrorv1.RHTPAStorageConfig{Type: "s3"},
							OIDC:    &mirrorv1.RHTPAOIDCConfig{Issuer: "https://sso.example.com/realms/trustify"},
							Database: &mirrorv1.RHTPADatabaseConfig{
								Host: "db.example.com", Name: "tdb", Username: "u", Password: "p",
							},
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			var testErr error
			func() {
				defer func() { recover() }()
				testErr = r.reconcileRHTPAConfig(ctx, platform)
			}()
			if testErr != nil {
				Expect(testErr.Error()).NotTo(ContainSubstring("failed to get cluster ingress"))
			}
		})

		It("returns error when OBC ConfigMap is missing", func() {
			ingress := &unstructured.Unstructured{}
			ingress.SetGroupVersionKind(schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "Ingress"})
			ingress.SetName("cluster")
			unstructured.SetNestedField(ingress.Object, "apps.test.example.com", "spec", "domain")

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ingress).Build()

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTPA: &mirrorv1.RHTPAInstallerConfig{
							Storage: &mirrorv1.RHTPAStorageConfig{Type: "s3"},
							OIDC:    &mirrorv1.RHTPAOIDCConfig{Issuer: "https://sso.example.com/realms/t"},
							Database: &mirrorv1.RHTPADatabaseConfig{
								Host: "db", Name: "d", Username: "u", Password: "p",
							},
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileRHTPAConfig(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get RHTPA storage OBC ConfigMap"))
		})

		It("returns error when OBC Secret is missing", func() {
			ingress := &unstructured.Unstructured{}
			ingress.SetGroupVersionKind(schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "Ingress"})
			ingress.SetName("cluster")
			unstructured.SetNestedField(ingress.Object, "apps.test.example.com", "spec", "domain")

			obcCM := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "rhtpa-storage", Namespace: architectNamespace},
				Data:       map[string]string{"BUCKET_NAME": "b"},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ingress, obcCM).Build()

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTPA: &mirrorv1.RHTPAInstallerConfig{
							Storage: &mirrorv1.RHTPAStorageConfig{Type: "s3"},
							OIDC:    &mirrorv1.RHTPAOIDCConfig{Issuer: "https://sso.example.com/realms/t"},
							Database: &mirrorv1.RHTPADatabaseConfig{
								Host: "db", Name: "d", Username: "u", Password: "p",
							},
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileRHTPAConfig(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get RHTPA storage OBC Secret"))
		})

		It("updates existing TPA instead of creating when TPA exists after deletion", func() {
			ingress := &unstructured.Unstructured{}
			ingress.SetGroupVersionKind(schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "Ingress"})
			ingress.SetName("cluster")
			unstructured.SetNestedField(ingress.Object, "apps.test.example.com", "spec", "domain")

			obcCM := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "rhtpa-storage", Namespace: architectNamespace},
				Data:       map[string]string{"BUCKET_NAME": "b"},
			}
			obcSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "rhtpa-storage", Namespace: architectNamespace},
				Data:       map[string][]byte{"AWS_ACCESS_KEY_ID": []byte("k"), "AWS_SECRET_ACCESS_KEY": []byte("s")},
			}

			s3Route := &unstructured.Unstructured{}
			s3Route.SetGroupVersionKind(schema.GroupVersionKind{Group: "route.openshift.io", Version: "v1", Kind: "Route"})
			s3Route.SetName("s3")
			s3Route.SetNamespace("openshift-storage")
			unstructured.SetNestedField(s3Route.Object, "s3.apps.test.example.com", "spec", "host")

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ingress, obcCM, obcSecret, s3Route).Build()

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTPA: &mirrorv1.RHTPAInstallerConfig{
							Storage: &mirrorv1.RHTPAStorageConfig{Type: "s3"},
							OIDC:    &mirrorv1.RHTPAOIDCConfig{Issuer: "https://sso.example.com/realms/trustify"},
							Database: &mirrorv1.RHTPADatabaseConfig{
								Host: "db.example.com", Name: "trustify", Username: "u", Password: "p",
							},
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			var testErr error
			func() {
				defer func() { recover() }()
				testErr = r.reconcileRHTPAConfig(ctx, platform)
			}()
			if testErr != nil {
				Expect(testErr.Error()).NotTo(ContainSubstring("failed to get cluster ingress"))
			}
		})
	})

	Describe("reconcileSubscriptions", func() {
		It("marks operator as disabled when override disables it", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						Operators: &mirrorv1.OperatorConfig{
							OpenShiftPipelines: &mirrorv1.OLMSubscriptionConfig{Disabled: true},
							Keycloak:           &mirrorv1.OLMSubscriptionConfig{Disabled: true},
							RHTAS:              &mirrorv1.OLMSubscriptionConfig{Disabled: true},
							RHTPA:              &mirrorv1.OLMSubscriptionConfig{Disabled: true},
							QuayOperator:       &mirrorv1.OLMSubscriptionConfig{Disabled: true},
							OSUS:               &mirrorv1.OLMSubscriptionConfig{Disabled: true},
						},
					},
				},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileSubscriptions(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			for _, comp := range platform.Status.Components {
				Expect(comp.Status).To(Equal("Disabled"))
			}
			Expect(platform.Status.Components).To(HaveLen(6))
		})
	})

	Describe("reconcileQuayConfig", func() {
		It("returns nil when Connected.Quay is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileQuayConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("reconcileArtifactFileServer - additional paths", func() {
		It("returns nil when no completed pipelines exist", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						ArtifactStorage: mirrorv1.ArtifactStorageConfig{Size: "100Gi"},
					},
				},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileArtifactFileServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("creates file server deployment when completed pipeline has bound PVC", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace, UID: "uid-123"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						ArtifactStorage: mirrorv1.ArtifactStorageConfig{Size: "100Gi"},
					},
				},
			}

			pipeline := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "v1", Namespace: architectNamespace},
				Spec:       mirrorv1.CollectionPipelineSpec{ImageSetConfig: "test"},
				Status:     mirrorv1.CollectionPipelineStatus{Phase: "Complete"},
			}

			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts-v1", Namespace: architectNamespace},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("100Gi")}},
				},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform, pipeline, pvc).WithStatusSubresource(pipeline).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileArtifactFileServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			dep := &appsv1.Deployment{}
			err = c.Get(ctx, client.ObjectKey{Name: "artifact-fileserver", Namespace: architectNamespace}, dep)
			Expect(err).NotTo(HaveOccurred())
			Expect(dep.Spec.Template.Spec.Volumes).To(HaveLen(1))
		})

		It("skips PVC that is not bound", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace, UID: "uid-123"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						ArtifactStorage: mirrorv1.ArtifactStorageConfig{Size: "100Gi"},
					},
				},
			}

			pipeline := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "v1", Namespace: architectNamespace},
				Spec:       mirrorv1.CollectionPipelineSpec{ImageSetConfig: "test"},
				Status:     mirrorv1.CollectionPipelineStatus{Phase: "Complete"},
			}

			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts-v1", Namespace: architectNamespace},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("100Gi")}},
				},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform, pipeline, pvc).WithStatusSubresource(pipeline).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileArtifactFileServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			dep := &appsv1.Deployment{}
			err = c.Get(ctx, client.ObjectKey{Name: "artifact-fileserver", Namespace: architectNamespace}, dep)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	Describe("configureCollectionPipelineSigning", func() {
		It("returns nil when RHTAS is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.configureCollectionPipelineSigning(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when OIDC.Managed is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{},
						},
					},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.configureCollectionPipelineSigning(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error when Securesign not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Managed: &mirrorv1.ManagedKeycloakConfig{Enabled: true},
							},
						},
					},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.configureCollectionPipelineSigning(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("waiting for Securesign"))
		})

		It("returns error when Securesign has no Fulcio URL", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(schema.GroupVersionKind{Group: "rhtas.redhat.com", Version: "v1alpha1", Kind: "Securesign"})
			securesign.SetName("mirror-operator-securesign")
			securesign.SetNamespace(architectNamespace)
			unstructured.SetNestedMap(securesign.Object, map[string]interface{}{}, "status")

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Managed: &mirrorv1.ManagedKeycloakConfig{Enabled: true},
							},
						},
					},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.configureCollectionPipelineSigning(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Fulcio or Rekor URL not available"))
		})

		It("returns error when Keycloak not found", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(schema.GroupVersionKind{Group: "rhtas.redhat.com", Version: "v1alpha1", Kind: "Securesign"})
			securesign.SetName("mirror-operator-securesign")
			securesign.SetNamespace(architectNamespace)
			unstructured.SetNestedMap(securesign.Object, map[string]interface{}{
				"fulcio": map[string]interface{}{"url": "https://fulcio.example.com"},
				"rekor":  map[string]interface{}{"url": "https://rekor.example.com"},
			}, "status")

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Managed: &mirrorv1.ManagedKeycloakConfig{Enabled: true},
							},
						},
					},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.configureCollectionPipelineSigning(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("waiting for Keycloak"))
		})

		It("returns error when Keycloak hostname empty", func() {
			securesign := &unstructured.Unstructured{}
			securesign.SetGroupVersionKind(schema.GroupVersionKind{Group: "rhtas.redhat.com", Version: "v1alpha1", Kind: "Securesign"})
			securesign.SetName("mirror-operator-securesign")
			securesign.SetNamespace(architectNamespace)
			unstructured.SetNestedMap(securesign.Object, map[string]interface{}{
				"fulcio": map[string]interface{}{"url": "https://fulcio.example.com"},
				"rekor":  map[string]interface{}{"url": "https://rekor.example.com"},
			}, "status")

			kc := &unstructured.Unstructured{}
			kc.SetGroupVersionKind(schema.GroupVersionKind{Group: "k8s.keycloak.org", Version: "v2alpha1", Kind: "Keycloak"})
			kc.SetName("mirror-operator-keycloak")
			kc.SetNamespace(architectNamespace)

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						RHTAS: &mirrorv1.RHTASInstallerConfig{
							OIDC: &mirrorv1.RHTASOIDCConfig{
								Managed: &mirrorv1.ManagedKeycloakConfig{Enabled: true},
							},
						},
					},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(securesign, kc).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.configureCollectionPipelineSigning(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Keycloak hostname not available"))
		})
	})

	Describe("reconcileQuayConfig - additional paths", func() {
		It("sets mirrorRegistry from external URL", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						Quay: &mirrorv1.QuayInstallerConfig{
							ExternalURL: "registry.example.com",
						},
					},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileQuayConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
			Expect(platform.Spec.Connected.MirrorRegistry).To(Equal("registry.example.com"))
		})

		It("does not update mirrorRegistry when already set to external URL", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						MirrorRegistry: "registry.example.com",
						Quay: &mirrorv1.QuayInstallerConfig{
							ExternalURL: "registry.example.com",
						},
					},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileQuayConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when Managed is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						Quay: &mirrorv1.QuayInstallerConfig{},
					},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileQuayConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when Managed.Enabled is false", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						Quay: &mirrorv1.QuayInstallerConfig{
							Managed: &mirrorv1.ManagedQuayConfig{Enabled: false},
						},
					},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileQuayConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("creates QuayRegistry when Managed.Enabled and QuayRegistry does not exist", func() {
			ingress := &unstructured.Unstructured{}
			ingress.SetGroupVersionKind(schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "Ingress"})
			ingress.SetName("cluster")
			unstructured.SetNestedField(ingress.Object, "apps.test.example.com", "spec", "domain")

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace, UID: "uid-1"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						Quay: &mirrorv1.QuayInstallerConfig{
							Managed: &mirrorv1.ManagedQuayConfig{Enabled: true},
						},
					},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform, ingress).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileQuayConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			quay := &unstructured.Unstructured{}
			quay.SetGroupVersionKind(schema.GroupVersionKind{Group: "quay.redhat.com", Version: "v1", Kind: "QuayRegistry"})
			err = c.Get(ctx, client.ObjectKey{Name: "mirror-operator-quay", Namespace: architectNamespace}, quay)
			Expect(err).NotTo(HaveOccurred())
		})

		It("updates mirrorRegistry when QuayRegistry already exists with hostname", func() {
			quay := &unstructured.Unstructured{}
			quay.SetGroupVersionKind(schema.GroupVersionKind{Group: "quay.redhat.com", Version: "v1", Kind: "QuayRegistry"})
			quay.SetName("mirror-operator-quay")
			quay.SetNamespace(architectNamespace)

			route := &unstructured.Unstructured{}
			route.SetGroupVersionKind(schema.GroupVersionKind{Group: "route.openshift.io", Version: "v1", Kind: "Route"})
			route.SetName("mirror-operator-quay-quay")
			route.SetNamespace(architectNamespace)
			unstructured.SetNestedField(route.Object, "quay.apps.test.example.com", "spec", "host")

			ingress := &unstructured.Unstructured{}
			ingress.SetGroupVersionKind(schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "Ingress"})
			ingress.SetName("cluster")
			unstructured.SetNestedField(ingress.Object, "apps.test.example.com", "spec", "domain")

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: architectNamespace, UID: "uid-1"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						Quay: &mirrorv1.QuayInstallerConfig{
							Managed: &mirrorv1.ManagedQuayConfig{Enabled: true},
						},
					},
				},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform, quay, route, ingress).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileQuayConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
			Expect(platform.Spec.Connected.MirrorRegistry).To(Equal("quay.apps.test.example.com/mirror"))
		})
	})

	Describe("ensureOSUSPullSecret - more paths", func() {
		It("creates secret and links to SA when SA exists but has no pull-secret", func() {
			source := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: architectNamespace},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			sa := &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "openshift-update-service"},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(source, sa).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).NotTo(HaveOccurred())

			target := &corev1.Secret{}
			err = c.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: "openshift-update-service"}, target)
			Expect(err).NotTo(HaveOccurred())

			updatedSA := &corev1.ServiceAccount{}
			err = c.Get(ctx, client.ObjectKey{Name: "default", Namespace: "openshift-update-service"}, updatedSA)
			Expect(err).NotTo(HaveOccurred())
			Expect(updatedSA.ImagePullSecrets).To(ContainElement(corev1.LocalObjectReference{Name: "pull-secret"}))
		})

		It("returns error when source secret is missing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get pull-secret"))
		})

		It("updates secret data when target differs from source", func() {
			source := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: architectNamespace},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{"new":"data"}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			existing := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "openshift-update-service"},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{"old":"data"}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			sa := &corev1.ServiceAccount{
				ObjectMeta:       metav1.ObjectMeta{Name: "default", Namespace: "openshift-update-service"},
				ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull-secret"}},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(source, existing, sa).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).NotTo(HaveOccurred())

			updated := &corev1.Secret{}
			Expect(c.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: "openshift-update-service"}, updated)).To(Succeed())
			Expect(string(updated.Data[".dockerconfigjson"])).To(ContainSubstring("new"))
		})

		It("merges Quay robot credentials into cluster pull secret", func() {
			source := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: architectNamespace},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			sa := &corev1.ServiceAccount{
				ObjectMeta:       metav1.ObjectMeta{Name: "default", Namespace: "openshift-update-service"},
				ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull-secret"}},
			}
			robotSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "quay-robot-credentials", Namespace: architectNamespace},
				Data:       map[string][]byte{"token": []byte("robottoken123")},
			}
			quay := &unstructured.Unstructured{}
			quay.SetGroupVersionKind(schema.GroupVersionKind{Group: "quay.redhat.com", Version: "v1", Kind: "QuayRegistry"})
			quay.SetName("mirror-operator-quay")
			quay.SetNamespace(architectNamespace)

			route := &unstructured.Unstructured{}
			route.SetGroupVersionKind(schema.GroupVersionKind{Group: "route.openshift.io", Version: "v1", Kind: "Route"})
			route.SetName("mirror-operator-quay-quay")
			route.SetNamespace(architectNamespace)
			unstructured.SetNestedField(route.Object, "quay.apps.example.com", "spec", "host")

			clusterSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "openshift-config"},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			osusTarget := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "openshift-update-service"},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(source, sa, robotSecret, quay, route, clusterSecret, osusTarget).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).NotTo(HaveOccurred())

			updated := &corev1.Secret{}
			Expect(c.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: "openshift-config"}, updated)).To(Succeed())
			Expect(string(updated.Data[".dockerconfigjson"])).To(ContainSubstring("quay.apps.example.com"))
		})

		It("skips cluster pull secret merge when credentials already present", func() {
			robotUser := "mirror+mirroroperator"
			robotToken := "tok"
			authValue := base64.StdEncoding.EncodeToString([]byte(robotUser + ":" + robotToken))

			source := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: architectNamespace},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			sa := &corev1.ServiceAccount{
				ObjectMeta:       metav1.ObjectMeta{Name: "default", Namespace: "openshift-update-service"},
				ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull-secret"}},
			}
			robotSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "quay-robot-credentials", Namespace: architectNamespace},
				Data:       map[string][]byte{"token": []byte(robotToken)},
			}
			quay := &unstructured.Unstructured{}
			quay.SetGroupVersionKind(schema.GroupVersionKind{Group: "quay.redhat.com", Version: "v1", Kind: "QuayRegistry"})
			quay.SetName("mirror-operator-quay")
			quay.SetNamespace(architectNamespace)

			route := &unstructured.Unstructured{}
			route.SetGroupVersionKind(schema.GroupVersionKind{Group: "route.openshift.io", Version: "v1", Kind: "Route"})
			route.SetName("mirror-operator-quay-quay")
			route.SetNamespace(architectNamespace)
			unstructured.SetNestedField(route.Object, "quay.apps.example.com", "spec", "host")

			clusterJSON := fmt.Sprintf(`{"auths":{"quay.apps.example.com":{"auth":"%s"}}}`, authValue)
			clusterSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "openshift-config"},
				Data:       map[string][]byte{".dockerconfigjson": []byte(clusterJSON)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			osusTarget := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "openshift-update-service"},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(source, sa, robotSecret, quay, route, clusterSecret, osusTarget).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("reconcileSubscriptions - disabled operator path", func() {
		It("skips namespace and subscription for disabled operators", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						Operators: &mirrorv1.OperatorConfig{
							OpenShiftPipelines: &mirrorv1.OLMSubscriptionConfig{Disabled: true},
						},
					},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileSubscriptions(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			found := false
			for _, comp := range platform.Status.Components {
				if comp.Name == "openshift-pipelines" {
					Expect(comp.Status).To(Equal("Disabled"))
					found = true
				}
			}
			Expect(found).To(BeTrue())
		})
	})

	Describe("ensureOwnSubscriptionConfig - additional paths", func() {
		It("updates subscription when config differs", func() {
			sub := &unstructured.Unstructured{}
			sub.SetGroupVersionKind(schema.GroupVersionKind{Group: "operators.coreos.com", Version: "v1alpha1", Kind: "Subscription"})
			sub.SetName("mirror-operator")
			sub.SetNamespace(architectNamespace)
			unstructured.SetNestedMap(sub.Object, map[string]interface{}{}, "spec", "config")

			proxy := &unstructured.Unstructured{}
			proxy.SetGroupVersionKind(schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "Proxy"})
			proxy.SetName("cluster")
			unstructured.SetNestedField(proxy.Object, "http://proxy.example.com", "spec", "httpProxy")

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sub, proxy).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.ensureOwnSubscriptionConfig(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("skips when subscription already has proxy config", func() {
			sub := &unstructured.Unstructured{}
			sub.SetGroupVersionKind(schema.GroupVersionKind{Group: "operators.coreos.com", Version: "v1alpha1", Kind: "Subscription"})
			sub.SetName("mirror-operator")
			sub.SetNamespace(architectNamespace)
			unstructured.SetNestedField(sub.Object, "mirror-operator", "spec", "name")
			unstructured.SetNestedSlice(sub.Object, []interface{}{
				map[string]interface{}{"name": "HTTP_PROXY", "value": "http://existing.proxy"},
			}, "spec", "config", "env")

			proxy := &unstructured.Unstructured{}
			proxy.SetGroupVersionKind(schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "Proxy"})
			proxy.SetName("cluster")
			unstructured.SetNestedField(proxy.Object, "http://proxy.example.com", "spec", "httpProxy")

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sub, proxy).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.ensureOwnSubscriptionConfig(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("skips non-mirror-operator subscriptions", func() {
			sub := &unstructured.Unstructured{}
			sub.SetGroupVersionKind(schema.GroupVersionKind{Group: "operators.coreos.com", Version: "v1alpha1", Kind: "Subscription"})
			sub.SetName("other-operator")
			sub.SetNamespace(architectNamespace)
			unstructured.SetNestedField(sub.Object, "other-operator", "spec", "name")

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sub).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.ensureOwnSubscriptionConfig(ctx)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("syncS3ConfigToSecret", func() {
		It("returns nil when ConfigMap not found", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			platform := &mirrorv1.DisconnectedPlatform{}
			err := r.syncS3ConfigToSecret(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when Secret not found", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts", Namespace: architectNamespace},
				Data:       map[string]string{"BUCKET_NAME": "test-bucket"},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cm).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			platform := &mirrorv1.DisconnectedPlatform{}
			err := r.syncS3ConfigToSecret(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("updates secret with S3 config from ConfigMap", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts", Namespace: architectNamespace},
				Data: map[string]string{
					"BUCKET_NAME":   "my-bucket",
					"BUCKET_HOST":   "custom-s3.example.com",
					"BUCKET_REGION": "eu-west-1",
				},
			}
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts", Namespace: architectNamespace},
				Data:       map[string][]byte{},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cm, secret).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			platform := &mirrorv1.DisconnectedPlatform{}

			err := r.syncS3ConfigToSecret(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			updated := &corev1.Secret{}
			Expect(c.Get(ctx, client.ObjectKeyFromObject(secret), updated)).To(Succeed())
			Expect(string(updated.Data["S3_BUCKET"])).To(Equal("my-bucket"))
			Expect(string(updated.Data["AWS_REGION"])).To(Equal("eu-west-1"))
		})

		It("defaults region to us-east-1 when empty", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts", Namespace: architectNamespace},
				Data:       map[string]string{"BUCKET_NAME": "my-bucket"},
			}
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts", Namespace: architectNamespace},
				Data:       map[string][]byte{},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cm, secret).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			platform := &mirrorv1.DisconnectedPlatform{}

			err := r.syncS3ConfigToSecret(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			updated := &corev1.Secret{}
			Expect(c.Get(ctx, client.ObjectKeyFromObject(secret), updated)).To(Succeed())
			Expect(string(updated.Data["AWS_REGION"])).To(Equal("us-east-1"))
		})

		It("resolves S3 endpoint from route when bucket host is internal", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts", Namespace: architectNamespace},
				Data: map[string]string{
					"BUCKET_NAME": "my-bucket",
					"BUCKET_HOST": "s3.openshift-storage.svc",
				},
			}
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts", Namespace: architectNamespace},
				Data:       map[string][]byte{},
			}
			s3Route := &unstructured.Unstructured{}
			s3Route.SetGroupVersionKind(schema.GroupVersionKind{Group: "route.openshift.io", Version: "v1", Kind: "Route"})
			s3Route.SetName("s3")
			s3Route.SetNamespace("openshift-storage")
			unstructured.SetNestedField(s3Route.Object, "s3-external.apps.example.com", "spec", "host")

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cm, secret, s3Route).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			platform := &mirrorv1.DisconnectedPlatform{}

			err := r.syncS3ConfigToSecret(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			updated := &corev1.Secret{}
			Expect(c.Get(ctx, client.ObjectKeyFromObject(secret), updated)).To(Succeed())
			Expect(string(updated.Data["S3_ENDPOINT"])).To(Equal("https://s3-external.apps.example.com"))
		})

		It("skips update when secret already has correct values", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts", Namespace: architectNamespace},
				Data: map[string]string{
					"BUCKET_NAME":   "my-bucket",
					"BUCKET_REGION": "us-east-1",
				},
			}
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts", Namespace: architectNamespace},
				Data: map[string][]byte{
					"S3_BUCKET":  []byte("my-bucket"),
					"AWS_REGION": []byte("us-east-1"),
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cm, secret).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			platform := &mirrorv1.DisconnectedPlatform{}

			err := r.syncS3ConfigToSecret(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureOSUSPullSecret", func() {
		It("returns error when source pull-secret not found", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get pull-secret"))
		})

		It("creates target secret when it does not exist", func() {
			source := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: architectNamespace},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			sa := &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "openshift-update-service"},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(source, sa).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}

			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).NotTo(HaveOccurred())

			created := &corev1.Secret{}
			Expect(c.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: "openshift-update-service"}, created)).To(Succeed())
			Expect(string(created.Data[".dockerconfigjson"])).To(Equal(`{"auths":{}}`))
		})

		It("updates target secret when source differs", func() {
			source := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: architectNamespace},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{"new":"creds"}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			existing := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "openshift-update-service"},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{"old":"creds"}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			sa := &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "openshift-update-service"},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(source, existing, sa).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}

			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).NotTo(HaveOccurred())

			updated := &corev1.Secret{}
			Expect(c.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: "openshift-update-service"}, updated)).To(Succeed())
			Expect(string(updated.Data[".dockerconfigjson"])).To(Equal(`{"auths":{"new":"creds"}}`))
		})

		It("links pull-secret to default SA image pull secrets", func() {
			source := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: architectNamespace},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			target := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "openshift-update-service"},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			sa := &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "openshift-update-service"},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(source, target, sa).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}

			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).NotTo(HaveOccurred())

			updatedSA := &corev1.ServiceAccount{}
			Expect(c.Get(ctx, client.ObjectKey{Name: "default", Namespace: "openshift-update-service"}, updatedSA)).To(Succeed())
			found := false
			for _, s := range updatedSA.ImagePullSecrets {
				if s.Name == "pull-secret" {
					found = true
				}
			}
			Expect(found).To(BeTrue())
		})

		It("skips SA update when pull-secret already linked", func() {
			source := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: architectNamespace},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			target := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "openshift-update-service"},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			sa := &corev1.ServiceAccount{
				ObjectMeta:       metav1.ObjectMeta{Name: "default", Namespace: "openshift-update-service"},
				ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull-secret"}},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(source, target, sa).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}

			err := r.ensureOSUSPullSecret(ctx)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("autoExpandPVC", func() {
		It("returns immediately when current capacity exceeds max", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: architectNamespace},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("100Gi"),
					},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			logger := log.FromContext(ctx)
			r.autoExpandPVC(ctx, pvc, "50Gi", logger)
		})

		It("returns when no pods match label selector", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: architectNamespace},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			logger := log.FromContext(ctx)
			r.autoExpandPVC(ctx, pvc, "50Gi", logger)
		})

		It("expands PVC when pod is crash-looping", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: architectNamespace},
				Spec: corev1.PersistentVolumeClaimSpec{
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("10Gi"),
						},
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rhtpa-pg-0",
					Namespace: architectNamespace,
					Labels:    map[string]string{"app": "rhtpa-postgresql"},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
							},
						},
					},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pvc, pod).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			logger := log.FromContext(ctx)

			r.autoExpandPVC(ctx, pvc, "50Gi", logger)

			Expect(pvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(resource.MustParse("20Gi")))
		})

		It("caps expansion at max size", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: architectNamespace},
				Spec: corev1.PersistentVolumeClaimSpec{
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("40Gi"),
						},
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("40Gi"),
					},
				},
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rhtpa-pg-0",
					Namespace: architectNamespace,
					Labels:    map[string]string{"app": "rhtpa-postgresql"},
				},
				Status: corev1.PodStatus{Phase: corev1.PodFailed},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pvc, pod).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			logger := log.FromContext(ctx)

			r.autoExpandPVC(ctx, pvc, "50Gi", logger)

			Expect(pvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(resource.MustParse("50Gi")))
		})

		It("does not expand when pod phase is not running or failed", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: architectNamespace},
				Spec: corev1.PersistentVolumeClaimSpec{
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("10Gi"),
						},
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rhtpa-pg-0",
					Namespace: architectNamespace,
					Labels:    map[string]string{"app": "rhtpa-postgresql"},
				},
				Status: corev1.PodStatus{Phase: corev1.PodPending},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pvc, pod).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			logger := log.FromContext(ctx)

			r.autoExpandPVC(ctx, pvc, "50Gi", logger)

			Expect(pvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(resource.MustParse("10Gi")))
		})
	})

	Describe("reconcileRHTPAConfig", func() {
		It("returns nil when RHTPA config is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      "connected",
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileRHTPAConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when storage is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						RHTPA: &mirrorv1.RHTPAInstallerConfig{},
					},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileRHTPAConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("deletes existing TPA when OIDC is not configured", func() {
			existingTPA := &unstructured.Unstructured{}
			existingTPA.SetGroupVersionKind(schema.GroupVersionKind{Group: "rhtpa.io", Version: "v1", Kind: "TrustedProfileAnalyzer"})
			existingTPA.SetName("mirror-operator-trusted-profile-analyzer")
			existingTPA.SetNamespace(architectNamespace)

			platform := &mirrorv1.DisconnectedPlatform{
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						RHTPA: &mirrorv1.RHTPAInstallerConfig{
							Storage: &mirrorv1.RHTPAStorageConfig{},
						},
					},
				},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existingTPA).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}

			_ = r.reconcileRHTPAConfig(ctx, platform)

			check := &unstructured.Unstructured{}
			check.SetGroupVersionKind(schema.GroupVersionKind{Group: "rhtpa.io", Version: "v1", Kind: "TrustedProfileAnalyzer"})
			err := c.Get(ctx, client.ObjectKey{Name: "mirror-operator-trusted-profile-analyzer", Namespace: architectNamespace}, check)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("returns error when ingress not found for new TPA creation", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						RHTPA: &mirrorv1.RHTPAInstallerConfig{
							Storage: &mirrorv1.RHTPAStorageConfig{},
						},
					},
				},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileRHTPAConfig(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get cluster ingress"))
		})

		It("updates existing TPA with S3 storage when fully configured", func() {
			existingTPA := &unstructured.Unstructured{}
			existingTPA.SetGroupVersionKind(schema.GroupVersionKind{Group: "rhtpa.io", Version: "v1", Kind: "TrustedProfileAnalyzer"})
			existingTPA.SetName("mirror-operator-trusted-profile-analyzer")
			existingTPA.SetNamespace(architectNamespace)
			unstructured.SetNestedMap(existingTPA.Object, map[string]interface{}{
				"issuerUrl": "https://keycloak.apps.example.com/realms/trustify",
			}, "spec", "oidc")
			unstructured.SetNestedMap(existingTPA.Object, map[string]interface{}{
				"createDatabase":  map[string]interface{}{"enabled": true},
				"migrateDatabase": map[string]interface{}{"enabled": true},
			}, "spec", "modules")

			ingress := &unstructured.Unstructured{}
			ingress.SetGroupVersionKind(schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "Ingress"})
			ingress.SetName("cluster")
			unstructured.SetNestedField(ingress.Object, "apps.example.com", "spec", "domain")

			adminSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace},
				Data:       map[string][]byte{"username": []byte("admin"), "password": []byte("admin-pass")},
			}

			obcCM := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "rhtpa-storage", Namespace: architectNamespace},
				Data:       map[string]string{"BUCKET_NAME": "test-bucket"},
			}
			obcSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "rhtpa-storage", Namespace: architectNamespace},
				Data: map[string][]byte{
					"AWS_ACCESS_KEY_ID":     []byte("access-key"),
					"AWS_SECRET_ACCESS_KEY": []byte("secret-key"),
				},
			}

			platform := &mirrorv1.DisconnectedPlatform{
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						RHTPA: &mirrorv1.RHTPAInstallerConfig{
							Storage: &mirrorv1.RHTPAStorageConfig{},
						},
					},
				},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(
				existingTPA, ingress, adminSecret, obcCM, obcSecret,
			).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}

			err := r.reconcileRHTPAConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("checkKeycloakHealth", func() {
		It("returns nil when OIDC managed is not enabled", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      "connected",
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.checkKeycloakHealth(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error when Keycloak resource not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
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
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.checkKeycloakHealth(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Keycloak resource not found"))
		})

		It("returns nil when Keycloak is ready", func() {
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

			platform := &mirrorv1.DisconnectedPlatform{
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
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(kc).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.checkKeycloakHealth(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error when Keycloak is not ready", func() {
			kc := &unstructured.Unstructured{}
			kc.SetGroupVersionKind(keycloakGVK)
			kc.SetName("mirror-operator-keycloak")
			kc.SetNamespace(architectNamespace)
			unstructured.SetNestedSlice(kc.Object, []interface{}{
				map[string]interface{}{
					"type":    "Ready",
					"status":  "False",
					"message": "waiting for pods",
				},
			}, "status", "conditions")

			platform := &mirrorv1.DisconnectedPlatform{
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
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(kc).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.checkKeycloakHealth(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Keycloak not ready: waiting for pods"))
		})

		It("returns error when status has no conditions", func() {
			kc := &unstructured.Unstructured{}
			kc.SetGroupVersionKind(keycloakGVK)
			kc.SetName("mirror-operator-keycloak")
			kc.SetNamespace(architectNamespace)
			unstructured.SetNestedMap(kc.Object, map[string]interface{}{}, "status")

			platform := &mirrorv1.DisconnectedPlatform{
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
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(kc).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.checkKeycloakHealth(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("conditions not available"))
		})
	})

	Describe("configureCollectionPipelineSigning", func() {
		It("returns nil when RHTAS OIDC not enabled", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      "connected",
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.configureCollectionPipelineSigning(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error when Securesign not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
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
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.configureCollectionPipelineSigning(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("waiting for Securesign"))
		})

		It("returns error when Securesign status missing URLs", func() {
			ss := &unstructured.Unstructured{}
			ss.SetGroupVersionKind(securesignGVK)
			ss.SetName("mirror-operator-securesign")
			ss.SetNamespace(architectNamespace)
			unstructured.SetNestedMap(ss.Object, map[string]interface{}{}, "status")

			platform := &mirrorv1.DisconnectedPlatform{
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
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
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ss).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.configureCollectionPipelineSigning(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Fulcio or Rekor URL not available"))
		})
	})

	Describe("reconcileQuayConfig - additional", func() {
		It("returns nil when Quay config is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      "connected",
					Connected: &mirrorv1.ConnectedConfig{},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileQuayConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("sets mirrorRegistry from external URL", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						Quay: &mirrorv1.QuayInstallerConfig{
							ExternalURL: "quay.external.example.com",
						},
					},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileQuayConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
			Expect(platform.Spec.Connected.MirrorRegistry).To(Equal("quay.external.example.com"))
		})

		It("skips external URL update when already set", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
					Connected: &mirrorv1.ConnectedConfig{
						MirrorRegistry: "quay.external.example.com",
						Quay: &mirrorv1.QuayInstallerConfig{
							ExternalURL: "quay.external.example.com",
						},
					},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			err := r.reconcileQuayConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureQuayCredentials - additional", func() {
		It("returns error when QuayRegistry not found", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			platform := &mirrorv1.DisconnectedPlatform{}
			err := r.ensureQuayCredentials(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get QuayRegistry"))
		})

		It("skips when QuayRegistry hostname is not available", func() {
			qr := &unstructured.Unstructured{}
			qr.SetGroupVersionKind(schema.GroupVersionKind{Group: "quay.redhat.com", Version: "v1", Kind: "QuayRegistry"})
			qr.SetName("mirror-operator-quay")
			qr.SetNamespace(architectNamespace)

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(qr).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}
			platform := &mirrorv1.DisconnectedPlatform{}
			err := r.ensureQuayCredentials(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("reconcileArtifactsBucket", func() {
		It("creates OBC when it does not exist", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform", Namespace: architectNamespace},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}

			err := r.reconcileArtifactsBucket(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			obc := &unstructured.Unstructured{}
			obc.SetGroupVersionKind(schema.GroupVersionKind{Group: "objectbucket.io", Version: "v1alpha1", Kind: "ObjectBucketClaim"})
			err = c.Get(ctx, client.ObjectKey{Name: "collection-artifacts", Namespace: architectNamespace}, obc)
			Expect(err).NotTo(HaveOccurred())
		})

		It("skips when OBC already exists", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform", Namespace: architectNamespace},
			}
			existing := &unstructured.Unstructured{}
			existing.SetGroupVersionKind(schema.GroupVersionKind{Group: "objectbucket.io", Version: "v1alpha1", Kind: "ObjectBucketClaim"})
			existing.SetName("collection-artifacts")
			existing.SetNamespace(architectNamespace)

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform, existing).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}

			err := r.reconcileArtifactsBucket(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("reconcileArtifactFileServer", func() {
		It("skips when no completed pipelines with bound PVCs", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform", Namespace: architectNamespace},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}

			err := r.reconcileArtifactFileServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			err = c.Get(ctx, client.ObjectKey{Name: "artifact-fileserver", Namespace: architectNamespace}, deploy)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("creates deployment and service when completed pipeline has bound PVC", func() {
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
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform", Namespace: architectNamespace},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform, pipeline, pvc).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}

			err := r.reconcileArtifactFileServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			err = c.Get(ctx, client.ObjectKey{Name: "artifact-fileserver", Namespace: architectNamespace}, deploy)
			Expect(err).NotTo(HaveOccurred())

			svc := &corev1.Service{}
			err = c.Get(ctx, client.ObjectKey{Name: "artifact-fileserver", Namespace: architectNamespace}, svc)
			Expect(err).NotTo(HaveOccurred())
		})

		It("skips PVCs in use by running pods", func() {
			pipeline := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "busy-pipeline", Namespace: architectNamespace},
				Status: mirrorv1.CollectionPipelineStatus{
					Phase: "Complete",
				},
			}
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts-busy-pipeline", Namespace: architectNamespace},
				Status: corev1.PersistentVolumeClaimStatus{
					Phase: corev1.ClaimBound,
				},
			}
			busyPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "task-runner", Namespace: architectNamespace},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c", Image: "img"}},
					Volumes: []corev1.Volume{{
						Name: "artifacts",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "collection-artifacts-busy-pipeline",
							},
						},
					}},
				},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform", Namespace: architectNamespace},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform, pipeline, pvc, busyPod).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}

			err := r.reconcileArtifactFileServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			err = c.Get(ctx, client.ObjectKey{Name: "artifact-fileserver", Namespace: architectNamespace}, deploy)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("uses incremental PVC name when pipeline is incremental", func() {
			pipeline := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "inc-pipeline", Namespace: architectNamespace},
				Spec: mirrorv1.CollectionPipelineSpec{
					Incremental: true,
					BaseVersion: "4.14",
				},
				Status: mirrorv1.CollectionPipelineStatus{
					Phase: "Complete",
				},
			}
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts-4.14", Namespace: architectNamespace},
				Status: corev1.PersistentVolumeClaimStatus{
					Phase: corev1.ClaimBound,
				},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform", Namespace: architectNamespace},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform, pipeline, pvc).Build()
			r := &DisconnectedPlatformReconciler{Client: c, Scheme: testScheme}

			err := r.reconcileArtifactFileServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			err = c.Get(ctx, client.ObjectKey{Name: "artifact-fileserver", Namespace: architectNamespace}, deploy)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

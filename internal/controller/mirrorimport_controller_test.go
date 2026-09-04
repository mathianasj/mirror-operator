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
	pipelinev1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	knativeapis "knative.dev/pkg/apis"
	knativeduckv1 "knative.dev/pkg/apis/duck/v1"
)

var _ = Describe("MirrorImportReconciler", func() {
	var (
		ctx        context.Context
		testScheme *runtime.Scheme
	)

	BeforeEach(func() {
		ctx = context.Background()
		testScheme = runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(testScheme)).To(Succeed())
		Expect(mirrorv1.AddToScheme(testScheme)).To(Succeed())
		Expect(pipelinev1.AddToScheme(testScheme)).To(Succeed())
	})

	Describe("Reconcile", func() {
		It("handles a MirrorImport that does not exist", func() {
			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "missing", Namespace: "default"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))
		})

		It("adds finalizer on first reconcile", func() {
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-import", Namespace: "default"},
				Spec: mirrorv1.MirrorImportSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Bundle: mirrorv1.BundleSource{
						PVC:      "import-pvc",
						Filename: "bundle.tar",
					},
					TargetRegistry: mirrorv1.RegistryConfig{
						URL: "https://quay.airgap.local",
					},
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(importCR).Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-import", Namespace: "default"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			updated := &mirrorv1.MirrorImport{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-import", Namespace: "default"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Finalizers).To(ContainElement(importFinalizer))
		})

		It("removes finalizer on deletion so object can be garbage collected", func() {
			now := metav1.Now()
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-import",
					Namespace:         "default",
					Finalizers:        []string{importFinalizer},
					DeletionTimestamp: &now,
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(importCR).Build(),
				Scheme: testScheme,
			}

			_, err := r.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "test-import", Namespace: "default"},
			})
			Expect(err).NotTo(HaveOccurred())

			By("object is deleted after finalizer is removed")
			updated := &mirrorv1.MirrorImport{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-import", Namespace: "default"}, updated)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("transitions from empty phase to Importing", func() {
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-import",
					Namespace:  "default",
					Finalizers: []string{importFinalizer},
				},
				Spec: mirrorv1.MirrorImportSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Bundle: mirrorv1.BundleSource{
						PVC:      "import-pvc",
						Filename: "bundle.tar",
					},
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.MirrorImport{}).
					WithObjects(importCR).
					Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-import", Namespace: "default"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			updated := &mirrorv1.MirrorImport{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-import", Namespace: "default"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.Phase).To(Equal("Importing"))
			Expect(result).To(Equal(reconcile.Result{}))
		})
	})

	Describe("startImport dependency validation", func() {
		It("fails import when collectionVersion already in platform import history", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Status: mirrorv1.DisconnectedPlatformStatus{
					ImportHistory: []mirrorv1.ImportInfo{
						{Version: "v2025.01.15.001-manual"},
					},
				},
			}
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-import",
					Namespace:  "default",
					Finalizers: []string{importFinalizer},
				},
				Spec: mirrorv1.MirrorImportSpec{
					ImageSetConfig:    "kind: ImageSetConfiguration",
					CollectionVersion: "v2025.01.15.001-manual",
					Bundle: mirrorv1.BundleSource{
						PVC:      "import-pvc",
						Filename: "bundle.tar",
					},
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.MirrorImport{}, &mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform, importCR).
					Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-import", Namespace: "default"}}
			_, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			updated := &mirrorv1.MirrorImport{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-import", Namespace: "default"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.Phase).To(Equal("Failed"))
			Expect(updated.Status.Conditions).To(ContainElement(
				HaveField("Type", Equal("DependencyCheck")),
			))
		})

		It("proceeds to Importing when collectionVersion not in platform history", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Status: mirrorv1.DisconnectedPlatformStatus{
					ImportHistory: []mirrorv1.ImportInfo{
						{Version: "v2025.01.01.001-manual"},
					},
				},
			}
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-import",
					Namespace:  "default",
					Finalizers: []string{importFinalizer},
				},
				Spec: mirrorv1.MirrorImportSpec{
					ImageSetConfig:    "kind: ImageSetConfiguration",
					CollectionVersion: "v2025.01.15.001-manual",
					Bundle: mirrorv1.BundleSource{
						PVC:      "import-pvc",
						Filename: "bundle.tar",
					},
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.MirrorImport{}, &mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform, importCR).
					Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-import", Namespace: "default"}}
			_, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			updated := &mirrorv1.MirrorImport{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-import", Namespace: "default"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.Phase).To(Equal("Importing"))
		})

		It("proceeds to Importing when collectionVersion set but no platform exists", func() {
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-import",
					Namespace:  "default",
					Finalizers: []string{importFinalizer},
				},
				Spec: mirrorv1.MirrorImportSpec{
					ImageSetConfig:    "kind: ImageSetConfiguration",
					CollectionVersion: "v2025.01.15.001-manual",
					Bundle: mirrorv1.BundleSource{
						PVC:      "import-pvc",
						Filename: "bundle.tar",
					},
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.MirrorImport{}).
					WithObjects(importCR).
					Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-import", Namespace: "default"}}
			_, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			updated := &mirrorv1.MirrorImport{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-import", Namespace: "default"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.Phase).To(Equal("Importing"))
		})
	})

	Describe("updatePlatformImportHistory", func() {
		It("updates platform import history with collectionVersion", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
			}
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-import",
					Namespace: "default",
				},
				Spec: mirrorv1.MirrorImportSpec{
					CollectionVersion: "v2025.01.15.001-manual",
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform, importCR).
					Build(),
				Scheme: testScheme,
			}

			r.updatePlatformImportHistory(ctx, importCR)

			updated := &mirrorv1.DisconnectedPlatform{}
			err := r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.ImportHistory).To(HaveLen(1))
			Expect(updated.Status.ImportHistory[0].Version).To(Equal("v2025.01.15.001-manual"))
			Expect(updated.Status.ImportHistory[0].Status).To(Equal("Complete"))
			Expect(updated.Status.LastImport).NotTo(BeNil())
			Expect(updated.Status.LastImport.Version).To(Equal("v2025.01.15.001-manual"))
		})

		It("generates version from name when collectionVersion empty", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
			}
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-import",
					Namespace: "default",
				},
				Spec: mirrorv1.MirrorImportSpec{},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform, importCR).
					Build(),
				Scheme: testScheme,
			}

			r.updatePlatformImportHistory(ctx, importCR)

			updated := &mirrorv1.DisconnectedPlatform{}
			err := r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.ImportHistory).To(HaveLen(1))
			Expect(updated.Status.ImportHistory[0].Version).To(ContainSubstring("import-test-import"))
		})

		It("appends to existing import history", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Status: mirrorv1.DisconnectedPlatformStatus{
					ImportHistory: []mirrorv1.ImportInfo{
						{Version: "v2025.01.01.001-manual"},
					},
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
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform, importCR).
					Build(),
				Scheme: testScheme,
			}

			r.updatePlatformImportHistory(ctx, importCR)

			updated := &mirrorv1.DisconnectedPlatform{}
			err := r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.ImportHistory).To(HaveLen(2))
		})

		It("does nothing when no platform exists", func() {
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-import"},
				Spec: mirrorv1.MirrorImportSpec{
					CollectionVersion: "v2025.01.15.001-manual",
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			r.updatePlatformImportHistory(ctx, importCR)
		})
	})

	Describe("buildImportPipelineRun", func() {
		It("uses custom mirror image when set", func() {
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-import", Namespace: "default"},
				Spec: mirrorv1.MirrorImportSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Bundle: mirrorv1.BundleSource{
						PVC:      "import-pvc",
						Filename: "bundle.tar",
					},
					TargetRegistry: mirrorv1.RegistryConfig{
						URL: "https://quay.airgap.local",
					},
				},
			}

			r := &MirrorImportReconciler{
				Client:      fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme:      testScheme,
				MirrorImage: "custom-mirror:latest",
			}

			pr, err := r.buildImportPipelineRun(ctx, importCR, "import-config")
			Expect(err).NotTo(HaveOccurred())
			Expect(pr.Spec.PipelineRef.Name).To(Equal("import-pipeline-template"))
			for _, p := range pr.Spec.Params {
				if p.Name == "mirror-image" {
					Expect(p.Value.StringVal).To(Equal("custom-mirror:latest"))
				}
			}
		})

		It("includes cosign verify params when public key secret is configured", func() {
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-import", Namespace: "default"},
				Spec: mirrorv1.MirrorImportSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Bundle: mirrorv1.BundleSource{
						PVC:      "import-pvc",
						Filename: "release-v4.17.tar",
					},
					TargetRegistry: mirrorv1.RegistryConfig{
						URL: "https://quay.airgap.local",
					},
					Verify: &mirrorv1.CosignVerificationConfig{
						PublicKeySecretRef: &corev1.LocalObjectReference{Name: "cosign-pub-secret"},
					},
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			pr, err := r.buildImportPipelineRun(ctx, importCR, "import-config")
			Expect(err).NotTo(HaveOccurred())

			paramMap := map[string]string{}
			for _, p := range pr.Spec.Params {
				paramMap[p.Name] = p.Value.StringVal
			}
			Expect(paramMap["verify-enabled"]).To(Equal("true"))
			Expect(paramMap["cosign-pub-secret"]).To(Equal("cosign-pub-secret"))
			Expect(paramMap["bundle-filename"]).To(Equal("release-v4.17.tar"))

			hasSecretWorkspace := false
			for _, ws := range pr.Spec.Workspaces {
				if ws.Name == "cosign-pub" && ws.Secret != nil {
					Expect(ws.Secret.SecretName).To(Equal("cosign-pub-secret"))
					hasSecretWorkspace = true
				}
			}
			Expect(hasSecretWorkspace).To(BeTrue())
		})

		It("passes bundle filename and target registry as params", func() {
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-import", Namespace: "default"},
				Spec: mirrorv1.MirrorImportSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Bundle: mirrorv1.BundleSource{
						PVC:      "import-pvc",
						Filename: "release-v4.17.tar",
					},
					TargetRegistry: mirrorv1.RegistryConfig{
						URL: "https://quay.airgap.local",
					},
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			pr, err := r.buildImportPipelineRun(ctx, importCR, "import-config")
			Expect(err).NotTo(HaveOccurred())

			paramMap := map[string]string{}
			for _, p := range pr.Spec.Params {
				paramMap[p.Name] = p.Value.StringVal
			}
			Expect(paramMap["bundle-filename"]).To(Equal("release-v4.17.tar"))
			Expect(paramMap["target-registry"]).To(Equal("https://quay.airgap.local"))
			Expect(pr.Spec.PipelineRef.Name).To(Equal("import-pipeline-template"))
		})

		It("sets owner reference to MirrorImport CR", func() {
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-import", Namespace: "default"},
				Spec: mirrorv1.MirrorImportSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Bundle: mirrorv1.BundleSource{
						PVC:      "import-pvc",
						Filename: "bundle.tar",
					},
					TargetRegistry: mirrorv1.RegistryConfig{
						URL: "https://quay.airgap.local",
					},
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			pr, err := r.buildImportPipelineRun(ctx, importCR, "import-config")
			Expect(err).NotTo(HaveOccurred())
			Expect(pr.OwnerReferences).To(HaveLen(1))
			Expect(pr.OwnerReferences[0].Kind).To(Equal("MirrorImport"))
			Expect(pr.OwnerReferences[0].Name).To(Equal("test-import"))
		})
	})

	Describe("importPipelineRunPhase", func() {
		It("returns Complete when completion time set and Succeeded is True", func() {
			now := metav1.Now()
			pr := &pipelinev1.PipelineRun{
				Status: pipelinev1.PipelineRunStatus{
					Status: knativeduckv1.Status{
						Conditions: knativeduckv1.Conditions{
							{
								Type:   knativeapis.ConditionSucceeded,
								Status: corev1.ConditionTrue,
							},
						},
					},
					PipelineRunStatusFields: pipelinev1.PipelineRunStatusFields{
						CompletionTime: &now,
					},
				},
			}
			Expect(importPipelineRunPhase(pr)).To(Equal("Complete"))
		})

		It("returns Failed when completion time set but Succeeded is False", func() {
			now := metav1.Now()
			pr := &pipelinev1.PipelineRun{
				Status: pipelinev1.PipelineRunStatus{
					Status: knativeduckv1.Status{
						Conditions: knativeduckv1.Conditions{
							{
								Type:   knativeapis.ConditionSucceeded,
								Status: corev1.ConditionFalse,
							},
						},
					},
					PipelineRunStatusFields: pipelinev1.PipelineRunStatusFields{
						CompletionTime: &now,
					},
				},
			}
			Expect(importPipelineRunPhase(pr)).To(Equal("Failed"))
		})

		It("returns Running when start time is set but no completion", func() {
			now := metav1.Now()
			pr := &pipelinev1.PipelineRun{
				Status: pipelinev1.PipelineRunStatus{
					PipelineRunStatusFields: pipelinev1.PipelineRunStatusFields{
						StartTime: &now,
					},
				},
			}
			Expect(importPipelineRunPhase(pr)).To(Equal("Running"))
		})

		It("returns Pending when nothing is set", func() {
			pr := &pipelinev1.PipelineRun{}
			Expect(importPipelineRunPhase(pr)).To(Equal("Pending"))
		})
	})

	Describe("findPlatform", func() {
		It("returns the first DisconnectedPlatform", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			found, err := r.findPlatform(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(found).NotTo(BeNil())
			Expect(found.Name).To(Equal("test-platform"))
		})

		It("returns nil when no platform exists", func() {
			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			found, err := r.findPlatform(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeNil())
		})
	})

	Describe("ensureImportConfigMap", func() {
		It("creates ConfigMap with imageSetConfig from import CR", func() {
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-import", Namespace: "default"},
				Spec: mirrorv1.MirrorImportSpec{
					ImageSetConfig: "kind: ImageSetConfiguration\napiVersion: mirror.openshift.io/v1alpha2",
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			cm, err := r.ensureImportConfigMap(ctx, importCR, "import-config-test")
			Expect(err).NotTo(HaveOccurred())
			Expect(cm).NotTo(BeNil())
			Expect(cm.Name).To(Equal("import-config-test"))
			Expect(cm.Data["imageset-config.yaml"]).To(ContainSubstring("ImageSetConfiguration"))
		})

		It("returns existing ConfigMap without creating a duplicate", func() {
			existingCM := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "import-config-test",
					Namespace: "default",
				},
				Data: map[string]string{"imageset-config.yaml": "existing-content"},
			}
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-import", Namespace: "default"},
				Spec: mirrorv1.MirrorImportSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existingCM).Build(),
				Scheme: testScheme,
			}

			cm, err := r.ensureImportConfigMap(ctx, importCR, "import-config-test")
			Expect(err).NotTo(HaveOccurred())
			Expect(cm.Data["imageset-config.yaml"]).To(Equal("existing-content"))
		})
	})

	Describe("buildImportPipelineRun workspaces", func() {
		It("has bundle-data, config, pull-secret, and cosign-pub workspaces", func() {
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-import", Namespace: "default"},
				Spec: mirrorv1.MirrorImportSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Bundle: mirrorv1.BundleSource{
						PVC:      "import-pvc",
						Filename: "bundle.tar",
					},
					TargetRegistry: mirrorv1.RegistryConfig{
						URL: "https://quay.airgap.local",
					},
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			pr, err := r.buildImportPipelineRun(ctx, importCR, "import-config")
			Expect(err).NotTo(HaveOccurred())

			wsNames := []string{}
			for _, ws := range pr.Spec.Workspaces {
				wsNames = append(wsNames, ws.Name)
			}
			Expect(wsNames).To(ContainElements("bundle-data", "config", "pull-secret", "cosign-pub"))
		})

		It("uses emptyDir for cosign-pub when verification is not enabled", func() {
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-import", Namespace: "default"},
				Spec: mirrorv1.MirrorImportSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Bundle: mirrorv1.BundleSource{
						PVC:      "import-pvc",
						Filename: "bundle.tar",
					},
					TargetRegistry: mirrorv1.RegistryConfig{
						URL: "https://quay.airgap.local",
					},
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			pr, err := r.buildImportPipelineRun(ctx, importCR, "import-config")
			Expect(err).NotTo(HaveOccurred())

			for _, ws := range pr.Spec.Workspaces {
				if ws.Name == "cosign-pub" {
					Expect(ws.EmptyDir).NotTo(BeNil())
					Expect(ws.Secret).To(BeNil())
				}
			}
		})
	})

	Describe("buildImportPipelineRun timeout", func() {
		It("sets a pipeline timeout", func() {
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-import", Namespace: "default"},
				Spec: mirrorv1.MirrorImportSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Bundle: mirrorv1.BundleSource{
						PVC:      "import-pvc",
						Filename: "bundle.tar",
					},
					TargetRegistry: mirrorv1.RegistryConfig{
						URL: "https://quay.airgap.local",
					},
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			pr, err := r.buildImportPipelineRun(ctx, importCR, "import-config")
			Expect(err).NotTo(HaveOccurred())
			Expect(pr.Spec.Timeouts).NotTo(BeNil())
		})
	})
})

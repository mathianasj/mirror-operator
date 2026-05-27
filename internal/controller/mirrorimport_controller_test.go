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
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
	batchv1 "k8s.io/api/batch/v1"
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
		Expect(batchv1.AddToScheme(testScheme)).To(Succeed())
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

		It("creates a Job when in Importing phase", func() {
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
					TargetRegistry: mirrorv1.RegistryConfig{
						URL: "https://quay.airgap.local",
					},
				},
				Status: mirrorv1.MirrorImportStatus{
					Phase: "Importing",
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(importCR).Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-import", Namespace: "default"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			job := &batchv1.Job{}
			err = r.Get(ctx, types.NamespacedName{Name: "mirror-import-test-import", Namespace: "default"}, job)
			Expect(err).NotTo(HaveOccurred())
			Expect(job.Spec.Template.Spec.Containers[0].Image).To(Equal(defaultMirrorImage))
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

	Describe("ensureCatalogSource", func() {
		It("creates a CatalogSource unstructured resource", func() {
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-import", Namespace: "default"},
				Spec: mirrorv1.MirrorImportSpec{
					TargetRegistry: mirrorv1.RegistryConfig{
						URL: "https://quay.airgap.local",
					},
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			err := r.ensureCatalogSource(ctx, importCR)
			Expect(err).NotTo(HaveOccurred())
		})

		It("succeeds when CatalogSource already exists", func() {
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-import", Namespace: "default"},
				Spec: mirrorv1.MirrorImportSpec{
					TargetRegistry: mirrorv1.RegistryConfig{
						URL: "https://quay.airgap.local",
					},
				},
			}

			existing := &unstructured.Unstructured{}
			existing.SetAPIVersion("operators.coreos.com/v1alpha1")
			existing.SetKind("CatalogSource")
			existing.SetName("mirror-catalog-test-import")
			existing.SetNamespace("openshift-marketplace")

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithObjects(existing).
					Build(),
				Scheme: testScheme,
			}

			err := r.ensureCatalogSource(ctx, importCR)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureIDMS", func() {
		It("creates an ImageDigestMirrorSet unstructured resource", func() {
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-import", Namespace: "default"},
				Spec: mirrorv1.MirrorImportSpec{
					TargetRegistry: mirrorv1.RegistryConfig{
						URL: "https://quay.airgap.local",
					},
				},
			}

			r := &MirrorImportReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			err := r.ensureIDMS(ctx, importCR)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("buildImportJob", func() {
		It("uses custom mirror image when set", func() {
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-import", Namespace: "default"},
				Spec: mirrorv1.MirrorImportSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Bundle: mirrorv1.BundleSource{
						PVC:      "import-pvc",
						Filename: "bundle.tar",
					},
				},
			}

			r := &MirrorImportReconciler{
				Client:      fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme:      testScheme,
				MirrorImage: "custom-mirror:latest",
			}

			job, err := r.buildImportJob(ctx, importCR, "import-config")
			Expect(err).NotTo(HaveOccurred())
			Expect(job.Spec.Template.Spec.Containers[0].Image).To(Equal("custom-mirror:latest"))
		})

		It("includes cosign verify when public key secret is configured", func() {
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

			job, err := r.buildImportJob(ctx, importCR, "import-config")
			Expect(err).NotTo(HaveOccurred())
			Expect(job.Spec.Template.Spec.Containers[0].Args[0]).To(ContainSubstring("cosign verify-blob"))
			Expect(job.Spec.Template.Spec.Containers[0].Args[0]).To(ContainSubstring("--key /workspace/cosign-pub/cosign.pub"))
			Expect(job.Spec.Template.Spec.Containers[0].Args[0]).To(ContainSubstring("release-v4.17.tar"))
			Expect(job.Spec.Template.Spec.Volumes).To(ContainElement(corev1.Volume{
				Name: "cosign-pub",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: "cosign-pub-secret",
					},
				},
			}))
		})

		It("includes the bundle filename and target registry in command args", func() {
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

			job, err := r.buildImportJob(ctx, importCR, "import-config")
			Expect(err).NotTo(HaveOccurred())
			Expect(job.Spec.Template.Spec.Containers[0].Args[0]).To(ContainSubstring("release-v4.17.tar"))
			Expect(job.Spec.Template.Spec.Containers[0].Args[0]).To(ContainSubstring("quay.airgap.local"))
			Expect(job.Spec.Template.Spec.Containers[0].Args[0]).To(ContainSubstring("oc-mirror"))
			Expect(job.Spec.Template.Spec.Containers[0].Args[0]).To(ContainSubstring("--from file:///workspace"))
			Expect(job.Spec.Template.Spec.Containers[0].Args[0]).To(ContainSubstring("--v2"))
		})
	})
})

package controller

import (
	"fmt"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

var testCounter atomic.Int64

func uniqueNamespace(prefix string) string {
	n := testCounter.Add(1)
	return fmt.Sprintf("%s-%d", prefix, n)
}

func createNamespace(name string) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	err := k8sClient.Create(ctx, ns)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		Expect(err).NotTo(HaveOccurred())
	}
}

var _ = Describe("Integration: CollectionPipeline", func() {

	Describe("Finalizer lifecycle", func() {
		It("adds finalizer on first reconcile", func() {
			ns := uniqueNamespace("cp-fin")
			createNamespace(ns)

			pipeline := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-finalizer",
					Namespace: ns,
				},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration\napiVersion: mirror.openshift.io/v1alpha2\nmirror:\n  platform:\n    channels:\n    - name: stable-4.18\n",
				},
			}
			Expect(k8sClient.Create(ctx, pipeline)).To(Succeed())

			r := &CollectionPipelineReconciler{
				Client:      k8sClient,
				Scheme:      scheme.Scheme,
				MirrorImage: "quay.io/test/oc-mirror:v2",
			}

			result, err := r.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "test-finalizer", Namespace: ns},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			fetched := &mirrorv1.CollectionPipeline{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "test-finalizer", Namespace: ns}, fetched)).To(Succeed())
			Expect(fetched.Finalizers).To(ContainElement("mirror.mathianasj.github.com/pipeline-finalizer"))
		})
	})

	Describe("ConfigMap creation", func() {
		It("creates a ConfigMap with the imageSetConfig on second reconcile", func() {
			ns := uniqueNamespace("cp-cm")
			createNamespace(ns)

			imageSetConfig := `kind: ImageSetConfiguration
apiVersion: mirror.openshift.io/v1alpha2
mirror:
  platform:
    channels:
    - name: stable-4.18
      minVersion: "4.18.3"
`
			pipeline := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-configmap",
					Namespace: ns,
				},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: imageSetConfig,
				},
			}
			Expect(k8sClient.Create(ctx, pipeline)).To(Succeed())

			r := &CollectionPipelineReconciler{
				Client:      k8sClient,
				Scheme:      scheme.Scheme,
				MirrorImage: "quay.io/test/oc-mirror:v2",
			}
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-configmap", Namespace: ns}}

			// First reconcile: adds finalizer
			_, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// Second reconcile: creates ConfigMap and sets status
			// May error trying to create PipelineRun (Tekton CRDs not in envtest) — that's fine
			_, _ = r.Reconcile(ctx, req)

			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      "mirror-config-test-configmap",
				Namespace: ns,
			}, cm)).To(Succeed())
			Expect(cm.Data["imageset-config.yaml"]).To(ContainSubstring("stable-4.18"))

			fetched := &mirrorv1.CollectionPipeline{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "test-configmap", Namespace: ns}, fetched)).To(Succeed())
			Expect(fetched.Status.ConfigMapRef).To(Equal("mirror-config-test-configmap"))
		})
	})

	Describe("Trigger annotation reset", func() {
		It("resets status and removes annotation when triggered on a completed pipeline", func() {
			ns := uniqueNamespace("cp-trig")
			createNamespace(ns)

			pipeline := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-trigger",
					Namespace: ns,
					Finalizers: []string{
						"mirror.mathianasj.github.com/pipeline-finalizer",
					},
					Annotations: map[string]string{
						"mirror.mathianasj.github.com/trigger": "now",
					},
				},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration\napiVersion: mirror.openshift.io/v1alpha2\nmirror:\n  platform:\n    channels:\n    - name: stable-4.18\n",
				},
			}
			Expect(k8sClient.Create(ctx, pipeline)).To(Succeed())

			// Seed status with a completed run
			pipeline.Status.PipelineRunRef = "some-old-run-12345"
			pipeline.Status.Phase = "Complete"
			pipeline.Status.Version = "v2026.01.01.001-manual"
			Expect(k8sClient.Status().Update(ctx, pipeline)).To(Succeed())

			// Also create the ConfigMap that ensureConfigMap expects
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-config-test-trigger",
					Namespace: ns,
				},
				Data: map[string]string{"imageset-config.yaml": pipeline.Spec.ImageSetConfig},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())

			r := &CollectionPipelineReconciler{
				Client:      k8sClient,
				Scheme:      scheme.Scheme,
				MirrorImage: "quay.io/test/oc-mirror:v2",
			}
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-trigger", Namespace: ns}}

			// Reconcile — should detect trigger annotation and reset status
			_, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			fetched := &mirrorv1.CollectionPipeline{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "test-trigger", Namespace: ns}, fetched)).To(Succeed())

			// Annotation should be removed
			Expect(fetched.Annotations).NotTo(HaveKey("mirror.mathianasj.github.com/trigger"))

			// Re-fetch to see status (annotation removal and status reset happen in two steps)
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "test-trigger", Namespace: ns}, fetched)).To(Succeed())
			Expect(fetched.Status.PipelineRunRef).To(Equal(""))
			Expect(fetched.Status.Phase).To(Equal(""))
			Expect(fetched.Status.Version).To(Equal(""))
		})
	})

	Describe("Idempotency", func() {
		It("creates only one ConfigMap across multiple reconciles", func() {
			ns := uniqueNamespace("cp-idem")
			createNamespace(ns)

			pipeline := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-idempotent",
					Namespace: ns,
				},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration\napiVersion: mirror.openshift.io/v1alpha2\nmirror:\n  platform:\n    channels:\n    - name: stable-4.18\n",
				},
			}
			Expect(k8sClient.Create(ctx, pipeline)).To(Succeed())

			r := &CollectionPipelineReconciler{
				Client:      k8sClient,
				Scheme:      scheme.Scheme,
				MirrorImage: "quay.io/test/oc-mirror:v2",
			}
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-idempotent", Namespace: ns}}

			// Reconcile 3 times (first adds finalizer, next two should not create duplicate ConfigMaps)
			for i := 0; i < 3; i++ {
				_, err := r.Reconcile(ctx, req)
				// Later reconciles may error on PipelineRun creation (Tekton not in envtest) — that's fine
				if err != nil {
					break
				}
			}

			cmList := &corev1.ConfigMapList{}
			Expect(k8sClient.List(ctx, cmList)).To(Succeed())
			count := 0
			for _, cm := range cmList.Items {
				if cm.Namespace == ns && cm.Name == "mirror-config-test-idempotent" {
					count++
				}
			}
			Expect(count).To(Equal(1))
		})
	})

	Describe("Deletion cleanup", func() {
		It("removes finalizer and cleans up PVC on deletion", func() {
			ns := uniqueNamespace("cp-del")
			createNamespace(ns)

			pipeline := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-delete",
					Namespace: ns,
					Finalizers: []string{
						"mirror.mathianasj.github.com/pipeline-finalizer",
					},
				},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
				},
			}
			Expect(k8sClient.Create(ctx, pipeline)).To(Succeed())

			// Create a PVC that cleanup should delete
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "collection-storage-test-delete",
					Namespace: ns,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: *mustParseQuantity("10Gi"),
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, pvc)).To(Succeed())

			// Delete the pipeline (sets DeletionTimestamp since finalizer is present)
			Expect(k8sClient.Delete(ctx, pipeline)).To(Succeed())

			r := &CollectionPipelineReconciler{
				Client:      k8sClient,
				Scheme:      scheme.Scheme,
				MirrorImage: "quay.io/test/oc-mirror:v2",
			}
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-delete", Namespace: ns}}

			// Reconcile handles deletion
			_, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// Pipeline should be gone (finalizer removed → API server deletes it)
			fetched := &mirrorv1.CollectionPipeline{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: "test-delete", Namespace: ns}, fetched)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			// PVC should be marked for deletion (may still exist due to pvc-protection finalizer in envtest)
			fetchedPVC := &corev1.PersistentVolumeClaim{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: "collection-storage-test-delete", Namespace: ns}, fetchedPVC)
			if err == nil {
				Expect(fetchedPVC.DeletionTimestamp.IsZero()).To(BeFalse(), "PVC should have DeletionTimestamp set")
			} else {
				Expect(apierrors.IsNotFound(err)).To(BeTrue())
			}
		})
	})
})

var _ = Describe("Integration: MirrorImport", func() {

	Describe("Finalizer and phase transition", func() {
		It("adds finalizer on first reconcile and transitions to Importing on second", func() {
			ns := uniqueNamespace("mi-fin")
			createNamespace(ns)

			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-import",
					Namespace: ns,
				},
				Spec: mirrorv1.MirrorImportSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Bundle: mirrorv1.BundleSource{
						PVC:      "import-pvc",
						Filename: "bundle.tar",
					},
					TargetRegistry: mirrorv1.RegistryConfig{
						URL: "registry.example.com:5000",
					},
				},
			}
			Expect(k8sClient.Create(ctx, importCR)).To(Succeed())

			r := &MirrorImportReconciler{
				Client: k8sClient,
				Scheme: scheme.Scheme,
			}
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-import", Namespace: ns}}

			// First reconcile: adds finalizer
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			fetched := &mirrorv1.MirrorImport{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "test-import", Namespace: ns}, fetched)).To(Succeed())
			Expect(fetched.Finalizers).To(ContainElement("mirror.mathianasj.github.com/import-finalizer"))

			// Second reconcile: status transitions to Importing
			result, err = r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "test-import", Namespace: ns}, fetched)).To(Succeed())
			Expect(fetched.Status.Phase).To(Equal("Importing"))
		})
	})

	Describe("Duplicate version detection", func() {
		It("fails the import when version was already imported", func() {
			ns := uniqueNamespace("mi-dup")
			createNamespace(ns)

			// Create a platform with the version already in import history
			platformName := uniqueNamespace("platform-dup")
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name: platformName,
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
				},
			}
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer func() {
				p := &mirrorv1.DisconnectedPlatform{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: platformName}, p); err == nil {
					p.SetFinalizers(nil)
					_ = k8sClient.Update(ctx, p)
					_ = k8sClient.Delete(ctx, p)
				}
			}()

			platform.Status.ImportHistory = []mirrorv1.ImportInfo{
				{Version: "v1.0", Status: "Complete", Timestamp: metav1.Now()},
			}
			Expect(k8sClient.Status().Update(ctx, platform)).To(Succeed())

			// Create a MirrorImport referencing that same version
			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-dup-import",
					Namespace: ns,
					Finalizers: []string{
						"mirror.mathianasj.github.com/import-finalizer",
					},
				},
				Spec: mirrorv1.MirrorImportSpec{
					ImageSetConfig:    "kind: ImageSetConfiguration",
					CollectionVersion: "v1.0",
					Bundle: mirrorv1.BundleSource{
						PVC:      "import-pvc",
						Filename: "bundle.tar",
					},
					TargetRegistry: mirrorv1.RegistryConfig{
						URL: "registry.example.com:5000",
					},
				},
			}
			Expect(k8sClient.Create(ctx, importCR)).To(Succeed())

			r := &MirrorImportReconciler{
				Client: k8sClient,
				Scheme: scheme.Scheme,
			}
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-dup-import", Namespace: ns}}

			// Reconcile — startImport should detect duplicate
			_, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			fetched := &mirrorv1.MirrorImport{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "test-dup-import", Namespace: ns}, fetched)).To(Succeed())
			Expect(fetched.Status.Phase).To(Equal("Failed"))

			found := false
			for _, c := range fetched.Status.Conditions {
				if c.Reason == "VersionAlreadyImported" {
					found = true
					Expect(c.Message).To(ContainSubstring("v1.0"))
				}
			}
			Expect(found).To(BeTrue(), "expected VersionAlreadyImported condition")
		})
	})

	Describe("Deletion cleanup", func() {
		It("removes finalizer on deletion", func() {
			ns := uniqueNamespace("mi-del")
			createNamespace(ns)

			importCR := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-delete-import",
					Namespace: ns,
					Finalizers: []string{
						"mirror.mathianasj.github.com/import-finalizer",
					},
				},
				Spec: mirrorv1.MirrorImportSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Bundle: mirrorv1.BundleSource{
						PVC:      "import-pvc",
						Filename: "bundle.tar",
					},
					TargetRegistry: mirrorv1.RegistryConfig{
						URL: "registry.example.com:5000",
					},
				},
			}
			Expect(k8sClient.Create(ctx, importCR)).To(Succeed())

			// Delete (sets DeletionTimestamp since finalizer is present)
			Expect(k8sClient.Delete(ctx, importCR)).To(Succeed())

			r := &MirrorImportReconciler{
				Client: k8sClient,
				Scheme: scheme.Scheme,
			}
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-delete-import", Namespace: ns}}

			// Reconcile handles deletion
			_, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// Resource should be gone (finalizer removed → API server deletes it)
			fetched := &mirrorv1.MirrorImport{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: "test-delete-import", Namespace: ns}, fetched)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	Describe("Not-found resource", func() {
		It("returns no error for a non-existent MirrorImport", func() {
			r := &MirrorImportReconciler{
				Client: k8sClient,
				Scheme: scheme.Scheme,
			}
			result, err := r.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "does-not-exist", Namespace: "default"},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))
		})
	})
})

func mustParseQuantity(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}

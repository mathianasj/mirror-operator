package controller

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
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

var _ = Describe("CollectionPipelineReconciler", func() {
	var (
		ctx        context.Context
		pipeline   *mirrorv1.CollectionPipeline
		testScheme *runtime.Scheme
	)

	BeforeEach(func() {
		ctx = context.Background()
		testScheme = runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(testScheme)).To(Succeed())
		Expect(mirrorv1.AddToScheme(testScheme)).To(Succeed())
		Expect(pipelinev1.AddToScheme(testScheme)).To(Succeed())
	})

	Describe("generateVersion", func() {
		It("generates version with correct format for manual trigger", func() {
			v := generateVersion(mirrorv1.TriggerTypeManual)
			Expect(v).To(MatchRegexp(`^v\d{4}\.\d{2}\.\d{2}\.001-manual$`))
		})

		It("generates version with correct format for scheduled trigger", func() {
			v := generateVersion(mirrorv1.TriggerTypeScheduled)
			Expect(v).To(MatchRegexp(`^v\d{4}\.\d{2}\.\d{2}\.001-scheduled$`))
		})

		It("defaults to manual when trigger type is empty", func() {
			v := generateVersion("")
			Expect(v).To(MatchRegexp(`^v\d{4}\.\d{2}\.\d{2}\.001-manual$`))
		})
	})

	Describe("versionExists", func() {
		It("returns true when version is found in history", func() {
			history := []mirrorv1.ImportInfo{
				{Version: "v2025.01.01.001-manual"},
				{Version: "v2025.01.02.001-scheduled"},
			}
			Expect(versionExists(history, "v2025.01.02.001-scheduled")).To(BeTrue())
		})

		It("returns false when version is not in history", func() {
			history := []mirrorv1.ImportInfo{
				{Version: "v2025.01.01.001-manual"},
			}
			Expect(versionExists(history, "v2025.01.03.001-scheduled")).To(BeFalse())
		})

		It("returns false on empty history", func() {
			Expect(versionExists(nil, "v2025.01.01.001-manual")).To(BeFalse())
		})
	})

	Describe("ensureConfigMap", func() {
		It("creates a ConfigMap from the spec", func() {
			pipeline = &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pipeline",
					Namespace: "default",
				},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration\napiVersion: mirror.openshift.io/v1alpha2",
				},
			}

			r := &CollectionPipelineReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			cm, err := r.ensureConfigMap(ctx, pipeline, "mirror-config-test-pipeline")
			Expect(err).NotTo(HaveOccurred())
			Expect(cm).NotTo(BeNil())
			Expect(cm.Data["imageset-config.yaml"]).To(ContainSubstring("ImageSetConfiguration"))
		})

		It("returns existing ConfigMap without creating a duplicate", func() {
			pipeline = &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pipeline",
					Namespace: "default",
				},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
				},
			}

			existingCM := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-config-test-pipeline",
					Namespace: "default",
				},
				Data: map[string]string{"imageset-config.yaml": "existing"},
			}

			r := &CollectionPipelineReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existingCM).Build(),
			}

			cm, err := r.ensureConfigMap(ctx, pipeline, "mirror-config-test-pipeline")
			Expect(err).NotTo(HaveOccurred())
			Expect(cm.Data["imageset-config.yaml"]).To(Equal("existing"))
		})
	})

	Describe("buildPipelineRun", func() {
		var (
			cm *corev1.ConfigMap
			r  *CollectionPipelineReconciler
		)

		BeforeEach(func() {
			cm = &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-config-test", Namespace: "default"},
			}
			r = &CollectionPipelineReconciler{
				Client:      fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme:      testScheme,
				MirrorImage: "custom-oc-mirror:latest",
			}
		})

		It("creates a PipelineRun referencing the template with correct params", func() {
			pipeline = &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pipeline", Namespace: "default"},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Storage: mirrorv1.ArtifactOutput{
						Output: &mirrorv1.BundleOutput{
							PVC:      "my-pvc",
							Filename: "output.tar",
						},
					},
				},
			}

			pr, err := r.buildPipelineRun(ctx, pipeline, cm)
			Expect(err).NotTo(HaveOccurred())
			Expect(pr.Spec.PipelineRef).NotTo(BeNil())
			Expect(pr.Spec.PipelineRef.Name).To(Equal("collection-pipeline-template"))

			paramMap := make(map[string]string)
			for _, p := range pr.Spec.Params {
				paramMap[p.Name] = p.Value.StringVal
			}
			Expect(paramMap["config-map-name"]).To(Equal(cm.Name))
			Expect(paramMap["mirror-image"]).To(Equal("custom-oc-mirror:latest"))
			Expect(paramMap["working-pvc-name"]).To(Equal("collection-storage-test-pipeline"))
		})

		It("adds cosign-key workspace when signing config is set", func() {
			pipeline = &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pipeline", Namespace: "default"},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Storage: mirrorv1.ArtifactOutput{
						Output: &mirrorv1.BundleOutput{
							PVC: "my-pvc",
						},
					},
					Signing: &mirrorv1.CosignSigningConfig{
						KeySecretRef: &corev1.LocalObjectReference{Name: "cosign-key-secret"},
					},
				},
			}

			pr, err := r.buildPipelineRun(ctx, pipeline, cm)
			Expect(err).NotTo(HaveOccurred())
			Expect(pr.Spec.Workspaces).To(ContainElement(pipelinev1.WorkspaceBinding{
				Name: "cosign-key",
				Secret: &corev1.SecretVolumeSource{
					SecretName: "cosign-key-secret",
				},
			}))
		})

		It("adds cosign-key workspace when key secret ref is set with password", func() {
			pipeline = &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pipeline", Namespace: "default"},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Storage: mirrorv1.ArtifactOutput{
						Output: &mirrorv1.BundleOutput{
							PVC: "my-pvc",
						},
					},
					Signing: &mirrorv1.CosignSigningConfig{
						KeySecretRef:      &corev1.LocalObjectReference{Name: "cosign-key-secret"},
						PasswordSecretRef: &corev1.LocalObjectReference{Name: "cosign-pass-secret"},
					},
				},
			}

			pr, err := r.buildPipelineRun(ctx, pipeline, cm)
			Expect(err).NotTo(HaveOccurred())
			Expect(pr.Spec.PipelineRef).NotTo(BeNil())
			Expect(pr.Spec.Workspaces).To(ContainElement(pipelinev1.WorkspaceBinding{
				Name: "cosign-key",
				Secret: &corev1.SecretVolumeSource{
					SecretName: "cosign-key-secret",
				},
			}))
		})

		It("sets S3 params when OBC ConfigMap exists", func() {
			obcConfigMap := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts", Namespace: "default"},
				Data: map[string]string{
					"BUCKET_NAME":   "my-bucket",
					"BUCKET_HOST":   "https://s3.example.com",
					"BUCKET_REGION": "us-east-1",
				},
			}
			r.Client = fake.NewClientBuilder().WithScheme(testScheme).WithObjects(obcConfigMap).Build()

			pipeline = &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pipeline", Namespace: "default"},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
				},
			}

			pr, err := r.buildPipelineRun(ctx, pipeline, cm)
			Expect(err).NotTo(HaveOccurred())

			paramMap := make(map[string]string)
			for _, p := range pr.Spec.Params {
				paramMap[p.Name] = p.Value.StringVal
			}
			Expect(paramMap["has-s3"]).To(Equal("true"))
			Expect(paramMap["s3-bucket"]).To(Equal("my-bucket"))
			Expect(paramMap["s3-region"]).To(Equal("us-east-1"))
			Expect(paramMap["s3-endpoint"]).To(Equal("https://s3.example.com"))
			Expect(paramMap["s3-secret-name"]).To(Equal("collection-artifacts"))
		})

		It("uses default image when MirrorImage is empty", func() {
			r.MirrorImage = ""
			pipeline = &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pipeline", Namespace: "default"},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
				},
			}

			pr, err := r.buildPipelineRun(ctx, pipeline, cm)
			Expect(err).NotTo(HaveOccurred())

			paramMap := make(map[string]string)
			for _, p := range pr.Spec.Params {
				paramMap[p.Name] = p.Value.StringVal
			}
			Expect(paramMap["mirror-image"]).To(Equal(defaultMirrorImage))
		})
	})

	Describe("Reconcile", func() {
		It("handles a CollectionPipeline that does not exist", func() {
			r := &CollectionPipelineReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "missing", Namespace: "default"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))
		})

		It("adds finalizer on first reconcile", func() {
			pipeline = &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pipeline", Namespace: "default"},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Storage: mirrorv1.ArtifactOutput{
						Output: &mirrorv1.BundleOutput{
							PVC: "test-pvc",
						},
					},
				},
			}

			r := &CollectionPipelineReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pipeline).Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-pipeline", Namespace: "default"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			updated := &mirrorv1.CollectionPipeline{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-pipeline", Namespace: "default"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Finalizers).To(ContainElement(pipelineFinalizer))
		})

		It("removes finalizer on deletion so object can be garbage collected", func() {
			now := metav1.Now()
			pipeline = &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-pipeline",
					Namespace:         "default",
					Finalizers:        []string{pipelineFinalizer},
					DeletionTimestamp: &now,
				},
			}

			r := &CollectionPipelineReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pipeline).Build(),
				Scheme: testScheme,
			}

			_, err := r.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "test-pipeline", Namespace: "default"},
			})
			Expect(err).NotTo(HaveOccurred())

			By("object is deleted after finalizer is removed")
			updated := &mirrorv1.CollectionPipeline{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-pipeline", Namespace: "default"}, updated)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("fails incremental pipeline when baseVersion not in platform import history", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
			}
			pipeline = &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-pipeline",
					Namespace:  "default",
					Finalizers: []string{pipelineFinalizer},
				},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Storage: mirrorv1.ArtifactOutput{
						Output: &mirrorv1.BundleOutput{PVC: "test-pvc"},
					},
					Incremental: true,
					BaseVersion: "v2025.01.01.001-manual",
				},
			}

			r := &CollectionPipelineReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}, &mirrorv1.CollectionPipeline{}).
					WithObjects(platform, pipeline).
					Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-pipeline", Namespace: "default"}}
			_, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			updated := &mirrorv1.CollectionPipeline{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-pipeline", Namespace: "default"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.Phase).To(Equal("Failed"))
			Expect(updated.Status.Conditions).To(ContainElement(
				HaveField("Type", Equal("DependencyCheck")),
			))
			Expect(updated.Status.CompletionTime).NotTo(BeNil())
		})

		It("proceeds when incremental but baseVersion exists in platform import history", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Status: mirrorv1.DisconnectedPlatformStatus{
					ImportHistory: []mirrorv1.ImportInfo{
						{Version: "v2025.01.01.001-manual"},
					},
				},
			}
			basePVC := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "collection-storage-v2025.01.01.001-manual",
					Namespace: "default",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("100Gi"),
						},
					},
				},
			}
			pipeline = &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-pipeline",
					Namespace:  "default",
					Finalizers: []string{pipelineFinalizer},
				},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Storage: mirrorv1.ArtifactOutput{
						Output: &mirrorv1.BundleOutput{PVC: "test-pvc"},
					},
					Incremental: true,
					BaseVersion: "v2025.01.01.001-manual",
				},
			}

			r := &CollectionPipelineReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(
						&mirrorv1.DisconnectedPlatform{},
						&mirrorv1.CollectionPipeline{},
					).
					WithObjects(platform, pipeline, basePVC).
					Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-pipeline", Namespace: "default"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			updated := &mirrorv1.CollectionPipeline{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-pipeline", Namespace: "default"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.Phase).NotTo(Equal("Failed"))
		})

		It("proceeds when incremental but no platform exists", func() {
			basePVC := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "collection-storage-v2025.01.01.001-manual",
					Namespace: "default",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("100Gi"),
						},
					},
				},
			}
			pipeline = &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-pipeline",
					Namespace:  "default",
					Finalizers: []string{pipelineFinalizer},
				},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Storage: mirrorv1.ArtifactOutput{
						Output: &mirrorv1.BundleOutput{PVC: "test-pvc"},
					},
					Incremental: true,
					BaseVersion: "v2025.01.01.001-manual",
				},
			}

			r := &CollectionPipelineReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.CollectionPipeline{}).
					WithObjects(pipeline, basePVC).
					Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-pipeline", Namespace: "default"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			updated := &mirrorv1.CollectionPipeline{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-pipeline", Namespace: "default"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.Phase).NotTo(Equal("Failed"))
		})

		It("sets version on first reconcile", func() {
			pipeline = &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-pipeline",
					Namespace:  "default",
					Finalizers: []string{pipelineFinalizer},
				},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
					Storage: mirrorv1.ArtifactOutput{
						Output: &mirrorv1.BundleOutput{PVC: "test-pvc"},
					},
				},
			}

			r := &CollectionPipelineReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.CollectionPipeline{}).
					WithObjects(pipeline).
					Build(),
				Scheme: testScheme,
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-pipeline", Namespace: "default"}}
			result, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			updated := &mirrorv1.CollectionPipeline{}
			err = r.Get(ctx, types.NamespacedName{Name: "test-pipeline", Namespace: "default"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.Version).To(MatchRegexp(`^v\d{4}\.\d{2}\.\d{2}\.001-manual$`))
		})
	})

	Describe("updatePlatformCollectionHistory", func() {
		It("updates DisconnectedPlatform with collection info", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Finalizers: []string{platformFinalizer},
				},
			}
			pipeline = &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pipeline", Namespace: "default"},
				Status: mirrorv1.CollectionPipelineStatus{
					Version: "v2025.01.15.001-manual",
				},
			}

			r := &CollectionPipelineReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform, pipeline).
					Build(),
				Scheme: testScheme,
			}

			r.updatePlatformCollectionHistory(ctx, pipeline)

			updated := &mirrorv1.DisconnectedPlatform{}
			err := r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.CollectionHistory).To(HaveLen(1))
			Expect(updated.Status.CollectionHistory[0].Version).To(Equal("v2025.01.15.001-manual"))
			Expect(updated.Status.CollectionHistory[0].Status).To(Equal("Complete"))
			Expect(updated.Status.LastCollection).NotTo(BeNil())
			Expect(updated.Status.LastCollection.Version).To(Equal("v2025.01.15.001-manual"))
		})

		It("appends to existing collection history", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-platform",
					Finalizers: []string{platformFinalizer},
				},
				Status: mirrorv1.DisconnectedPlatformStatus{
					CollectionHistory: []mirrorv1.CollectionInfo{
						{Version: "v2025.01.01.001-manual"},
					},
				},
			}
			pipeline = &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pipeline", Namespace: "default"},
				Status: mirrorv1.CollectionPipelineStatus{
					Version: "v2025.01.15.001-manual",
				},
			}

			r := &CollectionPipelineReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(testScheme).
					WithStatusSubresource(&mirrorv1.DisconnectedPlatform{}).
					WithObjects(platform, pipeline).
					Build(),
				Scheme: testScheme,
			}

			r.updatePlatformCollectionHistory(ctx, pipeline)

			updated := &mirrorv1.DisconnectedPlatform{}
			err := r.Get(ctx, types.NamespacedName{Name: "test-platform"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status.CollectionHistory).To(HaveLen(2))
			Expect(updated.Status.LastCollection.Version).To(Equal("v2025.01.15.001-manual"))
		})

		It("does nothing when no platform exists", func() {
			pipeline = &mirrorv1.CollectionPipeline{
				Status: mirrorv1.CollectionPipelineStatus{Version: "v2025.01.15.001-manual"},
			}

			r := &CollectionPipelineReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			// Should not panic or error
			r.updatePlatformCollectionHistory(ctx, pipeline)
		})
	})

	Describe("collectionPipelineRunPhase", func() {
		It("returns Complete when completion time is set and Succeeded is True", func() {
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
			Expect(collectionPipelineRunPhase(pr)).To(Equal("Complete"))
		})

		It("returns Failed when completion time is set but Succeeded is not True", func() {
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
			Expect(collectionPipelineRunPhase(pr)).To(Equal("Failed"))
		})

		It("returns Collecting when start time is set but no completion", func() {
			now := metav1.Now()
			pr := &pipelinev1.PipelineRun{
				Status: pipelinev1.PipelineRunStatus{
					PipelineRunStatusFields: pipelinev1.PipelineRunStatusFields{
						StartTime: &now,
					},
				},
			}
			Expect(collectionPipelineRunPhase(pr)).To(Equal("Collecting"))
		})

		It("returns Pending when no start or completion time", func() {
			pr := &pipelinev1.PipelineRun{}
			Expect(collectionPipelineRunPhase(pr)).To(Equal("Pending"))
		})
	})

	Describe("getStorageSize", func() {
		It("returns spec storage size when set", func() {
			size := resource.MustParse("200Gi")
			pipeline = &mirrorv1.CollectionPipeline{
				Spec: mirrorv1.CollectionPipelineSpec{
					StorageSize: &size,
				},
			}
			result := getStorageSize(pipeline)
			Expect(result.String()).To(Equal("200Gi"))
		})

		It("returns default 100Gi when not specified", func() {
			pipeline = &mirrorv1.CollectionPipeline{}
			result := getStorageSize(pipeline)
			Expect(result.String()).To(Equal("100Gi"))
		})
	})

	Describe("normalizeImageRef", func() {
		It("adds docker.io prefix to short-form refs", func() {
			Expect(normalizeImageRef("amazon/aws-cli:latest")).To(Equal("docker.io/amazon/aws-cli:latest"))
		})

		It("adds docker.io prefix to library images", func() {
			Expect(normalizeImageRef("library/nginx")).To(Equal("docker.io/library/nginx"))
		})

		It("preserves fully-qualified refs", func() {
			Expect(normalizeImageRef("quay.io/myorg/myimage:v1")).To(Equal("quay.io/myorg/myimage:v1"))
		})

		It("preserves registry.redhat.io refs", func() {
			Expect(normalizeImageRef("registry.redhat.io/redhat/ubi9:latest")).To(Equal("registry.redhat.io/redhat/ubi9:latest"))
		})

		It("preserves localhost refs", func() {
			Expect(normalizeImageRef("localhost/myimage:v1")).To(Equal("localhost/myimage:v1"))
		})

		It("preserves refs with port numbers", func() {
			Expect(normalizeImageRef("myregistry:5000/myimage:v1")).To(Equal("myregistry:5000/myimage:v1"))
		})
	})

	Describe("rewriteImageReference", func() {
		It("rewrites registry.redhat.io to intermediate", func() {
			result := rewriteImageReference("registry.redhat.io/redhat/ubi9:latest", "quay.apps.example.com/mirror")
			Expect(result).To(Equal("quay.apps.example.com/mirror/ubi9:latest"))
		})

		It("rewrites quay.io refs", func() {
			result := rewriteImageReference("quay.io/openshift/origin-cli:v4.18", "quay.apps.example.com/mirror")
			Expect(result).To(Equal("quay.apps.example.com/mirror/origin-cli:v4.18"))
		})

		It("strips docker:// prefix", func() {
			result := rewriteImageReference("docker://registry.redhat.io/redhat/ubi9:latest", "quay.apps.example.com/mirror")
			Expect(result).To(Equal("quay.apps.example.com/mirror/ubi9:latest"))
		})

		It("preserves wildcard patterns", func() {
			result := rewriteImageReference("quay.io/my-org/*", "intermediate.local/mirror")
			Expect(result).To(Equal("intermediate.local/mirror/my-org/*"))
		})
	})

	Describe("injectDefaultArchitecture", func() {
		It("injects amd64 when no architectures specified", func() {
			config := `kind: ImageSetConfiguration
apiVersion: mirror.openshift.io/v1alpha2
mirror:
  platform:
    channels:
    - name: stable-4.17`

			result := injectDefaultArchitecture(config)
			Expect(result).To(ContainSubstring("amd64"))
		})

		It("does not modify when architectures already set", func() {
			config := `kind: ImageSetConfiguration
apiVersion: mirror.openshift.io/v1alpha2
mirror:
  platform:
    architectures:
    - arm64
    channels:
    - name: stable-4.17`

			result := injectDefaultArchitecture(config)
			Expect(result).To(ContainSubstring("arm64"))
			Expect(result).NotTo(ContainSubstring("amd64"))
		})

		It("returns original when no platform section", func() {
			config := `kind: ImageSetConfiguration
mirror:
  operators:
  - catalog: registry.redhat.io/redhat/redhat-operator-index:v4.17`

			result := injectDefaultArchitecture(config)
			Expect(result).NotTo(ContainSubstring("amd64"))
		})

		It("returns original for invalid YAML", func() {
			config := "not: valid: yaml: {["
			result := injectDefaultArchitecture(config)
			Expect(result).To(Equal(config))
		})
	})

	Describe("hasChildPipelines", func() {
		It("returns false when no other pipelines reference this one", func() {
			pipeline = &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "parent-pipeline", Namespace: "default"},
			}

			r := &CollectionPipelineReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pipeline).Build(),
				Scheme: testScheme,
			}

			has, err := r.hasChildPipelines(ctx, pipeline)
			Expect(err).NotTo(HaveOccurred())
			Expect(has).To(BeFalse())
		})

		It("returns true when a child pipeline references this one as parent", func() {
			pipeline = &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "parent-pipeline", Namespace: "default"},
			}
			child := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "child-pipeline", Namespace: "default"},
				Spec: mirrorv1.CollectionPipelineSpec{
					ParentPipeline: "parent-pipeline",
				},
			}

			r := &CollectionPipelineReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pipeline, child).Build(),
				Scheme: testScheme,
			}

			has, err := r.hasChildPipelines(ctx, pipeline)
			Expect(err).NotTo(HaveOccurred())
			Expect(has).To(BeTrue())
		})
	})

	Describe("findPlatform", func() {
		It("returns the first DisconnectedPlatform", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
			}

			r := &CollectionPipelineReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			found, err := r.findPlatform(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(found).NotTo(BeNil())
			Expect(found.Name).To(Equal("test-platform"))
		})

		It("returns nil when no platform exists", func() {
			r := &CollectionPipelineReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			found, err := r.findPlatform(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeNil())
		})
	})

	Describe("getMirrorImage", func() {
		It("returns custom image when set", func() {
			r := &CollectionPipelineReconciler{
				MirrorImage: "custom-mirror:v2",
			}
			Expect(r.getMirrorImage()).To(Equal("custom-mirror:v2"))
		})

		It("returns default image when not set", func() {
			r := &CollectionPipelineReconciler{}
			Expect(r.getMirrorImage()).To(Equal(defaultMirrorImage))
		})
	})

	Describe("deleteWorkingPVC", func() {
		It("deletes the PVC", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "collection-storage-test-pipeline",
					Namespace: "default",
				},
			}
			pipeline = &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pipeline", Namespace: "default"},
				Status: mirrorv1.CollectionPipelineStatus{
					WorkingPVCName: "collection-storage-test-pipeline",
				},
			}

			r := &CollectionPipelineReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pvc).Build(),
				Scheme: testScheme,
			}

			r.deleteWorkingPVC(ctx, pipeline)

			existing := &corev1.PersistentVolumeClaim{}
			err := r.Get(ctx, types.NamespacedName{Name: "collection-storage-test-pipeline", Namespace: "default"}, existing)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	// Suppress unused import of time
	Describe("time import", func() {
		It("time package is usable", func() {
			Expect(time.Now()).NotTo(BeZero())
		})
	})
})

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
	pipelinev1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	batchv1 "k8s.io/api/batch/v1"
)

const (
	pipelineFinalizer  = "mirror.mathianasj.github.com/pipeline-finalizer"
	defaultMirrorImage = "quay.io/mathianasj/oc-mirror:v2"
	configMapKey       = "imageset-config.yaml"
)

type CollectionPipelineReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	MirrorImage string
	ClientSet   kubernetes.Interface
}

// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=collectionpipelines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=collectionpipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=collectionpipelines/finalizers,verbs=update
// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=disconnectedplatforms,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=disconnectedplatforms/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=tekton.dev,resources=pipelineruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tekton.dev,resources=pipelineruns/status,verbs=get
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch

func (r *CollectionPipelineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	pipeline := &mirrorv1.CollectionPipeline{}
	if err := r.Get(ctx, req.NamespacedName, pipeline); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !pipeline.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.cleanup(ctx, pipeline)
	}

	if !containsString(pipeline.GetFinalizers(), pipelineFinalizer) {
		pipeline.SetFinalizers(append(pipeline.GetFinalizers(), pipelineFinalizer))
		return ctrl.Result{}, r.Update(ctx, pipeline)
	}

	configName := fmt.Sprintf("mirror-config-%s", pipeline.Name)
	cm, err := r.ensureConfigMap(ctx, pipeline, configName)
	if err != nil {
		logger.Error(err, "failed to ensure ConfigMap")
		return ctrl.Result{}, err
	}

	if pipeline.Status.ConfigMapRef != configName {
		pipeline.Status.ConfigMapRef = configName
		if err := r.Status().Update(ctx, pipeline); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Check for trigger annotation to start a new collection
	if triggerValue, exists := pipeline.Annotations["mirror.mathianasj.github.com/trigger"]; exists {
		// If there's a completed/failed run, reset status to trigger new run
		if pipeline.Status.PipelineRunRef != "" && (pipeline.Status.Phase == "Succeeded" || pipeline.Status.Phase == "Complete" || pipeline.Status.Phase == "Failed" || pipeline.Status.Phase == "Stale") {
			logger.Info("Trigger annotation detected, starting new collection", "trigger", triggerValue, "previous-run", pipeline.Status.PipelineRunRef)

			// Remove trigger annotation to prevent continuous re-triggering
			delete(pipeline.Annotations, "mirror.mathianasj.github.com/trigger")
			if err := r.Update(ctx, pipeline); err != nil {
				return ctrl.Result{}, err
			}

			// Reset status to allow new run creation
			pipeline.Status.PipelineRunRef = ""
			pipeline.Status.Version = ""
			pipeline.Status.Phase = ""
			pipeline.Status.StartTime = nil
			pipeline.Status.CompletionTime = nil
			return ctrl.Result{}, r.Status().Update(ctx, pipeline)
		}
	}

	// Expand PVC if storageSize increased, regardless of pipeline state
	if pipeline.Spec.StorageSize != nil {
		pvcName := pipeline.Status.WorkingPVCName
		if pvcName == "" {
			if pipeline.Spec.ParentPipeline != "" {
				pvcName = fmt.Sprintf("collection-storage-%s", pipeline.Spec.ParentPipeline)
			} else if pipeline.Spec.Incremental && pipeline.Spec.BaseVersion != "" {
				pvcName = fmt.Sprintf("collection-storage-%s", pipeline.Spec.BaseVersion)
			} else {
				pvcName = fmt.Sprintf("collection-storage-%s", pipeline.Name)
			}
		}
		if pvcName != "" {
			existingPVC := &corev1.PersistentVolumeClaim{}
			if err := r.Get(ctx, types.NamespacedName{Namespace: pipeline.Namespace, Name: pvcName}, existingPVC); err == nil {
				desiredSize := *pipeline.Spec.StorageSize
				currentSize := existingPVC.Spec.Resources.Requests[corev1.ResourceStorage]
				if desiredSize.Cmp(currentSize) > 0 {
					logger.Info("Expanding working PVC", "pvc", pvcName, "from", currentSize.String(), "to", desiredSize.String())
					existingPVC.Spec.Resources.Requests[corev1.ResourceStorage] = desiredSize
					if err := r.Update(ctx, existingPVC); err != nil {
						logger.Error(err, "failed to expand PVC", "pvc", pvcName)
						return ctrl.Result{}, err
					}
				}
			}
		}
	}

	if pipeline.Status.PipelineRunRef != "" {
		// Check if signing config has changed since PipelineRun was created
		// If it has and the run hasn't started yet, recreate it
		pr := &pipelinev1.PipelineRun{}
		if err := r.Get(ctx, types.NamespacedName{Name: pipeline.Status.PipelineRunRef, Namespace: pipeline.Namespace}, pr); err == nil {
			// Check if PipelineRun is still pending (not started)
			if pr.Status.StartTime == nil {
				// Check if signing config in spec matches what was used to create PipelineRun
				signingConfigChanged := false

				// Simple check: if pipeline has keyless signing but PipelineRun doesn't have Fulcio env vars
				if pipeline.Spec.Signing != nil && pipeline.Spec.Signing.Keyless != nil {
					// Find the cosign-sign task (it should be named "cosign-sign")
					var signTask *pipelinev1.PipelineTask
					for i := range pr.Spec.PipelineSpec.Tasks {
						if pr.Spec.PipelineSpec.Tasks[i].Name == "cosign-sign" {
							signTask = &pr.Spec.PipelineSpec.Tasks[i]
							break
						}
					}

					if signTask != nil {
						hasFulcioURL := false
						if signTask.TaskSpec != nil && len(signTask.TaskSpec.Steps) > 0 {
							for _, env := range signTask.TaskSpec.Steps[0].Env {
								if env.Name == "FULCIO_URL" {
									hasFulcioURL = true
									break
								}
							}
						}
						if !hasFulcioURL {
							signingConfigChanged = true
							logger.Info("Signing config changed to keyless, recreating PipelineRun", "pipelineRun", pr.Name)
						}
					}
				}

				if signingConfigChanged {
					// Delete the PipelineRun and reset status to trigger recreation
					if err := r.Delete(ctx, pr); err != nil {
						logger.Error(err, "failed to delete outdated PipelineRun")
						return ctrl.Result{}, err
					}
					pipeline.Status.PipelineRunRef = ""
					pipeline.Status.Phase = ""
					return ctrl.Result{}, r.Status().Update(ctx, pipeline)
				}
			}
		}
		return r.trackPipelineRun(ctx, pipeline, req)
	}

	// Validate parent pipeline if specified
	if pipeline.Spec.ParentPipeline != "" {
		parentPipeline := &mirrorv1.CollectionPipeline{}
		if err := r.Get(ctx, types.NamespacedName{Name: pipeline.Spec.ParentPipeline, Namespace: pipeline.Namespace}, parentPipeline); err != nil {
			if apierrors.IsNotFound(err) {
				pipeline.Status.Phase = "Failed"
				now := metav1.Now()
				pipeline.Status.CompletionTime = &now
				pipeline.Status.Conditions = []metav1.Condition{
					{
						Type:               "ParentPipelineValid",
						Status:             metav1.ConditionFalse,
						Reason:             "ParentNotFound",
						Message:            fmt.Sprintf("parent pipeline %s not found", pipeline.Spec.ParentPipeline),
						LastTransitionTime: now,
					},
				}
				return ctrl.Result{}, r.Status().Update(ctx, pipeline)
			}
			logger.Error(err, "failed to get parent pipeline")
			return ctrl.Result{}, err
		}

		// Validate parent is completed
		if parentPipeline.Status.Phase != string(mirrorv1.CollectionPhaseComplete) {
			logger.Info("Waiting for parent pipeline to complete", "parent", pipeline.Spec.ParentPipeline, "parentPhase", parentPipeline.Status.Phase)
			// Requeue after a delay to check again
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}

		// Capture parent version in status if not already set
		if pipeline.Status.ParentPipelineVersion == "" {
			pipeline.Status.ParentPipelineVersion = parentPipeline.Status.Version
			if err := r.Status().Update(ctx, pipeline); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	if pipeline.Spec.Incremental && pipeline.Spec.BaseVersion != "" {
		platform, err := r.findPlatform(ctx)
		if err != nil {
			logger.Error(err, "failed to lookup disconnected platform for dependency check")
			return ctrl.Result{}, err
		}
		if platform != nil && !versionExists(platform.Status.ImportHistory, pipeline.Spec.BaseVersion) {
			pipeline.Status.Phase = "Failed"
			now := metav1.Now()
			pipeline.Status.CompletionTime = &now
			pipeline.Status.Conditions = append(pipeline.Status.Conditions, metav1.Condition{
				Type:    "DependencyCheck",
				Status:  "False",
				Reason:  "BaseVersionNotImported",
				Message: fmt.Sprintf("baseVersion %s has not been imported; import it before running incremental collection", pipeline.Spec.BaseVersion),
			})
			return ctrl.Result{}, r.Status().Update(ctx, pipeline)
		}
	}

	if pipeline.Status.Version == "" {
		pipeline.Status.Version = generateVersion(pipeline.Spec.TriggerType)
		if err := r.Status().Update(ctx, pipeline); err != nil {
			return ctrl.Result{}, err
		}
	}

	if err := r.ensurePVC(ctx, pipeline); err != nil {
		logger.Error(err, "failed to ensure PVC")
		return ctrl.Result{}, err
	}

	// Wait for signing configuration to be applied by DisconnectedPlatform controller
	// Check if there's a DisconnectedPlatform with managed signing configured
	platform, err := r.findPlatform(ctx)
	if err != nil {
		logger.Info("Could not find platform for signing config check", "error", err)
	} else if platform == nil {
		logger.Info("No platform found for signing config check")
	} else {
		logger.Info("Platform found", "hasConnected", platform.Spec.Connected != nil,
			"hasRHTAS", platform.Spec.Connected != nil && platform.Spec.Connected.RHTAS != nil,
			"hasOIDC", platform.Spec.Connected != nil && platform.Spec.Connected.RHTAS != nil && platform.Spec.Connected.RHTAS.OIDC != nil,
			"hasManaged", platform.Spec.Connected != nil && platform.Spec.Connected.RHTAS != nil && platform.Spec.Connected.RHTAS.OIDC != nil && platform.Spec.Connected.RHTAS.OIDC.Managed != nil)

		if platform.Spec.Connected != nil && platform.Spec.Connected.RHTAS != nil && platform.Spec.Connected.RHTAS.OIDC != nil && platform.Spec.Connected.RHTAS.OIDC.Managed != nil {
			// If platform has managed signing configured, wait for it to be applied to this pipeline
			// Check if signing config has been applied yet
			if pipeline.Spec.Signing == nil || pipeline.Spec.Signing.Keyless == nil {
				logger.Info("Waiting for signing configuration to be applied by DisconnectedPlatform controller")
				// Requeue after a short delay to allow DisconnectedPlatform controller to update signing config
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
			logger.Info("Signing configuration already applied, proceeding with PipelineRun creation")
		}
	}

	pr, err := r.buildPipelineRun(ctx, pipeline, cm)
	if err != nil {
		logger.Error(err, "failed to build PipelineRun")
		return ctrl.Result{}, err
	}

	if err := r.Create(ctx, pr); err != nil {
		logger.Error(err, "failed to create PipelineRun")
		// If another reconcile already created a PipelineRun, requeue to pick it up
		if apierrors.IsAlreadyExists(err) || apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}

	// After Create, pr.Name has the generated name with random suffix
	// Refetch to ensure no other reconcile set PipelineRunRef
	latest := &mirrorv1.CollectionPipeline{}
	if err := r.Get(ctx, types.NamespacedName{Name: pipeline.Name, Namespace: pipeline.Namespace}, latest); err != nil {
		return ctrl.Result{}, err
	}

	// If another reconcile already set PipelineRunRef, delete our PipelineRun and requeue
	if latest.Status.PipelineRunRef != "" && latest.Status.PipelineRunRef != pr.Name {
		logger.Info("Another reconcile already created PipelineRun, cleaning up duplicate", "ours", pr.Name, "theirs", latest.Status.PipelineRunRef)
		if err := r.Delete(ctx, pr); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to delete duplicate PipelineRun")
		}
		return ctrl.Result{Requeue: true}, nil
	}

	now := metav1.Now()
	latest.Status.PipelineRunRef = pr.Name
	latest.Status.Phase = "Pending"
	latest.Status.StartTime = &now
	return ctrl.Result{}, r.Status().Update(ctx, latest)
}

func (r *CollectionPipelineReconciler) trackPipelineRun(ctx context.Context, pipeline *mirrorv1.CollectionPipeline, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	pr := &pipelinev1.PipelineRun{}
	err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: pipeline.Status.PipelineRunRef}, pr)
	if err != nil {
		if apierrors.IsNotFound(err) {
			pipeline.Status.PipelineRunRef = ""
			pipeline.Status.Phase = "Stale"
			return ctrl.Result{}, r.Status().Update(ctx, pipeline)
		}
		return ctrl.Result{}, err
	}

	phase := collectionPipelineRunPhase(pr)
	pipeline.Status.Phase = phase
	if pr.Status.CompletionTime != nil {
		pipeline.Status.CompletionTime = pr.Status.CompletionTime.DeepCopy()
	}
	if pr.Status.StartTime != nil {
		pipeline.Status.StartTime = pr.Status.StartTime.DeepCopy()
	}

	// Set bundle download URL when collection completes successfully
	if phase == "Complete" || phase == "Succeeded" {
		// Always try to get URLs from pipeline results first (to get latest results)
		bundleURL := ""
		signatureURL := ""
		sbomUrl := ""

		// Check for task results in PipelineRun status
		if pr.Status.Results != nil {
			for _, result := range pr.Status.Results {
				if result.Name == "bundle-url" {
					bundleURL = result.Value.StringVal
				} else if result.Name == "signature-url" {
					signatureURL = result.Value.StringVal
				} else if result.Name == "sbom-url" {
					sbomUrl = result.Value.StringVal
					logger.Info("Read sbom-url from PipelineRun result", "sbomUrl", sbomUrl)
				}
			}
		}

		// If results are available, use them (always update from latest PipelineRun)
		if bundleURL != "" {
			pipeline.Status.BundleURL = bundleURL
		}
		if signatureURL != "" {
			pipeline.Status.SignatureURL = signatureURL
		}
		if sbomUrl != "" {
			pipeline.Status.SbomUrl = sbomUrl
			logger.Info("Set pipeline status sbomUrl", "sbomUrl", sbomUrl)
		}

		// Fallback: construct URL from OBC config if results are not available
		if pipeline.Status.BundleURL == "" {
			// Get S3 bucket info from ObjectBucketClaim ConfigMap
			obcConfigMap := &corev1.ConfigMap{}
			if err := r.Get(ctx, types.NamespacedName{Name: "collection-artifacts", Namespace: pipeline.Namespace}, obcConfigMap); err == nil {
				bucketName := obcConfigMap.Data["BUCKET_NAME"]
				bucketHost := obcConfigMap.Data["BUCKET_HOST"]

				// Use external S3 route instead of internal service endpoint
				if bucketHost == "s3.openshift-storage.svc" {
					// Get the external S3 route
					s3Route := &unstructured.Unstructured{}
					s3Route.SetGroupVersionKind(schema.GroupVersionKind{
						Group:   "route.openshift.io",
						Version: "v1",
						Kind:    "Route",
					})
					if err := r.Get(ctx, types.NamespacedName{Name: "s3", Namespace: "openshift-storage"}, s3Route); err == nil {
						if host, found, _ := unstructured.NestedString(s3Route.Object, "spec", "host"); found && host != "" {
							bucketHost = host
						}
					}
				}

				if bucketName != "" && bucketHost != "" {
					// S3 URL format for the bundle file
					bundleName := fmt.Sprintf("%s.tar.gz", pipeline.Name)
					pipeline.Status.BundleURL = fmt.Sprintf("https://%s/%s/%s/%s", bucketHost, bucketName, pipeline.Name, bundleName)
					pipeline.Status.SignatureURL = fmt.Sprintf("https://%s/%s/%s/%s.sig", bucketHost, bucketName, pipeline.Name, bundleName)
				}
			}
		}
	}

	if err := r.Status().Update(ctx, pipeline); err != nil {
		return ctrl.Result{}, err
	}

	if phase == "Complete" {
		r.updatePlatformCollectionHistory(ctx, pipeline)
	}

	if !pr.IsDone() {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func (r *CollectionPipelineReconciler) updatePlatformCollectionHistory(ctx context.Context, pipeline *mirrorv1.CollectionPipeline) {
	platform, err := r.findPlatform(ctx)
	if err != nil || platform == nil {
		return
	}

	info := mirrorv1.CollectionInfo{
		Version:   pipeline.Status.Version,
		Timestamp: metav1.Now(),
		Status:    "Complete",
	}
	platform.Status.LastCollection = &info
	platform.Status.CollectionHistory = append(platform.Status.CollectionHistory, info)
	if err := r.Status().Update(ctx, platform); err != nil {
		log.FromContext(ctx).Error(err, "failed to update platform collection history")
	}
}

func (r *CollectionPipelineReconciler) cleanup(ctx context.Context, pipeline *mirrorv1.CollectionPipeline) (ctrl.Result, error) {
	if containsString(pipeline.GetFinalizers(), pipelineFinalizer) {
		pipeline.SetFinalizers(removeString(pipeline.GetFinalizers(), pipelineFinalizer))
		return ctrl.Result{}, r.Update(ctx, pipeline)
	}
	return ctrl.Result{}, nil
}

func (r *CollectionPipelineReconciler) findPlatform(ctx context.Context) (*mirrorv1.DisconnectedPlatform, error) {
	list := &mirrorv1.DisconnectedPlatformList{}
	if err := r.List(ctx, list); err != nil {
		return nil, err
	}
	if len(list.Items) == 0 {
		return nil, nil
	}
	return &list.Items[0], nil
}

func generateVersion(triggerType mirrorv1.TriggerType) string {
	now := time.Now()
	tt := string(triggerType)
	if tt == "" {
		tt = "manual"
	}
	return fmt.Sprintf("v%s.%s.%s.001-%s", now.Format("2006"), now.Format("01"), now.Format("02"), tt)
}

func versionExists(history []mirrorv1.ImportInfo, version string) bool {
	for _, h := range history {
		if h.Version == version {
			return true
		}
	}
	return false
}

func (r *CollectionPipelineReconciler) ensureConfigMap(ctx context.Context, pipeline *mirrorv1.CollectionPipeline, name string) (*corev1.ConfigMap, error) {
	existing := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Namespace: pipeline.Namespace, Name: name}, existing)
	if err == nil {
		return existing, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	// Inject Airgap Architect images into the ImageSetConfiguration as additionalImages
	enrichedConfig, err := r.injectArchitectImages(ctx, pipeline.Spec.ImageSetConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to inject architect images: %w", err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: pipeline.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(pipeline, mirrorv1.GroupVersion.WithKind("CollectionPipeline")),
			},
		},
		Data: map[string]string{configMapKey: enrichedConfig},
	}
	return cm, r.Create(ctx, cm)
}

func (r *CollectionPipelineReconciler) ensurePVC(ctx context.Context, pipeline *mirrorv1.CollectionPipeline) error {
	logger := log.FromContext(ctx)
	// Always create working PVC - it's needed for temporary storage during collection
	// Even when using S3 for final storage, we need working space

	// Determine working PVC name with 1:1 mapping to pipeline
	// Priority order: parent pipeline > incremental base version > own PVC
	var workingPVCName string
	if pipeline.Spec.ParentPipeline != "" {
		// Reuse parent pipeline's working PVC
		parentPipeline := &mirrorv1.CollectionPipeline{}
		if err := r.Get(ctx, types.NamespacedName{Name: pipeline.Spec.ParentPipeline, Namespace: pipeline.Namespace}, parentPipeline); err != nil {
			return fmt.Errorf("failed to get parent pipeline for PVC lookup: %w", err)
		}
		if parentPipeline.Status.WorkingPVCName != "" {
			workingPVCName = parentPipeline.Status.WorkingPVCName
		} else {
			// Fallback: derive from parent's name if status not yet populated
			workingPVCName = fmt.Sprintf("collection-storage-%s", pipeline.Spec.ParentPipeline)
		}
	} else if pipeline.Spec.Incremental && pipeline.Spec.BaseVersion != "" {
		// Use the base version's PVC name for incremental builds
		workingPVCName = fmt.Sprintf("collection-storage-%s", pipeline.Spec.BaseVersion)
	} else {
		// Each CollectionPipeline gets its own PVC (1:1 mapping)
		workingPVCName = fmt.Sprintf("collection-storage-%s", pipeline.Name)
	}

	// Track the working PVC name in status
	if pipeline.Status.WorkingPVCName != workingPVCName {
		pipeline.Status.WorkingPVCName = workingPVCName
		if err := r.Status().Update(ctx, pipeline); err != nil {
			return err
		}
	}

	// Ensure working PVC exists
	existingWorking := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Namespace: pipeline.Namespace, Name: workingPVCName}, existingWorking)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		// Create new working PVC - only for non-incremental, non-parent-referencing collections
		if pipeline.Spec.Incremental {
			return fmt.Errorf("incremental collection requires base working PVC %s to exist", workingPVCName)
		}
		if pipeline.Spec.ParentPipeline != "" {
			return fmt.Errorf("parent pipeline collection requires parent working PVC %s to exist", workingPVCName)
		}

		workingPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      workingPVCName,
				Namespace: pipeline.Namespace,
				Labels: map[string]string{
					"app.kubernetes.io/managed-by":            "mirror-operator",
					"app.kubernetes.io/component":             "collection-storage",
					"mirror.mathianasj.github.com/collection": pipeline.Name,
				},
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(pipeline, mirrorv1.GroupVersion.WithKind("CollectionPipeline")),
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: getStorageSize(pipeline),
					},
				},
			},
		}
		if err := r.Create(ctx, workingPVC); err != nil {
			return err
		}
	} else {
		// PVC exists — expand if requested size is larger
		desiredSize := getStorageSize(pipeline)
		currentSize := existingWorking.Spec.Resources.Requests[corev1.ResourceStorage]
		if desiredSize.Cmp(currentSize) > 0 {
			logger.Info("Expanding working PVC", "pvc", workingPVCName, "from", currentSize.String(), "to", desiredSize.String())
			existingWorking.Spec.Resources.Requests[corev1.ResourceStorage] = desiredSize
			if err := r.Update(ctx, existingWorking); err != nil {
				return fmt.Errorf("failed to expand PVC %s: %w", workingPVCName, err)
			}
		}
	}

	return nil
}

func collectionPipelineRunPhase(pr *pipelinev1.PipelineRun) string {
	if pr.Status.CompletionTime != nil {
		for _, c := range pr.Status.Conditions {
			if c.Type == "Succeeded" && c.Status == "True" {
				return "Complete"
			}
		}
		return "Failed"
	}
	if pr.Status.StartTime != nil {
		return "Collecting"
	}
	return "Pending"
}

// buildPipelineRun creates a PipelineRun that references the collection-pipeline-template
func (r *CollectionPipelineReconciler) buildPipelineRun(ctx context.Context, pipeline *mirrorv1.CollectionPipeline, cm *corev1.ConfigMap) (*pipelinev1.PipelineRun, error) {
	logger := log.FromContext(ctx)

	// Use the working PVC name from status (set by ensurePVC)
	workingPVCName := pipeline.Status.WorkingPVCName
	if workingPVCName == "" {
		// Fallback: determine working PVC name using same logic as ensurePVC
		if pipeline.Spec.ParentPipeline != "" {
			parentPipeline := &mirrorv1.CollectionPipeline{}
			if err := r.Get(ctx, types.NamespacedName{Name: pipeline.Spec.ParentPipeline, Namespace: pipeline.Namespace}, parentPipeline); err != nil {
				return nil, fmt.Errorf("failed to get parent pipeline for PVC lookup: %w", err)
			}
			if parentPipeline.Status.WorkingPVCName != "" {
				workingPVCName = parentPipeline.Status.WorkingPVCName
			} else {
				workingPVCName = fmt.Sprintf("collection-storage-%s", pipeline.Spec.ParentPipeline)
			}
		} else if pipeline.Spec.Incremental && pipeline.Spec.BaseVersion != "" {
			workingPVCName = fmt.Sprintf("collection-storage-%s", pipeline.Spec.BaseVersion)
		} else {
			workingPVCName = fmt.Sprintf("collection-storage-%s", pipeline.Name)
		}
	}

	// Get runtime config from DisconnectedPlatform
	platform, err := r.findPlatform(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to find platform: %w", err)
	}

	// Build parameters based on platform configuration
	params := []pipelinev1.Param{
		{Name: "config-map-name", Value: pipelinev1.ParamValue{Type: "string", StringVal: cm.Name}},
		{Name: "mirror-image", Value: pipelinev1.ParamValue{Type: "string", StringVal: r.getMirrorImage()}},
		{Name: "working-pvc-name", Value: pipelinev1.ParamValue{Type: "string", StringVal: workingPVCName}},
	}

	// Determine intermediate registry (for m2m workflow)
	intermediateRegistry := ""
	if platform != nil && platform.Spec.Connected != nil && platform.Spec.Connected.Quay != nil &&
		platform.Spec.Connected.Quay.Managed != nil && platform.Spec.Connected.Quay.Managed.Enabled {
		// Build Quay route hostname (domain already includes 'apps.')
		intermediateRegistry = fmt.Sprintf("mirror-operator-quay-quay-%s.%s/%s",
			pipeline.Namespace, r.getClusterDomain(ctx), platform.Spec.Connected.Quay.Managed.OrganizationName)
	}
	params = append(params, pipelinev1.Param{
		Name:  "intermediate-registry",
		Value: pipelinev1.ParamValue{Type: "string", StringVal: intermediateRegistry},
	})

	// Check for keyless signing configuration
	hasKeylessSigning := "false"
	fulcioURL, rekorURL, tufURL, oidcIssuer, oidcClientID := "", "", "", "", ""

	if pipeline.Spec.Signing != nil && pipeline.Spec.Signing.Keyless != nil {
		hasKeylessSigning = "true"
		fulcioURL = pipeline.Spec.Signing.Keyless.FulcioURL
		rekorURL = pipeline.Spec.Signing.Keyless.RekorURL
		tufURL = pipeline.Spec.Signing.Keyless.TUFURL
		oidcIssuer = pipeline.Spec.Signing.Keyless.OIDCIssuer
		oidcClientID = pipeline.Spec.Signing.Keyless.OIDCClientID
	}

	params = append(params,
		pipelinev1.Param{Name: "has-keyless-signing", Value: pipelinev1.ParamValue{Type: "string", StringVal: hasKeylessSigning}},
		pipelinev1.Param{Name: "fulcio-url", Value: pipelinev1.ParamValue{Type: "string", StringVal: fulcioURL}},
		pipelinev1.Param{Name: "rekor-url", Value: pipelinev1.ParamValue{Type: "string", StringVal: rekorURL}},
		pipelinev1.Param{Name: "tuf-url", Value: pipelinev1.ParamValue{Type: "string", StringVal: tufURL}},
		pipelinev1.Param{Name: "oidc-issuer", Value: pipelinev1.ParamValue{Type: "string", StringVal: oidcIssuer}},
		pipelinev1.Param{Name: "oidc-client-id", Value: pipelinev1.ParamValue{Type: "string", StringVal: oidcClientID}},
	)

	// Check for TPA configuration
	hasTPA := "false"
	tpaHost, tpaOidcIssuer, tpaOidcClientID := "", "", ""
	if platform != nil && platform.Spec.Connected != nil && platform.Spec.Connected.RHTPA != nil {
		tpaHost, tpaOidcIssuer, tpaOidcClientID = r.getTPAAndKeycloakHosts(ctx)
		if tpaHost != "" {
			hasTPA = "true"
		}
	}

	params = append(params,
		pipelinev1.Param{Name: "has-tpa", Value: pipelinev1.ParamValue{Type: "string", StringVal: hasTPA}},
		pipelinev1.Param{Name: "tpa-host", Value: pipelinev1.ParamValue{Type: "string", StringVal: tpaHost}},
		pipelinev1.Param{Name: "tpa-oidc-issuer", Value: pipelinev1.ParamValue{Type: "string", StringVal: tpaOidcIssuer}},
		pipelinev1.Param{Name: "tpa-oidc-client-id", Value: pipelinev1.ParamValue{Type: "string", StringVal: tpaOidcClientID}},
	)

	// Read S3 configuration from ObjectBucketClaim ConfigMap and Secret
	s3Bucket, s3Endpoint, s3Region, s3SecretName := "", "", "", ""
	obcConfigMap := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Name: "collection-artifacts", Namespace: pipeline.Namespace}, obcConfigMap); err == nil {
		s3Bucket = obcConfigMap.Data["BUCKET_NAME"]
		s3Endpoint = obcConfigMap.Data["BUCKET_HOST"]
		// Add http:// scheme if not present (AWS CLI requires it)
		if s3Endpoint != "" && !strings.HasPrefix(s3Endpoint, "http://") && !strings.HasPrefix(s3Endpoint, "https://") {
			s3Endpoint = "http://" + s3Endpoint
		}
		s3Region = obcConfigMap.Data["BUCKET_REGION"]
		if s3Region == "" {
			s3Region = "us-east-1" // Default region for NooBaa
		}
		s3SecretName = "collection-artifacts" // OBC creates secret with same name as claim
		logger.Info("Found S3 configuration from OBC", "bucket", s3Bucket, "endpoint", s3Endpoint)
	}

	hasS3 := "false"
	if s3Bucket != "" {
		hasS3 = "true"
	}

	params = append(params,
		pipelinev1.Param{Name: "has-s3", Value: pipelinev1.ParamValue{Type: "string", StringVal: hasS3}},
		pipelinev1.Param{Name: "s3-bucket", Value: pipelinev1.ParamValue{Type: "string", StringVal: s3Bucket}},
		pipelinev1.Param{Name: "s3-endpoint", Value: pipelinev1.ParamValue{Type: "string", StringVal: s3Endpoint}},
		pipelinev1.Param{Name: "s3-region", Value: pipelinev1.ParamValue{Type: "string", StringVal: s3Region}},
		pipelinev1.Param{Name: "s3-secret-name", Value: pipelinev1.ParamValue{Type: "string", StringVal: s3SecretName}},
	)

	// Add Airgap Architect image parameters
	params = append(params,
		pipelinev1.Param{Name: "architect-frontend-image", Value: pipelinev1.ParamValue{Type: "string", StringVal: r.getArchitectFrontendImage(ctx)}},
		pipelinev1.Param{Name: "architect-backend-image", Value: pipelinev1.ParamValue{Type: "string", StringVal: r.getArchitectBackendImage(ctx)}},
	)

	// Auto-detect CLI tool versions from ImageSetConfiguration
	ocVersion := "stable-4.16" // default fallback
	mirrorRegistryVersion := "latest"

	// Parse the ImageSetConfiguration YAML to find the OCP version
	var imageSetConfig map[string]interface{}
	if err := yaml.Unmarshal([]byte(pipeline.Spec.ImageSetConfig), &imageSetConfig); err == nil {
		// Navigate to mirror.platform.channels[0] for OCP version
		if mirror, ok := imageSetConfig["mirror"].(map[string]interface{}); ok {
			if platformConfig, ok := mirror["platform"].(map[string]interface{}); ok {
				if channels, ok := platformConfig["channels"].([]interface{}); ok && len(channels) > 0 {
					if channel, ok := channels[0].(map[string]interface{}); ok {
						// Check for minVersion first (more specific)
						if minVersion, ok := channel["minVersion"].(string); ok && minVersion != "" {
							ocVersion = minVersion // e.g., "4.16.15"
							logger.Info("Detected OCP version from ImageSetConfig minVersion", "version", ocVersion)
						} else if name, ok := channel["name"].(string); ok {
							ocVersion = name // e.g., "stable-4.16"
							logger.Info("Detected OCP version from ImageSetConfig channel name", "version", ocVersion)
						}
					}
				}
			}
		}
	} else {
		logger.Info("Failed to parse ImageSetConfig for version detection, using default", "error", err, "default", ocVersion)
	}

	// Add CLI tool version parameters
	params = append(params,
		pipelinev1.Param{Name: "oc-version", Value: pipelinev1.ParamValue{Type: "string", StringVal: ocVersion}},
		pipelinev1.Param{Name: "mirror-registry-version", Value: pipelinev1.ParamValue{Type: "string", StringVal: mirrorRegistryVersion}},
		pipelinev1.Param{Name: "cli-tools-enabled", Value: pipelinev1.ParamValue{Type: "string", StringVal: "true"}},
	)

	// Define workspaces
	workspaces := []pipelinev1.WorkspaceBinding{
		{
			Name: "config",
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: cm.Name},
			},
		},
		{
			Name: "pull-secret",
			Secret: &corev1.SecretVolumeSource{
				SecretName: "pull-secret",
			},
		},
		{
			Name: "output",
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: workingPVCName,
			},
		},
	}

	// Add optional OIDC secret workspace
	if hasKeylessSigning == "true" && pipeline.Spec.Signing != nil && pipeline.Spec.Signing.Keyless != nil && pipeline.Spec.Signing.Keyless.OIDCClientSecret != nil {
		workspaces = append(workspaces, pipelinev1.WorkspaceBinding{
			Name: "oidc-secret",
			Secret: &corev1.SecretVolumeSource{
				SecretName: pipeline.Spec.Signing.Keyless.OIDCClientSecret.Name,
			},
		})
	}

	// Add optional TPA OIDC secret workspace
	if hasTPA == "true" {
		workspaces = append(workspaces, pipelinev1.WorkspaceBinding{
			Name: "tpa-oidc-secret",
			Secret: &corev1.SecretVolumeSource{
				SecretName: "rhtpa-oidc-cli-secret",
			},
		})
	}

	// Add optional cosign key workspace
	if pipeline.Spec.Signing != nil && pipeline.Spec.Signing.KeySecretRef != nil {
		workspaces = append(workspaces, pipelinev1.WorkspaceBinding{
			Name: "cosign-key",
			Secret: &corev1.SecretVolumeSource{
				SecretName: pipeline.Spec.Signing.KeySecretRef.Name,
			},
		})
	}

	// Add Airgap Architect import script workspace
	workspaces = append(workspaces, pipelinev1.WorkspaceBinding{
		Name: "architect-script",
		ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: "airgap-architect-import-script",
			},
		},
	})

	// Set timeout
	timeout := &metav1.Duration{Duration: 12 * time.Hour}
	if pipeline.Spec.Timeout != nil {
		timeout = pipeline.Spec.Timeout
	}

	// Create PipelineRun
	pipelineRunNamePrefix := fmt.Sprintf("collection-pipeline-%s-", pipeline.Name)
	pr := &pipelinev1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: pipelineRunNamePrefix,
			Namespace:    pipeline.Namespace,
			Annotations: map[string]string{
				"results.tekton.dev/log":    "false",
				"results.tekton.dev/result": "false",
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(pipeline, mirrorv1.GroupVersion.WithKind("CollectionPipeline")),
			},
		},
		Spec: pipelinev1.PipelineRunSpec{
			PipelineRef: &pipelinev1.PipelineRef{
				Name: "collection-pipeline-template",
			},
			Params:     params,
			Workspaces: workspaces,
			Timeouts: &pipelinev1.TimeoutFields{
				Pipeline: timeout,
			},
		},
	}

	logger.Info("Created PipelineRun referencing template",
		"intermediate-registry", intermediateRegistry,
		"has-keyless-signing", hasKeylessSigning,
		"has-tpa", hasTPA)

	return pr, nil
}

func (r *CollectionPipelineReconciler) getMirrorImage() string {
	if r.MirrorImage != "" {
		return r.MirrorImage
	}
	return defaultMirrorImage
}

func (r *CollectionPipelineReconciler) getArchitectFrontendImage(ctx context.Context) string {
	platform, err := r.findPlatform(ctx)
	if err == nil && platform != nil && platform.Spec.Architect != nil && platform.Spec.Architect.FrontendImage != "" {
		return platform.Spec.Architect.FrontendImage
	}
	return "quay.io/mirror-operator/airgap-architect-frontend:latest"
}

func (r *CollectionPipelineReconciler) getArchitectBackendImage(ctx context.Context) string {
	platform, err := r.findPlatform(ctx)
	if err == nil && platform != nil && platform.Spec.Architect != nil && platform.Spec.Architect.BackendImage != "" {
		return platform.Spec.Architect.BackendImage
	}
	return "quay.io/mirror-operator/airgap-architect-backend:latest"
}

func (r *CollectionPipelineReconciler) getArchitectConsolePluginImage(ctx context.Context) string {
	platform, err := r.findPlatform(ctx)
	if err == nil && platform != nil && platform.Spec.Architect != nil && platform.Spec.Architect.ConsolePlugin.Image != "" {
		return platform.Spec.Architect.ConsolePlugin.Image
	}
	return "quay.io/mirror-operator/airgap-architect-console-plugin:latest"
}

func (r *CollectionPipelineReconciler) injectArchitectImages(ctx context.Context, originalConfigYAML string) (string, error) {
	// Parse the original ImageSetConfiguration
	var config ImageSetConfiguration
	if err := yaml.Unmarshal([]byte(originalConfigYAML), &config); err != nil {
		return "", fmt.Errorf("failed to parse ImageSetConfiguration: %w", err)
	}

	// Get architect images from DisconnectedPlatform
	frontendImage := r.getArchitectFrontendImage(ctx)
	backendImage := r.getArchitectBackendImage(ctx)
	pluginImage := r.getArchitectConsolePluginImage(ctx)

	// Initialize additionalImages if nil
	if config.Mirror.AdditionalImages == nil {
		config.Mirror.AdditionalImages = []ImageConfig{}
	}

	// Add architect images if not already present
	architectImages := []string{frontendImage, backendImage, pluginImage}
	for _, img := range architectImages {
		found := false
		for _, existing := range config.Mirror.AdditionalImages {
			if existing.Name == img {
				found = true
				break
			}
		}
		if !found {
			config.Mirror.AdditionalImages = append(config.Mirror.AdditionalImages, ImageConfig{Name: img})
		}
	}

	// Serialize back to YAML
	enrichedYAML, err := yaml.Marshal(&config)
	if err != nil {
		return "", fmt.Errorf("failed to marshal enriched config: %w", err)
	}

	return string(enrichedYAML), nil
}

func (r *CollectionPipelineReconciler) getClusterDomain(ctx context.Context) string {
	// Get cluster domain from Ingress controller
	ingress := &unstructured.Unstructured{}
	ingress.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "Ingress",
	})
	ingress.SetName("cluster")

	if err := r.Get(ctx, types.NamespacedName{Name: "cluster"}, ingress); err != nil {
		log.FromContext(ctx).Error(err, "failed to get cluster ingress config, using fallback domain")
		return "cluster.example.com"
	}

	domain, found, err := unstructured.NestedString(ingress.Object, "spec", "domain")
	if err != nil || !found {
		log.FromContext(ctx).Info("cluster domain not found in ingress config, using fallback")
		return "cluster.example.com"
	}

	return domain
}

// getTPAAndKeycloakHosts retrieves the TPA and Keycloak hostnames from Ingresses
func (r *CollectionPipelineReconciler) getTPAAndKeycloakHosts(ctx context.Context) (string, string, string) {
	logger := log.FromContext(ctx)

	// Find TPA instance
	tpaList := &unstructured.UnstructuredList{}
	tpaList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "rhtpa.io",
		Version: "v1",
		Kind:    "TrustedProfileAnalyzerList",
	})
	if err := r.List(ctx, tpaList); err != nil {
		logger.Error(err, "failed to list TPA instances")
		return "", "", ""
	}

	if len(tpaList.Items) == 0 {
		return "", "", ""
	}

	tpa := tpaList.Items[0]
	tpaUID := tpa.GetUID()

	// Find TPA Ingress
	ingressList := &unstructured.UnstructuredList{}
	ingressList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "networking.k8s.io",
		Version: "v1",
		Kind:    "IngressList",
	})
	if err := r.List(ctx, ingressList, client.InNamespace(tpa.GetNamespace())); err != nil {
		logger.Error(err, "failed to list Ingresses")
		return "", "", ""
	}

	var tpaHostname string
	for _, ingress := range ingressList.Items {
		owners := ingress.GetOwnerReferences()
		for _, owner := range owners {
			if owner.UID == tpaUID {
				rules, found, _ := unstructured.NestedSlice(ingress.Object, "spec", "rules")
				if found && len(rules) > 0 {
					if rule, ok := rules[0].(map[string]interface{}); ok {
						if host, ok := rule["host"].(string); ok {
							tpaHostname = host
							break
						}
					}
				}
			}
		}
		if tpaHostname != "" {
			break
		}
	}

	if tpaHostname == "" {
		return "", "", ""
	}

	// Get OIDC issuer URL from TPA spec
	oidcIssuer, found, err := unstructured.NestedString(tpa.Object, "spec", "oidc", "issuerUrl")
	if err != nil || !found || oidcIssuer == "" {
		logger.Error(err, "failed to get OIDC issuer from TPA spec")
		return tpaHostname, "", ""
	}

	oidcClientID := "cli"

	return tpaHostname, oidcIssuer, oidcClientID
}

// ImageSetConfiguration represents the oc-mirror v2 configuration structure
type ImageSetConfiguration struct {
	Kind       string                      `json:"kind" yaml:"kind"`
	APIVersion string                      `json:"apiVersion" yaml:"apiVersion"`
	Mirror     ImageSetConfigurationMirror `json:"mirror" yaml:"mirror"`
}

type ImageSetConfigurationMirror struct {
	Platform         *PlatformConfig  `json:"platform,omitempty" yaml:"platform,omitempty"`
	Operators        []OperatorConfig `json:"operators,omitempty" yaml:"operators,omitempty"`
	AdditionalImages []ImageConfig    `json:"additionalImages,omitempty" yaml:"additionalImages,omitempty"`
	Helm             *HelmConfig      `json:"helm,omitempty" yaml:"helm,omitempty"`
}

type PlatformConfig struct {
	Channels      []ChannelConfig `json:"channels,omitempty" yaml:"channels,omitempty"`
	Graph         bool            `json:"graph,omitempty" yaml:"graph,omitempty"`
	Architectures []string        `json:"architectures,omitempty" yaml:"architectures,omitempty"`
}

type ChannelConfig struct {
	Name       string `json:"name" yaml:"name"`
	MinVersion string `json:"minVersion,omitempty" yaml:"minVersion,omitempty"`
	MaxVersion string `json:"maxVersion,omitempty" yaml:"maxVersion,omitempty"`
	Full       bool   `json:"full,omitempty" yaml:"full,omitempty"`
}

type OperatorConfig struct {
	Catalog  string          `json:"catalog" yaml:"catalog"`
	Packages []PackageConfig `json:"packages,omitempty" yaml:"packages,omitempty"`
}

type PackageConfig struct {
	Name     string          `json:"name" yaml:"name"`
	Channels []ChannelConfig `json:"channels,omitempty" yaml:"channels,omitempty"`
}

type ImageConfig struct {
	Name string `json:"name" yaml:"name"`
}

type HelmConfig struct {
	Repositories []HelmRepoConfig  `json:"repositories,omitempty" yaml:"repositories,omitempty"`
	Charts       []HelmChartConfig `json:"charts,omitempty" yaml:"charts,omitempty"`
}

type HelmRepoConfig struct {
	Name string `json:"name" yaml:"name"`
	URL  string `json:"url" yaml:"url"`
}

type HelmChartConfig struct {
	Name       string `json:"name" yaml:"name"`
	Repository string `json:"repository" yaml:"repository"`
	Version    string `json:"version,omitempty" yaml:"version,omitempty"`
}

// generateIntermediateImageSetConfig creates an ImageSetConfiguration that pulls from the intermediate registry
// It parses the original ImageSetConfiguration and rewrites all image/catalog references to point to the intermediate registry
func (r *CollectionPipelineReconciler) generateIntermediateImageSetConfig(pipeline *mirrorv1.CollectionPipeline, intermediateRegistry string) (string, error) {
	// Parse original ImageSetConfiguration
	var originalConfig ImageSetConfiguration
	if err := yaml.Unmarshal([]byte(pipeline.Spec.ImageSetConfig), &originalConfig); err != nil {
		return "", fmt.Errorf("failed to parse ImageSetConfiguration: %w", err)
	}

	// Create new config that will pull from intermediate registry
	intermediateConfig := ImageSetConfiguration{
		Kind:       originalConfig.Kind,
		APIVersion: originalConfig.APIVersion,
		Mirror:     ImageSetConfigurationMirror{},
	}

	// Rewrite platform references
	if originalConfig.Mirror.Platform != nil {
		// Platform releases are mirrored to intermediate with standard paths
		// Keep platform config as-is since oc-mirror handles release paths automatically
		intermediateConfig.Mirror.Platform = originalConfig.Mirror.Platform
	}

	// Rewrite operator catalog references
	if len(originalConfig.Mirror.Operators) > 0 {
		intermediateConfig.Mirror.Operators = make([]OperatorConfig, 0, len(originalConfig.Mirror.Operators))
		for _, op := range originalConfig.Mirror.Operators {
			// Rewrite catalog reference to point to intermediate registry
			// Original: registry.redhat.io/redhat/redhat-operator-index:v4.18
			// Intermediate: quay.apps.example.com/mirror/redhat-operator-index:v4.18
			intermediateCatalog := rewriteImageReference(op.Catalog, intermediateRegistry)

			intermediateConfig.Mirror.Operators = append(intermediateConfig.Mirror.Operators, OperatorConfig{
				Catalog:  intermediateCatalog,
				Packages: op.Packages, // Package selection stays the same
			})
		}
	}

	// Rewrite additional images
	if len(originalConfig.Mirror.AdditionalImages) > 0 {
		intermediateConfig.Mirror.AdditionalImages = make([]ImageConfig, 0, len(originalConfig.Mirror.AdditionalImages))
		for _, img := range originalConfig.Mirror.AdditionalImages {
			// Rewrite image reference to point to intermediate registry
			intermediateImage := rewriteImageReference(img.Name, intermediateRegistry)
			intermediateConfig.Mirror.AdditionalImages = append(intermediateConfig.Mirror.AdditionalImages, ImageConfig{
				Name: intermediateImage,
			})
		}
	}

	// Helm charts - these are typically stored as OCI artifacts in the intermediate registry
	if originalConfig.Mirror.Helm != nil {
		// Note: Helm chart mirroring to intermediate registry may require
		// converting HTTP(S) repositories to OCI references
		// For now, preserve original helm config as oc-mirror handles this
		intermediateConfig.Mirror.Helm = originalConfig.Mirror.Helm
	}

	// Marshal back to YAML
	configBytes, err := yaml.Marshal(&intermediateConfig)
	if err != nil {
		return "", fmt.Errorf("failed to marshal intermediate config: %w", err)
	}

	return string(configBytes), nil
}

// rewriteImageReference rewrites an image reference to point to the intermediate registry
// Examples:
//
//	registry.redhat.io/redhat/ubi9:latest -> quay.apps.example.com/mirror/ubi9:latest
//	quay.io/openshift/origin-cli:v4.18 -> quay.apps.example.com/mirror/origin-cli:v4.18
//	registry.redhat.io/redhat/redhat-operator-index:v4.18 -> quay.apps.example.com/mirror/redhat-operator-index:v4.18
func rewriteImageReference(originalRef, intermediateRegistry string) string {
	// Remove any docker:// prefix if present
	ref := strings.TrimPrefix(originalRef, "docker://")

	// Split into registry/repository and tag/digest
	parts := strings.SplitN(ref, "/", 2)
	if len(parts) < 2 {
		// No registry specified (e.g., "nginx:latest") - add intermediate registry
		return intermediateRegistry + "/" + ref
	}

	// Extract repository path (everything after first /)
	repositoryPath := parts[1]

	// Extract just the image name (last component of path) and tag/digest
	pathComponents := strings.Split(repositoryPath, "/")
	imageName := pathComponents[len(pathComponents)-1]

	// Handle wildcards (e.g., quay.io/my-org/*)
	if strings.HasSuffix(originalRef, "/*") {
		// For wildcards, preserve the organization/namespace structure
		// quay.io/my-org/* -> intermediate/my-org/*
		return intermediateRegistry + "/" + repositoryPath
	}

	// For specific images, use flat structure in intermediate registry
	// This matches oc-mirror's default behavior of flattening paths
	return intermediateRegistry + "/" + imageName
}

func getStorageSize(pipeline *mirrorv1.CollectionPipeline) resource.Quantity {
	if pipeline.Spec.StorageSize != nil {
		return *pipeline.Spec.StorageSize
	}
	return resource.MustParse("100Gi")
}

// getParentDisconnectedPlatform finds the DisconnectedPlatform that owns this CollectionPipeline
func (r *CollectionPipelineReconciler) getParentDisconnectedPlatform(ctx context.Context, pipeline *mirrorv1.CollectionPipeline) (*mirrorv1.DisconnectedPlatform, error) {
	// Check if this pipeline has an owner reference to a DisconnectedPlatform
	for _, ownerRef := range pipeline.GetOwnerReferences() {
		if ownerRef.Kind == "DisconnectedPlatform" {
			platform := &mirrorv1.DisconnectedPlatform{}
			err := r.Get(ctx, types.NamespacedName{
				Namespace: pipeline.Namespace,
				Name:      ownerRef.Name,
			}, platform)
			if err != nil {
				return nil, err
			}
			return platform, nil
		}
	}

	// No DisconnectedPlatform owner found
	return nil, nil
}

func (r *CollectionPipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mirrorv1.CollectionPipeline{}).
		Owns(&pipelinev1.PipelineRun{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}

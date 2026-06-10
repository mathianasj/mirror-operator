package controller

import (
	"context"
	"fmt"
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
	if err := r.Status().Update(ctx, pipeline); err != nil {
		return ctrl.Result{}, err
	}

	if phase == "Complete" {
		r.updatePlatformCollectionHistory(ctx, pipeline)
	}

	if !pr.IsDone() {
		return ctrl.Result{}, nil
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

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: pipeline.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(pipeline, mirrorv1.GroupVersion.WithKind("CollectionPipeline")),
			},
		},
		Data: map[string]string{configMapKey: pipeline.Spec.ImageSetConfig},
	}
	return cm, r.Create(ctx, cm)
}

func (r *CollectionPipelineReconciler) ensurePVC(ctx context.Context, pipeline *mirrorv1.CollectionPipeline) error {
	output := pipeline.Spec.Storage.Output
	if output == nil {
		return nil
	}

	// Determine PVC name with 1:1 mapping to pipeline
	// For incremental collections, reuse the base version's PVC
	var pvcName string
	if pipeline.Spec.Incremental && pipeline.Spec.BaseVersion != "" {
		// Use the base version's PVC name for incremental builds
		pvcName = fmt.Sprintf("collection-storage-%s", pipeline.Spec.BaseVersion)
	} else {
		// Each CollectionPipeline gets its own PVC (1:1 mapping)
		// Always append pipeline name to ensure uniqueness, even if user provides a base name
		pvcName = fmt.Sprintf("collection-storage-%s", pipeline.Name)
	}

	existing := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Namespace: pipeline.Namespace, Name: pvcName}, existing)
	if err == nil {
		return nil // PVC already exists
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	// Create new PVC - only for non-incremental collections
	// Incremental collections should reference an existing base PVC
	if pipeline.Spec.Incremental {
		return fmt.Errorf("incremental collection requires base PVC %s to exist", pvcName)
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: pipeline.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by":            "mirror-operator",
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
					corev1.ResourceStorage: resource.MustParse("100Gi"),
				},
			},
		},
	}
	return r.Create(ctx, pvc)
}

func collectionPipelineRunPhase(pr *pipelinev1.PipelineRun) string {
	if pr.Status.CompletionTime != nil {
		for _, c := range pr.Status.Conditions {
			if c.Reason == "Succeeded" {
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

func (r *CollectionPipelineReconciler) buildPipelineRun(ctx context.Context, pipeline *mirrorv1.CollectionPipeline, cm *corev1.ConfigMap) (*pipelinev1.PipelineRun, error) {
	declaredWorkspaces := []pipelinev1.PipelineWorkspaceDeclaration{
		{Name: "config"},
		{Name: "pull-secret"},
	}
	bindings := []pipelinev1.WorkspaceBinding{
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
	}

	var envVars []corev1.EnvVar

	output := pipeline.Spec.Storage.Output
	if output != nil {
		// Determine PVC name with 1:1 mapping to pipeline
		var pvcName string
		if pipeline.Spec.Incremental && pipeline.Spec.BaseVersion != "" {
			// Use the base version's PVC name for incremental builds
			pvcName = fmt.Sprintf("collection-storage-%s", pipeline.Spec.BaseVersion)
		} else {
			// Each CollectionPipeline gets its own PVC (1:1 mapping)
			pvcName = fmt.Sprintf("collection-storage-%s", pipeline.Name)
		}

		declaredWorkspaces = append(declaredWorkspaces, pipelinev1.PipelineWorkspaceDeclaration{Name: "output"})
		bindings = append(bindings, pipelinev1.WorkspaceBinding{
			Name: "output",
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcName,
			},
		})

		if output.S3 != nil {
			s3Secret := &corev1.Secret{}
			if err := r.Get(ctx, types.NamespacedName{
				Namespace: pipeline.Namespace,
				Name:      output.S3.SecretRef.Name,
			}, s3Secret); err != nil {
				return nil, fmt.Errorf("failed to read s3 secret %s: %w", output.S3.SecretRef.Name, err)
			}

			envVars = append(envVars,
				corev1.EnvVar{
					Name: "AWS_ACCESS_KEY_ID",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: s3Secret.Name},
							Key:                  "accessKeyId",
						},
					},
				},
				corev1.EnvVar{
					Name: "AWS_SECRET_ACCESS_KEY",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: s3Secret.Name},
							Key:                  "secretAccessKey",
						},
					},
				},
			)

			if output.S3.Endpoint != "" {
				envVars = append(envVars, corev1.EnvVar{
					Name:  "AWS_ENDPOINT_URL",
					Value: output.S3.Endpoint,
				})
			}
			if output.S3.Region != "" {
				envVars = append(envVars, corev1.EnvVar{
					Name:  "AWS_DEFAULT_REGION",
					Value: output.S3.Region,
				})
			}
		}
	}

	mirrorImage := r.MirrorImage
	if mirrorImage == "" {
		mirrorImage = defaultMirrorImage
	}

	taskWorkspaceBindings := []pipelinev1.WorkspacePipelineTaskBinding{
		{Name: "config", Workspace: "config"},
		{Name: "pull-secret", Workspace: "pull-secret"},
	}
	outputWorkspaceBinding := pipelinev1.WorkspacePipelineTaskBinding{Name: "output", Workspace: "output"}
	if output != nil && output.PVC != "" {
		taskWorkspaceBindings = append(taskWorkspaceBindings, outputWorkspaceBinding)
	}

	// Dry-run step to generate mapping.txt quickly
	dryRunStep := pipelinev1.Step{
		Name:    "dry-run",
		Image:   mirrorImage,
		Command: []string{"oc-mirror"},
		Args: []string{
			"--v2",
			"--config=/workspace/config/" + configMapKey,
			"--authfile=/workspace/pull-secret/.dockerconfigjson",
			"--cache-dir=/workspace/output/.cache",
			"--dry-run",
			"file:///workspace/output",
		},
		Env: envVars,
	}

	// Get intermediate registry from DisconnectedPlatform if available
	var intermediateRegistry string
	platform, err := r.getParentDisconnectedPlatform(ctx, pipeline)
	if err != nil || platform == nil {
		// If no parent platform, try to find any platform in the cluster
		platform, err = r.findPlatform(ctx)
	}
	if err == nil && platform != nil && platform.Spec.Connected != nil {
		intermediateRegistry = platform.Spec.Connected.MirrorRegistry
	}

	// Determine if we're using keyless signing (need to check this before configuring mirror steps)
	hasKeylessSigning := pipeline.Spec.Signing != nil && pipeline.Spec.Signing.Keyless != nil

	// Mirror step configuration depends on whether we're using intermediate registry
	var ocMirrorStep pipelinev1.Step
	var mirrorToIntermediateStep pipelinev1.Step
	var mirrorFromIntermediateStep pipelinev1.Step

	if intermediateRegistry != "" && hasKeylessSigning {
		// Three-phase workflow with intermediate registry:
		// 1. Mirror from internet to intermediate registry (m2m)
		// 2. Sign images in intermediate registry
		// 3. Mirror from intermediate registry to disk (m2d with signatures)

		// Phase 1: Mirror to intermediate registry (m2m requires --workspace with file:// prefix)
		mirrorToIntermediateStep = pipelinev1.Step{
			Name:    "mirror-to-intermediate",
			Image:   mirrorImage,
			Command: []string{"oc-mirror"},
			Args: []string{
				"--v2",
				"--config=/workspace/config/" + configMapKey,
				"--authfile=/workspace/pull-secret/.dockerconfigjson",
				"--cache-dir=/workspace/output/.cache",
				"--workspace=file:///workspace/output",
				"docker://" + intermediateRegistry,
			},
			Env: envVars,
		}

		// Phase 3: Mirror FROM intermediate registry TO disk (will include signatures)
		mirrorFromIntermediateStep = pipelinev1.Step{
			Name:    "mirror-from-intermediate",
			Image:   mirrorImage,
			Command: []string{"/bin/sh", "-c"},
			Args: []string{`
set -e
echo "=== Creating ImageSetConfiguration to mirror FROM intermediate registry ==="

# Find the mapping file from dry-run
MAPPING_FILE="/workspace/output/working-dir/dry-run/mapping.txt"
if [ ! -f "$MAPPING_FILE" ]; then
  MAPPING_FILE=$(find /workspace/output/working-dir -name "mapping.txt" -type f 2>/dev/null | head -1)
fi

if [ -z "$MAPPING_FILE" ] || [ ! -f "$MAPPING_FILE" ]; then
  echo "ERROR: No mapping.txt found, cannot generate intermediate config"
  exit 1
fi

# Build ImageSetConfiguration with all images from intermediate registry
cat > /tmp/intermediate-config.yaml <<'CONFIGHEADER'
kind: ImageSetConfiguration
apiVersion: mirror.openshift.io/v2alpha1
mirror:
  additionalImages:
CONFIGHEADER

# Read mapping.txt and convert each dest image to point to intermediate registry
while IFS='=' read -r source dest; do
  [ -z "$source" ] || [[ "$source" =~ ^# ]] && continue

  # Extract destination path (remove docker:// prefix and localhost:55000)
  dest_no_proto="${dest#docker://}"
  intermediate_ref="${dest_no_proto//localhost:55000/` + intermediateRegistry + `}"

  # Add to config (oc-mirror additionalImages expects just name, not docker:// prefix)
  echo "    - name: $intermediate_ref" >> /tmp/intermediate-config.yaml
done < "$MAPPING_FILE"

echo "Generated intermediate config with $(grep -c 'name:' /tmp/intermediate-config.yaml) images"

echo "=== Mirroring from intermediate registry to disk (with signatures) ==="
oc-mirror --v2 \
  --config=/tmp/intermediate-config.yaml \
  --authfile=/workspace/pull-secret/.dockerconfigjson \
  --cache-dir=/workspace/output/.cache-from-intermediate \
  file:///workspace/output

echo "=== Mirror from intermediate complete ==="
`},
			Env: envVars,
		}
	} else {
		// Standard workflow: direct mirror to disk
		ocMirrorStep = pipelinev1.Step{
			Name:    "mirror",
			Image:   mirrorImage,
			Command: []string{"oc-mirror"},
			Args: []string{
				"--v2",
				"--config=/workspace/config/" + configMapKey,
				"--authfile=/workspace/pull-secret/.dockerconfigjson",
				"--cache-dir=/workspace/output/.cache",
				"file:///workspace/output",
			},
			Env: envVars,
		}
	}

	// Generate SBOM by scanning images from the oc-mirror cache during mirror operation
	// Prefer embedded SBOMs when available, fall back to Syft scanning
	// Also generate per-image SBOMs for attestation
	syftStep := pipelinev1.Step{
		Name:    "sbom",
		Image:   mirrorImage,
		Command: []string{"/bin/sh", "-c"},
		Args: []string{`
set -e
echo "=== Generating comprehensive SBOM from mirrored images ==="

# Set up registry authentication
export HOME=/tmp
mkdir -p $HOME/.docker
cp /workspace/pull-secret/.dockerconfigjson $HOME/.docker/config.json
export DOCKER_CONFIG=$HOME/.docker

# Use mapping.txt from dry-run step (it contains the complete list of images that were mirrored)
MAPPING_FILE="/workspace/output/working-dir/dry-run/mapping.txt"
if [ ! -f "$MAPPING_FILE" ]; then
  echo "Searching for mapping.txt..."
  MAPPING_FILE=$(find /workspace/output/working-dir -name "mapping.txt" -type f 2>/dev/null | head -1)
  if [ -z "$MAPPING_FILE" ]; then
    echo "No mapping.txt found, creating empty SBOM"
    echo '{"bomFormat":"CycloneDX","specVersion":"1.4","version":1,"metadata":{"component":{"type":"container","name":"mirror-collection"}},"components":[]}' > /workspace/output/sbom.cyclonedx.json
    exit 0
  fi
fi

echo "Found mapping file: $MAPPING_FILE"
image_count=$(wc -l < "$MAPPING_FILE")
echo "Total images in mapping: $image_count"

# Scan images from the persistent cache directory in the output workspace
# oc-mirror stores images in cache-dir/.oc-mirror/.cache with docker/registry/v2 structure
CACHE_DIR="/workspace/output/.cache/.oc-mirror/.cache"
echo "Using cache directory: $CACHE_DIR"

# Create SBOM cache directory for storing per-image SBOMs (keyed by digest)
SBOM_CACHE_DIR="/workspace/output/sbom-cache"
mkdir -p "$SBOM_CACHE_DIR"
mkdir -p /tmp/sboms

# Create per-image attestation directory for cosign
ATTESTATION_DIR="/workspace/output/attestations"
mkdir -p "$ATTESTATION_DIR"

echo "Step 1: Extracting SBOMs from images..."
echo "SBOM cache directory: $SBOM_CACHE_DIR"

# Determine if we're using intermediate registry or local sidecar
if [ -n "$INTERMEDIATE_REGISTRY" ]; then
  echo "Using intermediate registry: $INTERMEDIATE_REGISTRY"
  SCAN_REGISTRY="$INTERMEDIATE_REGISTRY"
else
  echo "Using local registry sidecar"
  SCAN_REGISTRY="localhost:5000"
  # Wait for registry sidecar to be ready
  echo "Waiting for local registry to start..."
  for i in {1..30}; do
    if curl -s http://localhost:5000/v2/ >/dev/null 2>&1; then
      echo "Registry is ready"
      break
    fi
    echo "  Waiting for registry... ($i/30)"
    sleep 2
  done
fi

scan_count=0
embedded_count=0
scanned_packages=0
current_image=0

mkdir -p /tmp/sboms

# Read mapping.txt and process each unique image from the local registry
while IFS='=' read -r source dest; do
  [ -z "$source" ] || [[ "$source" =~ ^# ]] && continue

  current_image=$((current_image + 1))

  # For intermediate registry: use dest path but replace localhost:55000 with intermediate registry
  # For local sidecar: use dest format docker://localhost:55000/... -> localhost:5000
  dest_no_proto="${dest#docker://}"
  if [ -n "$INTERMEDIATE_REGISTRY" ]; then
    # Scan from intermediate registry - use registry: transport for remote registry
    local_image="registry:${dest_no_proto//localhost:55000/$INTERMEDIATE_REGISTRY}"
  else
    # Scan from local sidecar
    local_image="registry:${dest_no_proto//localhost:55000/localhost:5000}"
  fi

  echo "  [$current_image/$image_count] Processing: $source"

  # Extract digest from dest (format: docker://localhost:55000/repo/path@sha256:digest)
  # If dest doesn't have digest, try source
  image_digest=$(echo "$dest" | grep -oP 'sha256:[a-f0-9]+' || echo "")
  if [ -z "$image_digest" ]; then
    image_digest=$(echo "$source" | grep -oP 'sha256:[a-f0-9]+' || echo "")
    if [ -n "$image_digest" ]; then
      echo "    [digest] Found in source: $image_digest"
    fi
  else
    echo "    [digest] Found in dest: $image_digest"
  fi

  sbom_file="/tmp/sboms/$(echo "$dest_no_proto" | tr '/:@' '_').json"
  found_sbom=false
  from_cache=false

  # Check cache first if we have a digest
  if [ -n "$image_digest" ]; then
    cached_sbom="$SBOM_CACHE_DIR/$(echo "$image_digest" | tr ':' '_').json"
    echo "    [cache] Looking for cached SBOM: $(basename "$cached_sbom")"
    if [ -f "$cached_sbom" ]; then
      echo "    [cache] Found cached SBOM, validating..."
      cache_size=$(stat -f%z "$cached_sbom" 2>/dev/null || stat -c%s "$cached_sbom" 2>/dev/null || echo 0)
      echo "    [cache] Source size: $cache_size bytes"
      if cp "$cached_sbom" "$sbom_file"; then
        temp_size=$(stat -f%z "$sbom_file" 2>/dev/null || stat -c%s "$sbom_file" 2>/dev/null || echo 0)
        echo "    [cache] Copied to temp: $(basename "$sbom_file") ($temp_size bytes)"
      else
        echo "    [cache] ✗ Copy to temp failed"
      fi
      pkg_count=$(jq '.components | length // 0' "$sbom_file" 2>/dev/null || echo 0)
      pkg_count=${pkg_count:-0}
      if [ "$pkg_count" -gt 0 ] 2>/dev/null; then
        echo "    ✓ Using cached SBOM ($pkg_count packages)"
        scan_count=$((scan_count + 1))
        scanned_packages=$((scanned_packages + pkg_count))
        found_sbom=true
        from_cache=true
      else
        echo "    [cache] Cached SBOM invalid (0 packages), will rescan"
      fi
    else
      echo "    [cache] No cached SBOM found, will scan"
    fi
  else
    echo "    [cache] No digest available, skipping cache check"
  fi

  # Try to extract embedded SBOM using cosign (try modern attestations first, then deprecated attachments)
  # Skip if we already found a valid SBOM from cache
  # (cosign doesn't use protocol prefix, just registry:port/path)
  if [ "$found_sbom" = false ]; then
    if [ -n "$INTERMEDIATE_REGISTRY" ]; then
      # For intermediate registry, use dest path with intermediate registry
      cosign_ref="${dest_no_proto//localhost:55000/$INTERMEDIATE_REGISTRY}"
    else
      # For local sidecar
      cosign_ref="${dest_no_proto//localhost:55000/localhost:5000}"
    fi

    # Try attestations (modern approach)
    # Note: DOCKER_CONFIG is already exported at the top of the script
    if cosign download attestation "$cosign_ref" --predicate-type=https://spdx.dev/Document > "$sbom_file" 2>/dev/null; then
    pkg_count=$(jq '.components | length // 0' "$sbom_file" 2>/dev/null || echo 0)
    pkg_count=${pkg_count:-0}
    if [ "$pkg_count" -gt 0 ] 2>/dev/null; then
      embedded_count=$((embedded_count + 1))
      scanned_packages=$((scanned_packages + pkg_count))
      echo "    ✓ Extracted SBOM attestation ($pkg_count packages)"
      found_sbom=true
    fi
  fi

  # Try deprecated SBOM attachments if attestation not found
  # Note: DOCKER_CONFIG is already exported at the top of the script
  if [ "$found_sbom" = false ] && cosign download sbom "$cosign_ref" > "$sbom_file" 2>/dev/null; then
    pkg_count=$(jq '.components | length // 0' "$sbom_file" 2>/dev/null || echo 0)
    pkg_count=${pkg_count:-0}
    if [ "$pkg_count" -gt 0 ] 2>/dev/null; then
      embedded_count=$((embedded_count + 1))
      scanned_packages=$((scanned_packages + pkg_count))
      echo "    ✓ Extracted SBOM attachment ($pkg_count packages)"
      found_sbom=true
    fi
  fi

  # If no embedded SBOM, scan with Syft using registry: transport
  # Note: DOCKER_CONFIG is already exported at the top of the script
  if [ "$found_sbom" = false ]; then
    syft_err_file="/tmp/syft_err_$$"
    if syft "$local_image" -o cyclonedx-json > "$sbom_file" 2>"$syft_err_file"; then
      pkg_count=$(jq '.components | length // 0' "$sbom_file" 2>/dev/null || echo 0)
      pkg_count=${pkg_count:-0}
      if [ "$pkg_count" -gt 0 ] 2>/dev/null; then
        scan_count=$((scan_count + 1))
        scanned_packages=$((scanned_packages + pkg_count))
        echo "    ✓ Scanned with Syft ($pkg_count packages)"
        found_sbom=true
      fi
      rm -f "$syft_err_file"
    else
      echo "    ✗ Syft scan failed for: $local_image"
      if [ -f "$syft_err_file" ] && [ -s "$syft_err_file" ]; then
        echo "    Error: $(head -3 "$syft_err_file")"
      fi
      rm -f "$syft_err_file"
    fi
  fi
  fi  # End of if [ "$found_sbom" = false ] for cosign/syft attempts

  # Cache the SBOM if we successfully generated/extracted one (but not if it came from cache)
  if [ "$from_cache" = true ]; then
    echo "    [cache] Skip write: already cached"
  elif [ -z "$image_digest" ]; then
    echo "    [cache] Skip: no digest available"
  elif [ ! -f "$sbom_file" ]; then
    echo "    [cache] Skip: SBOM file missing"
  elif [ "$found_sbom" != true ]; then
    echo "    [cache] Skip: no valid SBOM found"
  else
    cached_sbom="$SBOM_CACHE_DIR/$(echo "$image_digest" | tr ':' '_').json"
    echo "    [cache] Writing to: $(basename "$cached_sbom")"
    if cp "$sbom_file" "$cached_sbom"; then
      echo "    [cache] ✓ Successfully cached SBOM"
    else
      echo "    [cache] ✗ Cache write failed"
    fi
  fi

  # Always save per-image SBOM for cosign attestation (even if from cache, different images can share digests)
  if [ "$found_sbom" = true ]; then
    attestation_file="$ATTESTATION_DIR/$(echo "$dest_no_proto" | tr '/:@' '_').json"
    echo "    [attestation] Checking source: $(basename "$sbom_file")"
    if [ -f "$sbom_file" ]; then
      src_size=$(stat -f%z "$sbom_file" 2>/dev/null || stat -c%s "$sbom_file" 2>/dev/null || echo 0)
      echo "    [attestation] Source exists: $src_size bytes"
      if [ "$src_size" -gt 0 ]; then
        if cp "$sbom_file" "$attestation_file" 2>/dev/null; then
          echo "    [attestation] ✓ Saved for signing"
        else
          echo "    [attestation] ✗ Copy failed"
        fi
      else
        echo "    [attestation] ✗ Source file is 0 bytes"
      fi
    else
      echo "    [attestation] ✗ Source file does not exist"
    fi
  fi

  if [ "$found_sbom" = false ]; then
    echo "    ✗ Could not scan image, will include metadata only"
    rm -f "$sbom_file"
  fi
done < "$MAPPING_FILE"

echo "Processed images: $embedded_count embedded SBOMs, $scan_count Syft scans, $scanned_packages total packages"

# Step 2: Build SBOM from mapping.txt and scanned data
echo "Step 2: Building comprehensive SBOM..."

# Debug: Check how many SBOM files exist in /tmp/sboms
sbom_count=$(ls -1 /tmp/sboms/*.json 2>/dev/null | wc -l)
echo "Found $sbom_count SBOM files in /tmp/sboms/"

# First, collect all container components with nested packages
echo "Aggregating packages from all images..."
rm -f /tmp/container_components.jsonl

image_count_for_packages=0
total_packages=0

# Parse mapping.txt (format: source=dest)
while IFS='=' read -r source dest; do
  # Skip empty lines and comments
  [ -z "$source" ] || [[ "$source" =~ ^# ]] && continue

  image_count_for_packages=$((image_count_for_packages + 1))

  # Extract image details
  image_full="$source"
  if [[ "$image_full" =~ @sha256: ]]; then
    image_ref="${image_full##*/}"
    image_name="${image_ref%%@*}"
    version="${image_full##*@}"
  else
    image_ref="${image_full##*/}"
    image_name="${image_ref%%:*}"
    version="${image_ref##*:}"
    version="${version:-latest}"
  fi

  # Try to find package data from extracted/scanned SBOMs
  dest_no_proto="${dest#docker://}"
  sbom_file="/tmp/sboms/$(echo "$dest_no_proto" | tr '/:@' '_').json"
  if [ -f "$sbom_file" ]; then
    packages=$(jq '.components // []' "$sbom_file" 2>/dev/null || echo "[]")
    pkg_count=$(echo "$packages" | jq 'length // 0' 2>/dev/null || echo 0)
    pkg_count=${pkg_count:-0}
    if [ "$pkg_count" -gt 0 ] 2>/dev/null; then
      total_packages=$((total_packages + pkg_count))
      echo "  [$image_count_for_packages] Found $pkg_count packages in $image_name"

      # Create a container-image component with nested packages
      # Extract digest from dest (format: registry/path@sha256:...)
      image_digest=$(echo "$dest" | grep -oP 'sha256:[a-f0-9]+' || echo "")
      image_purl=""
      if [ -n "$image_digest" ]; then
        # Build OCI purl: pkg:oci/name@digest?repository_url=registry/path
        image_repo=$(echo "$dest" | sed -E 's|^[^/]+/||' | sed -E 's|@sha256:.*||')
        image_registry=$(echo "$dest" | sed -E 's|/.*||')
        image_purl="pkg:oci/${image_name}@${image_digest}?repository_url=${image_registry}/${image_repo}"
      fi

      # Build container component with nested packages
      # Use slurpfile to avoid ARG_MAX with large package arrays
      packages_temp="/tmp/packages_$image_count_for_packages.json"
      echo "$packages" > "$packages_temp"

      jq -n \
        --arg name "$image_name" \
        --arg version "$version" \
        --arg img "$image_full" \
        --arg purl "$image_purl" \
        --arg digest "$image_digest" \
        --slurpfile packages "$packages_temp" \
        '{
          "type": "container-image",
          "name": $name,
          "version": $version,
          "purl": $purl,
          "properties": [
            {"name": "container:image", "value": $img},
            {"name": "container:digest", "value": $digest}
          ],
          "components": $packages[0]
        }' >> /tmp/container_components.jsonl

      rm -f "$packages_temp"
    fi
  fi
done < "$MAPPING_FILE"

echo "Total packages collected: $total_packages from $image_count_for_packages images"

# Build final hierarchical SBOM with container components
# Use jq to construct the SBOM to avoid ARG_MAX issues
timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
version=$(basename /workspace/output)

# Combine all container components into array
container_components="[]"
if [ -f /tmp/container_components.jsonl ]; then
  container_components=$(jq -s '.' /tmp/container_components.jsonl)
fi

echo "$container_components" | jq \
  --arg timestamp "$timestamp" \
  --arg version "$version" \
  '{
    bomFormat: "CycloneDX",
    specVersion: "1.4",
    version: 1,
    metadata: {
      timestamp: $timestamp,
      component: {
        type: "container",
        name: "mirror-collection",
        version: $version
      }
    },
    components: .
  }' > /workspace/output/sbom.cyclonedx.json

component_count=$image_count_for_packages
scanned_packages=$total_packages

# Validate and report
echo "=== SBOM Generation Complete ==="
echo "  Total images: $component_count"
echo "  Total packages discovered: $scanned_packages"
if jq empty /workspace/output/sbom.cyclonedx.json 2>/dev/null; then
  echo "  SBOM JSON is valid"
else
  echo "  ERROR: SBOM JSON is invalid!"
  exit 1
fi

rm -rf /tmp/sboms
		`},
		Env: []corev1.EnvVar{
			{
				Name:  "SYFT_CACHE_DIR",
				Value: "/workspace/output/syft-cache",
			},
		},
	}

	// Build image signing step for signing container images and attaching SBOM attestations
	var signImagesStep *pipelinev1.Step
	if pipeline.Spec.Signing != nil && pipeline.Spec.Signing.Keyless != nil {
		// Keyless signing - sign container images with SBOM attestations
		tufURL := pipeline.Spec.Signing.Keyless.TUFURL
		if tufURL == "" {
			tufURL = "http://tuf.mirror-operator-system.svc"
		}

		// Determine target registry (intermediate if available, otherwise local cache)
		var registryTarget string
		if intermediateRegistry != "" {
			registryTarget = intermediateRegistry
		} else {
			registryTarget = "localhost:5000" // Local registry sidecar
		}

		signImagesStep = &pipelinev1.Step{
			Name:    "sign-images",
			Image:   mirrorImage,
			Command: []string{"/bin/sh", "-c"},
			Args: []string{`
set -e

# Set up registry authentication
export HOME=/tmp
mkdir -p $HOME/.docker
cp /workspace/pull-secret/.dockerconfigjson $HOME/.docker/config.json
export DOCKER_CONFIG=$HOME/.docker

echo "=== Initializing TUF root ==="
cosign initialize --mirror="$TUF_URL" --root="$TUF_URL/root.json"

echo "=== Signing container images in $REGISTRY_TARGET ==="

# Debug: Check attestation files
total_att=$(ls -1 /workspace/output/attestations/*.json 2>/dev/null | wc -l)
nonzero_att=$(find /workspace/output/attestations -name "*.json" -size +0 2>/dev/null | wc -l)
echo "Attestation files: $total_att total, $nonzero_att non-zero"

# Find the mapping file from dry-run
MAPPING_FILE="/workspace/output/working-dir/dry-run/mapping.txt"
if [ ! -f "$MAPPING_FILE" ]; then
  MAPPING_FILE=$(find /workspace/output/working-dir -name "mapping.txt" -type f 2>/dev/null | head -1)
fi

if [ -z "$MAPPING_FILE" ] || [ ! -f "$MAPPING_FILE" ]; then
  echo "No mapping.txt found, skipping image signing"
  exit 0
fi

signed_count=0
attested_count=0
total_images=$(wc -l < "$MAPPING_FILE")

echo "Processing $total_images images from mapping file..."

while IFS='=' read -r source dest; do
  [ -z "$source" ] || [[ "$source" =~ ^# ]] && continue

  # Construct target image reference
  # For intermediate registry: use dest path but replace localhost:55000 with intermediate registry
  # For local cache: use localhost:5000
  dest_no_proto="${dest#docker://}"
  if [ "$REGISTRY_TARGET" = "localhost:5000" ]; then
    # Local cache mode - convert to localhost:5000
    image_ref="${dest_no_proto//localhost:55000/localhost:5000}"
  else
    # Intermediate registry mode - replace localhost:55000 with intermediate registry
    image_ref="${dest_no_proto//localhost:55000/$REGISTRY_TARGET}"
  fi

  # Convert tag references to digests for safer signing
  if [[ "$image_ref" =~ :[^@]+$ ]] && [[ ! "$image_ref" =~ @ ]]; then
    echo "  Resolving tag to digest for: $image_ref"
    digest=$(skopeo inspect --format '{{.Digest}}' "docker://$image_ref" 2>/dev/null || echo "")
    if [ -n "$digest" ]; then
      # Replace tag with digest
      image_ref="${image_ref%%:*}@$digest"
      echo "    Resolved to: $image_ref"
    else
      echo "    Warning: Could not resolve digest, signing with tag"
    fi
  fi

  echo "  Signing: $image_ref"

  # Sign the image
  if cosign sign \
    --fulcio-url "$FULCIO_URL" \
    --rekor-url "$REKOR_URL" \
    --fulcio-auth-flow=client_credentials \
    --oidc-issuer "$COSIGN_OIDC_ISSUER" \
    --oidc-client-id "$COSIGN_OIDC_CLIENT_ID" \
    --oidc-client-secret-file /workspace/oidc-secret/clientSecret \
    --yes \
    "$image_ref" 2>&1; then
    signed_count=$((signed_count + 1))
    echo "    ✓ Signed"
  else
    echo "    ✗ Failed to sign: $image_ref"
  fi

  # Attach SBOM attestation if available
  dest_no_proto="${dest#docker://}"
  attestation_file="/workspace/output/attestations/$(echo "$dest_no_proto" | tr '/:@' '_').json"
  echo "    [attestation] Checking: $(basename "$attestation_file")"
  if [ -f "$attestation_file" ]; then
    att_size=$(stat -f%z "$attestation_file" 2>/dev/null || stat -c%s "$attestation_file" 2>/dev/null || echo 0)
    echo "    [attestation] Found: $att_size bytes"
    if [ "$att_size" -gt 0 ]; then
      echo "    [attestation] Attaching SBOM attestation..."
      if cosign attest \
        --fulcio-url "$FULCIO_URL" \
        --rekor-url "$REKOR_URL" \
        --fulcio-auth-flow=client_credentials \
        --oidc-issuer "$COSIGN_OIDC_ISSUER" \
        --oidc-client-id "$COSIGN_OIDC_CLIENT_ID" \
        --oidc-client-secret-file /workspace/oidc-secret/clientSecret \
        --yes \
        --type cyclonedx \
        --predicate "$attestation_file" \
        "$image_ref" 2>&1; then
        attested_count=$((attested_count + 1))
        echo "    ✓ SBOM attestation attached"
      else
        echo "    ✗ Failed to attach SBOM attestation"
      fi
    else
      echo "    [attestation] ✗ File is 0 bytes, skipping"
    fi
  else
    echo "    [attestation] ✗ File not found"
  fi

done < "$MAPPING_FILE"

echo "=== Image Signing Complete ==="
echo "  Registry: $REGISTRY_TARGET"
echo "  Total images: $total_images"
echo "  Successfully signed: $signed_count"
echo "  SBOM attestations attached: $attested_count"
`},
			Env: []corev1.EnvVar{
				{Name: "COSIGN_EXPERIMENTAL", Value: "1"},
				{Name: "COSIGN_OIDC_ISSUER", Value: pipeline.Spec.Signing.Keyless.OIDCIssuer},
				{Name: "COSIGN_OIDC_CLIENT_ID", Value: pipeline.Spec.Signing.Keyless.OIDCClientID},
				{Name: "FULCIO_URL", Value: pipeline.Spec.Signing.Keyless.FulcioURL},
				{Name: "REKOR_URL", Value: pipeline.Spec.Signing.Keyless.RekorURL},
				{Name: "TUF_URL", Value: tufURL},
				{Name: "REGISTRY_TARGET", Value: registryTarget},
			},
		}
	}

	// Build bundle signing step based on configuration
	var cosignSignStep pipelinev1.Step
	if pipeline.Spec.Signing != nil && pipeline.Spec.Signing.Keyless != nil {
		// Keyless signing with Fulcio using client credentials flow
		// Initialize TUF root first, then sign blobs
		tufURL := pipeline.Spec.Signing.Keyless.TUFURL
		if tufURL == "" {
			tufURL = "http://tuf.mirror-operator-system.svc"
		}

		cosignSignStep = pipelinev1.Step{
			Name:    "sign-bundles",
			Image:   mirrorImage,
			Command: []string{"/bin/sh", "-c"},
			Args:    []string{`echo "=== Initializing TUF root ==="; cosign initialize --mirror="$TUF_URL" --root="$TUF_URL/root.json"; echo "=== Signing bundles ==="; for f in /workspace/output/*.tar; do [ -f "$f" ] || continue; bn=$(basename "$f" .tar); cosign sign-blob --fulcio-url "$FULCIO_URL" --rekor-url "$REKOR_URL" --fulcio-auth-flow=client_credentials --oidc-issuer "$COSIGN_OIDC_ISSUER" --oidc-client-id "$COSIGN_OIDC_CLIENT_ID" --oidc-client-secret-file /workspace/oidc-secret/clientSecret --yes "$f" --bundle "/workspace/output/${bn}.bundle"; done; echo "=== Generating attestation ==="; bh=$(sha256sum /workspace/output/*.tar 2>/dev/null | head -1 | cut -d' ' -f1); if [ -z "$bh" ]; then exit 0; fi; sh=$(sha256sum /workspace/output/sbom.cyclonedx.json 2>/dev/null | cut -d' ' -f1 || echo ""); if [ -z "$sh" ]; then exit 0; fi; printf '{"bundle":{"sha256":"%s"},"sbom":{"sha256":"%s"}}\n' "$bh" "$sh" > /workspace/output/attestation.json; echo "=== Signing attestation ==="; cosign sign-blob --fulcio-url "$FULCIO_URL" --rekor-url "$REKOR_URL" --fulcio-auth-flow=client_credentials --oidc-issuer "$COSIGN_OIDC_ISSUER" --oidc-client-id "$COSIGN_OIDC_CLIENT_ID" --oidc-client-secret-file /workspace/oidc-secret/clientSecret --yes /workspace/output/attestation.json --bundle /workspace/output/attestation.bundle`},
			Env: []corev1.EnvVar{
				{Name: "COSIGN_OIDC_ISSUER", Value: pipeline.Spec.Signing.Keyless.OIDCIssuer},
				{Name: "COSIGN_OIDC_CLIENT_ID", Value: pipeline.Spec.Signing.Keyless.OIDCClientID},
				{Name: "FULCIO_URL", Value: pipeline.Spec.Signing.Keyless.FulcioURL},
				{Name: "REKOR_URL", Value: pipeline.Spec.Signing.Keyless.RekorURL},
				{Name: "TUF_URL", Value: tufURL},
			},
		}
	} else {
		// Legacy key-based signing
		cosignSignStep = pipelinev1.Step{
			Name:    "sign-bundles",
			Image:   mirrorImage,
			Command: []string{"/bin/sh", "-c"},
			Args:    []string{`for f in /workspace/output/*.tar; do [ -f "$f" ] || continue; bn=$(basename "$f" .tar); cosign sign-blob --key /workspace/cosign-key/cosign.key "$f" --output-signature "/workspace/output/${bn}.sig"; done; bh=$(sha256sum /workspace/output/*.tar 2>/dev/null | head -1 | cut -d' ' -f1); if [ -z "$bh" ]; then exit 0; fi; sh=$(sha256sum /workspace/output/sbom.cyclonedx.json 2>/dev/null | cut -d' ' -f1 || echo ""); if [ -z "$sh" ]; then exit 0; fi; printf '{"bundle":{"sha256":"%s"},"sbom":{"sha256":"%s"}}\n' "$bh" "$sh" > /workspace/output/attestation.json; cosign sign-blob --key /workspace/cosign-key/cosign.key /workspace/output/attestation.json --output-signature /workspace/output/attestation.json.sig`},
		}
	}

	// SBOM upload step - get TPA URL
	var uploadSbomStep *pipelinev1.Step
	tpaHostname, keycloakHost, tpaNamespace := r.getTPAAndKeycloakHosts(ctx)
	if tpaHostname != "" && keycloakHost != "" {
		// Use internal service to avoid ingress timeout (99MB compressed SBOM takes time to process)
		tpaURL := fmt.Sprintf("https://server.%s.svc.cluster.local/api/v2/sbom", tpaNamespace)
		tokenURL := fmt.Sprintf("https://%s/realms/trustify/protocol/openid-connect/token", keycloakHost)

		uploadSbomStep = &pipelinev1.Step{
			Name:    "upload-sbom",
			Image:   mirrorImage,
			Command: []string{"/bin/sh", "-c"},
			Args: []string{fmt.Sprintf(`
set -e

if [ ! -f /workspace/output/sbom.cyclonedx.json ]; then
  echo "SBOM file not found, skipping upload"
  exit 0
fi

# Check SBOM size and compress if large
SBOM_SIZE=$(stat -f%%z /workspace/output/sbom.cyclonedx.json 2>/dev/null || stat -c%%s /workspace/output/sbom.cyclonedx.json)
echo "SBOM size: $(numfmt --to=iec-i --suffix=B $SBOM_SIZE 2>/dev/null || echo $SBOM_SIZE bytes)"

# Compress SBOM with gzip for upload
echo "Compressing SBOM..."
gzip -c /workspace/output/sbom.cyclonedx.json > /tmp/sbom.cyclonedx.json.gz
COMPRESSED_SIZE=$(stat -f%%z /tmp/sbom.cyclonedx.json.gz 2>/dev/null || stat -c%%s /tmp/sbom.cyclonedx.json.gz)
echo "Compressed size: $(numfmt --to=iec-i --suffix=B $COMPRESSED_SIZE 2>/dev/null || echo $COMPRESSED_SIZE bytes)"

echo "Getting OIDC token..."
TOKEN_RESPONSE=$(curl -k -s -X POST "%s" \
  -d "grant_type=client_credentials" \
  -d "client_id=cli" \
  -d "client_secret=$(cat /workspace/tpa-oidc-secret/clientSecret)")

ACCESS_TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.access_token')
if [ -z "$ACCESS_TOKEN" ] || [ "$ACCESS_TOKEN" = "null" ]; then
  echo "Failed to get access token: $TOKEN_RESPONSE"
  exit 1
fi

echo "Uploading compressed SBOM to TPA via internal service (timeout: 300s)..."
HTTP_CODE=$(curl -k -s --max-time 300 -w "%%{http_code}" -o /tmp/upload_response.txt -X POST "%s" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -H "Content-Encoding: gzip" \
  --data-binary @/tmp/sbom.cyclonedx.json.gz)

if [ "$HTTP_CODE" -ge 200 ] && [ "$HTTP_CODE" -lt 300 ]; then
  echo "SBOM uploaded successfully (HTTP $HTTP_CODE)"
  cat /tmp/upload_response.txt
else
  echo "SBOM upload failed with HTTP $HTTP_CODE"
  cat /tmp/upload_response.txt
  exit 1
fi
			`, tokenURL, tpaURL)},
		}
	}

	// Build task pipeline based on signing configuration and intermediate registry availability
	var pipelineTasks []pipelinev1.PipelineTask

	if hasKeylessSigning && intermediateRegistry != "" {
		// Intermediate registry workflow with keyless signing:
		// 1. dry-run (fast mapping generation)
		// 2. mirror-to-intermediate (m2m: internet -> intermediate quay)
		// 3. syft-sbom (generate SBOMs from intermediate registry)
		// 4. sign-images (sign images IN intermediate registry + attach SBOM attestations)
		// 5. mirror-from-intermediate (m2d: intermediate quay -> disk, includes signatures!)
		// 6. sign-bundles (sign the tar file)

		// Step 1: dry-run for mapping
		pipelineTasks = append(pipelineTasks, pipelinev1.PipelineTask{
			Name: "dry-run",
			TaskSpec: &pipelinev1.EmbeddedTask{
				TaskSpec: pipelinev1.TaskSpec{
					Steps: []pipelinev1.Step{dryRunStep},
				},
			},
			Workspaces: taskWorkspaceBindings,
		})

		// Step 2: Mirror to intermediate registry
		pipelineTasks = append(pipelineTasks, pipelinev1.PipelineTask{
			Name:     "mirror-to-intermediate",
			RunAfter: []string{"dry-run"},
			TaskSpec: &pipelinev1.EmbeddedTask{
				TaskSpec: pipelinev1.TaskSpec{
					Steps: []pipelinev1.Step{mirrorToIntermediateStep},
				},
			},
			Workspaces: taskWorkspaceBindings,
		})

		// Step 3: SBOM generation (from intermediate registry)
		// Create a modified syftStep with INTERMEDIATE_REGISTRY env var
		syftStepWithRegistry := syftStep
		syftStepWithRegistry.Env = append(syftStepWithRegistry.Env, corev1.EnvVar{
			Name:  "INTERMEDIATE_REGISTRY",
			Value: intermediateRegistry,
		})
		pipelineTasks = append(pipelineTasks, pipelinev1.PipelineTask{
			Name:     "syft-sbom",
			RunAfter: []string{"mirror-to-intermediate"},
			TaskSpec: &pipelinev1.EmbeddedTask{
				TaskSpec: pipelinev1.TaskSpec{
					Steps: []pipelinev1.Step{syftStepWithRegistry},
				},
			},
			Workspaces: taskWorkspaceBindings,
		})

		// Step 4: Sign images in intermediate registry
		signImagesWorkspaces := append(taskWorkspaceBindings, pipelinev1.WorkspacePipelineTaskBinding{Name: "oidc-secret", Workspace: "oidc-secret"})
		pipelineTasks = append(pipelineTasks, pipelinev1.PipelineTask{
			Name:     "sign-images",
			RunAfter: []string{"syft-sbom"},
			TaskSpec: &pipelinev1.EmbeddedTask{
				TaskSpec: pipelinev1.TaskSpec{
					Steps: []pipelinev1.Step{*signImagesStep},
				},
			},
			Workspaces: signImagesWorkspaces,
		})

		// Step 5: Mirror FROM intermediate TO disk (with signatures!)
		pipelineTasks = append(pipelineTasks, pipelinev1.PipelineTask{
			Name:     "mirror-from-intermediate",
			RunAfter: []string{"sign-images"},
			TaskSpec: &pipelinev1.EmbeddedTask{
				TaskSpec: pipelinev1.TaskSpec{
					Steps: []pipelinev1.Step{mirrorFromIntermediateStep},
				},
			},
			Workspaces: taskWorkspaceBindings,
		})

		// Step 6: Sign the tar bundle
		signBundlesWorkspaces := []pipelinev1.WorkspacePipelineTaskBinding{outputWorkspaceBinding}
		if hasKeylessSigning {
			signBundlesWorkspaces = append(signBundlesWorkspaces, pipelinev1.WorkspacePipelineTaskBinding{Name: "oidc-secret", Workspace: "oidc-secret"})
		}
		pipelineTasks = append(pipelineTasks, pipelinev1.PipelineTask{
			Name:     "sign-bundles",
			RunAfter: []string{"mirror-from-intermediate"},
			TaskSpec: &pipelinev1.EmbeddedTask{
				TaskSpec: pipelinev1.TaskSpec{
					Steps: []pipelinev1.Step{cosignSignStep},
				},
			},
			Workspaces: signBundlesWorkspaces,
		})

	} else if hasKeylessSigning {
		// Local cache workflow with keyless signing (fallback if no intermediate registry):
		// 1. dry-run (fast mapping generation)
		// 2. oc-mirror (populate cache)
		// 3. syft-sbom (generate SBOMs for attestation)
		// 4. sign-images (sign images + attach SBOM attestations in local cache)
		// 5. create-archive (tar with signed images)
		// 6. sign-bundles (sign the tar file)

		// Step 1: dry-run for mapping
		pipelineTasks = append(pipelineTasks, pipelinev1.PipelineTask{
			Name: "dry-run",
			TaskSpec: &pipelinev1.EmbeddedTask{
				TaskSpec: pipelinev1.TaskSpec{
					Steps: []pipelinev1.Step{dryRunStep},
				},
			},
			Workspaces: taskWorkspaceBindings,
		})

		// Step 2: Mirror to local cache
		pipelineTasks = append(pipelineTasks, pipelinev1.PipelineTask{
			Name:     "oc-mirror",
			RunAfter: []string{"dry-run"},
			TaskSpec: &pipelinev1.EmbeddedTask{
				TaskSpec: pipelinev1.TaskSpec{
					Steps: []pipelinev1.Step{ocMirrorStep},
				},
			},
			Workspaces: taskWorkspaceBindings,
		})

		// Step 3: SBOM generation (from local cache)
		pipelineTasks = append(pipelineTasks, pipelinev1.PipelineTask{
			Name:     "syft-sbom",
			RunAfter: []string{"oc-mirror"},
			TaskSpec: &pipelinev1.EmbeddedTask{
				TaskSpec: pipelinev1.TaskSpec{
					Steps: []pipelinev1.Step{syftStep},
					Sidecars: []pipelinev1.Sidecar{
						{
							Name:  "registry",
							Image: "registry:2",
							Env: []corev1.EnvVar{
								{
									Name:  "REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY",
									Value: "/workspace/output/.cache/.oc-mirror/.cache",
								},
								{
									Name:  "REGISTRY_HTTP_ADDR",
									Value: "0.0.0.0:5000",
								},
							},
						},
					},
				},
			},
			Workspaces: taskWorkspaceBindings,
		})

		// Step 4: Sign images in local cache
		pipelineTasks = append(pipelineTasks, pipelinev1.PipelineTask{
			Name:     "sign-images",
			RunAfter: []string{"syft-sbom"},
			TaskSpec: &pipelinev1.EmbeddedTask{
				TaskSpec: pipelinev1.TaskSpec{
					Steps: []pipelinev1.Step{*signImagesStep},
					Sidecars: []pipelinev1.Sidecar{
						{
							Name:  "registry",
							Image: "registry:2",
							Env: []corev1.EnvVar{
								{
									Name:  "REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY",
									Value: "/workspace/output/.cache/.oc-mirror/.cache",
								},
								{
									Name:  "REGISTRY_HTTP_ADDR",
									Value: "0.0.0.0:5000",
								},
							},
						},
					},
				},
			},
			Workspaces: taskWorkspaceBindings,
		})

		// Note: Signatures in local cache won't be included in tar created by oc-mirror
		// This fallback workflow is less ideal - recommend using intermediate registry

		// Step 5: Sign the tar bundle
		pipelineTasks = append(pipelineTasks, pipelinev1.PipelineTask{
			Name:     "sign-bundles",
			RunAfter: []string{"sign-images"},
			TaskSpec: &pipelinev1.EmbeddedTask{
				TaskSpec: pipelinev1.TaskSpec{
					Steps: []pipelinev1.Step{cosignSignStep},
				},
			},
			Workspaces: []pipelinev1.WorkspacePipelineTaskBinding{outputWorkspaceBinding},
		})
	} else {
		// Legacy workflow (no keyless signing):
		// 1. dry-run
		// 2. oc-mirror (creates archive)
		// 3. syft-sbom
		// 4. sign-bundles (tar only)

		pipelineTasks = []pipelinev1.PipelineTask{
			{
				Name: "dry-run",
				TaskSpec: &pipelinev1.EmbeddedTask{
					TaskSpec: pipelinev1.TaskSpec{
						Steps: []pipelinev1.Step{dryRunStep},
					},
				},
				Workspaces: taskWorkspaceBindings,
			},
			{
				Name:     "oc-mirror",
				RunAfter: []string{"dry-run"},
				TaskSpec: &pipelinev1.EmbeddedTask{
					TaskSpec: pipelinev1.TaskSpec{
						Steps: []pipelinev1.Step{ocMirrorStep},
					},
				},
				Workspaces: taskWorkspaceBindings,
			},
			{
				Name:     "syft-sbom",
				RunAfter: []string{"oc-mirror"},
				TaskSpec: &pipelinev1.EmbeddedTask{
					TaskSpec: pipelinev1.TaskSpec{
						Steps: []pipelinev1.Step{syftStep},
						Sidecars: []pipelinev1.Sidecar{
							{
								Name:  "registry",
								Image: "registry:2",
								Env: []corev1.EnvVar{
									{
										Name:  "REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY",
										Value: "/workspace/output/.cache/.oc-mirror/.cache",
									},
									{
										Name:  "REGISTRY_HTTP_ADDR",
										Value: "0.0.0.0:5000",
									},
								},
							},
						},
					},
				},
				Workspaces: taskWorkspaceBindings,
			},
			{
				Name:     "sign-bundles",
				RunAfter: []string{"syft-sbom"},
				TaskSpec: &pipelinev1.EmbeddedTask{
					TaskSpec: pipelinev1.TaskSpec{
						Steps: []pipelinev1.Step{cosignSignStep},
					},
				},
				Workspaces: []pipelinev1.WorkspacePipelineTaskBinding{outputWorkspaceBinding},
			},
		}
	}

	cosignTasks := pipelineTasks

	// Add SBOM upload task if TPA is available
	if uploadSbomStep != nil {
		uploadTask := pipelinev1.PipelineTask{
			Name:     "upload-sbom",
			RunAfter: []string{"sign-bundles"},
			TaskSpec: &pipelinev1.EmbeddedTask{
				TaskSpec: pipelinev1.TaskSpec{
					Steps: []pipelinev1.Step{*uploadSbomStep},
				},
			},
			Workspaces: []pipelinev1.WorkspacePipelineTaskBinding{
				outputWorkspaceBinding,
				{Name: "tpa-oidc-secret", Workspace: "tpa-oidc-secret"},
			},
		}
		cosignTasks = append(cosignTasks, uploadTask)

		// Add TPA OIDC secret workspace binding (using CLI client)
		tpaOidcSecretBinding := pipelinev1.WorkspaceBinding{
			Name: "tpa-oidc-secret",
			Secret: &corev1.SecretVolumeSource{
				SecretName: "rhtpa-oidc-cli-secret",
			},
		}
		bindings = append(bindings, tpaOidcSecretBinding)
		declaredWorkspaces = append(declaredWorkspaces, pipelinev1.PipelineWorkspaceDeclaration{Name: "tpa-oidc-secret"})
	}

	// OIDC secret workspace is added later at line 1426-1438, so skip duplicate here

	if pipeline.Spec.Signing != nil && pipeline.Spec.Signing.KeySecretRef != nil {
		cosignKeyBinding := pipelinev1.WorkspaceBinding{
			Name: "cosign-key",
			Secret: &corev1.SecretVolumeSource{
				SecretName: pipeline.Spec.Signing.KeySecretRef.Name,
			},
		}
		bindings = append(bindings, cosignKeyBinding)
		declaredWorkspaces = append(declaredWorkspaces, pipelinev1.PipelineWorkspaceDeclaration{Name: "cosign-key"})
		// Find the bundle signing task and add cosign-key workspace
		for i := range cosignTasks {
			if cosignTasks[i].Name == "sign-bundles" {
				cosignTasks[i].Workspaces = append(cosignTasks[i].Workspaces, pipelinev1.WorkspacePipelineTaskBinding{Name: "cosign-key", Workspace: "cosign-key"})
				break
			}
		}
	}

	if pipeline.Spec.Signing != nil && pipeline.Spec.Signing.PasswordSecretRef != nil {
		cosignTasks[3].TaskSpec.TaskSpec.Steps[0].Env = append(
			cosignTasks[3].TaskSpec.TaskSpec.Steps[0].Env,
			corev1.EnvVar{
				Name: "COSIGN_PASSWORD",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: *pipeline.Spec.Signing.PasswordSecretRef,
						Key:                  "password",
					},
				},
			},
		)
	}

	// Mount OIDC client secret as a workspace for keyless signing
	// Cosign requires --oidc-client-secret-file flag pointing to a file, not an env var
	if pipeline.Spec.Signing != nil && pipeline.Spec.Signing.Keyless != nil && pipeline.Spec.Signing.Keyless.OIDCClientSecret != nil {
		oidcSecretBinding := pipelinev1.WorkspaceBinding{
			Name: "oidc-secret",
			Secret: &corev1.SecretVolumeSource{
				SecretName: pipeline.Spec.Signing.Keyless.OIDCClientSecret.Name,
			},
		}
		bindings = append(bindings, oidcSecretBinding)
		declaredWorkspaces = append(declaredWorkspaces, pipelinev1.PipelineWorkspaceDeclaration{Name: "oidc-secret"})

		// Add oidc-secret workspace to sign-images task (only for local cache workflow)
		// For intermediate registry workflow, it's already added at line 1241
		if intermediateRegistry == "" {
			for i := range cosignTasks {
				if cosignTasks[i].Name == "sign-images" {
					cosignTasks[i].Workspaces = append(cosignTasks[i].Workspaces, pipelinev1.WorkspacePipelineTaskBinding{Name: "oidc-secret", Workspace: "oidc-secret"})
					break
				}
			}
		}
	}

	pipelineRunNamePrefix := fmt.Sprintf("collection-pipeline-%s-", pipeline.Name)

	// Set timeout - default to 12 hours if not specified
	timeout := &metav1.Duration{Duration: 12 * time.Hour}
	if pipeline.Spec.Timeout != nil {
		timeout = pipeline.Spec.Timeout
	}

	pr := &pipelinev1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: pipelineRunNamePrefix,
			Namespace:    pipeline.Namespace,
			Annotations: map[string]string{
				"results.tekton.dev/log":    "false", // Don't store logs in Results database
				"results.tekton.dev/result": "false", // Don't store results in Results database
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(pipeline, mirrorv1.GroupVersion.WithKind("CollectionPipeline")),
			},
		},
		Spec: pipelinev1.PipelineRunSpec{
			PipelineSpec: &pipelinev1.PipelineSpec{
				Workspaces: declaredWorkspaces,
				Tasks:      cosignTasks,
			},
			Workspaces: bindings,
			Timeouts: &pipelinev1.TimeoutFields{
				Pipeline: timeout,
			},
		},
	}

	return pr, nil
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
	tpaNamespace := tpa.GetNamespace()

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

	// Get Keycloak hostname
	ingress := &unstructured.Unstructured{}
	ingress.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "Ingress",
	})
	ingress.SetName("cluster")
	if err := r.Get(ctx, client.ObjectKeyFromObject(ingress), ingress); err != nil {
		logger.Error(err, "failed to get cluster ingress")
		return tpaHostname, "", tpaNamespace
	}

	domain, _, _ := unstructured.NestedString(ingress.Object, "spec", "domain")
	keycloakHost := "keycloak." + domain

	return tpaHostname, keycloakHost, tpaNamespace
}

// generateIntermediateImageSetConfig creates an ImageSetConfiguration that pulls from the intermediate registry
func (r *CollectionPipelineReconciler) generateIntermediateImageSetConfig(pipeline *mirrorv1.CollectionPipeline, intermediateRegistry string) string {
	// Parse the original config to understand what was mirrored
	// For now, we'll create a simple config that mirrors everything from the intermediate registry
	// This is a simplified version - in production you'd want to parse the original config
	// and rewrite all image references to point to the intermediate registry

	return `kind: ImageSetConfiguration
apiVersion: mirror.openshift.io/v2alpha1
mirror:
  additionalImages:
    - name: ` + intermediateRegistry + `/*
`
	// TODO: Properly parse original ImageSetConfiguration and rewrite registry references
	// This requires more sophisticated logic to handle:
	// - platform.release paths
	// - operator catalog references
	// - additionalImages
	// - helm charts
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

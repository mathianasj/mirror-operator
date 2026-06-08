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

	// Actual mirror step
	ocMirrorStep := pipelinev1.Step{
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

	// Generate SBOM by scanning images from the oc-mirror cache during mirror operation
	// Prefer embedded SBOMs when available, fall back to Syft scanning
	syftStep := pipelinev1.Step{
		Name:    "sbom",
		Image:   mirrorImage,
		Command: []string{"/bin/sh", "-c"},
		Args: []string{`
set -e
echo "=== Generating comprehensive SBOM from mirrored images ==="

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

echo "Step 1: Extracting SBOMs from images using local registry sidecar..."
echo "SBOM cache directory: $SBOM_CACHE_DIR"

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

scan_count=0
embedded_count=0
scanned_packages=0
current_image=0

mkdir -p /tmp/sboms

# Read mapping.txt and process each unique image from the local registry
while IFS='=' read -r source dest; do
  [ -z "$source" ] || [[ "$source" =~ ^# ]] && continue

  current_image=$((current_image + 1))

  # The dest format is: docker://localhost:55000/openshift/release@sha256:xxx
  # Strip docker:// and change to registry: transport, port 55000 -> 5000
  dest_no_proto="${dest#docker://}"
  local_image="registry:${dest_no_proto//localhost:55000/localhost:5000}"

  echo "  [$current_image/$image_count] Processing: $source"

  # Extract digest from dest (format: docker://localhost:55000/repo/path@sha256:digest)
  image_digest=$(echo "$dest" | grep -oP 'sha256:[a-f0-9]+' || echo "")

  sbom_file="/tmp/sboms/$(echo "$dest_no_proto" | tr '/:@' '_').json"
  found_sbom=false

  # Check cache first if we have a digest
  if [ -n "$image_digest" ]; then
    cached_sbom="$SBOM_CACHE_DIR/$(echo "$image_digest" | tr ':' '_').json"
    if [ -f "$cached_sbom" ]; then
      cp "$cached_sbom" "$sbom_file"
      pkg_count=$(jq '.components | length // 0' "$sbom_file" 2>/dev/null || echo 0)
      if [ "$pkg_count" -gt 0 ]; then
        echo "    ✓ Using cached SBOM ($pkg_count packages)"
        scan_count=$((scan_count + 1))
        scanned_packages=$((scanned_packages + pkg_count))
        found_sbom=true
      fi
    fi
  fi

  # Try to extract embedded SBOM using cosign (try modern attestations first, then deprecated attachments)
  # (cosign doesn't use protocol prefix, just registry:port/path)
  cosign_ref="${dest_no_proto//localhost:55000/localhost:5000}"

  # Try attestations (modern approach)
  if cosign download attestation "$cosign_ref" --predicate-type=https://spdx.dev/Document > "$sbom_file" 2>/dev/null; then
    pkg_count=$(jq '.components | length // 0' "$sbom_file" 2>/dev/null || echo 0)
    if [ "$pkg_count" -gt 0 ]; then
      embedded_count=$((embedded_count + 1))
      scanned_packages=$((scanned_packages + pkg_count))
      echo "    ✓ Extracted SBOM attestation ($pkg_count packages)"
      found_sbom=true
    fi
  fi

  # Try deprecated SBOM attachments if attestation not found
  if [ "$found_sbom" = false ] && cosign download sbom "$cosign_ref" > "$sbom_file" 2>/dev/null; then
    pkg_count=$(jq '.components | length // 0' "$sbom_file" 2>/dev/null || echo 0)
    if [ "$pkg_count" -gt 0 ]; then
      embedded_count=$((embedded_count + 1))
      scanned_packages=$((scanned_packages + pkg_count))
      echo "    ✓ Extracted SBOM attachment ($pkg_count packages)"
      found_sbom=true
    fi
  fi

  # If no embedded SBOM, scan with Syft using registry: transport
  if [ "$found_sbom" = false ]; then
    if syft "$local_image" -o cyclonedx-json > "$sbom_file" 2>&1; then
      pkg_count=$(jq '.components | length // 0' "$sbom_file" 2>/dev/null || echo 0)
      if [ "$pkg_count" -gt 0 ]; then
        scan_count=$((scan_count + 1))
        scanned_packages=$((scanned_packages + pkg_count))
        echo "    ✓ Scanned with Syft ($pkg_count packages)"
        found_sbom=true
      fi
    else
      echo "    ✗ Syft scan failed"
    fi
  fi

  # Cache the SBOM if we successfully generated/extracted one
  if [ "$found_sbom" = true ] && [ -n "$image_digest" ] && [ -f "$sbom_file" ]; then
    cached_sbom="$SBOM_CACHE_DIR/$(echo "$image_digest" | tr ':' '_').json"
    cp "$sbom_file" "$cached_sbom" 2>/dev/null || true
  fi

  if [ "$found_sbom" = false ]; then
    echo "    ✗ Could not scan image, will include metadata only"
    rm -f "$sbom_file"
  fi
done < "$MAPPING_FILE"

echo "Processed images: $embedded_count embedded SBOMs, $scan_count Syft scans, $scanned_packages total packages"

# Step 2: Build SBOM from mapping.txt and scanned data
echo "Step 2: Building comprehensive SBOM..."

cat > /workspace/output/sbom.cyclonedx.json <<'SBOM_HEADER'
{
  "bomFormat": "CycloneDX",
  "specVersion": "1.4",
  "version": 1,
  "metadata": {
    "timestamp": "TIMESTAMP_PLACEHOLDER",
    "component": {
      "type": "container",
      "name": "mirror-collection",
      "version": "VERSION_PLACEHOLDER"
    }
  },
  "components": [
SBOM_HEADER

first=true
component_count=0
scanned_packages=0

# Parse mapping.txt (format: source=dest)
while IFS='=' read -r source dest; do
  # Skip empty lines and comments
  [ -z "$source" ] || [[ "$source" =~ ^# ]] && continue

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

  # Add comma separator
  [ "$first" = false ] && echo "," >> /workspace/output/sbom.cyclonedx.json
  first=false

  # Try to find package data from extracted/scanned SBOMs
  packages="[]"
  sbom_file="/tmp/sboms/$(echo "$dest" | tr '/:@' '_').json"
  if [ -f "$sbom_file" ]; then
    packages=$(jq '.components // []' "$sbom_file" 2>/dev/null || echo "[]")
    pkg_count=$(echo "$packages" | jq 'length // 0')
    if [ "$pkg_count" -gt 0 ]; then
      scanned_packages=$((scanned_packages + pkg_count))
      echo "  Found $pkg_count packages for $image_name"
    fi
  fi

  # Write component entry
  cat >> /workspace/output/sbom.cyclonedx.json <<COMPONENT
    {
      "type": "container",
      "name": "$image_name",
      "version": "$version",
      "purl": "pkg:oci/$image_full",
      "components": $packages
    }
COMPONENT

  component_count=$((component_count + 1))
done < "$MAPPING_FILE"

# Close JSON
cat >> /workspace/output/sbom.cyclonedx.json <<'SBOM_FOOTER'
  ]
}
SBOM_FOOTER

# Update placeholders
sed -i "s/TIMESTAMP_PLACEHOLDER/$(date -u +%Y-%m-%dT%H:%M:%SZ)/" /workspace/output/sbom.cyclonedx.json
sed -i "s/VERSION_PLACEHOLDER/$(basename /workspace/output)/" /workspace/output/sbom.cyclonedx.json

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

	// Build signing step based on configuration
	var cosignSignStep pipelinev1.Step
	if pipeline.Spec.Signing != nil && pipeline.Spec.Signing.Keyless != nil {
		// Keyless signing with Fulcio using client credentials flow
		// Initialize TUF root first, then sign blobs
		tufURL := pipeline.Spec.Signing.Keyless.TUFURL
		if tufURL == "" {
			tufURL = "http://tuf.mirror-operator-system.svc"
		}

		cosignSignStep = pipelinev1.Step{
			Name:    "sign",
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
			Name:    "sign",
			Image:   mirrorImage,
			Command: []string{"/bin/sh", "-c"},
			Args:    []string{`for f in /workspace/output/*.tar; do [ -f "$f" ] || continue; bn=$(basename "$f" .tar); cosign sign-blob --key /workspace/cosign-key/cosign.key "$f" --output-signature "/workspace/output/${bn}.sig"; done; bh=$(sha256sum /workspace/output/*.tar 2>/dev/null | head -1 | cut -d' ' -f1); if [ -z "$bh" ]; then exit 0; fi; sh=$(sha256sum /workspace/output/sbom.cyclonedx.json 2>/dev/null | cut -d' ' -f1 || echo ""); if [ -z "$sh" ]; then exit 0; fi; printf '{"bundle":{"sha256":"%s"},"sbom":{"sha256":"%s"}}\n' "$bh" "$sh" > /workspace/output/attestation.json; cosign sign-blob --key /workspace/cosign-key/cosign.key /workspace/output/attestation.json --output-signature /workspace/output/attestation.json.sig`},
		}
	}

	// SBOM upload step - get TPA URL
	var uploadSbomStep *pipelinev1.Step
	tpaHostname, keycloakHost := r.getTPAAndKeycloakHosts(ctx)
	if tpaHostname != "" && keycloakHost != "" {
		tpaURL := fmt.Sprintf("https://%s/api/v2/sbom", tpaHostname)
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

echo "Getting OIDC token..."
TOKEN_RESPONSE=$(curl -k -s -X POST "%s" \
  -d "grant_type=client_credentials" \
  -d "client_id=sbom-uploader" \
  -d "client_secret=$(cat /workspace/tpa-oidc-secret/clientSecret)")

ACCESS_TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.access_token')
if [ -z "$ACCESS_TOKEN" ] || [ "$ACCESS_TOKEN" = "null" ]; then
  echo "Failed to get access token: $TOKEN_RESPONSE"
  exit 1
fi

echo "Uploading SBOM to TPA..."
UPLOAD_URL="%s?format=cyclonedx&labels="
HTTP_CODE=$(curl -k -s -w "%%{http_code}" -o /tmp/upload_response.txt -X POST "$UPLOAD_URL" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @/workspace/output/sbom.cyclonedx.json)

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

	cosignTasks := []pipelinev1.PipelineTask{
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
			Name:     "cosign-sign",
			RunAfter: []string{"syft-sbom"},
			TaskSpec: &pipelinev1.EmbeddedTask{
				TaskSpec: pipelinev1.TaskSpec{
					Steps: []pipelinev1.Step{cosignSignStep},
				},
			},
			Workspaces: []pipelinev1.WorkspacePipelineTaskBinding{outputWorkspaceBinding},
		},
	}

	// Add SBOM upload task if TPA is available
	if uploadSbomStep != nil {
		uploadTask := pipelinev1.PipelineTask{
			Name:     "upload-sbom",
			RunAfter: []string{"cosign-sign"},
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

		// Add TPA OIDC secret workspace binding (using dedicated sbom-uploader client)
		tpaOidcSecretBinding := pipelinev1.WorkspaceBinding{
			Name: "tpa-oidc-secret",
			Secret: &corev1.SecretVolumeSource{
				SecretName: "sbom-uploader-secret",
			},
		}
		bindings = append(bindings, tpaOidcSecretBinding)
		declaredWorkspaces = append(declaredWorkspaces, pipelinev1.PipelineWorkspaceDeclaration{Name: "tpa-oidc-secret"})
	}

	if pipeline.Spec.Signing != nil && pipeline.Spec.Signing.KeySecretRef != nil {
		cosignKeyBinding := pipelinev1.WorkspaceBinding{
			Name: "cosign-key",
			Secret: &corev1.SecretVolumeSource{
				SecretName: pipeline.Spec.Signing.KeySecretRef.Name,
			},
		}
		bindings = append(bindings, cosignKeyBinding)
		declaredWorkspaces = append(declaredWorkspaces, pipelinev1.PipelineWorkspaceDeclaration{Name: "cosign-key"})
		cosignTasks[3].Workspaces = append(cosignTasks[3].Workspaces, pipelinev1.WorkspacePipelineTaskBinding{Name: "cosign-key", Workspace: "cosign-key"})
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
		cosignTasks[3].Workspaces = append(cosignTasks[3].Workspaces, pipelinev1.WorkspacePipelineTaskBinding{Name: "oidc-secret", Workspace: "oidc-secret"})
	}

	pipelineRunNamePrefix := fmt.Sprintf("collection-pipeline-%s-", pipeline.Name)
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
		},
	}

	return pr, nil
}

// getTPAAndKeycloakHosts retrieves the TPA and Keycloak hostnames from Ingresses
func (r *CollectionPipelineReconciler) getTPAAndKeycloakHosts(ctx context.Context) (string, string) {
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
		return "", ""
	}

	if len(tpaList.Items) == 0 {
		return "", ""
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
		return "", ""
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
		return "", ""
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
		return tpaHostname, ""
	}

	domain, _, _ := unstructured.NestedString(ingress.Object, "spec", "domain")
	keycloakHost := "keycloak." + domain

	return tpaHostname, keycloakHost
}

func (r *CollectionPipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mirrorv1.CollectionPipeline{}).
		Owns(&pipelinev1.PipelineRun{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}

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
		if pipeline.Status.PipelineRunRef != "" && (pipeline.Status.Phase == "Succeeded" || pipeline.Status.Phase == "Failed" || pipeline.Status.Phase == "Stale") {
			logger.Info("Trigger annotation detected, starting new collection", "trigger", triggerValue, "previous-run", pipeline.Status.PipelineRunRef)
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
					// Check if the signing task has Fulcio URL environment variable
					if len(pr.Spec.PipelineSpec.Tasks) >= 3 {
						signTask := pr.Spec.PipelineSpec.Tasks[2]
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
		return ctrl.Result{}, err
	}

	now := metav1.Now()
	pipeline.Status.PipelineRunRef = pr.Name
	pipeline.Status.Phase = "Pending"
	pipeline.Status.StartTime = &now
	return ctrl.Result{}, r.Status().Update(ctx, pipeline)
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
	if output == nil || output.PVC == "" {
		return nil
	}

	existing := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Namespace: pipeline.Namespace, Name: output.PVC}, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      output.PVC,
			Namespace: pipeline.Namespace,
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
		if output.PVC != "" {
			declaredWorkspaces = append(declaredWorkspaces, pipelinev1.PipelineWorkspaceDeclaration{Name: "output"})
			bindings = append(bindings, pipelinev1.WorkspaceBinding{
				Name: "output",
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: output.PVC,
				},
			})
		}

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
			"--config=/workspace/config/" + configMapKey,
			"--authfile=/workspace/pull-secret/.dockerconfigjson",
			"file:///workspace/output",
			"--v2",
			"--dry-run",
		},
		Env: envVars,
	}

	// Actual mirror step
	ocMirrorStep := pipelinev1.Step{
		Name:    "mirror",
		Image:   mirrorImage,
		Command: []string{"oc-mirror"},
		Args: []string{
			"--config=/workspace/config/" + configMapKey,
			"--authfile=/workspace/pull-secret/.dockerconfigjson",
			"file:///workspace/output",
			"--v2",
		},
		Env: envVars,
	}

	// Generate SBOM using mapping.txt from dry-run and scan images with Syft
	syftStep := pipelinev1.Step{
		Name:    "sbom",
		Image:   mirrorImage,
		Command: []string{"/bin/sh", "-c"},
		Args: []string{`
set -e
echo "=== Generating comprehensive SBOM from mirrored images ==="

# Find mapping.txt from dry-run output
MAPPING_FILE="/workspace/output/working-dir/dry-run/mapping.txt"
if [ ! -f "$MAPPING_FILE" ]; then
  echo "ERROR: mapping.txt not found at expected location"
  echo "Searching for mapping.txt..."
  find /workspace/output -name "mapping.txt" -type f
  MAPPING_FILE=$(find /workspace/output -name "mapping.txt" -type f 2>/dev/null | head -1)
  if [ -z "$MAPPING_FILE" ]; then
    echo "No mapping.txt found, creating empty SBOM"
    echo '{"bomFormat":"CycloneDX","specVersion":"1.4","version":1,"metadata":{"component":{"type":"container","name":"mirror-collection"}},"components":[]}' > /workspace/output/sbom.cyclonedx.json
    exit 0
  fi
fi

echo "Found mapping file: $MAPPING_FILE"
echo "Total images in mapping: $(wc -l < $MAPPING_FILE)"

# Scan all tar archives with Syft to extract SBOMs
mkdir -p /tmp/scans

echo "Step 1: Scanning tar archives with Syft..."
scan_count=0
for tar_file in /workspace/output/mirror_*.tar; do
  if [ ! -f "$tar_file" ]; then
    continue
  fi

  tar_name=$(basename "$tar_file" .tar)
  echo "Scanning $tar_name..."

  # Scan tar archive with Syft
  syft "oci-archive:$tar_file" -o cyclonedx-json > "/tmp/scans/${tar_name}.json" 2>/dev/null && scan_count=$((scan_count + 1)) || echo "  Syft scan failed for $tar_name"
done

echo "Scanned $scan_count tar archives"

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

  # Try to find package data from Syft scans
  packages="[]"
  for scan_file in /tmp/scans/*.json; do
    [ -f "$scan_file" ] || continue
    # Check if this scan contains our image
    if grep -q "$image_name" "$scan_file" 2>/dev/null; then
      packages=$(jq '.components // []' "$scan_file" 2>/dev/null || echo "[]")
      pkg_count=$(echo "$packages" | jq 'length')
      if [ "$pkg_count" -gt 0 ]; then
        scanned_packages=$((scanned_packages + pkg_count))
        echo "  Found $pkg_count packages for $image_name"
        break
      fi
    fi
  done

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

rm -rf /tmp/scans
		`},
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
		tpaURL := fmt.Sprintf("https://%s/api/v1/sbom", tpaHostname)
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
  -d "client_id=cli" \
  -d "client_secret=$(cat /workspace/oidc-secret/clientSecret)")

ACCESS_TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.access_token')
if [ -z "$ACCESS_TOKEN" ] || [ "$ACCESS_TOKEN" = "null" ]; then
  echo "Failed to get access token: $TOKEN_RESPONSE"
  exit 1
fi

echo "Uploading SBOM to TPA..."
curl -k -v -X POST "%s" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d @/workspace/output/sbom.cyclonedx.json

echo "SBOM uploaded successfully"
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
				},
			},
			Workspaces: []pipelinev1.WorkspacePipelineTaskBinding{outputWorkspaceBinding},
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
				{Name: "oidc-secret", Workspace: "oidc-secret"},
			},
		}
		cosignTasks = append(cosignTasks, uploadTask)
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
		cosignTasks[2].Workspaces = append(cosignTasks[2].Workspaces, pipelinev1.WorkspacePipelineTaskBinding{Name: "cosign-key", Workspace: "cosign-key"})
	}

	if pipeline.Spec.Signing != nil && pipeline.Spec.Signing.PasswordSecretRef != nil {
		cosignTasks[2].TaskSpec.TaskSpec.Steps[0].Env = append(
			cosignTasks[2].TaskSpec.TaskSpec.Steps[0].Env,
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
		cosignTasks[2].Workspaces = append(cosignTasks[2].Workspaces, pipelinev1.WorkspacePipelineTaskBinding{Name: "oidc-secret", Workspace: "oidc-secret"})
	}

	pipelineRunName := fmt.Sprintf("collection-pipeline-%s", pipeline.Name)
	pr := &pipelinev1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pipelineRunName,
			Namespace: pipeline.Namespace,
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

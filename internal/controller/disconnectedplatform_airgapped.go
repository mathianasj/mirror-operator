package controller

import (
	"context"
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

func (r *DisconnectedPlatformReconciler) reconcileAirgapped(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling airgapped mode")

	if err := r.reconcileAirgappedQuay(ctx, platform); err != nil {
		logger.Error(err, "failed to reconcile airgapped Quay")
		return err
	}

	if err := r.ensureAirgappedRegistryCredentials(ctx, platform); err != nil {
		logger.Error(err, "failed to ensure airgapped registry credentials")
	}

	if platform.Spec.Airgapped.ImportPath != "" {
		if err := r.reconcileImportScanner(ctx, platform); err != nil {
			logger.Error(err, "failed to reconcile import scanner")
		}
	}

	if err := r.ensureAirgappedUpdateService(ctx, platform); err != nil {
		logger.Error(err, "failed to ensure airgapped UpdateService")
	}

	return nil
}

// reconcileAirgappedQuay deploys a QuayRegistry CR with filesystem-backed storage for airgapped clusters.
func (r *DisconnectedPlatformReconciler) reconcileAirgappedQuay(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)

	quayConfig := platform.Spec.Airgapped.Quay
	if quayConfig == nil || !quayConfig.Enabled {
		return nil
	}

	orgName := "mirror"
	if quayConfig.OrganizationName != "" {
		orgName = quayConfig.OrganizationName
	}

	quayRegistry := &unstructured.Unstructured{}
	quayRegistry.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "quay.redhat.com",
		Version: "v1",
		Kind:    "QuayRegistry",
	})
	quayRegistry.SetName("mirror-operator-quay")
	quayRegistry.SetNamespace(architectNamespace)

	err := r.Get(ctx, client.ObjectKeyFromObject(quayRegistry), quayRegistry)
	if err == nil {
		hostname, err := r.getQuayHostname(ctx, quayRegistry)
		if err != nil {
			logger.Error(err, "failed to get Quay hostname")
			return err
		}

		if hostname != "" {
			newRegistry := hostname + "/" + orgName
			if platform.Spec.Airgapped.MirrorRegistry != newRegistry {
				latest := &mirrorv1.DisconnectedPlatform{}
				if err := r.Get(ctx, client.ObjectKeyFromObject(platform), latest); err != nil {
					return fmt.Errorf("failed to refetch platform for mirrorRegistry update: %w", err)
				}
				latest.Spec.Airgapped.MirrorRegistry = newRegistry
				if err := r.Update(ctx, latest); err != nil {
					return fmt.Errorf("failed to update mirrorRegistry: %w", err)
				}
				platform.Spec.Airgapped.MirrorRegistry = newRegistry
				logger.Info("Updated airgapped mirrorRegistry from managed Quay", "registry", newRegistry)
			}
		}

		if quayConfig.Clair != nil && quayConfig.Clair.UseRedHatVEXOnly {
			if err := r.configureClairVEX(ctx, quayRegistry); err != nil {
				logger.Error(err, "failed to configure Clair VEX for airgapped Quay")
			}
		}

		platform.Status.Components = append(platform.Status.Components,
			mirrorv1.ComponentStatus{Name: "quay-registry", Status: "Running",
				Kind: "QuayRegistry", APIGroup: "quay.redhat.com", Namespace: architectNamespace},
		)
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	// Create config bundle secret with LocalStorage backend
	if err := r.createAirgappedQuayConfigSecret(ctx, platform); err != nil {
		return fmt.Errorf("failed to create airgapped Quay config secret: %w", err)
	}

	components := []interface{}{
		map[string]interface{}{"kind": "clair", "managed": true},
		map[string]interface{}{"kind": "postgres", "managed": true},
		map[string]interface{}{"kind": "objectstorage", "managed": false},
		map[string]interface{}{"kind": "redis", "managed": true},
		map[string]interface{}{"kind": "horizontalpodautoscaler", "managed": true},
		map[string]interface{}{"kind": "route", "managed": true},
		map[string]interface{}{"kind": "mirror", "managed": true},
		map[string]interface{}{"kind": "tls", "managed": true},
		map[string]interface{}{"kind": "quay", "managed": true},
	}

	if err := unstructured.SetNestedSlice(quayRegistry.Object, components, "spec", "components"); err != nil {
		return fmt.Errorf("failed to set QuayRegistry components: %w", err)
	}

	if err := unstructured.SetNestedField(quayRegistry.Object, "mirror-operator-quay-config-bundle", "spec", "configBundleSecret"); err != nil {
		return fmt.Errorf("failed to set configBundleSecret: %w", err)
	}

	if err := r.Create(ctx, quayRegistry); err != nil {
		return fmt.Errorf("failed to create airgapped QuayRegistry: %w", err)
	}

	logger.Info("Created airgapped Quay registry with LocalStorage backend")
	return nil
}

// createAirgappedQuayConfigSecret creates the config bundle secret for Quay using LocalStorage.
func (r *DisconnectedPlatformReconciler) createAirgappedQuayConfigSecret(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	secretName := "mirror-operator-quay-config-bundle"
	existing := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: architectNamespace}, existing); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	storageSize := "500Gi"
	if platform.Spec.Airgapped.Quay.Storage != nil && platform.Spec.Airgapped.Quay.Storage.Size != "" {
		storageSize = platform.Spec.Airgapped.Quay.Storage.Size
	}

	quayConfig := map[string]interface{}{
		"DISTRIBUTED_STORAGE_CONFIG": map[string]interface{}{
			"default": []interface{}{
				"LocalStorage",
				map[string]interface{}{
					"storage_path": "/datastorage/registry",
				},
			},
		},
		"DISTRIBUTED_STORAGE_DEFAULT_LOCATIONS": []interface{}{},
		"DISTRIBUTED_STORAGE_PREFERENCE":        []interface{}{"default"},
		"FEATURE_STORAGE_REPLICATION":           false,
		"MAXIMUM_LAYER_SIZE":                    storageSize,
	}

	configYAML, err := yaml.Marshal(quayConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal Quay config: %w", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: architectNamespace,
		},
		Data: map[string][]byte{
			"config.yaml": configYAML,
		},
		Type: corev1.SecretTypeOpaque,
	}

	if err := ctrl.SetControllerReference(platform, secret, r.Scheme); err != nil {
		log.FromContext(ctx).Error(err, "failed to set owner reference on Quay config secret")
	}

	return r.Create(ctx, secret)
}

// ensureAirgappedRegistryCredentials configures registry credentials for the airgapped cluster.
func (r *DisconnectedPlatformReconciler) ensureAirgappedRegistryCredentials(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)

	// If user provided explicit credentials, use those
	if platform.Spec.Airgapped.RegistryCredentials != nil {
		logger.Info("Using user-provided registry credentials", "secret", platform.Spec.Airgapped.RegistryCredentials.Name)
		return nil
	}

	// If managed Quay is enabled, set up robot account credentials
	if platform.Spec.Airgapped.Quay != nil && platform.Spec.Airgapped.Quay.Enabled {
		quayRegistry := &unstructured.Unstructured{}
		quayRegistry.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "quay.redhat.com",
			Version: "v1",
			Kind:    "QuayRegistry",
		})
		quayRegistry.SetName("mirror-operator-quay")
		quayRegistry.SetNamespace(architectNamespace)

		if err := r.Get(ctx, client.ObjectKeyFromObject(quayRegistry), quayRegistry); err != nil {
			if apierrors.IsNotFound(err) {
				logger.Info("QuayRegistry not yet created, skipping credential setup")
				return nil
			}
			return err
		}

		hostname, err := r.getQuayHostname(ctx, quayRegistry)
		if err != nil || hostname == "" {
			logger.Info("Quay hostname not yet available, skipping credential setup")
			return nil
		}

		orgName := "mirror"
		if platform.Spec.Airgapped.Quay.OrganizationName != "" {
			orgName = platform.Spec.Airgapped.Quay.OrganizationName
		}

		robotShortName := "mirroroperator"
		robotToken, err := r.ensureQuayRobotViaPython(ctx, orgName, robotShortName)
		if err != nil {
			return fmt.Errorf("failed to ensure Quay robot account: %w", err)
		}

		robotFullName := orgName + "+" + robotShortName
		if err := r.saveQuayRobotCredentials(ctx, robotFullName, robotToken); err != nil {
			logger.Error(err, "failed to save robot credentials")
		}

		logger.Info("Configured airgapped Quay credentials", "registry", hostname, "robot", robotFullName)
	}

	return nil
}

// reconcileImportScanner creates a CronJob that scans the import path for new bundles
// and creates MirrorImport CRs for each unprocessed bundle.
func (r *DisconnectedPlatformReconciler) reconcileImportScanner(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)

	schedule := "*/30 * * * *"
	if platform.Spec.Airgapped.ImportScanSchedule != "" {
		schedule = platform.Spec.Airgapped.ImportScanSchedule
	}

	importPath := platform.Spec.Airgapped.ImportPath

	if err := r.ensureImportScannerRBAC(ctx, platform); err != nil {
		return fmt.Errorf("failed to ensure import scanner RBAC: %w", err)
	}

	if err := r.ensureImportScannerScript(ctx, platform); err != nil {
		return fmt.Errorf("failed to ensure import scanner script: %w", err)
	}

	cronJobName := "import-bundle-scanner"
	existing := &batchv1.CronJob{}
	err := r.Get(ctx, types.NamespacedName{Name: cronJobName, Namespace: architectNamespace}, existing)
	if err == nil {
		needsUpdate := false
		if existing.Spec.Schedule != schedule {
			existing.Spec.Schedule = schedule
			needsUpdate = true
		}
		if needsUpdate {
			if err := r.Update(ctx, existing); err != nil {
				return fmt.Errorf("failed to update import scanner CronJob: %w", err)
			}
			logger.Info("Updated import scanner CronJob schedule", "schedule", schedule)
		}
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	backoffLimit := int32(1)
	successfulJobsLimit := int32(3)
	failedJobsLimit := int32(3)

	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cronJobName,
			Namespace: architectNamespace,
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   schedule,
			SuccessfulJobsHistoryLimit: &successfulJobsLimit,
			FailedJobsHistoryLimit:     &failedJobsLimit,
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					BackoffLimit: &backoffLimit,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ServiceAccountName: "import-bundle-scanner",
							RestartPolicy:      corev1.RestartPolicyNever,
							NodeSelector: map[string]string{
								"mirror-operator.io/import-node": "true",
							},
							Containers: []corev1.Container{
								{
									Name:    "scanner",
									Image:   "registry.redhat.io/openshift4/ose-cli:latest",
									Command: []string{"/bin/bash", "/scripts/scan-imports.sh"},
									Env: []corev1.EnvVar{
										{Name: "IMPORT_PATH", Value: importPath},
										{Name: "NAMESPACE", Value: architectNamespace},
										{Name: "PLATFORM_NAME", Value: platform.Name},
									},
									VolumeMounts: []corev1.VolumeMount{
										{
											Name:      "import-path",
											MountPath: importPath,
											ReadOnly:  true,
										},
										{
											Name:      "scanner-script",
											MountPath: "/scripts",
										},
									},
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("100m"),
											corev1.ResourceMemory: resource.MustParse("128Mi"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("500m"),
											corev1.ResourceMemory: resource.MustParse("256Mi"),
										},
									},
								},
							},
							Volumes: []corev1.Volume{
								{
									Name: "import-path",
									VolumeSource: corev1.VolumeSource{
										HostPath: &corev1.HostPathVolumeSource{
											Path: importPath,
										},
									},
								},
								{
									Name: "scanner-script",
									VolumeSource: corev1.VolumeSource{
										ConfigMap: &corev1.ConfigMapVolumeSource{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: "import-scanner-script",
											},
											DefaultMode: func() *int32 { m := int32(0755); return &m }(),
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(platform, cronJob, r.Scheme); err != nil {
		logger.Error(err, "failed to set owner reference on import scanner CronJob")
	}

	if err := r.Create(ctx, cronJob); err != nil {
		return fmt.Errorf("failed to create import scanner CronJob: %w", err)
	}

	logger.Info("Created import bundle scanner CronJob", "schedule", schedule, "importPath", importPath)
	return nil
}

// ensureImportScannerScript creates the ConfigMap containing the scanner shell script.
func (r *DisconnectedPlatformReconciler) ensureImportScannerScript(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	scannerScript := `#!/bin/bash
set -euo pipefail

echo "Scanning ${IMPORT_PATH} for new bundles..."

# List all .tar and .tar.gz files in the import path
shopt -s nullglob
bundles=("${IMPORT_PATH}"/*.tar "${IMPORT_PATH}"/*.tar.gz)

if [ ${#bundles[@]} -eq 0 ]; then
  echo "No bundles found in ${IMPORT_PATH}"
  exit 0
fi

for bundle in "${bundles[@]}"; do
  filename=$(basename "$bundle")
  # Sanitize filename for use as CR name
  cr_name="import-$(echo "$filename" | sed 's/[^a-zA-Z0-9-]/-/g' | tr '[:upper:]' '[:lower:]' | sed 's/--*/-/g' | sed 's/-$//' | cut -c1-63)"

  # Check if MirrorImport CR already exists
  if oc get mirrorimport "$cr_name" -n "${NAMESPACE}" &>/dev/null; then
    echo "MirrorImport $cr_name already exists, skipping"
    continue
  fi

  echo "Creating MirrorImport for $filename"
  cat <<EOFI | oc apply -f -
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: MirrorImport
metadata:
  name: ${cr_name}
  namespace: ${NAMESPACE}
spec:
  bundle:
    path: ${bundle}
    type: file
  targetRegistry: ""
EOFI
done

echo "Import scan complete"
`

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "import-scanner-script",
			Namespace: architectNamespace,
		},
		Data: map[string]string{
			"scan-imports.sh": scannerScript,
		},
	}

	existing := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(cm), existing); err == nil {
		if existing.Data["scan-imports.sh"] != scannerScript {
			existing.Data = cm.Data
			return r.Update(ctx, existing)
		}
		return nil
	} else if apierrors.IsNotFound(err) {
		if err := ctrl.SetControllerReference(platform, cm, r.Scheme); err != nil {
			log.FromContext(ctx).Error(err, "failed to set owner reference on scanner script ConfigMap")
		}
		return r.Create(ctx, cm)
	} else {
		return err
	}
}

// ensureImportScannerRBAC creates the ServiceAccount, Role, and RoleBinding for the scanner CronJob.
func (r *DisconnectedPlatformReconciler) ensureImportScannerRBAC(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "import-bundle-scanner",
			Namespace: architectNamespace,
		},
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(sa), sa); apierrors.IsNotFound(err) {
		if err := ctrl.SetControllerReference(platform, sa, r.Scheme); err != nil {
			log.FromContext(ctx).Error(err, "failed to set owner reference on scanner SA")
		}
		if err := r.Create(ctx, sa); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "import-bundle-scanner",
			Namespace: architectNamespace,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"mirror.mirror.mathianasj.github.com"},
				Resources: []string{"mirrorimports"},
				Verbs:     []string{"get", "list", "create"},
			},
		},
	}

	existingRole := &rbacv1.Role{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(role), existingRole); apierrors.IsNotFound(err) {
		if err := ctrl.SetControllerReference(platform, role, r.Scheme); err != nil {
			log.FromContext(ctx).Error(err, "failed to set owner reference on scanner Role")
		}
		if err := r.Create(ctx, role); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	} else if err == nil {
		existingRole.Rules = role.Rules
		if err := r.Update(ctx, existingRole); err != nil {
			return err
		}
	}

	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "import-bundle-scanner",
			Namespace: architectNamespace,
		},
		Subjects: []rbacv1.Subject{
			{Kind: "ServiceAccount", Name: "import-bundle-scanner", Namespace: architectNamespace},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     "import-bundle-scanner",
		},
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(rb), rb); apierrors.IsNotFound(err) {
		if err := ctrl.SetControllerReference(platform, rb, r.Scheme); err != nil {
			log.FromContext(ctx).Error(err, "failed to set owner reference on scanner RoleBinding")
		}
		if err := r.Create(ctx, rb); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}

	return nil
}

// ensureAirgappedUpdateService creates or updates the OSUS UpdateService CR pointing at the airgapped Quay.
func (r *DisconnectedPlatformReconciler) ensureAirgappedUpdateService(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)

	mirrorRegistry := platform.Spec.Airgapped.MirrorRegistry
	if mirrorRegistry == "" {
		logger.V(1).Info("Mirror registry not yet configured, skipping UpdateService")
		return nil
	}

	// Extract hostname from mirrorRegistry (hostname/org format)
	registryHost := mirrorRegistry
	if idx := strings.Index(mirrorRegistry, "/"); idx > 0 {
		registryHost = mirrorRegistry[:idx]
	}

	orgName := "mirror"
	if platform.Spec.Airgapped.Quay != nil && platform.Spec.Airgapped.Quay.OrganizationName != "" {
		orgName = platform.Spec.Airgapped.Quay.OrganizationName
	}

	// In airgapped mode, graph image must come from the airgapped registry
	graphDataImage := registryHost + "/" + orgName + "/openshift/graph-image:latest"
	releases := registryHost + "/" + orgName + "/openshift/release-images"

	us := &unstructured.Unstructured{}
	us.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "updateservice.operator.openshift.io", Version: "v1", Kind: "UpdateService",
	})
	us.SetName("update-service-oc-mirror")
	us.SetNamespace("openshift-update-service")

	desired := map[string]interface{}{
		"graphDataImage": graphDataImage,
		"releases":       releases,
		"replicas":       int64(2),
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(us.GroupVersionKind())
	if err := r.Get(ctx, client.ObjectKeyFromObject(us), existing); err == nil {
		currentSpec, _, _ := unstructured.NestedMap(existing.Object, "spec")
		needsUpdate := false
		for k, v := range desired {
			if fmt.Sprintf("%v", currentSpec[k]) != fmt.Sprintf("%v", v) {
				needsUpdate = true
				break
			}
		}
		if needsUpdate {
			if err := unstructured.SetNestedField(existing.Object, desired, "spec"); err != nil {
				return err
			}
			if err := r.Update(ctx, existing); err != nil {
				return fmt.Errorf("failed to update airgapped UpdateService: %w", err)
			}
			logger.Info("Updated airgapped UpdateService CR")
		}
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	us.Object["spec"] = desired
	if err := r.Create(ctx, us); err != nil {
		return fmt.Errorf("failed to create airgapped UpdateService: %w", err)
	}
	logger.Info("Created airgapped UpdateService CR", "graphDataImage", graphDataImage, "releases", releases)
	return nil
}

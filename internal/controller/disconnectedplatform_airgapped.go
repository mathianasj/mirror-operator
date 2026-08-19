package controller

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

func (r *DisconnectedPlatformReconciler) reconcileAirgapped(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) (needsRequeue bool, err error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling airgapped mode")

	// ACM must be installed first — its policies typically configure storage
	// classes and other infrastructure that Quay and other components depend on.
	if platform.Spec.Airgapped.ACM != nil && platform.Spec.Airgapped.ACM.Enabled {
		mchReady, err := r.reconcileAirgappedACM(ctx, platform)
		if err != nil {
			logger.Error(err, "failed to reconcile airgapped ACM")
			needsRequeue = true
		}
		if !mchReady {
			logger.Info("MultiClusterHub not yet running, deferring Quay and other components until ACM policies are applied")
			needsRequeue = true
			return needsRequeue, nil
		}

		if err := r.ensureProvisioningConfiguration(ctx); err != nil {
			logger.Error(err, "failed to ensure Provisioning configuration")
			needsRequeue = true
		}

		// Reconcile host inventory for assisted installer bare-metal provisioning
		if platform.Spec.Airgapped.ACM.HostInventory != nil && platform.Spec.Airgapped.ACM.HostInventory.Enabled {
			if err := r.ensureAssistedInstallerMirrorConfig(ctx, platform); err != nil {
				logger.Error(err, "failed to ensure assisted-installer mirror config")
				needsRequeue = true
			}

			if err := r.reconcileRHCOSServer(ctx, platform); err != nil {
				logger.Error(err, "failed to reconcile RHCOS server")
				needsRequeue = true
			}

			rhcosServerURL := fmt.Sprintf("http://rhcos-server.%s.svc:8080", architectNamespace)
			if err := r.ensureAgentServiceConfig(ctx, platform, rhcosServerURL); err != nil {
				logger.Error(err, "failed to ensure AgentServiceConfig")
				needsRequeue = true
			}

			if err := r.ensureClusterImageSets(ctx, platform); err != nil {
				logger.Error(err, "failed to ensure ClusterImageSets")
				needsRequeue = true
			}

			if err := r.ensureInfraEnv(ctx, platform); err != nil {
				logger.Error(err, "failed to ensure InfraEnv")
				needsRequeue = true
			}

			if err := r.ensureACMCredential(ctx, platform); err != nil {
				logger.Error(err, "failed to ensure ACM credential")
				needsRequeue = true
			}

			platform.Status.Components = append(platform.Status.Components,
				mirrorv1.ComponentStatus{
					Name: "host-inventory", Status: "Configured",
					Kind: "AgentServiceConfig", APIGroup: "agent-install.openshift.io",
				},
			)
		}
	}

	if err := r.reconcileAirgappedQuay(ctx, platform); err != nil {
		logger.Error(err, "failed to reconcile airgapped Quay (will retry)")
		needsRequeue = true
	}

	if err := r.ensureAirgappedRegistryCredentials(ctx, platform); err != nil {
		logger.Error(err, "failed to ensure airgapped registry credentials")
		needsRequeue = true
	}

	if platform.Spec.Airgapped.ImportPath != "" {
		if err := r.reconcileImportScanner(ctx, platform); err != nil {
			logger.Error(err, "failed to reconcile import scanner")
			needsRequeue = true
		}
	}

	if err := r.ensureAirgappedUpdateService(ctx, platform); err != nil {
		logger.Error(err, "failed to ensure airgapped UpdateService")
		needsRequeue = true
	}

	if r.TektonAvailable {
		if err := r.reconcileImportPipelineTemplate(ctx, platform); err != nil {
			logger.Error(err, "failed to reconcile import pipeline template")
			needsRequeue = true
		}
	}

	return needsRequeue, nil
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

	replicaOverride := r.resolveQuayReplicaOverride(ctx, quayConfig.Replicas)
	components := buildQuayComponents(replicaOverride, false)

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
		"MAXIMUM_LAYER_SIZE":                    "20G",
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

	if err := r.ensureImportJobRBAC(ctx, platform); err != nil {
		return fmt.Errorf("failed to ensure import job RBAC: %w", err)
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
										{Name: "TARGET_REGISTRY", Value: platform.Spec.Airgapped.MirrorRegistry},
										{Name: "IMPORT_PVC", Value: "import-data"},
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

  # Extract imageset-config.yaml from bundle for the import Job
  imagesetconfig=""
  tmpdir=$(mktemp -d)
  if tar -xf "$bundle" -C "$tmpdir" imageset-config.yaml 2>/dev/null; then
    imagesetconfig=$(cat "$tmpdir/imageset-config.yaml")
  fi
  rm -rf "$tmpdir"

  echo "Creating MirrorImport for $filename"
  cat <<EOFI | oc apply -f -
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: MirrorImport
metadata:
  name: ${cr_name}
  namespace: ${NAMESPACE}
spec:
  imageSetConfig: |
$(echo "$imagesetconfig" | sed 's/^/    /')
  bundle:
    pvc: ${IMPORT_PVC}
    filename: ${filename}
  targetRegistry:
    url: ${TARGET_REGISTRY}
  publish:
    catalogSource: true
    imageContentSourcePolicy: true
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

// ensureImportJobRBAC creates the ServiceAccount, ClusterRole, and ClusterRoleBinding
// for the import Job to apply IDMS/ITMS/CatalogSource manifests after oc-mirror completes.
func (r *DisconnectedPlatformReconciler) ensureImportJobRBAC(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mirror-import-job",
			Namespace: architectNamespace,
		},
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(sa), sa); apierrors.IsNotFound(err) {
		if err := ctrl.SetControllerReference(platform, sa, r.Scheme); err != nil {
			log.FromContext(ctx).Error(err, "failed to set owner reference on import job SA")
		}
		if err := r.Create(ctx, sa); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}

	clusterRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: "mirror-import-job",
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"config.openshift.io"},
				Resources: []string{"imagedigestmirrorsets", "imagetagmirrorsets"},
				Verbs:     []string{"get", "create", "update", "patch"},
			},
			{
				APIGroups: []string{"operators.coreos.com"},
				Resources: []string{"catalogsources"},
				Verbs:     []string{"get", "create", "update", "patch"},
			},
		},
	}

	existingCR := &rbacv1.ClusterRole{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(clusterRole), existingCR); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, clusterRole); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	} else if err == nil {
		existingCR.Rules = clusterRole.Rules
		if err := r.Update(ctx, existingCR); err != nil {
			return err
		}
	}

	crb := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: "mirror-import-job",
		},
		Subjects: []rbacv1.Subject{
			{Kind: "ServiceAccount", Name: "mirror-import-job", Namespace: architectNamespace},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "mirror-import-job",
		},
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(crb), &rbacv1.ClusterRoleBinding{}); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, crb); err != nil && !apierrors.IsAlreadyExists(err) {
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

// reconcileAirgappedACM installs the ACM operator via OLM and creates a MultiClusterHub CR.
// Returns mchReady=true when the MultiClusterHub is in Running phase.
func (r *DisconnectedPlatformReconciler) reconcileAirgappedACM(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) (mchReady bool, err error) {
	logger := log.FromContext(ctx)
	acmConfig := platform.Spec.Airgapped.ACM

	acmOp := operatorDef{
		name: "advanced-cluster-management",
		pkg:  "advanced-cluster-management",
		ns:   "open-cluster-management",
	}

	catalog, catalogNS, channel, err := r.discoverPackageInfo(ctx, acmOp.pkg)
	if err != nil {
		logger.Info("ACM package not available yet", "error", err.Error())
		return false, nil
	}
	acmOp.catalog = catalog
	acmOp.catalogNS = catalogNS
	acmOp.channel = channel

	var subCfg *mirrorv1.OLMSubscriptionConfig
	if acmConfig.Subscription != nil {
		subCfg = acmConfig.Subscription
	}

	if err := r.ensureNamespace(ctx, acmOp.ns); err != nil {
		return false, fmt.Errorf("namespace for ACM: %w", err)
	}

	if err := r.ensureOperatorGroup(ctx, acmOp); err != nil {
		return false, fmt.Errorf("operatorgroup for ACM: %w", err)
	}

	if err := r.ensureSubscription(ctx, acmOp, subCfg); err != nil {
		return false, fmt.Errorf("subscription for ACM: %w", err)
	}

	csvPhase := r.csvStatus(ctx, acmOp)
	subStatus := csvPhase
	if subStatus == "" {
		subStatus = "Installing"
	}
	platform.Status.Components = append(platform.Status.Components,
		mirrorv1.ComponentStatus{
			Name: "advanced-cluster-management", Status: subStatus,
			Kind: "Subscription", APIGroup: "operators.coreos.com", Namespace: acmOp.ns,
		},
	)

	if csvPhase != "Succeeded" {
		logger.Info("ACM operator CSV not yet ready, deferring MultiClusterHub creation", "csvPhase", csvPhase)
		return false, nil
	}

	if err := r.ensureACMPullSecret(ctx, platform); err != nil {
		logger.Error(err, "failed to ensure pull secret in ACM namespace")
	}

	if err := r.ensureMultiClusterHub(ctx, platform); err != nil {
		return false, fmt.Errorf("failed to ensure MultiClusterHub: %w", err)
	}

	// Check if MCH is Running
	mch := &unstructured.Unstructured{}
	mch.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "operator.open-cluster-management.io", Version: "v1", Kind: "MultiClusterHub",
	})
	mch.SetName("multiclusterhub")
	mch.SetNamespace("open-cluster-management")
	if err := r.Get(ctx, client.ObjectKeyFromObject(mch), mch); err == nil {
		phase, _, _ := unstructured.NestedString(mch.Object, "status", "phase")
		if phase == "Running" {
			logger.Info("MultiClusterHub is Running, proceeding with remaining airgapped components")
			return true, nil
		}
		logger.Info("MultiClusterHub not yet Running", "phase", phase)
	}

	return false, nil
}

// ensureACMPullSecret copies the pull secret to the open-cluster-management namespace.
func (r *DisconnectedPlatformReconciler) ensureACMPullSecret(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	acmNamespace := "open-cluster-management"

	sourceSecret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: architectNamespace}, sourceSecret); err != nil {
		return fmt.Errorf("failed to get pull-secret from %s: %w", architectNamespace, err)
	}

	targetSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pull-secret",
			Namespace: acmNamespace,
		},
		Data: sourceSecret.Data,
		Type: sourceSecret.Type,
	}

	existing := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(targetSecret), existing); err == nil {
		if string(existing.Data[".dockerconfigjson"]) != string(sourceSecret.Data[".dockerconfigjson"]) {
			existing.Data = sourceSecret.Data
			return r.Update(ctx, existing)
		}
		return nil
	} else if apierrors.IsNotFound(err) {
		return r.Create(ctx, targetSecret)
	} else {
		return err
	}
}

// ensureMultiClusterHub creates or monitors the MultiClusterHub CR with mirror registry configuration.
func (r *DisconnectedPlatformReconciler) ensureMultiClusterHub(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)
	acmConfig := platform.Spec.Airgapped.ACM

	mch := &unstructured.Unstructured{}
	mch.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "operator.open-cluster-management.io", Version: "v1", Kind: "MultiClusterHub",
	})
	mch.SetName("multiclusterhub")
	mch.SetNamespace("open-cluster-management")

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(mch.GroupVersionKind())
	if err := r.Get(ctx, client.ObjectKeyFromObject(mch), existing); err == nil {
		phase, _, _ := unstructured.NestedString(existing.Object, "status", "phase")
		mchStatus := "Installing"
		if phase == "Running" {
			mchStatus = "Running"
		}
		platform.Status.Components = append(platform.Status.Components,
			mirrorv1.ComponentStatus{
				Name: "multiclusterhub", Status: mchStatus,
				Kind: "MultiClusterHub", APIGroup: "operator.open-cluster-management.io",
				Namespace: "open-cluster-management",
			},
		)
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	spec := map[string]interface{}{}

	if acmConfig.MultiClusterHub != nil && acmConfig.MultiClusterHub.ImagePullSecret != nil {
		spec["imagePullSecret"] = acmConfig.MultiClusterHub.ImagePullSecret.Name
	} else if platform.Spec.Airgapped.RegistryCredentials != nil {
		spec["imagePullSecret"] = platform.Spec.Airgapped.RegistryCredentials.Name
	} else {
		spec["imagePullSecret"] = "pull-secret"
	}

	if acmConfig.MultiClusterHub != nil && acmConfig.MultiClusterHub.CustomCAConfigMap != "" {
		spec["customCAConfigmap"] = acmConfig.MultiClusterHub.CustomCAConfigMap
	}

	if acmConfig.MultiClusterHub != nil && acmConfig.MultiClusterHub.DisableHubSelfManagement {
		spec["disableHubSelfManagement"] = true
	}

	mch.Object["spec"] = spec

	if err := r.Create(ctx, mch); err != nil {
		return fmt.Errorf("failed to create MultiClusterHub: %w", err)
	}

	logger.Info("Created MultiClusterHub CR for airgapped ACM")
	platform.Status.Components = append(platform.Status.Components,
		mirrorv1.ComponentStatus{
			Name: "multiclusterhub", Status: "Installing",
			Kind: "MultiClusterHub", APIGroup: "operator.open-cluster-management.io",
			Namespace: "open-cluster-management",
		},
	)
	return nil
}

// ensureProvisioningConfiguration creates or updates the metal3.io Provisioning CR
// to disable the provisioning network and enable watching all namespaces.
// This is required after ACM installs for bare-metal management to function.
func (r *DisconnectedPlatformReconciler) ensureProvisioningConfiguration(ctx context.Context) error {
	logger := log.FromContext(ctx)

	provisioning := &unstructured.Unstructured{}
	provisioning.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "metal3.io",
		Version: "v1alpha1",
		Kind:    "Provisioning",
	})
	provisioning.SetName("provisioning-configuration")

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(provisioning.GroupVersionKind())
	if err := r.Get(ctx, client.ObjectKeyFromObject(provisioning), existing); err == nil {
		needsUpdate := false
		provNet, _, _ := unstructured.NestedString(existing.Object, "spec", "provisioningNetwork")
		watchAll, _, _ := unstructured.NestedBool(existing.Object, "spec", "watchAllNamespaces")
		if provNet != "Disabled" {
			needsUpdate = true
		}
		if !watchAll {
			needsUpdate = true
		}
		if needsUpdate {
			if err := unstructured.SetNestedField(existing.Object, "Disabled", "spec", "provisioningNetwork"); err != nil {
				return err
			}
			if err := unstructured.SetNestedField(existing.Object, true, "spec", "watchAllNamespaces"); err != nil {
				return err
			}
			if err := r.Update(ctx, existing); err != nil {
				return fmt.Errorf("failed to update Provisioning configuration: %w", err)
			}
			logger.Info("Updated Provisioning configuration")
		}
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	spec := map[string]interface{}{
		"provisioningNetwork": "Disabled",
		"watchAllNamespaces":  true,
	}
	provisioning.Object["spec"] = spec

	if err := r.Create(ctx, provisioning); err != nil {
		return fmt.Errorf("failed to create Provisioning configuration: %w", err)
	}
	logger.Info("Created Provisioning configuration for bare-metal management")
	return nil
}

// reconcileRHCOSServer deploys the RHCOS server image from the mirror registry as a Deployment + Service.
func (r *DisconnectedPlatformReconciler) reconcileRHCOSServer(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)

	mirrorRegistry := platform.Spec.Airgapped.MirrorRegistry
	if mirrorRegistry == "" {
		logger.Info("Mirror registry not yet configured, skipping RHCOS server")
		return nil
	}

	acmConfig := platform.Spec.Airgapped.ACM
	if acmConfig == nil || acmConfig.HostInventory == nil || !acmConfig.HostInventory.Enabled {
		return nil
	}

	// Determine OCP version for the RHCOS server image tag
	rhcosVersion := ""
	if len(acmConfig.HostInventory.Versions) > 0 {
		rhcosVersion = acmConfig.HostInventory.Versions[0].OpenshiftVersion
	}
	if rhcosVersion == "" {
		logger.Info("No OCP version configured for RHCOS server, skipping")
		return nil
	}

	name := "rhcos-server"
	namespace := architectNamespace
	rhcosImage := ""
	if acmConfig.HostInventory.RHCOSImage != "" {
		rhcosImage = acmConfig.HostInventory.RHCOSImage
	} else {
		rhcosImage = fmt.Sprintf("%s/rhcos-server:%s", mirrorRegistry, rhcosVersion)
	}

	// Create or update Deployment
	replicas := int32(1)
	labels := map[string]string{"app": name}
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "nginx",
							Image: rhcosImage,
							Ports: []corev1.ContainerPort{
								{ContainerPort: 8080, Protocol: corev1.ProtocolTCP},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("128Mi"),
								},
							},
						},
					},
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(platform, deployment, r.Scheme); err != nil {
		logger.Error(err, "failed to set owner reference on RHCOS server deployment")
	}

	existingDeploy := &appsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(deployment), existingDeploy); err == nil {
		if existingDeploy.Spec.Template.Spec.Containers[0].Image != rhcosImage {
			existingDeploy.Spec.Template.Spec.Containers[0].Image = rhcosImage
			if err := r.Update(ctx, existingDeploy); err != nil {
				return fmt.Errorf("failed to update RHCOS server deployment: %w", err)
			}
			logger.Info("Updated RHCOS server deployment image", "image", rhcosImage)
		}
	} else if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, deployment); err != nil {
			return fmt.Errorf("failed to create RHCOS server deployment: %w", err)
		}
		logger.Info("Created RHCOS server deployment", "image", rhcosImage)
	} else {
		return err
	}

	// Create or update Service
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       8080,
					TargetPort: intstr.FromInt(8080),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(platform, service, r.Scheme); err != nil {
		logger.Error(err, "failed to set owner reference on RHCOS server service")
	}

	existingSvc := &corev1.Service{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(service), existingSvc); err == nil {
		// Service exists
	} else if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, service); err != nil {
			return fmt.Errorf("failed to create RHCOS server service: %w", err)
		}
		logger.Info("Created RHCOS server service")
	} else {
		return err
	}

	return nil
}

// ensureAssistedInstallerMirrorConfig creates the ConfigMap in multicluster-engine namespace
// with mirror registry CA and registries.conf for the assisted installer.
// It reads ImageDigestMirrorSets and ImageTagMirrorSets from the cluster and adds
// the managed Quay registry as an additional mirror for each source.
func (r *DisconnectedPlatformReconciler) ensureAssistedInstallerMirrorConfig(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)

	mirrorRegistry := platform.Spec.Airgapped.MirrorRegistry
	if mirrorRegistry == "" {
		return nil
	}

	registryHost := mirrorRegistry
	if idx := strings.Index(mirrorRegistry, "/"); idx > 0 {
		registryHost = mirrorRegistry[:idx]
	}

	caBundlePEM := r.getMirrorRegistryCA(ctx, platform)

	type mirrorEntry struct {
		source     string
		mirrors    []string
		digestOnly bool
	}

	seen := map[string]*mirrorEntry{}
	var orderedSources []string

	addEntry := func(source string, mirrors []string, digestOnly bool) {
		if e, ok := seen[source]; ok {
			for _, m := range mirrors {
				found := false
				for _, existing := range e.mirrors {
					if existing == m {
						found = true
						break
					}
				}
				if !found {
					e.mirrors = append(e.mirrors, m)
				}
			}
		} else {
			seen[source] = &mirrorEntry{source: source, mirrors: mirrors, digestOnly: digestOnly}
			orderedSources = append(orderedSources, source)
		}
	}

	idmsList := &unstructured.UnstructuredList{}
	idmsList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "ImageDigestMirrorSet",
	})
	if err := r.List(ctx, idmsList); err == nil {
		for _, idms := range idmsList.Items {
			entries, _, _ := unstructured.NestedSlice(idms.Object, "spec", "imageDigestMirrors")
			for _, entry := range entries {
				e, ok := entry.(map[string]interface{})
				if !ok {
					continue
				}
				source, _, _ := unstructured.NestedString(e, "source")
				if source == "" {
					continue
				}
				mirrorsRaw, _, _ := unstructured.NestedStringSlice(e, "mirrors")
				addEntry(source, mirrorsRaw, true)
			}
		}
	} else {
		logger.V(1).Info("Could not list ImageDigestMirrorSets", "error", err)
	}

	itmsList := &unstructured.UnstructuredList{}
	itmsList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "ImageTagMirrorSet",
	})
	if err := r.List(ctx, itmsList); err == nil {
		for _, itms := range itmsList.Items {
			entries, _, _ := unstructured.NestedSlice(itms.Object, "spec", "imageTagMirrors")
			for _, entry := range entries {
				e, ok := entry.(map[string]interface{})
				if !ok {
					continue
				}
				source, _, _ := unstructured.NestedString(e, "source")
				if source == "" {
					continue
				}
				mirrorsRaw, _, _ := unstructured.NestedStringSlice(e, "mirrors")
				addEntry(source, mirrorsRaw, false)
			}
		}
	} else {
		logger.V(1).Info("Could not list ImageTagMirrorSets", "error", err)
	}

	// Add Quay as an additional mirror for each source
	for _, source := range orderedSources {
		e := seen[source]
		quayMirror := registryHost
		// Preserve the path suffix from the source for namespaced images
		parts := strings.SplitN(source, "/", 2)
		if len(parts) == 2 {
			quayMirror = registryHost + "/" + parts[1]
		}
		found := false
		for _, m := range e.mirrors {
			if m == quayMirror {
				found = true
				break
			}
		}
		if !found {
			e.mirrors = append(e.mirrors, quayMirror)
		}
	}

	var sb strings.Builder
	sb.WriteString("unqualified-search-registries = [\"registry.access.redhat.com\", \"docker.io\"]\n")

	for _, source := range orderedSources {
		e := seen[source]
		sb.WriteString("\n[[registry]]\n")
		sb.WriteString("  prefix = \"\"\n")
		sb.WriteString(fmt.Sprintf("  location = \"%s\"\n", e.source))
		sb.WriteString(fmt.Sprintf("  mirror-by-digest-only = %t\n", e.digestOnly))
		for _, m := range e.mirrors {
			sb.WriteString("\n  [[registry.mirror]]\n")
			sb.WriteString(fmt.Sprintf("    location = \"%s\"\n", m))
		}
	}

	registriesConf := sb.String()

	if err := r.ensureNamespace(ctx, "multicluster-engine"); err != nil {
		return fmt.Errorf("failed to ensure multicluster-engine namespace: %w", err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "assisted-installer-mirror-config",
			Namespace: "multicluster-engine",
			Labels: map[string]string{
				"app": "assisted-service",
			},
		},
		Data: map[string]string{
			"ca-bundle.crt":   caBundlePEM,
			"registries.conf": registriesConf,
		},
	}

	existing := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(cm), existing); err == nil {
		needsUpdate := existing.Data["ca-bundle.crt"] != caBundlePEM ||
			existing.Data["registries.conf"] != registriesConf
		if needsUpdate {
			existing.Data = cm.Data
			existing.Labels = cm.Labels
			if err := r.Update(ctx, existing); err != nil {
				return fmt.Errorf("failed to update assisted-installer-mirror-config: %w", err)
			}
			logger.Info("Updated assisted-installer-mirror-config ConfigMap")
		}
		return nil
	} else if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, cm); err != nil {
			return fmt.Errorf("failed to create assisted-installer-mirror-config: %w", err)
		}
		logger.Info("Created assisted-installer-mirror-config ConfigMap")
		return nil
	} else {
		return err
	}
}

// getMirrorRegistryCA extracts the CA certificate for the mirror registry.
func (r *DisconnectedPlatformReconciler) getMirrorRegistryCA(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) string {
	logger := log.FromContext(ctx)

	// Try to get CA from the managed Quay TLS secret
	if platform.Spec.Airgapped.Quay != nil && platform.Spec.Airgapped.Quay.Enabled {
		secret := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      "mirror-operator-quay-config-bundle",
			Namespace: architectNamespace,
		}, secret); err == nil {
			if ca, ok := secret.Data["ssl.cert"]; ok && len(ca) > 0 {
				return string(ca)
			}
		}

		// Try the Quay-generated TLS secret
		if err := r.Get(ctx, types.NamespacedName{
			Name:      "mirror-operator-quay-quay-ssl",
			Namespace: architectNamespace,
		}, secret); err == nil {
			if ca, ok := secret.Data["tls.crt"]; ok && len(ca) > 0 {
				return string(ca)
			}
		}
	}

	// Try the cluster's additional trust bundle
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      "user-ca-bundle",
		Namespace: "openshift-config",
	}, cm); err == nil {
		if ca, ok := cm.Data["ca-bundle.crt"]; ok && ca != "" {
			return ca
		}
	}

	logger.Info("Could not find mirror registry CA certificate, using empty bundle")
	return ""
}

// ensureAgentServiceConfig creates the AgentServiceConfig CR for the assisted installer.
func (r *DisconnectedPlatformReconciler) ensureAgentServiceConfig(ctx context.Context, platform *mirrorv1.DisconnectedPlatform, rhcosServerURL string) error {
	logger := log.FromContext(ctx)

	hostInv := platform.Spec.Airgapped.ACM.HostInventory

	asc := &unstructured.Unstructured{}
	asc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "agent-install.openshift.io",
		Version: "v1beta1",
		Kind:    "AgentServiceConfig",
	})
	asc.SetName("agent")

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(asc.GroupVersionKind())
	if err := r.Get(ctx, client.ObjectKeyFromObject(asc), existing); err == nil {
		logger.Info("AgentServiceConfig already exists")
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	dbSize := "200Gi"
	if hostInv.DatabaseStorageSize != "" {
		dbSize = hostInv.DatabaseStorageSize
	}
	fsSize := "200Gi"
	if hostInv.FilesystemStorageSize != "" {
		fsSize = hostInv.FilesystemStorageSize
	}
	imgSize := "100Gi"
	if hostInv.ImageStorageSize != "" {
		imgSize = hostInv.ImageStorageSize
	}

	osImages := []interface{}{}
	for _, v := range hostInv.Versions {
		arch := v.CpuArchitecture
		if arch == "" {
			arch = "x86_64"
		}
		osImages = append(osImages, map[string]interface{}{
			"openshiftVersion": v.OpenshiftVersion,
			"cpuArchitecture":  arch,
			"rootFSUrl":        fmt.Sprintf("%s/rhcos-live-rootfs.%s.img", rhcosServerURL, arch),
			"url":              fmt.Sprintf("%s/rhcos-live.%s.iso", rhcosServerURL, arch),
			"version":          v.OpenshiftVersion,
		})
	}

	buildStorage := func(size string) map[string]interface{} {
		storage := map[string]interface{}{
			"accessModes": []interface{}{"ReadWriteOnce"},
			"resources":   map[string]interface{}{"requests": map[string]interface{}{"storage": size}},
		}
		if hostInv.StorageClass != "" {
			storage["storageClassName"] = hostInv.StorageClass
		}
		return storage
	}

	spec := map[string]interface{}{
		"databaseStorage":   buildStorage(dbSize),
		"filesystemStorage": buildStorage(fsSize),
		"imageStorage":      buildStorage(imgSize),
		"mirrorRegistryRef": map[string]interface{}{
			"name": "assisted-installer-mirror-config",
		},
		"osImages": osImages,
	}

	asc.Object["spec"] = spec

	if err := r.Create(ctx, asc); err != nil {
		return fmt.Errorf("failed to create AgentServiceConfig: %w", err)
	}
	logger.Info("Created AgentServiceConfig for host inventory")
	return nil
}

// ensureClusterImageSets creates ClusterImageSet CRs for each configured OCP version.
func (r *DisconnectedPlatformReconciler) ensureClusterImageSets(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)

	acmConfig := platform.Spec.Airgapped.ACM
	if acmConfig == nil || acmConfig.HostInventory == nil || !acmConfig.HostInventory.Enabled {
		return nil
	}

	mirrorRegistry := platform.Spec.Airgapped.MirrorRegistry
	if mirrorRegistry == "" {
		return nil
	}

	for _, v := range acmConfig.HostInventory.Versions {
		cisName := fmt.Sprintf("openshift-%s", strings.ReplaceAll(v.OpenshiftVersion, ".", "-"))

		cis := &unstructured.Unstructured{}
		cis.SetGroupVersionKind(schema.GroupVersionKind{
			Group: "hive.openshift.io", Version: "v1", Kind: "ClusterImageSet",
		})
		cis.SetName(cisName)

		existing := &unstructured.Unstructured{}
		existing.SetGroupVersionKind(cis.GroupVersionKind())
		if err := r.Get(ctx, client.ObjectKeyFromObject(cis), existing); err == nil {
			continue
		} else if !apierrors.IsNotFound(err) {
			return err
		}

		// Extract registry host for release images path
		registryHost := mirrorRegistry
		if idx := strings.Index(mirrorRegistry, "/"); idx > 0 {
			registryHost = mirrorRegistry[:idx]
		}

		releaseImage := fmt.Sprintf("%s/openshift/release-images:%s-x86_64", registryHost, v.OpenshiftVersion)
		cis.Object["spec"] = map[string]interface{}{
			"releaseImage": releaseImage,
		}

		if err := r.Create(ctx, cis); err != nil {
			return fmt.Errorf("failed to create ClusterImageSet %s: %w", cisName, err)
		}
		logger.Info("Created ClusterImageSet", "name", cisName, "releaseImage", releaseImage)
	}

	return nil
}

// ensureInfraEnv creates an InfraEnv resource for assisted installer discovery, injecting cluster-level settings.
func (r *DisconnectedPlatformReconciler) ensureInfraEnv(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)

	hostInv := platform.Spec.Airgapped.ACM.HostInventory
	if hostInv.InfraEnv == nil || !hostInv.InfraEnv.Enabled {
		return nil
	}

	infraEnvCfg := hostInv.InfraEnv
	ns := infraEnvCfg.Namespace
	if ns == "" {
		ns = architectNamespace
	}

	infraEnv := &unstructured.Unstructured{}
	infraEnv.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "agent-install.openshift.io",
		Version: "v1beta1",
		Kind:    "InfraEnv",
	})
	infraEnv.SetName("mirror-operator-infraenv")
	infraEnv.SetNamespace(ns)

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(infraEnv.GroupVersionKind())
	if err := r.Get(ctx, client.ObjectKeyFromObject(infraEnv), existing); err == nil {
		logger.Info("InfraEnv already exists")
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	spec := map[string]interface{}{
		"pullSecretRef": map[string]interface{}{
			"name": "pull-secret",
		},
	}

	// Inject cluster version
	if version := r.getClusterVersion(ctx); version != "" {
		spec["osImageVersion"] = version
	}

	// CPU architecture
	arch := infraEnvCfg.CpuArchitecture
	if arch == "" {
		arch = "x86_64"
	}
	spec["cpuArchitecture"] = arch

	// Image type
	imageType := infraEnvCfg.ImageType
	if imageType == "" {
		imageType = "full-iso"
	}
	spec["imageType"] = imageType

	// SSH key: explicit CRD value, then cluster MachineConfig fallback
	sshKey := infraEnvCfg.SSHAuthorizedKey
	if sshKey == "" {
		sshKey = r.getClusterSSHKey(ctx)
	}
	if sshKey != "" {
		spec["sshAuthorizedKey"] = sshKey
	}

	// Proxy settings from cluster Proxy CR
	httpProxy, httpsProxy, noProxy := r.getClusterProxy(ctx)
	if httpProxy != "" || httpsProxy != "" || noProxy != "" {
		proxySpec := map[string]interface{}{}
		if httpProxy != "" {
			proxySpec["httpProxy"] = httpProxy
		}
		if httpsProxy != "" {
			proxySpec["httpsProxy"] = httpsProxy
		}
		if noProxy != "" {
			proxySpec["noProxy"] = noProxy
		}
		spec["proxy"] = proxySpec
	}

	// NTP sources from CRD
	if len(infraEnvCfg.AdditionalNTPSources) > 0 {
		ntpSources := make([]interface{}, len(infraEnvCfg.AdditionalNTPSources))
		for i, s := range infraEnvCfg.AdditionalNTPSources {
			ntpSources[i] = s
		}
		spec["additionalNTPSources"] = ntpSources
	}

	// CA trust bundle
	if caBundle := r.getMirrorRegistryCA(ctx, platform); caBundle != "" {
		spec["additionalTrustBundle"] = caBundle
	}

	// Mirror registry ref (created by ensureAssistedInstallerMirrorConfig)
	spec["mirrorRegistryRef"] = map[string]interface{}{
		"name":      "assisted-installer-mirror-config",
		"namespace": "multicluster-engine",
	}

	// Static networking label selector
	if infraEnvCfg.NetworkType == "static" && len(infraEnvCfg.NMStateConfigLabels) > 0 {
		spec["nmStateConfigLabelSelector"] = map[string]interface{}{
			"matchLabels": toStringInterfaceMap(infraEnvCfg.NMStateConfigLabels),
		}
	}

	// Sigstore signature verification: inject policy.json and registries.d config
	if ignitionOverride := r.buildIgnitionConfigOverride(ctx); ignitionOverride != "" {
		spec["ignitionConfigOverride"] = ignitionOverride
	}

	infraEnv.Object["spec"] = spec

	if err := r.Create(ctx, infraEnv); err != nil {
		return fmt.Errorf("failed to create InfraEnv: %w", err)
	}
	logger.Info("Created InfraEnv for host inventory discovery", "namespace", ns)
	return nil
}

func (r *DisconnectedPlatformReconciler) ensureACMCredential(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)

	hostInv := platform.Spec.Airgapped.ACM.HostInventory
	if hostInv.Credential == nil || !hostInv.Credential.Enabled {
		return nil
	}

	credCfg := hostInv.Credential
	name := credCfg.Name
	if name == "" {
		name = "mirror-operator-credential"
	}
	ns := credCfg.Namespace
	if ns == "" {
		ns = "open-cluster-management"
	}

	existing := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, existing); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	sourceSecret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: architectNamespace}, sourceSecret); err != nil {
		return fmt.Errorf("failed to get pull-secret for ACM credential: %w", err)
	}
	pullSecretJSON := string(sourceSecret.Data[".dockerconfigjson"])

	sshPublicKey := ""
	if hostInv.InfraEnv != nil && hostInv.InfraEnv.SSHAuthorizedKey != "" {
		sshPublicKey = hostInv.InfraEnv.SSHAuthorizedKey
	}
	if sshPublicKey == "" {
		sshPublicKey = r.getClusterSSHKey(ctx)
	}

	credential := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels: map[string]string{
				"cluster.open-cluster-management.io/type":        "ans",
				"cluster.open-cluster-management.io/credentials": "",
			},
		},
		Type: corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"pullSecret":     pullSecretJSON,
			"ssh-publickey":  sshPublicKey,
			"ssh-privatekey": "",
			"baseDomain":     credCfg.BaseDomain,
		},
	}

	if err := r.Create(ctx, credential); err != nil {
		return fmt.Errorf("failed to create ACM credential: %w", err)
	}
	logger.Info("Created ACM infrastructure provider credential", "name", name, "namespace", ns)
	return nil
}

// buildIgnitionConfigOverride builds an ignition config that injects the proper
// sigstore signature verification policy and registries.d config onto the discovery ISO.
// It reads ClusterImagePolicy resources to get the public keys and IDMS entries to
// build remapIdentity mappings so the discovery node can verify cosign signatures
// from the mirror registry.
func (r *DisconnectedPlatformReconciler) buildIgnitionConfigOverride(ctx context.Context) string {
	logger := log.FromContext(ctx)

	// Read ClusterImagePolicy resources to get public keys and scopes
	cipList := &unstructured.UnstructuredList{}
	cipList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "ClusterImagePolicy",
	})

	type policyEntry struct {
		keyData string
		scopes  []string
	}
	var policies []policyEntry

	if err := r.List(ctx, cipList); err == nil {
		for _, cip := range cipList.Items {
			keyData, _, _ := unstructured.NestedString(cip.Object, "spec", "policy", "rootOfTrust", "publicKey", "keyData")
			if keyData == "" {
				continue
			}
			scopesRaw, _, _ := unstructured.NestedStringSlice(cip.Object, "spec", "scopes")
			if len(scopesRaw) == 0 {
				continue
			}
			policies = append(policies, policyEntry{keyData: keyData, scopes: scopesRaw})
		}
	} else {
		logger.V(1).Info("Could not list ClusterImagePolicy resources", "error", err)
	}

	if len(policies) == 0 {
		return ""
	}

	// Read IDMS to find mirror→source mappings
	type mirrorMapping struct {
		mirror string
		source string
	}
	var mappings []mirrorMapping

	idmsList := &unstructured.UnstructuredList{}
	idmsList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "ImageDigestMirrorSet",
	})
	if err := r.List(ctx, idmsList); err == nil {
		for _, idms := range idmsList.Items {
			entries, _, _ := unstructured.NestedSlice(idms.Object, "spec", "imageDigestMirrors")
			for _, entry := range entries {
				e, ok := entry.(map[string]interface{})
				if !ok {
					continue
				}
				source, _, _ := unstructured.NestedString(e, "source")
				mirrors, _, _ := unstructured.NestedStringSlice(e, "mirrors")
				for _, m := range mirrors {
					mappings = append(mappings, mirrorMapping{mirror: m, source: source})
				}
			}
		}
	}

	// Build policy.json
	// Default: insecureAcceptAnything (same as hub nodes)
	// For each ClusterImagePolicy scope + its mirror paths: sigstoreSigned with remapIdentity
	dockerTransport := map[string]interface{}{}

	for _, pol := range policies {
		for _, scope := range pol.scopes {
			// Add entry for the source scope itself
			dockerTransport[scope] = []interface{}{
				map[string]interface{}{
					"type":    "sigstoreSigned",
					"keyData": pol.keyData,
					"signedIdentity": map[string]interface{}{
						"type": "matchRepoDigestOrExact",
					},
				},
			}

			// Add entries for each mirror of this scope
			for _, mm := range mappings {
				if mm.source == scope {
					dockerTransport[mm.mirror] = []interface{}{
						map[string]interface{}{
							"type":    "sigstoreSigned",
							"keyData": pol.keyData,
							"signedIdentity": map[string]interface{}{
								"type":         "remapIdentity",
								"prefix":       mm.mirror,
								"signedPrefix": scope,
							},
						},
					}
				}
			}
		}
	}

	dockerTransport[""] = []interface{}{
		map[string]interface{}{"type": "insecureAcceptAnything"},
	}

	policyJSON := map[string]interface{}{
		"default": []interface{}{
			map[string]interface{}{"type": "insecureAcceptAnything"},
		},
		"transports": map[string]interface{}{
			"atomic": dockerTransport,
			"docker": dockerTransport,
			"docker-daemon": map[string]interface{}{
				"": []interface{}{
					map[string]interface{}{"type": "insecureAcceptAnything"},
				},
			},
		},
	}

	policyBytes, err := json.Marshal(policyJSON)
	if err != nil {
		logger.Error(err, "Failed to marshal policy.json")
		return ""
	}

	// Build registries.d/sigstore-registries.yaml
	// Enable use-sigstore-attachments for all mirror registries and source scopes
	registriesD := map[string]interface{}{}
	dockerEntries := map[string]interface{}{}

	for _, pol := range policies {
		for _, scope := range pol.scopes {
			dockerEntries[scope] = map[string]interface{}{
				"use-sigstore-attachments": true,
			}
			for _, mm := range mappings {
				if mm.source == scope {
					dockerEntries[mm.mirror] = map[string]interface{}{
						"use-sigstore-attachments": true,
					}
				}
			}
		}
	}
	registriesD["docker"] = dockerEntries

	registriesDBytes, err := yaml.Marshal(registriesD)
	if err != nil {
		logger.Error(err, "Failed to marshal sigstore-registries.yaml")
		return ""
	}

	// Build ignition config with both files
	policyB64 := base64.StdEncoding.EncodeToString(policyBytes)
	registriesDB64 := base64.StdEncoding.EncodeToString(registriesDBytes)

	ignitionConfig := map[string]interface{}{
		"ignition": map[string]interface{}{
			"version": "3.2.0",
		},
		"storage": map[string]interface{}{
			"files": []interface{}{
				map[string]interface{}{
					"path":      "/etc/containers/policy.json",
					"mode":      420,
					"overwrite": true,
					"contents": map[string]interface{}{
						"source": "data:text/plain;charset=utf-8;base64," + policyB64,
					},
				},
				map[string]interface{}{
					"path":      "/etc/containers/registries.d/sigstore-registries.yaml",
					"mode":      420,
					"overwrite": true,
					"contents": map[string]interface{}{
						"source": "data:text/plain;charset=utf-8;base64," + registriesDB64,
					},
				},
			},
		},
	}

	ignitionBytes, err := json.Marshal(ignitionConfig)
	if err != nil {
		logger.Error(err, "Failed to marshal ignition config")
		return ""
	}

	return string(ignitionBytes)
}

func toStringInterfaceMap(m map[string]string) map[string]interface{} {
	result := make(map[string]interface{}, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

// deleteHostInventoryResources removes resources created for host inventory support.
func (r *DisconnectedPlatformReconciler) deleteHostInventoryResources(ctx context.Context) {
	logger := log.FromContext(ctx)

	// Delete InfraEnv
	infraEnv := &unstructured.Unstructured{}
	infraEnv.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "agent-install.openshift.io", Version: "v1beta1", Kind: "InfraEnv",
	})
	infraEnv.SetName("mirror-operator-infraenv")
	infraEnv.SetNamespace(architectNamespace)
	if err := r.Delete(ctx, infraEnv); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to delete InfraEnv")
	}

	// Delete AgentServiceConfig
	asc := &unstructured.Unstructured{}
	asc.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "agent-install.openshift.io", Version: "v1beta1", Kind: "AgentServiceConfig",
	})
	asc.SetName("agent")
	if err := r.Delete(ctx, asc); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to delete AgentServiceConfig")
	}

	// Delete RHCOS server deployment
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "rhcos-server", Namespace: architectNamespace},
	}
	if err := r.Delete(ctx, deploy); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to delete RHCOS server deployment")
	}

	// Delete RHCOS server service
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "rhcos-server", Namespace: architectNamespace},
	}
	if err := r.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to delete RHCOS server service")
	}

	// Delete assisted installer mirror config
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "assisted-installer-mirror-config", Namespace: "multicluster-engine"},
	}
	if err := r.Delete(ctx, cm); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to delete assisted-installer-mirror-config")
	}

	// Delete ACM infrastructure provider credential
	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-credential", Namespace: "open-cluster-management"},
	}
	if err := r.Delete(ctx, credSecret); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to delete ACM credential")
	}
}

// deleteAirgappedACM removes the MultiClusterHub CR and ACM subscription.
func (r *DisconnectedPlatformReconciler) deleteAirgappedACM(ctx context.Context) {
	mch := &unstructured.Unstructured{}
	mch.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "operator.open-cluster-management.io", Version: "v1", Kind: "MultiClusterHub",
	})
	mch.SetName("multiclusterhub")
	mch.SetNamespace("open-cluster-management")
	if err := r.Delete(ctx, mch); err != nil && !apierrors.IsNotFound(err) {
		log.FromContext(ctx).Error(err, "failed to delete MultiClusterHub")
	}

	sub := &unstructured.Unstructured{}
	sub.SetGroupVersionKind(subscriptionGVK)
	sub.SetName("mirror-operator-advanced-cluster-management")
	sub.SetNamespace("open-cluster-management")
	if err := r.Delete(ctx, sub); err != nil && !apierrors.IsNotFound(err) {
		log.FromContext(ctx).Error(err, "failed to delete ACM subscription")
	}
}

func (r *DisconnectedPlatformReconciler) reconcileImportPipelineTemplate(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)

	if platform.Spec.Mode != mirrorv1.PlatformModeAirgapped {
		return nil
	}

	namespace := architectNamespace
	pipelineName := "import-pipeline-template"

	pipeline := &unstructured.Unstructured{}
	pipeline.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "tekton.dev",
		Version: "v1",
		Kind:    "Pipeline",
	})
	pipeline.SetName(pipelineName)
	pipeline.SetNamespace(namespace)

	params := []map[string]interface{}{
		{"name": "bundle-filename", "type": "string", "description": "Tar filename on the bundle PVC"},
		{"name": "target-registry", "type": "string", "description": "Destination mirror registry URL"},
		{"name": "mirror-image", "type": "string", "default": "quay.io/mathianasj/oc-mirror:v2", "description": "oc-mirror container image"},
		{"name": "verify-enabled", "type": "string", "default": "false", "description": "Enable cosign signature verification"},
		{"name": "cosign-pub-secret", "type": "string", "default": "", "description": "Secret name containing cosign public key"},
	}

	workspaces := []map[string]interface{}{
		{"name": "bundle-data", "description": "PVC containing the bundle tar file"},
		{"name": "config", "description": "ConfigMap with imageset-config.yaml"},
		{"name": "pull-secret", "description": "Registry pull secret for authentication"},
		{"name": "cosign-pub", "description": "Cosign public key secret for verification", "optional": true},
	}

	tasks := r.buildImportPipelineTasks()

	pipelineSpec := map[string]interface{}{
		"params":     params,
		"workspaces": workspaces,
		"tasks":      tasks,
	}

	specJSON, err := json.Marshal(pipelineSpec)
	if err != nil {
		return fmt.Errorf("failed to marshal import pipeline spec: %w", err)
	}

	var specMap map[string]interface{}
	if err := json.Unmarshal(specJSON, &specMap); err != nil {
		return fmt.Errorf("failed to unmarshal import pipeline spec: %w", err)
	}

	pipeline.Object["spec"] = specMap

	if err := ctrl.SetControllerReference(platform, pipeline, r.Scheme); err != nil {
		return fmt.Errorf("failed to set owner reference on import pipeline: %w", err)
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(pipeline.GroupVersionKind())
	if err := r.Get(ctx, client.ObjectKeyFromObject(pipeline), existing); err == nil {
		pipeline.SetResourceVersion(existing.GetResourceVersion())
		if err := r.Update(ctx, pipeline); err != nil {
			return fmt.Errorf("failed to update import pipeline template: %w", err)
		}
		logger.Info("Updated import pipeline template")
	} else if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, pipeline); err != nil {
			return fmt.Errorf("failed to create import pipeline template: %w", err)
		}
		logger.Info("Created import pipeline template")
	} else {
		return err
	}

	return nil
}

func (r *DisconnectedPlatformReconciler) buildImportPipelineTasks() []map[string]interface{} {
	return []map[string]interface{}{
		{
			"name": "verify-bundle",
			"when": []map[string]interface{}{
				{"input": "$(params.verify-enabled)", "operator": "in", "values": []string{"true"}},
			},
			"taskSpec": map[string]interface{}{
				"steps": []map[string]interface{}{
					{
						"name":    "verify",
						"image":   "$(params.mirror-image)",
						"command": []string{"/bin/bash", "-c"},
						"args": []string{`
set -ex
BUNDLE_FILE="$(params.bundle-filename)"
bn=$(basename "$BUNDLE_FILE" .tar)

echo "=== Verifying bundle signature ==="
cosign verify-blob \
  --key /workspace/cosign-pub/cosign.pub \
  --signature "/workspace/bundle-data/${bn}.sig" \
  "/workspace/bundle-data/${BUNDLE_FILE}"

echo "=== Verifying attestation signature ==="
cosign verify-blob \
  --key /workspace/cosign-pub/cosign.pub \
  --signature /workspace/bundle-data/attestation.json.sig \
  /workspace/bundle-data/attestation.json

echo "=== Verifying attestation hashes ==="
abh=$(jq -r '.bundle.sha256' /workspace/bundle-data/attestation.json)
ash=$(jq -r '.sbom.sha256' /workspace/bundle-data/attestation.json)
cbh=$(sha256sum "/workspace/bundle-data/${BUNDLE_FILE}" | cut -d" " -f1)

if [ "$cbh" != "$abh" ]; then
  echo "ERROR: Bundle hash mismatch: expected $abh, got $cbh"
  exit 1
fi
echo "Bundle hash verified: $cbh"

if [ -f /workspace/bundle-data/sbom.cyclonedx.json ]; then
  csh=$(sha256sum /workspace/bundle-data/sbom.cyclonedx.json | cut -d" " -f1)
  if [ "$csh" != "$ash" ]; then
    echo "ERROR: SBOM hash mismatch: expected $ash, got $csh"
    exit 1
  fi
  echo "SBOM hash verified: $csh"
fi

echo "=== All verifications passed ==="
`},
					},
				},
			},
			"workspaces": []map[string]interface{}{
				{"name": "bundle-data"},
				{"name": "cosign-pub"},
			},
		},

		{
			"name":     "extract-bundle",
			"runAfter": []string{"verify-bundle"},
			"taskSpec": map[string]interface{}{
				"steps": []map[string]interface{}{
					{
						"name":    "extract",
						"image":   "registry.access.redhat.com/ubi9/ubi:latest",
						"command": []string{"/bin/sh", "-c"},
						"args": []string{`
set -ex
BUNDLE_FILE="$(params.bundle-filename)"
echo "=== Extracting bundle: ${BUNDLE_FILE} ==="
tar -xvf "/workspace/bundle-data/${BUNDLE_FILE}" -C /workspace/bundle-data
echo "=== Extraction complete ==="
echo "Contents:"
ls -lh /workspace/bundle-data/
`},
					},
				},
			},
			"workspaces": []map[string]interface{}{
				{"name": "bundle-data"},
			},
		},

		{
			"name":     "mirror-content",
			"runAfter": []string{"extract-bundle"},
			"taskSpec": map[string]interface{}{
				"steps": []map[string]interface{}{
					{
						"name":    "oc-mirror",
						"image":   "$(params.mirror-image)",
						"command": []string{"/bin/bash", "-c"},
						"args": []string{`
set -ex
mkdir -p $HOME/.docker
cp /workspace/pull-secret/.dockerconfigjson $HOME/.docker/config.json
echo "=== Mirroring content to $(params.target-registry) ==="
oc-mirror \
  --config /workspace/config/imageset-config.yaml \
  --from file:///workspace/bundle-data/archives \
  docker://$(params.target-registry) \
  --v2
echo "=== Mirror complete ==="
`},
					},
				},
			},
			"workspaces": []map[string]interface{}{
				{"name": "config"},
				{"name": "bundle-data"},
				{"name": "pull-secret"},
			},
		},

		{
			"name":     "apply-manifests",
			"runAfter": []string{"mirror-content"},
			"taskSpec": map[string]interface{}{
				"steps": []map[string]interface{}{
					{
						"name":    "apply",
						"image":   "$(params.mirror-image)",
						"command": []string{"/bin/bash", "-c"},
						"args": []string{`
set -ex
echo "=== Applying cluster manifests ==="
MANIFEST_DIR=$(find /workspace/bundle-data -path "*/cluster-resources" -type d 2>/dev/null | head -1)

if [ -z "$MANIFEST_DIR" ]; then
  echo "WARNING: No cluster-resources directory found, skipping manifest apply"
  exit 0
fi

echo "Found manifests in: $MANIFEST_DIR"

for f in "$MANIFEST_DIR"/idms-*.yaml; do
  if [ -f "$f" ]; then
    oc apply -f "$f" && echo "Applied IDMS: $(basename $f)"
  fi
done

for f in "$MANIFEST_DIR"/itms-*.yaml; do
  if [ -f "$f" ]; then
    oc apply -f "$f" && echo "Applied ITMS: $(basename $f)"
  fi
done

for f in "$MANIFEST_DIR"/cs-*.yaml; do
  if [ -f "$f" ]; then
    oc apply -f "$f" && echo "Applied CatalogSource: $(basename $f)"
  fi
done

echo "=== Cluster manifests applied ==="
`},
					},
				},
			},
			"workspaces": []map[string]interface{}{
				{"name": "bundle-data"},
			},
		},

		{
			"name":     "cleanup-workspace",
			"runAfter": []string{"apply-manifests"},
			"taskSpec": map[string]interface{}{
				"steps": []map[string]interface{}{
					{
						"name":    "cleanup",
						"image":   "registry.access.redhat.com/ubi9/ubi-minimal:latest",
						"command": []string{"/bin/sh", "-c"},
						"args": []string{`
set -ex
echo "=== Cleaning up extracted content ==="
rm -rf /workspace/bundle-data/archives /workspace/bundle-data/imageset-config.yaml /workspace/bundle-data/idms-oc-mirror.yaml /workspace/bundle-data/itms-oc-mirror.yaml
echo "Workspace cleaned"
`},
					},
				},
			},
			"workspaces": []map[string]interface{}{
				{"name": "bundle-data"},
			},
		},
	}
}

package controller

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	importFinalizer = "mirror.mathianasj.github.com/import-finalizer"
)

type MirrorImportReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	MirrorImage string
}

// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=mirrorimports,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=mirrorimports/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=mirrorimports/finalizers,verbs=update
// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=disconnectedplatforms,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=disconnectedplatforms/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups=operators.coreos.com,resources=catalogsources,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=config.openshift.io,resources=imagecontentsourcepolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch

func (r *MirrorImportReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	importCR := &mirrorv1.MirrorImport{}
	if err := r.Get(ctx, req.NamespacedName, importCR); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if importCR.ObjectMeta.DeletionTimestamp.IsZero() {
		if !containsString(importCR.GetFinalizers(), importFinalizer) {
			importCR.SetFinalizers(append(importCR.GetFinalizers(), importFinalizer))
			if err := r.Update(ctx, importCR); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	} else {
		if containsString(importCR.GetFinalizers(), importFinalizer) {
			importCR.SetFinalizers(removeString(importCR.GetFinalizers(), importFinalizer))
			if err := r.Update(ctx, importCR); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	switch importCR.Status.Phase {
	case "":
		return r.startImport(ctx, importCR)

	case "Importing":
		return r.trackImportJob(ctx, importCR, req)

	case "Publishing":
		return r.finalizeImport(ctx, importCR)
	}

	return ctrl.Result{}, nil
}

func (r *MirrorImportReconciler) startImport(ctx context.Context, importCR *mirrorv1.MirrorImport) (ctrl.Result, error) {
	if importCR.Spec.CollectionVersion != "" {
		platform, err := r.findPlatform(ctx)
		if err != nil {
			log.FromContext(ctx).Error(err, "failed to lookup platform for dependency check")
			return ctrl.Result{}, err
		}
		if platform != nil {
			// Validate that all prior versions in the chain exist.
			// If collectionVersion is set, check it's not already imported,
			// and verify at least one import exists (for incremental chains)
			// or start fresh (for the first import).
			if versionExists(platform.Status.ImportHistory, importCR.Spec.CollectionVersion) {
				importCR.Status.Phase = "Failed"
				importCR.Status.Conditions = append(importCR.Status.Conditions, metav1.Condition{
					Type:    "DependencyCheck",
					Status:  "False",
					Reason:  "VersionAlreadyImported",
					Message: fmt.Sprintf("version %s has already been imported", importCR.Spec.CollectionVersion),
				})
				return ctrl.Result{}, r.Status().Update(ctx, importCR)
			}
		}
	}

	importCR.Status.Phase = "Importing"
	return ctrl.Result{}, r.Status().Update(ctx, importCR)
}

func (r *MirrorImportReconciler) trackImportJob(ctx context.Context, importCR *mirrorv1.MirrorImport, req ctrl.Request) (ctrl.Result, error) {
	configName := fmt.Sprintf("mirror-import-config-%s", importCR.Name)
	if _, err := r.ensureImportConfigMap(ctx, importCR, configName); err != nil {
		log.FromContext(ctx).Error(err, "failed to ensure import ConfigMap")
		return ctrl.Result{}, err
	}

	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{
		Namespace: req.Namespace,
		Name:      "mirror-import-" + importCR.Name,
	}, job)
	if err != nil {
		if apierrors.IsNotFound(err) {
			job, err := r.buildImportJob(ctx, importCR, configName)
			if err != nil {
				log.FromContext(ctx).Error(err, "failed to build import job")
				return ctrl.Result{}, err
			}
			if err := r.Create(ctx, job); err != nil {
				log.FromContext(ctx).Error(err, "failed to create import job")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobComplete {
			importCR.Status.Phase = "Publishing"
			return ctrl.Result{}, r.Status().Update(ctx, importCR)
		}
		if cond.Type == batchv1.JobFailed {
			importCR.Status.Phase = "Failed"
			return ctrl.Result{}, r.Status().Update(ctx, importCR)
		}
	}
	return ctrl.Result{}, nil
}

func (r *MirrorImportReconciler) finalizeImport(ctx context.Context, importCR *mirrorv1.MirrorImport) (ctrl.Result, error) {
	if importCR.Spec.Publish.CatalogSource {
		if err := r.ensureCatalogSource(ctx, importCR); err != nil {
			log.FromContext(ctx).Error(err, "failed to ensure CatalogSource")
			return ctrl.Result{}, err
		}
	}
	if importCR.Spec.Publish.ImageContentSourcePolicy {
		if err := r.ensureICSP(ctx, importCR); err != nil {
			log.FromContext(ctx).Error(err, "failed to ensure ImageContentSourcePolicy")
			return ctrl.Result{}, err
		}
	}
	importCR.Status.Phase = "Complete"

	if err := r.Status().Update(ctx, importCR); err != nil {
		return ctrl.Result{}, err
	}

	r.updatePlatformImportHistory(ctx, importCR)
	return ctrl.Result{}, nil
}

func (r *MirrorImportReconciler) updatePlatformImportHistory(ctx context.Context, importCR *mirrorv1.MirrorImport) {
	platform, err := r.findPlatform(ctx)
	if err != nil || platform == nil {
		return
	}

	version := importCR.Spec.CollectionVersion
	if version == "" {
		version = fmt.Sprintf("import-%s-%s", importCR.Name, importCR.CreationTimestamp.Format("20060102"))
	}

	info := mirrorv1.ImportInfo{
		Version:   version,
		Timestamp: metav1.Now(),
		Status:    "Complete",
	}
	platform.Status.LastImport = &info
	platform.Status.ImportHistory = append(platform.Status.ImportHistory, info)
	if err := r.Status().Update(ctx, platform); err != nil {
		log.FromContext(ctx).Error(err, "failed to update platform import history")
	}
}

func (r *MirrorImportReconciler) findPlatform(ctx context.Context) (*mirrorv1.DisconnectedPlatform, error) {
	list := &mirrorv1.DisconnectedPlatformList{}
	if err := r.List(ctx, list); err != nil {
		return nil, err
	}
	if len(list.Items) == 0 {
		return nil, nil
	}
	return &list.Items[0], nil
}

func (r *MirrorImportReconciler) ensureImportConfigMap(ctx context.Context, importCR *mirrorv1.MirrorImport, name string) (*corev1.ConfigMap, error) {
	existing := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Namespace: importCR.Namespace, Name: name}, existing)
	if err == nil {
		return existing, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: importCR.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(importCR, mirrorv1.GroupVersion.WithKind("MirrorImport")),
			},
		},
		Data: map[string]string{configMapKey: importCR.Spec.ImageSetConfig},
	}
	return cm, r.Create(ctx, cm)
}

func (r *MirrorImportReconciler) buildImportJob(ctx context.Context, importCR *mirrorv1.MirrorImport, configName string) (*batchv1.Job, error) {
	mirrorImage := r.MirrorImage
	if mirrorImage == "" {
		mirrorImage = defaultMirrorImage
	}

	importArgs := "tar -xvf /data/" + importCR.Spec.Bundle.Filename +
		" -C /workspace && oc-mirror --config /config/" + configMapKey +
		" --from file:///workspace docker://" + importCR.Spec.TargetRegistry.URL + " --v2"

	volumeMounts := []corev1.VolumeMount{
		{Name: "bundle-data", MountPath: "/data"},
		{Name: "config", MountPath: "/config"},
	}
	volumes := []corev1.Volume{
		{
			Name: "bundle-data",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: importCR.Spec.Bundle.PVC,
				},
			},
		},
		{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: configName},
				},
			},
		},
	}

	if importCR.Spec.Verify != nil && importCR.Spec.Verify.PublicKeySecretRef != nil {
		pubKeyName := importCR.Spec.Verify.PublicKeySecretRef.Name
		verifyCmd := fmt.Sprintf(
			"cosign verify-blob --key /workspace/cosign-pub/cosign.pub --signature /data/${bn}.sig /data/%s && ",
			importCR.Spec.Bundle.Filename,
		)
		bnCmd := fmt.Sprintf("bn=$(basename /data/%s .tar) && ", importCR.Spec.Bundle.Filename)
		importArgs = bnCmd + verifyCmd + importArgs

		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name: "cosign-pub", MountPath: "/workspace/cosign-pub",
		})
		volumes = append(volumes, corev1.Volume{
			Name: "cosign-pub",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: pubKeyName,
				},
			},
		})
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mirror-import-" + importCR.Name,
			Namespace: importCR.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(importCR, mirrorv1.GroupVersion.WithKind("MirrorImport")),
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:         "import",
							Image:        mirrorImage,
							Command:      []string{"/bin/sh", "-c"},
							Args:         []string{importArgs},
							VolumeMounts: volumeMounts,
						},
					},
					Volumes:       volumes,
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
		},
	}, nil
}

func (r *MirrorImportReconciler) ensureCatalogSource(ctx context.Context, importCR *mirrorv1.MirrorImport) error {
	logger := log.FromContext(ctx)

	cs := &unstructured.Unstructured{}
	cs.SetAPIVersion("operators.coreos.com/v1alpha1")
	cs.SetKind("CatalogSource")
	cs.SetName("mirror-catalog-" + importCR.Name)
	cs.SetNamespace("openshift-marketplace")

	if err := unstructured.SetNestedField(cs.Object, "grpc", "spec", "sourceType"); err != nil {
		return err
	}
	if err := unstructured.SetNestedField(cs.Object, importCR.Spec.TargetRegistry.URL, "spec", "address"); err != nil {
		return err
	}
	if err := unstructured.SetNestedField(cs.Object, "Mirrored Catalog - "+importCR.Name, "spec", "displayName"); err != nil {
		return err
	}
	if err := unstructured.SetNestedField(cs.Object, "Mirror Operator", "spec", "publisher"); err != nil {
		return err
	}

	if err := r.Client.Create(ctx, cs); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			logger.Error(err, "failed to create CatalogSource")
			return err
		}
		logger.Info("CatalogSource already exists, skipping")
	}
	return nil
}

func (r *MirrorImportReconciler) ensureICSP(ctx context.Context, importCR *mirrorv1.MirrorImport) error {
	return r.ensureIDMS(ctx, importCR)
}

func (r *MirrorImportReconciler) ensureIDMS(ctx context.Context, importCR *mirrorv1.MirrorImport) error {
	logger := log.FromContext(ctx)

	idms := &unstructured.Unstructured{}
	idms.SetAPIVersion("config.openshift.io/v1")
	idms.SetKind("ImageDigestMirrorSet")
	idms.SetName("mirror-" + importCR.Name)

	mirrors := []interface{}{
		map[string]interface{}{
			"source": "registry.redhat.io",
			"mirrors": []interface{}{
				importCR.Spec.TargetRegistry.URL + "/registry.redhat.io",
			},
		},
		map[string]interface{}{
			"source": "registry.connect.redhat.com",
			"mirrors": []interface{}{
				importCR.Spec.TargetRegistry.URL + "/registry.connect.redhat.com",
			},
		},
	}

	if err := unstructured.SetNestedSlice(idms.Object, mirrors, "spec", "imageDigestMirrors"); err != nil {
		return err
	}

	if err := r.Client.Create(ctx, idms); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			logger.Error(err, "failed to create ImageDigestMirrorSet")
			return err
		}
		logger.Info("ImageDigestMirrorSet already exists, skipping")
	}
	return nil
}

func (r *MirrorImportReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mirrorv1.MirrorImport{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}

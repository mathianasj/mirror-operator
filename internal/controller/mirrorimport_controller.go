package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
	pipelinev1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
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
// +kubebuilder:rbac:groups=tekton.dev,resources=pipelineruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tekton.dev,resources=pipelineruns/status,verbs=get
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups=operators.coreos.com,resources=catalogsources,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=config.openshift.io,resources=imagecontentsourcepolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=config.openshift.io,resources=imagedigestmirrorsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=config.openshift.io,resources=imagetagmirrorsets,verbs=get;list;watch;create;update;patch;delete
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
		return r.trackImportPipelineRun(ctx, importCR, req)

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

func (r *MirrorImportReconciler) trackImportPipelineRun(ctx context.Context, importCR *mirrorv1.MirrorImport, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	configName := fmt.Sprintf("mirror-import-config-%s", importCR.Name)
	if _, err := r.ensureImportConfigMap(ctx, importCR, configName); err != nil {
		logger.Error(err, "failed to ensure import ConfigMap")
		return ctrl.Result{}, err
	}

	if importCR.Status.PipelineRunRef != "" {
		pr := &pipelinev1.PipelineRun{}
		err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: importCR.Status.PipelineRunRef}, pr)
		if err != nil {
			if apierrors.IsNotFound(err) {
				importCR.Status.PipelineRunRef = ""
				return ctrl.Result{}, r.Status().Update(ctx, importCR)
			}
			return ctrl.Result{}, err
		}

		phase := importPipelineRunPhase(pr)
		switch phase {
		case "Complete":
			importCR.Status.Phase = "Publishing"
			return ctrl.Result{}, r.Status().Update(ctx, importCR)
		case "Failed":
			importCR.Status.Phase = "Failed"
			return ctrl.Result{}, r.Status().Update(ctx, importCR)
		}
		return ctrl.Result{}, nil
	}

	pr, err := r.buildImportPipelineRun(ctx, importCR, configName)
	if err != nil {
		logger.Error(err, "failed to build import PipelineRun")
		return ctrl.Result{}, err
	}
	if err := r.Create(ctx, pr); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		logger.Error(err, "failed to create import PipelineRun")
		return ctrl.Result{}, err
	}

	importCR.Status.PipelineRunRef = pr.Name
	return ctrl.Result{}, r.Status().Update(ctx, importCR)
}

func importPipelineRunPhase(pr *pipelinev1.PipelineRun) string {
	if pr.Status.CompletionTime != nil {
		for _, c := range pr.Status.Conditions {
			if c.Type == "Succeeded" && c.Status == "True" {
				return "Complete"
			}
		}
		return "Failed"
	}
	if pr.Status.StartTime != nil {
		return "Running"
	}
	return "Pending"
}

func (r *MirrorImportReconciler) buildImportPipelineRun(ctx context.Context, importCR *mirrorv1.MirrorImport, configName string) (*pipelinev1.PipelineRun, error) {
	mirrorImage := r.MirrorImage
	if mirrorImage == "" {
		mirrorImage = defaultMirrorImage
	}

	verifyEnabled := "false"
	cosignPubSecret := ""
	if importCR.Spec.Verify != nil && importCR.Spec.Verify.PublicKeySecretRef != nil {
		verifyEnabled = "true"
		cosignPubSecret = importCR.Spec.Verify.PublicKeySecretRef.Name
	}

	params := []pipelinev1.Param{
		{Name: "bundle-filename", Value: pipelinev1.ParamValue{Type: "string", StringVal: importCR.Spec.Bundle.Filename}},
		{Name: "target-registry", Value: pipelinev1.ParamValue{Type: "string", StringVal: importCR.Spec.TargetRegistry.URL}},
		{Name: "mirror-image", Value: pipelinev1.ParamValue{Type: "string", StringVal: mirrorImage}},
		{Name: "verify-enabled", Value: pipelinev1.ParamValue{Type: "string", StringVal: verifyEnabled}},
		{Name: "cosign-pub-secret", Value: pipelinev1.ParamValue{Type: "string", StringVal: cosignPubSecret}},
	}

	workspaces := []pipelinev1.WorkspaceBinding{
		{
			Name: "bundle-data",
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: importCR.Spec.Bundle.PVC,
			},
		},
		{
			Name: "config",
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: configName},
			},
		},
		{
			Name:     "workspace",
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	}

	if verifyEnabled == "true" {
		workspaces = append(workspaces, pipelinev1.WorkspaceBinding{
			Name: "cosign-pub",
			Secret: &corev1.SecretVolumeSource{
				SecretName: cosignPubSecret,
			},
		})
	} else {
		workspaces = append(workspaces, pipelinev1.WorkspaceBinding{
			Name:     "cosign-pub",
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		})
	}

	pr := &pipelinev1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("import-%s-", importCR.Name),
			Namespace:    importCR.Namespace,
			Annotations: map[string]string{
				"results.tekton.dev/log":    "false",
				"results.tekton.dev/result": "false",
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(importCR, mirrorv1.GroupVersion.WithKind("MirrorImport")),
			},
		},
		Spec: pipelinev1.PipelineRunSpec{
			PipelineRef: &pipelinev1.PipelineRef{
				Name: "import-pipeline-template",
			},
			Params:     params,
			Workspaces: workspaces,
			Timeouts: &pipelinev1.TimeoutFields{
				Pipeline: &metav1.Duration{Duration: 6 * time.Hour},
				Tasks:    &metav1.Duration{Duration: 4 * time.Hour},
			},
		},
	}

	return pr, nil
}

func (r *MirrorImportReconciler) finalizeImport(ctx context.Context, importCR *mirrorv1.MirrorImport) (ctrl.Result, error) {
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

func (r *MirrorImportReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mirrorv1.MirrorImport{}).
		Owns(&pipelinev1.PipelineRun{}).
		Complete(r)
}

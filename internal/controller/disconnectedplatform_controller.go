package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

const (
	platformFinalizer     = "mirror.mathianasj.github.com/platform-finalizer"
	architectNamespace    = "mirror-operator-system"
	defaultPullSecretName = "pull-secret"
	defaultPullSecretNS   = "openshift-config"
	pullSecretVolumeName  = "pull-secret"
	pullSecretMountPath   = "/var/run/secrets/openshift.io/pull-secret"
	pullSecretKey         = ".dockerconfigjson"
)

var (
	subscriptionGVK = schema.GroupVersionKind{
		Group:   "operators.coreos.com",
		Version: "v1alpha1",
		Kind:    "Subscription",
	}
	operatorGroupGVK = schema.GroupVersionKind{
		Group:   "operators.coreos.com",
		Version: "v1",
		Kind:    "OperatorGroup",
	}
	csvGVK = schema.GroupVersionKind{
		Group:   "operators.coreos.com",
		Version: "v1alpha1",
		Kind:    "ClusterServiceVersion",
	}
)

type DisconnectedPlatformReconciler struct {
	client.Client
	Scheme                 *runtime.Scheme
	ArchitectFrontendImage string
	ArchitectBackendImage  string
}

// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=disconnectedplatforms,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=disconnectedplatforms/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=disconnectedplatforms/finalizers,verbs=update
// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=collectionpipelines,verbs=get;list;watch
// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=mirrorimports,verbs=get;list;watch
// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=clusterbootstraps,verbs=get;list;watch
// +kubebuilder:rbac:groups=operators.coreos.com,resources=subscriptions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operators.coreos.com,resources=operatorgroups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operators.coreos.com,resources=clusterserviceversions,verbs=get;list;watch
// +kubebuilder:rbac:groups=rhtpa.io,resources=trustedprofileanalyzers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=route.openshift.io,resources=routes,verbs=get;list;watch;create;update;patch;delete

func (r *DisconnectedPlatformReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	platform := &mirrorv1.DisconnectedPlatform{}
	if err := r.Get(ctx, req.NamespacedName, platform); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !platform.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.cleanup(ctx, platform)
	}

	if !containsString(platform.GetFinalizers(), platformFinalizer) {
		platform.SetFinalizers(append(platform.GetFinalizers(), platformFinalizer))
		return ctrl.Result{}, r.Update(ctx, platform)
	}

	if platform.Status.Phase == "" {
		platform.Status.Phase = mirrorv1.PlatformPhaseReady
	}

	platform.Status.Phase = mirrorv1.PlatformPhaseReady
	platform.Status.Components = nil

	if platform.Spec.Mode == mirrorv1.PlatformModeConnected {
		if err := r.reconcileSubscriptions(ctx, platform); err != nil {
			log.FromContext(ctx).Error(err, "failed to reconcile OLM subscriptions")
			platform.Status.Phase = mirrorv1.PlatformPhaseError
			return ctrl.Result{}, r.Status().Update(ctx, platform)
		}
	}

	verifyingIntegrity := false
	for _, c := range platform.Status.Components {
		if c.Name == "trusted-profile-analyzer" && c.Status == "Succeeded" {
			verifyingIntegrity = true
			break
		}
	}
	if verifyingIntegrity {
		if err := r.reconcileRHTPAConfig(ctx, platform); err != nil {
			log.FromContext(ctx).Error(err, "failed to reconcile RHTPA config")
		}
	}

	if err := r.reconcileArchitect(ctx, platform); err != nil {
		log.FromContext(ctx).Error(err, "failed to reconcile airgap-architect")
		platform.Status.Phase = mirrorv1.PlatformPhaseError
		return ctrl.Result{}, r.Status().Update(ctx, platform)
	}

	// Aggregate collection history from CollectionPipeline resources.
	pipelines := &mirrorv1.CollectionPipelineList{}
	if err := r.List(ctx, pipelines); err == nil {
		var history []mirrorv1.CollectionInfo
		for _, p := range pipelines.Items {
			if p.Status.Version != "" && collectionVersionComplete(p.Status.Phase) {
				info := mirrorv1.CollectionInfo{
					Version:   p.Status.Version,
					Timestamp: metav1.Now(),
					Status:    p.Status.Phase,
				}
				history = append(history, info)
			}
		}
		if len(history) > 0 {
			platform.Status.CollectionHistory = history
			platform.Status.LastCollection = &history[len(history)-1]
		}
	}

	// Aggregate import history from MirrorImport resources.
	imports := &mirrorv1.MirrorImportList{}
	if err := r.List(ctx, imports); err == nil {
		var history []mirrorv1.ImportInfo
		for _, imp := range imports.Items {
			if imp.Status.Phase == "Complete" {
				version := imp.Spec.CollectionVersion
				if version == "" {
					version = imp.Name
				}
				info := mirrorv1.ImportInfo{
					Version:   version,
					Timestamp: imp.CreationTimestamp,
					Status:    imp.Status.Phase,
				}
				history = append(history, info)
			}
		}
		if len(history) > 0 {
			platform.Status.ImportHistory = history
			platform.Status.LastImport = &history[len(history)-1]
		}
	}

	platform.Status.Components = append(platform.Status.Components, mirrorv1.ComponentStatus{
		Name: "disconnected-platform", Status: "Running",
	})

	return ctrl.Result{}, r.Status().Update(ctx, platform)
}

func collectionVersionComplete(phase string) bool {
	return phase == "Complete" || phase == "Succeeded"
}

func (r *DisconnectedPlatformReconciler) cleanup(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) (ctrl.Result, error) {
	if containsString(platform.GetFinalizers(), platformFinalizer) {
		if err := r.deleteArchitectResources(ctx, platform); err != nil {
			return ctrl.Result{}, err
		}
		r.deleteRHTPAConfig(ctx)
		platform.SetFinalizers(removeString(platform.GetFinalizers(), platformFinalizer))
		return ctrl.Result{}, r.Update(ctx, platform)
	}
	return ctrl.Result{}, nil
}

type operatorDef struct {
	name      string
	pkg       string
	channel   string
	catalog   string
	catalogNS string
	ns        string
}

var defaultOperators = []operatorDef{
	{
		name:      "openshift-pipelines",
		pkg:       "openshift-pipelines-operator-rh",
		channel:   "latest",
		catalog:   "redhat-operators",
		catalogNS: "openshift-marketplace",
		ns:        "openshift-operators",
	},
	{
		name:      "trusted-artifact-signer",
		pkg:       "rhtas-operator",
		channel:   "stable",
		catalog:   "redhat-operators",
		catalogNS: "openshift-marketplace",
		ns:        "openshift-operators",
	},
	{
		name:      "trusted-profile-analyzer",
		pkg:       "rhtpa-operator",
		channel:   "stable-v1.1",
		catalog:   "redhat-operators",
		catalogNS: "openshift-marketplace",
		ns:        "rhtpa-operator",
	},
}

func getOperatorOverrides(platform *mirrorv1.DisconnectedPlatform) map[string]*mirrorv1.OLMSubscriptionConfig {
	if platform.Spec.Connected == nil || platform.Spec.Connected.Operators == nil {
		return nil
	}
	op := platform.Spec.Connected.Operators
	overrides := make(map[string]*mirrorv1.OLMSubscriptionConfig)
	if op.OpenShiftPipelines != nil {
		overrides["openshift-pipelines"] = op.OpenShiftPipelines
	}
	if op.RHTAS != nil {
		overrides["trusted-artifact-signer"] = op.RHTAS
	}
	if op.RHTPA != nil {
		overrides["trusted-profile-analyzer"] = op.RHTPA
	}
	return overrides
}

func (r *DisconnectedPlatformReconciler) reconcileSubscriptions(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	overrides := getOperatorOverrides(platform)

	var components []mirrorv1.ComponentStatus

	for _, op := range defaultOperators {
		var cfg *mirrorv1.OLMSubscriptionConfig
		if overrides != nil {
			cfg = overrides[op.name]
		}
		if cfg != nil && cfg.Disabled {
			components = append(components, mirrorv1.ComponentStatus{
				Name:   op.name,
				Status: "Disabled",
			})
			continue
		}

		if err := r.ensureNamespace(ctx, op.ns); err != nil {
			components = append(components, mirrorv1.ComponentStatus{
				Name:   op.name,
				Status: "Error",
			})
			platform.Status.Components = components
			return fmt.Errorf("namespace for %s: %w", op.name, err)
		}

		if err := r.ensureOperatorGroup(ctx, op); err != nil {
			components = append(components, mirrorv1.ComponentStatus{
				Name:   op.name,
				Status: "Error",
			})
			platform.Status.Components = components
			return fmt.Errorf("operatorgroup for %s: %w", op.name, err)
		}

		if err := r.ensureSubscription(ctx, op, cfg); err != nil {
			components = append(components, mirrorv1.ComponentStatus{
				Name:   op.name,
				Status: "Error",
			})
			platform.Status.Components = components
			return fmt.Errorf("subscription for %s: %w", op.name, err)
		}

		compStatus := r.csvStatus(ctx, op)
		if compStatus == "" {
			compStatus = "Installing"
		}
		components = append(components, mirrorv1.ComponentStatus{
			Name:   op.name,
			Status: compStatus,
		})
	}

	platform.Status.Components = components
	return nil
}

func (r *DisconnectedPlatformReconciler) ensureNamespace(ctx context.Context, ns string) error {
	if ns == "openshift-operators" {
		return nil
	}
	n := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}
	if err := r.Get(ctx, client.ObjectKeyFromObject(n), n); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	return r.Create(ctx, n)
}

func (r *DisconnectedPlatformReconciler) csvStatus(ctx context.Context, op operatorDef) string {
	sub := &unstructured.Unstructured{}
	sub.SetGroupVersionKind(subscriptionGVK)
	sub.SetName("mirror-operator-" + op.name)
	sub.SetNamespace(op.ns)
	if err := r.Get(ctx, client.ObjectKeyFromObject(sub), sub); err != nil {
		return ""
	}
	csvName, _, _ := unstructured.NestedString(sub.Object, "status", "currentCSV")
	if csvName == "" {
		return ""
	}
	csv := &unstructured.Unstructured{}
	csv.SetGroupVersionKind(csvGVK)
	csv.SetName(csvName)
	csv.SetNamespace(op.ns)
	if err := r.Get(ctx, client.ObjectKeyFromObject(csv), csv); err != nil {
		return ""
	}
	phase, _, _ := unstructured.NestedString(csv.Object, "status", "phase")
	return phase
}

func (r *DisconnectedPlatformReconciler) reconcileRHTPAConfig(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	cfg := platform.Spec.Connected.RHTPA
	if cfg == nil || cfg.Storage == nil || cfg.Database == nil {
		return nil
	}

	tpa := &unstructured.Unstructured{}
	tpa.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "rhtpa.io",
		Version: "v1",
		Kind:    "TrustedProfileAnalyzer",
	})
	tpa.SetName("mirror-operator-trusted-profile-analyzer")
	tpa.SetNamespace("rhtpa-operator")

	if err := r.Get(ctx, client.ObjectKeyFromObject(tpa), tpa); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	ingress := &unstructured.Unstructured{}
	ingress.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "Ingress",
	})
	ingress.SetName("cluster")
	if err := r.Get(ctx, client.ObjectKeyFromObject(ingress), ingress); err != nil {
		return fmt.Errorf("failed to get cluster ingress: %w", err)
	}
	domain, _, _ := unstructured.NestedString(ingress.Object, "spec", "domain")
	appDomain := "rhtpa." + domain

	spec := map[string]interface{}{
		"appDomain": appDomain,
		"openshift": map[string]interface{}{
			"useServiceCa": true,
		},
		"database": map[string]interface{}{
			"host":     cfg.Database.Host,
			"name":     cfg.Database.Name,
			"username": cfg.Database.Username,
			"password": cfg.Database.Password,
		},
		"modules": map[string]interface{}{
			"importer": map[string]interface{}{
				"enabled":     true,
				"concurrency": 1,
				"replicas":    1,
				"resources": map[string]interface{}{
					"requests": map[string]interface{}{
						"cpu":    "1",
						"memory": "8Gi",
					},
				},
			},
			"server": map[string]interface{}{
				"enabled":  true,
				"replicas": 1,
				"resources": map[string]interface{}{
					"requests": map[string]interface{}{
						"cpu":    "1",
						"memory": "8Gi",
					},
				},
			},
		},
	}

	st := map[string]interface{}{
		"type": cfg.Storage.Type,
	}
	if cfg.Storage.AccessKey != "" {
		st["accessKey"] = cfg.Storage.AccessKey
		st["secretKey"] = cfg.Storage.SecretKey
		st["bucket"] = cfg.Storage.Bucket
		st["region"] = cfg.Storage.Region
	} else {
		st["size"] = cfg.Storage.Size
	}
	spec["storage"] = st

	tpa = &unstructured.Unstructured{}
	tpa.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "rhtpa.io",
		Version: "v1",
		Kind:    "TrustedProfileAnalyzer",
	})
	tpa.SetName("mirror-operator-trusted-profile-analyzer")
	tpa.SetNamespace("rhtpa-operator")
	tpa.Object["spec"] = spec

	return r.Create(ctx, tpa)
}

func (r *DisconnectedPlatformReconciler) deleteRHTPAConfig(ctx context.Context) {
	tpa := &unstructured.Unstructured{}
	tpa.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "rhtpa.io",
		Version: "v1",
		Kind:    "TrustedProfileAnalyzer",
	})
	tpa.SetName("mirror-operator-trusted-profile-analyzer")
	tpa.SetNamespace("rhtpa-operator")
	if err := r.Delete(ctx, tpa); err != nil && !apierrors.IsNotFound(err) {
		log.FromContext(ctx).Error(err, "failed to delete RHTPA config")
	}
}

func (r *DisconnectedPlatformReconciler) ensureOperatorGroup(ctx context.Context, op operatorDef) error {
	// Check if any OperatorGroup already exists in the namespace.
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(operatorGroupGVK)
	if err := r.List(ctx, list, client.InNamespace(op.ns)); err == nil && len(list.Items) > 0 {
		return nil
	}

	og := &unstructured.Unstructured{}
	og.SetGroupVersionKind(operatorGroupGVK)
	og.SetName("mirror-operator-" + op.name)
	og.SetNamespace(op.ns)
	unstructured.SetNestedStringSlice(og.Object, []string{op.ns}, "spec", "targetNamespaces")

	return r.Create(ctx, og)
}

func (r *DisconnectedPlatformReconciler) ensureSubscription(ctx context.Context, op operatorDef, cfg *mirrorv1.OLMSubscriptionConfig) error {
	sub := &unstructured.Unstructured{}
	sub.SetGroupVersionKind(subscriptionGVK)
	sub.SetName("mirror-operator-" + op.name)
	sub.SetNamespace(op.ns)

	if err := r.Get(ctx, client.ObjectKeyFromObject(sub), sub); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	channel := op.channel
	if cfg != nil && cfg.Channel != "" {
		channel = cfg.Channel
	}
	catalog := op.catalog
	if cfg != nil && cfg.CatalogSource != "" {
		catalog = cfg.CatalogSource
	}
	catalogNS := op.catalogNS
	if cfg != nil && cfg.CatalogSourceNS != "" {
		catalogNS = cfg.CatalogSourceNS
	}
	approval := "Automatic"
	if cfg != nil && cfg.ApprovalStrategy != "" {
		approval = cfg.ApprovalStrategy
	}

	sub = &unstructured.Unstructured{}
	sub.SetGroupVersionKind(subscriptionGVK)
	sub.SetName("mirror-operator-" + op.name)
	sub.SetNamespace(op.ns)
	unstructured.SetNestedField(sub.Object, op.pkg, "spec", "name")
	unstructured.SetNestedField(sub.Object, channel, "spec", "channel")
	unstructured.SetNestedField(sub.Object, catalog, "spec", "source")
	unstructured.SetNestedField(sub.Object, catalogNS, "spec", "sourceNamespace")
	unstructured.SetNestedField(sub.Object, approval, "spec", "installPlanApproval")

	if cfg != nil && cfg.StartingCSV != "" {
		unstructured.SetNestedField(sub.Object, cfg.StartingCSV, "spec", "startingCSV")
	}

	return r.Create(ctx, sub)
}

const (
	architectPort = 4000
	frontendPort  = 5173
)

var (
	deploymentGVK = schema.GroupVersionKind{
		Group:   "apps",
		Version: "v1",
		Kind:    "Deployment",
	}
	serviceGVK = schema.GroupVersionKind{
		Group:   "",
		Version: "v1",
		Kind:    "Service",
	}
	routeGVK = schema.GroupVersionKind{
		Group:   "route.openshift.io",
		Version: "v1",
		Kind:    "Route",
	}
)

func getPullSecretReference(cfg *mirrorv1.AirgapArchitectConfig) (name, namespace string) {
	if cfg != nil && cfg.PullSecret != nil && cfg.PullSecret.Name != "" {
		return cfg.PullSecret.Name, architectNamespace
	}
	return defaultPullSecretName, defaultPullSecretNS
}

func (r *DisconnectedPlatformReconciler) ensurePullSecret(ctx context.Context, secretName, sourceNamespace string) error {
	// If using custom pull secret in operator namespace, assume it already exists
	if sourceNamespace == architectNamespace {
		return nil
	}

	// Copy pull secret from source namespace to operator namespace
	sourceSecret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Name: secretName, Namespace: sourceNamespace}, sourceSecret); err != nil {
		return fmt.Errorf("failed to get pull secret from %s/%s: %w", sourceNamespace, secretName, err)
	}

	targetSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: architectNamespace,
		},
		Data: sourceSecret.Data,
		Type: sourceSecret.Type,
	}

	existing := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(targetSecret), existing); err == nil {
		// Secret already exists, update it if needed
		if string(existing.Data[pullSecretKey]) != string(sourceSecret.Data[pullSecretKey]) {
			targetSecret.ResourceVersion = existing.ResourceVersion
			if err := r.Update(ctx, targetSecret); err != nil {
				return err
			}
			// Trigger pod restart by deleting pods (deployment will recreate them with updated secret)
			return r.restartBackendPods(ctx)
		}
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	return r.Create(ctx, targetSecret)
}

func (r *DisconnectedPlatformReconciler) restartBackendPods(ctx context.Context) error {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(architectNamespace), client.MatchingLabels{
		"app.kubernetes.io/component": "backend",
		"app.kubernetes.io/part-of":   "mirror-operator",
	}); err != nil {
		return err
	}

	for _, pod := range pods.Items {
		if err := r.Delete(ctx, &pod); err != nil && !apierrors.IsNotFound(err) {
			log.FromContext(ctx).Error(err, "failed to delete backend pod for restart", "pod", pod.Name)
		}
	}
	return nil
}

func architectResourceName(platform *mirrorv1.DisconnectedPlatform, component string) string {
	return "mirror-operator-" + platform.Name + "-" + component
}

func architectComponentLabels(component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "airgap-architect-" + component,
		"app.kubernetes.io/component":  component,
		"app.kubernetes.io/part-of":    "mirror-operator",
		"app.kubernetes.io/managed-by": "mirror-operator",
	}
}

func setOwnerReference(obj *unstructured.Unstructured, owner *mirrorv1.DisconnectedPlatform) {
	obj.SetOwnerReferences([]metav1.OwnerReference{
		{
			APIVersion: owner.APIVersion,
			Kind:       owner.Kind,
			Name:       owner.Name,
			UID:        owner.UID,
			Controller: func() *bool { b := true; return &b }(),
		},
	})
}

func (r *DisconnectedPlatformReconciler) reconcileArchitect(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	cfg := platform.Spec.Architect
	if cfg == nil || !cfg.Enabled {
		return r.deleteArchitectResources(ctx, platform)
	}

	replicas := int32(1)
	if cfg.Replicas > 0 {
		replicas = cfg.Replicas
	}

	frontendImage := cfg.FrontendImage
	if frontendImage == "" {
		frontendImage = r.ArchitectFrontendImage
	}
	backendImage := cfg.BackendImage
	if backendImage == "" {
		backendImage = r.ArchitectBackendImage
	}

	// Determine pull secret to use and ensure it exists in operator namespace
	pullSecretName, pullSecretNamespace := getPullSecretReference(cfg)
	if err := r.ensurePullSecret(ctx, pullSecretName, pullSecretNamespace); err != nil {
		return err
	}

	// Backend resources
	backendName := architectResourceName(platform, "airgap-architect-backend")
	backendLabels := architectComponentLabels("backend")
	if err := r.ensureArchitectBackend(ctx, platform, backendName, backendImage, replicas, backendLabels, pullSecretName, pullSecretNamespace); err != nil {
		return err
	}
	if err := r.ensureArchitectService(ctx, platform, backendName, int32(architectPort), backendLabels); err != nil {
		return err
	}

	// Routes (create routes first to get hostnames)
	var frontendRouteHostname, backendRouteHostname string
	if cfg.Route != nil {
		// Frontend route
		frontendRouteName := "airgap-architect"
		if err := r.ensureArchitectRoute(ctx, platform, frontendRouteName, cfg.Route, architectResourceName(platform, "airgap-architect-frontend")); err != nil {
			return err
		}
		// Get frontend route hostname for VITE_ALLOWED_HOSTS
		frontendRoute := &unstructured.Unstructured{}
		frontendRoute.SetGroupVersionKind(routeGVK)
		frontendRoute.SetName(frontendRouteName)
		frontendRoute.SetNamespace(architectNamespace)
		if err := r.Get(ctx, client.ObjectKeyFromObject(frontendRoute), frontendRoute); err == nil {
			if cfg.Route.Host != "" {
				frontendRouteHostname = cfg.Route.Host
			} else {
				frontendRouteHostname, _, _ = unstructured.NestedString(frontendRoute.Object, "spec", "host")
			}
		}

		// Backend API route
		backendRouteName := "airgap-architect-api"
		if err := r.ensureArchitectRoute(ctx, platform, backendRouteName, cfg.Route, architectResourceName(platform, "airgap-architect-backend")); err != nil {
			return err
		}
		// Get backend route hostname for VITE_API_BASE
		backendRoute := &unstructured.Unstructured{}
		backendRoute.SetGroupVersionKind(routeGVK)
		backendRoute.SetName(backendRouteName)
		backendRoute.SetNamespace(architectNamespace)
		if err := r.Get(ctx, client.ObjectKeyFromObject(backendRoute), backendRoute); err == nil {
			if cfg.Route.Host != "" {
				// If custom host specified, use subdomain pattern
				backendRouteHostname = "api-" + cfg.Route.Host
			} else {
				backendRouteHostname, _, _ = unstructured.NestedString(backendRoute.Object, "spec", "host")
			}
		}
	} else {
		if err := r.deleteResource(ctx, routeGVK, "airgap-architect"); err != nil {
			return err
		}
		if err := r.deleteResource(ctx, routeGVK, "airgap-architect-api"); err != nil {
			return err
		}
	}

	// Frontend resources (after routes to get hostnames)
	frontendName := architectResourceName(platform, "airgap-architect-frontend")
	frontendLabels := architectComponentLabels("frontend")
	if err := r.ensureArchitectFrontend(ctx, platform, frontendName, frontendImage, replicas, frontendLabels, backendRouteHostname, frontendRouteHostname); err != nil {
		return err
	}
	if err := r.ensureArchitectService(ctx, platform, frontendName, int32(frontendPort), frontendLabels); err != nil {
		return err
	}

	platform.Status.Components = append(platform.Status.Components,
		mirrorv1.ComponentStatus{Name: "airgap-architect-frontend", Status: "Running"},
		mirrorv1.ComponentStatus{Name: "airgap-architect-backend", Status: "Running"},
	)
	return nil
}

// Backend

func (r *DisconnectedPlatformReconciler) ensureArchitectBackend(ctx context.Context, platform *mirrorv1.DisconnectedPlatform, name, image string, replicas int32, labels map[string]string, pullSecretName, pullSecretNamespace string) error {
	dep := &unstructured.Unstructured{}
	dep.SetGroupVersionKind(deploymentGVK)
	dep.SetName(name)
	dep.SetNamespace(architectNamespace)
	if err := r.Get(ctx, client.ObjectKeyFromObject(dep), dep); err == nil {
		return r.updateArchitectDeployment(ctx, platform, name, image, replicas, labels, pullSecretName, pullSecretNamespace, backendContainer)
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	newDep := architectBackendDeployment(name, image, replicas, labels, pullSecretName, pullSecretNamespace)
	setOwnerReference(newDep, platform)
	return r.Create(ctx, newDep)
}

func architectBackendDeployment(name, image string, replicas int32, labels map[string]string, pullSecretName, pullSecretNamespace string) *unstructured.Unstructured {
	dep := &unstructured.Unstructured{}
	dep.SetGroupVersionKind(deploymentGVK)
	dep.SetName(name)
	dep.SetNamespace(architectNamespace)
	dep.SetLabels(labels)
	dep.Object["spec"] = architectDeploymentSpec(name, image, replicas, labels, pullSecretName, pullSecretNamespace, backendContainer)
	return dep
}

func backendContainer(name, image string, labels map[string]string) map[string]interface{} {
	return map[string]interface{}{
		"name":  "airgap-architect-backend",
		"image": image,
		"ports": []interface{}{
			map[string]interface{}{
				"containerPort": int64(architectPort),
				"protocol":      "TCP",
			},
		},
		"env": []interface{}{
			map[string]interface{}{
				"name":  "DATA_DIR",
				"value": "/data",
			},
			map[string]interface{}{
				"name":  "NODE_ENV",
				"value": "production",
			},
			map[string]interface{}{
				"name":  "PULL_SECRET_FILE",
				"value": pullSecretMountPath + "/" + pullSecretKey,
			},
			map[string]interface{}{
				"name":  "OPENSHIFT_OPERATOR_MANAGED",
				"value": "true",
			},
		},
		"volumeMounts": []interface{}{
			map[string]interface{}{
				"name":      "data",
				"mountPath": "/data",
			},
			map[string]interface{}{
				"name":      pullSecretVolumeName,
				"mountPath": pullSecretMountPath,
				"readOnly":  true,
			},
		},
		"readinessProbe": map[string]interface{}{
			"tcpSocket": map[string]interface{}{
				"port": int64(architectPort),
			},
			"initialDelaySeconds": int64(5),
			"periodSeconds":       int64(10),
		},
		"livenessProbe": map[string]interface{}{
			"tcpSocket": map[string]interface{}{
				"port": int64(architectPort),
			},
			"initialDelaySeconds": int64(15),
			"periodSeconds":       int64(20),
		},
	}
}

func (r *DisconnectedPlatformReconciler) ensureArchitectService(ctx context.Context, platform *mirrorv1.DisconnectedPlatform, name string, port int32, labels map[string]string) error {
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(serviceGVK)
	existing.SetName(name)
	existing.SetNamespace(architectNamespace)
	if err := r.Get(ctx, client.ObjectKeyFromObject(existing), existing); err == nil {
		// Update existing service
		svc := architectService(name, port, labels)
		svc.SetResourceVersion(existing.GetResourceVersion())
		setOwnerReference(svc, platform)
		return r.Update(ctx, svc)
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	svc := architectService(name, port, labels)
	setOwnerReference(svc, platform)
	return r.Create(ctx, svc)
}

func architectService(name string, port int32, labels map[string]string) *unstructured.Unstructured {
	svc := &unstructured.Unstructured{}
	svc.SetGroupVersionKind(serviceGVK)
	svc.SetName(name)
	svc.SetNamespace(architectNamespace)
	svc.SetLabels(labels)
	unstructured.SetNestedField(svc.Object, "ClusterIP", "spec", "type")
	unstructured.SetNestedSlice(svc.Object, []interface{}{
		map[string]interface{}{
			"name":       "http",
			"port":       int64(port),
			"protocol":   "TCP",
			"targetPort": int64(port),
		},
	}, "spec", "ports")
	sel := make(map[string]interface{}, len(labels))
	for k, v := range labels {
		sel[k] = v
	}
	unstructured.SetNestedMap(svc.Object, sel, "spec", "selector")
	return svc
}

// Frontend

func (r *DisconnectedPlatformReconciler) ensureArchitectFrontend(ctx context.Context, platform *mirrorv1.DisconnectedPlatform, name, image string, replicas int32, labels map[string]string, backendRouteHostname, frontendRouteHostname string) error {
	dep := &unstructured.Unstructured{}
	dep.SetGroupVersionKind(deploymentGVK)
	dep.SetName(name)
	dep.SetNamespace(architectNamespace)
	if err := r.Get(ctx, client.ObjectKeyFromObject(dep), dep); err == nil {
		return r.updateArchitectDeployment(ctx, platform, name, image, replicas, labels, "", "", func(name, image string, labels map[string]string) map[string]interface{} {
			return frontendContainer(name, image, labels, backendRouteHostname, frontendRouteHostname)
		})
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	newDep := architectFrontendDeployment(name, image, replicas, labels, backendRouteHostname, frontendRouteHostname)
	setOwnerReference(newDep, platform)
	return r.Create(ctx, newDep)
}

func architectFrontendDeployment(name, image string, replicas int32, labels map[string]string, backendRouteHostname, frontendRouteHostname string) *unstructured.Unstructured {
	dep := &unstructured.Unstructured{}
	dep.SetGroupVersionKind(deploymentGVK)
	dep.SetName(name)
	dep.SetNamespace(architectNamespace)
	dep.SetLabels(labels)
	dep.Object["spec"] = architectDeploymentSpec(name, image, replicas, labels, "", "", func(name, image string, labels map[string]string) map[string]interface{} {
		return frontendContainer(name, image, labels, backendRouteHostname, frontendRouteHostname)
	})
	return dep
}

func frontendContainer(name, image string, labels map[string]string, backendRouteHostname, frontendRouteHostname string) map[string]interface{} {
	apiBase := ""
	if backendRouteHostname != "" {
		apiBase = "https://" + backendRouteHostname
	}
	envVars := []interface{}{
		map[string]interface{}{
			"name":  "NODE_ENV",
			"value": "production",
		},
		map[string]interface{}{
			"name":  "OPENSHIFT_OPERATOR_MANAGED",
			"value": "true",
		},
	}
	if apiBase != "" {
		envVars = append(envVars, map[string]interface{}{
			"name":  "VITE_API_BASE",
			"value": apiBase,
		})
	}
	if frontendRouteHostname != "" {
		envVars = append(envVars, map[string]interface{}{
			"name":  "VITE_ALLOWED_HOSTS",
			"value": frontendRouteHostname,
		})
	}
	return map[string]interface{}{
		"name":  "airgap-architect-frontend",
		"image": image,
		"ports": []interface{}{
			map[string]interface{}{
				"containerPort": int64(frontendPort),
				"protocol":      "TCP",
			},
		},
		"env": envVars,
		"volumeMounts": []interface{}{
			map[string]interface{}{
				"name":      "data",
				"mountPath": "/app/node_modules/.vite",
			},
		},
		"readinessProbe": map[string]interface{}{
			"tcpSocket": map[string]interface{}{
				"port": int64(frontendPort),
			},
			"initialDelaySeconds": int64(5),
			"periodSeconds":       int64(10),
		},
		"livenessProbe": map[string]interface{}{
			"tcpSocket": map[string]interface{}{
				"port": int64(frontendPort),
			},
			"initialDelaySeconds": int64(15),
			"periodSeconds":       int64(20),
		},
	}
}

// Shared deployment helpers

type containerBuilder func(name, image string, labels map[string]string) map[string]interface{}

func architectDeploymentSpec(name, image string, replicas int32, labels map[string]string, pullSecretName, pullSecretNamespace string, buildContainer containerBuilder) map[string]interface{} {
	component := labels["app.kubernetes.io/component"]
	templateLabels := map[string]interface{}{}
	for k, v := range labels {
		templateLabels[k] = v
	}

	volumes := []interface{}{
		map[string]interface{}{
			"name":     "data",
			"emptyDir": map[string]interface{}{},
		},
	}

	// Add pull secret volume only for backend component
	if component == "backend" && pullSecretName != "" {
		volumes = append(volumes, map[string]interface{}{
			"name": pullSecretVolumeName,
			"secret": map[string]interface{}{
				"secretName": pullSecretName,
			},
		})
	}

	return map[string]interface{}{
		"replicas": int64(replicas),
		"selector": map[string]interface{}{
			"matchLabels": map[string]interface{}{
				"app.kubernetes.io/name":      "airgap-architect-" + component,
				"app.kubernetes.io/component": component,
			},
		},
		"template": map[string]interface{}{
			"metadata": map[string]interface{}{
				"labels": templateLabels,
			},
			"spec": map[string]interface{}{
				"containers": []interface{}{
					buildContainer(name, image, labels),
				},
				"volumes": volumes,
			},
		},
	}
}

func (r *DisconnectedPlatformReconciler) updateArchitectDeployment(ctx context.Context, platform *mirrorv1.DisconnectedPlatform, name, image string, replicas int32, labels map[string]string, pullSecretName, pullSecretNamespace string, buildContainer containerBuilder) error {
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(deploymentGVK)
	existing.SetName(name)
	existing.SetNamespace(architectNamespace)
	if err := r.Get(ctx, client.ObjectKeyFromObject(existing), existing); err != nil {
		return err
	}
	dep := &unstructured.Unstructured{}
	dep.SetGroupVersionKind(deploymentGVK)
	dep.SetName(name)
	dep.SetNamespace(architectNamespace)
	dep.SetLabels(labels)
	dep.SetResourceVersion(existing.GetResourceVersion())
	setOwnerReference(dep, platform)
	dep.Object["spec"] = architectDeploymentSpec(name, image, replicas, labels, pullSecretName, pullSecretNamespace, buildContainer)
	return r.Update(ctx, dep)
}

// Route

func (r *DisconnectedPlatformReconciler) ensureArchitectRoute(ctx context.Context, platform *mirrorv1.DisconnectedPlatform, routeName string, routeCfg *mirrorv1.RouteConfig, frontendServiceName string) error {
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(routeGVK)
	route.SetName(routeName)
	route.SetNamespace(architectNamespace)
	if err := r.Get(ctx, client.ObjectKeyFromObject(route), route); err == nil {
		return r.updateArchitectRoute(ctx, platform, routeName, routeCfg, frontendServiceName)
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	newRoute := architectRoute(routeName, routeCfg, frontendServiceName)
	setOwnerReference(newRoute, platform)
	return r.Create(ctx, newRoute)
}

func (r *DisconnectedPlatformReconciler) updateArchitectRoute(ctx context.Context, platform *mirrorv1.DisconnectedPlatform, routeName string, routeCfg *mirrorv1.RouteConfig, frontendServiceName string) error {
	route := architectRoute(routeName, routeCfg, frontendServiceName)
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(routeGVK)
	existing.SetName(routeName)
	existing.SetNamespace(architectNamespace)
	if err := r.Get(ctx, client.ObjectKeyFromObject(existing), existing); err != nil {
		return err
	}
	route.SetResourceVersion(existing.GetResourceVersion())
	setOwnerReference(route, platform)
	return r.Update(ctx, route)
}

func architectRoute(routeName string, routeCfg *mirrorv1.RouteConfig, frontendServiceName string) *unstructured.Unstructured {
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(routeGVK)
	route.SetName(routeName)
	route.SetNamespace(architectNamespace)
	route.SetLabels(map[string]string{
		"app.kubernetes.io/name":       "airgap-architect",
		"app.kubernetes.io/part-of":    "mirror-operator",
		"app.kubernetes.io/managed-by": "mirror-operator",
	})
	unstructured.SetNestedField(route.Object, "Service", "spec", "to", "kind")
	unstructured.SetNestedField(route.Object, frontendServiceName, "spec", "to", "name")
	unstructured.SetNestedField(route.Object, "http", "spec", "port", "targetPort")
	if routeCfg.Host != "" {
		unstructured.SetNestedField(route.Object, routeCfg.Host, "spec", "host")
	}
	if routeCfg.TLS != nil {
		termination := routeCfg.TLS.Termination
		if termination == "" {
			termination = "edge"
		}
		unstructured.SetNestedField(route.Object, termination, "spec", "tls", "termination")
	}
	return route
}

// Cleanup

func (r *DisconnectedPlatformReconciler) deleteArchitectResources(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	backendName := architectResourceName(platform, "airgap-architect-backend")
	frontendName := architectResourceName(platform, "airgap-architect-frontend")

	_ = r.deleteResource(ctx, deploymentGVK, backendName)
	_ = r.deleteResource(ctx, deploymentGVK, frontendName)
	_ = r.deleteResource(ctx, serviceGVK, backendName)
	_ = r.deleteResource(ctx, serviceGVK, frontendName)
	_ = r.deleteResource(ctx, routeGVK, "airgap-architect")
	_ = r.deleteResource(ctx, routeGVK, "airgap-architect-api")
	return nil
}

func (r *DisconnectedPlatformReconciler) deleteResource(ctx context.Context, gvk schema.GroupVersionKind, name string) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetName(name)
	obj.SetNamespace(architectNamespace)
	if err := r.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return r.Delete(ctx, obj)
}

// secretEventHandler watches for changes to the OpenShift pull secret and triggers reconciliation
type secretEventHandler struct {
	client client.Client
}

func (h *secretEventHandler) Create(ctx context.Context, e event.TypedCreateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	// No action needed on create
}

func (h *secretEventHandler) Update(ctx context.Context, e event.TypedUpdateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.handleSecret(ctx, e.ObjectNew, q)
}

func (h *secretEventHandler) Delete(ctx context.Context, e event.TypedDeleteEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	// No action needed on delete
}

func (h *secretEventHandler) Generic(ctx context.Context, e event.TypedGenericEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	// No action needed
}

func (h *secretEventHandler) handleSecret(ctx context.Context, obj client.Object, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	secret, ok := obj.(*corev1.Secret)
	if !ok {
		return
	}

	// Only watch the OpenShift pull secret
	if secret.Namespace != defaultPullSecretNS || secret.Name != defaultPullSecretName {
		return
	}

	// Trigger reconciliation for all DisconnectedPlatform resources
	platformList := &mirrorv1.DisconnectedPlatformList{}
	if err := h.client.List(ctx, platformList); err != nil {
		return
	}

	for _, platform := range platformList.Items {
		q.Add(reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      platform.Name,
				Namespace: platform.Namespace,
			},
		})
	}
}

func (r *DisconnectedPlatformReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mirrorv1.DisconnectedPlatform{}).
		Owns(&unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
		}}).
		Owns(&unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Service",
		}}).
		Owns(&unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "route.openshift.io/v1",
			"kind":       "Route",
		}}).
		Watches(&corev1.Secret{}, &secretEventHandler{client: r.Client}).
		Complete(r)
}

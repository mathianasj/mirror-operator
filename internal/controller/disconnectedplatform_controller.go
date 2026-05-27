package controller

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

const (
	platformFinalizer = "mirror.mathianasj.github.com/platform-finalizer"
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
)

type DisconnectedPlatformReconciler struct {
	client.Client
	Scheme *runtime.Scheme
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

	platform.Status.Components = nil

	if platform.Spec.Mode == mirrorv1.PlatformModeConnected {
		if err := r.reconcileSubscriptions(ctx, platform); err != nil {
			log.FromContext(ctx).Error(err, "failed to reconcile OLM subscriptions")
			platform.Status.Phase = mirrorv1.PlatformPhaseError
			return ctrl.Result{}, r.Status().Update(ctx, platform)
		}
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
		pkg:       "trusted-artifact-signer",
		channel:   "stable",
		catalog:   "redhat-operators",
		catalogNS: "openshift-marketplace",
		ns:        "openshift-operators",
	},
	{
		name:      "trusted-profile-analyzer",
		pkg:       "trusted-profile-analyzer",
		channel:   "stable",
		catalog:   "redhat-operators",
		catalogNS: "openshift-marketplace",
		ns:        "openshift-operators",
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

		components = append(components, mirrorv1.ComponentStatus{
			Name:   op.name,
			Status: "Installing",
		})
	}

	platform.Status.Components = components
	return nil
}

func (r *DisconnectedPlatformReconciler) ensureOperatorGroup(ctx context.Context, op operatorDef) error {
	og := &unstructured.Unstructured{}
	og.SetGroupVersionKind(operatorGroupGVK)
	og.SetName("mirror-operator-" + op.name)
	og.SetNamespace(op.ns)

	if err := r.Get(ctx, client.ObjectKeyFromObject(og), og); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	og = &unstructured.Unstructured{}
	og.SetGroupVersionKind(operatorGroupGVK)
	og.SetName("mirror-operator-" + op.name)
	og.SetNamespace(op.ns)
	unstructured.SetNestedStringSlice(og.Object, []string{""}, "spec", "targetNamespaces")

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
	unstructured.SetNestedField(sub.Object, op.pkg, "spec", "package")
	unstructured.SetNestedField(sub.Object, channel, "spec", "channel")
	unstructured.SetNestedField(sub.Object, catalog, "spec", "catalogSource")
	unstructured.SetNestedField(sub.Object, catalogNS, "spec", "catalogSourceNamespace")
	unstructured.SetNestedField(sub.Object, approval, "spec", "installPlanApproval")

	if cfg != nil && cfg.StartingCSV != "" {
		unstructured.SetNestedField(sub.Object, cfg.StartingCSV, "spec", "startingCSV")
	}

	return r.Create(ctx, sub)
}

func (r *DisconnectedPlatformReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mirrorv1.DisconnectedPlatform{}).
		Complete(r)
}

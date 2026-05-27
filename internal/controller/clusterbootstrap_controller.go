package controller

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

const (
	bootstrapFinalizer = "mirror.mathianasj.github.com/bootstrap-finalizer"
)

type ClusterBootstrapReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=clusterbootstraps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=clusterbootstraps/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mirror.mirror.mathianasj.github.com,resources=clusterbootstraps/finalizers,verbs=update

func (r *ClusterBootstrapReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	bootstrap := &mirrorv1.ClusterBootstrap{}
	if err := r.Get(ctx, req.NamespacedName, bootstrap); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !bootstrap.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.cleanup(ctx, bootstrap)
	}

	if !containsString(bootstrap.GetFinalizers(), bootstrapFinalizer) {
		bootstrap.SetFinalizers(append(bootstrap.GetFinalizers(), bootstrapFinalizer))
		return ctrl.Result{}, r.Update(ctx, bootstrap)
	}

	// Stub: bootstrap controller will be implemented in a future phase.
	// Expected workflow:
	//   1. Pending -> Validating (validate install-config, platform config)
	//   2. Validating -> Installing (orchestrate openshift-install)
	//   3. Installing -> Complete (publish kubeconfig, console URL)
	//   4. Any -> Failed on error
	if bootstrap.Status.Phase == "" {
		bootstrap.Status.Phase = mirrorv1.BootstrapPhasePending
		logger.Info("cluster bootstrap requested", "platform", bootstrap.Spec.Platform, "version", bootstrap.Spec.Version)
		return ctrl.Result{}, r.Status().Update(ctx, bootstrap)
	}

	return ctrl.Result{}, nil
}

func (r *ClusterBootstrapReconciler) cleanup(ctx context.Context, bootstrap *mirrorv1.ClusterBootstrap) (ctrl.Result, error) {
	if containsString(bootstrap.GetFinalizers(), bootstrapFinalizer) {
		bootstrap.SetFinalizers(removeString(bootstrap.GetFinalizers(), bootstrapFinalizer))
		return ctrl.Result{}, r.Update(ctx, bootstrap)
	}
	return ctrl.Result{}, nil
}

func (r *ClusterBootstrapReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mirrorv1.ClusterBootstrap{}).
		Complete(r)
}

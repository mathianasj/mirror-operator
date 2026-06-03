package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
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
	securesignGVK = schema.GroupVersionKind{
		Group:   "rhtas.redhat.com",
		Version: "v1alpha1",
		Kind:    "Securesign",
	}
	keycloakGVK = schema.GroupVersionKind{
		Group:   "k8s.keycloak.org",
		Version: "v2alpha1",
		Kind:    "Keycloak",
	}
	keycloakRealmGVK = schema.GroupVersionKind{
		Group:   "k8s.keycloak.org",
		Version: "v2alpha1",
		Kind:    "KeycloakRealmImport",
	}
	certificateGVK = schema.GroupVersionKind{
		Group:   "cert-manager.io",
		Version: "v1",
		Kind:    "Certificate",
	}
	issuerGVK = schema.GroupVersionKind{
		Group:   "cert-manager.io",
		Version: "v1",
		Kind:    "Issuer",
	}
	clusterIssuerGVK = schema.GroupVersionKind{
		Group:   "cert-manager.io",
		Version: "v1",
		Kind:    "ClusterIssuer",
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
// +kubebuilder:rbac:groups=rhtas.redhat.com,resources=securesigns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=k8s.keycloak.org,resources=keycloaks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=k8s.keycloak.org,resources=keycloakrealmimports,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cert-manager.io,resources=certificates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cert-manager.io,resources=issuers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cert-manager.io,resources=clusterissuers,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete
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

	keycloakReady := false
	signingImages := false
	verifyingIntegrity := false
	for _, c := range platform.Status.Components {
		if c.Name == "rhbk-operator" && c.Status == "Succeeded" {
			keycloakReady = true
		}
		if c.Name == "trusted-artifact-signer" && c.Status == "Succeeded" {
			signingImages = true
		}
		if c.Name == "trusted-profile-analyzer" && c.Status == "Succeeded" {
			verifyingIntegrity = true
		}
	}
	if keycloakReady {
		if err := r.reconcileManagedKeycloak(ctx, platform); err != nil {
			log.FromContext(ctx).Error(err, "failed to reconcile managed Keycloak")
		}
	}
	if signingImages {
		if err := r.reconcileRHTASConfig(ctx, platform); err != nil {
			log.FromContext(ctx).Error(err, "failed to reconcile RHTAS config")
		}
		if err := r.extractRHTASRootKeys(ctx, platform); err != nil {
			log.FromContext(ctx).Error(err, "failed to extract RHTAS root keys")
		}
		if err := r.configureCollectionPipelineSigning(ctx, platform); err != nil {
			log.FromContext(ctx).Error(err, "failed to configure collection pipeline signing")
		}

		// Perform inline RHTAS health checks and self-healing
		if err := r.performRHTASHealthChecks(ctx, platform); err != nil {
			log.FromContext(ctx).Error(err, "RHTAS health check failed")
			r.updateDegradedCondition(ctx, platform, "RHTASHealthCheckFailed", err.Error())
		} else {
			r.clearDegradedCondition(ctx, platform, "RHTASHealthCheckFailed")
		}

		// Read Securesign health status from RHTASHealthCheck controller
		if err := r.updateStatusFromSecuresignHealth(ctx, platform); err != nil {
			log.FromContext(ctx).Error(err, "failed to read Securesign health status")
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
		r.deleteManagedKeycloak(ctx)
		r.deleteRHTASConfig(ctx)
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
		name:      "rhbk-operator",
		pkg:       "rhbk-operator",
		channel:   "stable-v24",
		catalog:   "redhat-operators",
		catalogNS: "openshift-marketplace",
		ns:        architectNamespace,
	},
	{
		name:      "trusted-artifact-signer",
		pkg:       "rhtas-operator",
		channel:   "stable",
		catalog:   "redhat-operators",
		catalogNS: "openshift-marketplace",
		ns:        "openshift-operators", // RHTAS requires AllNamespaces mode
	},
	{
		name:      "trusted-profile-analyzer",
		pkg:       "rhtpa-operator",
		channel:   "stable-v1.1",
		catalog:   "redhat-operators",
		catalogNS: "openshift-marketplace",
		ns:        architectNamespace,
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
	if op.Keycloak != nil {
		overrides["rhbk-operator"] = op.Keycloak
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
	tpa.SetNamespace(architectNamespace)

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
	tpa.SetNamespace(architectNamespace)
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
	tpa.SetNamespace(architectNamespace)
	if err := r.Delete(ctx, tpa); err != nil && !apierrors.IsNotFound(err) {
		log.FromContext(ctx).Error(err, "failed to delete RHTPA config")
	}
}

func (r *DisconnectedPlatformReconciler) reconcileManagedKeycloak(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	cfg := platform.Spec.Connected.RHTAS
	if cfg == nil || cfg.OIDC == nil || cfg.OIDC.Managed == nil || !cfg.OIDC.Managed.Enabled {
		return nil
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
	hostname := "keycloak." + domain

	tlsSecretName := "keycloak-tls"
	if cfg.OIDC.Managed.TLSSecret != nil && cfg.OIDC.Managed.TLSSecret.Name != "" {
		tlsSecretName = cfg.OIDC.Managed.TLSSecret.Name
	} else {
		if err := r.ensureKeycloakTLS(ctx, platform, hostname, tlsSecretName); err != nil {
			return fmt.Errorf("failed to ensure Keycloak TLS: %w", err)
		}
	}

	// Determine database configuration
	var dbHost, dbName, dbUsername, dbPassword string
	var dbPort int
	useExternalDB := cfg.Database != nil && cfg.Database.Host != "" && cfg.Database.Username != "" && cfg.Database.Password != ""

	if useExternalDB {
		// Use provided external database
		dbHost = cfg.Database.Host
		dbName = cfg.Database.Name
		if dbName == "" {
			dbName = "keycloak"
		}
		dbPort = int(cfg.Database.Port)
		if dbPort == 0 {
			dbPort = 5432
		}
		dbUsername = cfg.Database.Username
		dbPassword = cfg.Database.Password
	} else {
		// Deploy managed PostgreSQL StatefulSet
		if err := r.ensureManagedPostgreSQL(ctx, platform); err != nil {
			return fmt.Errorf("failed to ensure managed PostgreSQL: %w", err)
		}
		dbHost = "keycloak-postgresql.mirror-operator-system.svc"
		dbName = "keycloak"
		dbPort = 5432
		dbUsername = "keycloak"
		dbPassword = "keycloak-admin-password" // Generated password stored in secret
	}

	// Create database credentials secret
	dbSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "keycloak-db-secret",
			Namespace: architectNamespace,
		},
		StringData: map[string]string{
			"username": dbUsername,
			"password": dbPassword,
		},
	}
	existingSecret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(dbSecret), existingSecret); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, dbSecret); err != nil {
			return fmt.Errorf("failed to create Keycloak database secret: %w", err)
		}
	}

	kc := &unstructured.Unstructured{}
	kc.SetGroupVersionKind(keycloakGVK)
	kc.SetName("mirror-operator-keycloak")
	kc.SetNamespace(architectNamespace)

	kcSpec := map[string]interface{}{
		"instances": int64(1),
		"hostname": map[string]interface{}{
			"hostname": hostname,
		},
		"http": map[string]interface{}{
			"tlsSecret": tlsSecretName,
		},
		"additionalOptions": []map[string]interface{}{
			{
				"name":  "KEYCLOAK_ADMIN",
				"value": "admin",
			},
			{
				"name": "KEYCLOAK_ADMIN_PASSWORD",
				"secret": map[string]interface{}{
					"name": "mirror-operator-keycloak-initial-admin",
					"key":  "password",
				},
			},
		},
		"db": map[string]interface{}{
			"vendor":   "postgres",
			"host":     dbHost,
			"port":     int64(dbPort),
			"database": dbName,
			"usernameSecret": map[string]interface{}{
				"name": "keycloak-db-secret",
				"key":  "username",
			},
			"passwordSecret": map[string]interface{}{
				"name": "keycloak-db-secret",
				"key":  "password",
			},
		},
	}

	if err := r.Get(ctx, client.ObjectKeyFromObject(kc), kc); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}

		kc.Object["spec"] = kcSpec
		if err := r.Create(ctx, kc); err != nil {
			return fmt.Errorf("failed to create Keycloak: %w", err)
		}
	} else {
		// Update existing Keycloak with database config if not present
		currentSpec, _, _ := unstructured.NestedMap(kc.Object, "spec")
		if _, hasDB := currentSpec["db"]; !hasDB {
			kc.Object["spec"] = kcSpec
			if err := r.Update(ctx, kc); err != nil {
				return fmt.Errorf("failed to update Keycloak: %w", err)
			}
		}
	}

	realmName := cfg.OIDC.Managed.Realm
	if realmName == "" {
		realmName = "trusted-artifact-signer"
	}

	// Wait for Keycloak to be ready before configuring via API
	kcReady := false
	kcStatus, found, _ := unstructured.NestedMap(kc.Object, "status")
	if found {
		conditions, found, _ := unstructured.NestedSlice(kcStatus, "conditions")
		if found {
			for _, cond := range conditions {
				condMap, ok := cond.(map[string]interface{})
				if !ok {
					continue
				}
				condType, _, _ := unstructured.NestedString(condMap, "type")
				condStatus, _, _ := unstructured.NestedString(condMap, "status")
				if condType == "Ready" && condStatus == "True" {
					kcReady = true
					break
				}
			}
		}
	}

	if !kcReady {
		log.FromContext(ctx).Info("Keycloak not ready yet, will retry")
		return nil
	}

	// Configure realm and client via API instead of KeycloakRealmImport
	if err := r.configureKeycloakRealmAndClient(ctx, hostname, realmName, "trusted-artifact-signer"); err != nil {
		return fmt.Errorf("failed to configure Keycloak realm and client: %w", err)
	}

	// Skip KeycloakRealmImport - we manage directly via API
	// Delete any existing realm import to clean up
	realm := &unstructured.Unstructured{}
	realm.SetGroupVersionKind(keycloakRealmGVK)
	realm.SetName("mirror-operator-keycloak-realm")
	realm.SetNamespace(architectNamespace)

	if err := r.Get(ctx, client.ObjectKeyFromObject(realm), realm); err == nil {
		// Realm import exists, delete it
		if err := r.Delete(ctx, realm); err != nil && !apierrors.IsNotFound(err) {
			log.FromContext(ctx).Error(err, "failed to delete KeycloakRealmImport")
		}
	}

	// Remove the old realm import creation code
	/*
		if err := r.Get(ctx, client.ObjectKeyFromObject(realm), realm); err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}

			realmSpec := map[string]interface{}{
				"keycloakCRName": "mirror-operator-keycloak",
				"realm": map[string]interface{}{
					"realm":       realmName,
					"enabled":     true,
					"displayName": "Trusted Artifact Signer",
					"clients": []interface{}{
						map[string]interface{}{
							"clientId":                  "trusted-artifact-signer",
							"enabled":                   true,
							"publicClient":              false,
							"serviceAccountsEnabled":    true,
							"directAccessGrantsEnabled": true,
							"standardFlowEnabled":       true,
							"redirectUris":              []interface{}{"*"},
							"webOrigins":                []interface{}{"*"},
							"defaultClientScopes":       []interface{}{"email", "profile"},
							"optionalClientScopes":      []interface{}{},
						},
						map[string]interface{}{
							"clientId":                  "trusted-artifact-signer",
							"enabled":                   true,
							"publicClient":              false,
							"serviceAccountsEnabled":    true,
							"directAccessGrantsEnabled": true,
							"standardFlowEnabled":       false,
							"redirectUris":              []interface{}{},
							"webOrigins":                []interface{}{},
							"defaultClientScopes":       []interface{}{"email", "profile"},
							"optionalClientScopes":      []interface{}{},
							"protocolMappers": []interface{}{
								map[string]interface{}{
									"name":            "email-mapper",
									"protocol":        "openid-connect",
									"protocolMapper":  "oidc-hardcoded-claim-mapper",
									"consentRequired": false,
									"config": map[string]interface{}{
										"claim.name":           "email",
										"claim.value":          "service-account-trusted-artifact-signer@keycloak.local",
										"id.token.claim":       "true",
										"access.token.claim":   "true",
										"userinfo.token.claim": "true",
										"jsonType.label":       "String",
									},
								},
								map[string]interface{}{
									"name":            "email-verified-mapper",
									"protocol":        "openid-connect",
									"protocolMapper":  "oidc-hardcoded-claim-mapper",
									"consentRequired": false,
									"config": map[string]interface{}{
										"claim.name":           "email_verified",
										"claim.value":          "true",
										"id.token.claim":       "true",
										"access.token.claim":   "true",
										"userinfo.token.claim": "true",
										"jsonType.label":       "boolean",
									},
								},
							},
						},
					},
				},
			}

			realm.Object["spec"] = realmSpec

			if err := r.Create(ctx, realm); err != nil {
				return fmt.Errorf("failed to create KeycloakRealmImport: %w", err)
			}
		} else {
			// Realm import exists - check if it's done and update client secret
			conditions, _, _ := unstructured.NestedSlice(realm.Object, "status", "conditions")
			done := false
			for _, cond := range conditions {
				condMap := cond.(map[string]interface{})
				if condMap["type"] == "Done" && condMap["status"] == "True" {
					done = true
					break
				}
			}

			if done {
				// Retrieve the actual client secret from Keycloak and update the OIDC secret
				if err := r.updateKeycloakClientSecret(ctx, hostname, "trusted-artifact-signer", realmName); err != nil {
					log.FromContext(ctx).Error(err, "failed to update Keycloak client secret")
				}

				// Configure email protocol mapper for service account tokens
				if err := r.configureKeycloakEmailMapper(ctx, hostname, "trusted-artifact-signer", realmName); err != nil {
					log.FromContext(ctx).Error(err, "failed to configure Keycloak email mapper")
				}
			}
		}
	*/

	return nil
}

func (r *DisconnectedPlatformReconciler) ensureManagedPostgreSQL(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	// Create PostgreSQL StatefulSet for Keycloak persistence
	pvcName := "keycloak-postgresql-data"

	// Create PVC
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: architectNamespace,
			Labels: map[string]string{
				"app": "keycloak-postgresql",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("5Gi"),
				},
			},
		},
	}
	existingPVC := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pvc), existingPVC); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, pvc); err != nil {
			return fmt.Errorf("failed to create PostgreSQL PVC: %w", err)
		}
	}

	// Create PostgreSQL StatefulSet
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "keycloak-postgresql",
			Namespace: architectNamespace,
			Labels: map[string]string{
				"app": "keycloak-postgresql",
			},
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: "keycloak-postgresql",
			Replicas:    func() *int32 { r := int32(1); return &r }(),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "keycloak-postgresql",
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": "keycloak-postgresql",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "postgresql",
							Image: "registry.redhat.io/rhel9/postgresql-15:latest",
							Env: []corev1.EnvVar{
								{Name: "POSTGRESQL_USER", Value: "keycloak"},
								{Name: "POSTGRESQL_PASSWORD", Value: "keycloak-admin-password"},
								{Name: "POSTGRESQL_DATABASE", Value: "keycloak"},
							},
							Ports: []corev1.ContainerPort{
								{ContainerPort: 5432, Name: "postgresql"},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "data", MountPath: "/var/lib/pgsql/data"},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: pvcName,
								},
							},
						},
					},
				},
			},
		},
	}
	existingSts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(sts), existingSts); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, sts); err != nil {
			return fmt.Errorf("failed to create PostgreSQL StatefulSet: %w", err)
		}
	}

	// Create PostgreSQL Service
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "keycloak-postgresql",
			Namespace: architectNamespace,
			Labels: map[string]string{
				"app": "keycloak-postgresql",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app": "keycloak-postgresql",
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "postgresql",
					Port:       5432,
					TargetPort: intstr.FromInt(5432),
				},
			},
			ClusterIP: "None",
		},
	}
	existingSvc := &corev1.Service{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(svc), existingSvc); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, svc); err != nil {
			return fmt.Errorf("failed to create PostgreSQL Service: %w", err)
		}
	}

	return nil
}

func (r *DisconnectedPlatformReconciler) configureKeycloakEmailMapper(ctx context.Context, keycloakHost, clientID, realmName string) error {
	// Get admin credentials
	adminSecret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace}, adminSecret); err != nil {
		return fmt.Errorf("failed to get admin secret: %w", err)
	}
	adminPassword := string(adminSecret.Data["password"])

	// Create HTTP client that skips TLS verification
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Timeout: 30 * time.Second,
	}

	// Get admin access token
	tokenURL := fmt.Sprintf("https://%s/realms/master/protocol/openid-connect/token", keycloakHost)
	tokenData := url.Values{}
	tokenData.Set("username", "admin")
	tokenData.Set("password", adminPassword)
	tokenData.Set("grant_type", "password")
	tokenData.Set("client_id", "admin-cli")

	tokenResp, err := httpClient.PostForm(tokenURL, tokenData)
	if err != nil {
		return fmt.Errorf("failed to get admin token: %w", err)
	}
	defer tokenResp.Body.Close()

	tokenBody, _ := io.ReadAll(tokenResp.Body)
	if tokenResp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get admin token: status %d, body: %s", tokenResp.StatusCode, string(tokenBody))
	}

	var tokenResult map[string]interface{}
	if err := json.Unmarshal(tokenBody, &tokenResult); err != nil {
		return fmt.Errorf("failed to decode token response: %w, body: %s", err, string(tokenBody))
	}

	adminToken, ok := tokenResult["access_token"].(string)
	if !ok {
		return fmt.Errorf("no access_token in response: %v", tokenResult)
	}

	// Get client UUID
	clientsURL := fmt.Sprintf("https://%s/admin/realms/%s/clients?clientId=%s", keycloakHost, realmName, clientID)
	req, _ := http.NewRequest("GET", clientsURL, nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)

	clientsResp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to get clients: %w", err)
	}
	defer clientsResp.Body.Close()

	var clients []map[string]interface{}
	if err := json.NewDecoder(clientsResp.Body).Decode(&clients); err != nil {
		return fmt.Errorf("failed to decode clients response: %w", err)
	}

	if len(clients) == 0 {
		return fmt.Errorf("client %s not found", clientID)
	}

	clientUUID, ok := clients[0]["id"].(string)
	if !ok {
		return fmt.Errorf("no id in client response")
	}

	// Check if email mapper already exists
	mappersURL := fmt.Sprintf("https://%s/admin/realms/%s/clients/%s/protocol-mappers/models", keycloakHost, realmName, clientUUID)
	req, _ = http.NewRequest("GET", mappersURL, nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)

	mappersResp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to get protocol mappers: %w", err)
	}
	defer mappersResp.Body.Close()

	var mappers []map[string]interface{}
	if err := json.NewDecoder(mappersResp.Body).Decode(&mappers); err != nil {
		return fmt.Errorf("failed to decode mappers response: %w", err)
	}

	// Check if mappers already exist
	hasEmailMapper := false
	hasEmailVerifiedMapper := false
	for _, mapper := range mappers {
		if name, ok := mapper["name"].(string); ok {
			if name == "email-mapper" {
				hasEmailMapper = true
			}
			if name == "email-verified-mapper" {
				hasEmailVerifiedMapper = true
			}
		}
	}

	// Add email protocol mapper if missing
	if !hasEmailMapper {
		mapperConfig := map[string]interface{}{
			"name":            "email-mapper",
			"protocol":        "openid-connect",
			"protocolMapper":  "oidc-hardcoded-claim-mapper",
			"consentRequired": false,
			"config": map[string]interface{}{
				"claim.name":           "email",
				"claim.value":          "service-account-sigstore@keycloak.local",
				"id.token.claim":       "true",
				"access.token.claim":   "true",
				"userinfo.token.claim": "true",
				"jsonType.label":       "String",
			},
		}

		mapperJSON, _ := json.Marshal(mapperConfig)
		req, _ = http.NewRequest("POST", mappersURL, bytes.NewBuffer(mapperJSON))
		req.Header.Set("Authorization", "Bearer "+adminToken)
		req.Header.Set("Content-Type", "application/json")

		mapperResp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to create email protocol mapper: %w", err)
		}
		defer mapperResp.Body.Close()

		if mapperResp.StatusCode != http.StatusCreated && mapperResp.StatusCode != http.StatusNoContent {
			body, _ := io.ReadAll(mapperResp.Body)
			return fmt.Errorf("failed to create email protocol mapper: status %d, body: %s", mapperResp.StatusCode, string(body))
		}
	}

	// Add email_verified protocol mapper if missing
	if !hasEmailVerifiedMapper {
		mapperConfig := map[string]interface{}{
			"name":            "email-verified-mapper",
			"protocol":        "openid-connect",
			"protocolMapper":  "oidc-hardcoded-claim-mapper",
			"consentRequired": false,
			"config": map[string]interface{}{
				"claim.name":           "email_verified",
				"claim.value":          "true",
				"id.token.claim":       "true",
				"access.token.claim":   "true",
				"userinfo.token.claim": "true",
				"jsonType.label":       "boolean",
			},
		}

		mapperJSON, _ := json.Marshal(mapperConfig)
		req, _ = http.NewRequest("POST", mappersURL, bytes.NewBuffer(mapperJSON))
		req.Header.Set("Authorization", "Bearer "+adminToken)
		req.Header.Set("Content-Type", "application/json")

		mapperResp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to create email_verified protocol mapper: %w", err)
		}
		defer mapperResp.Body.Close()

		if mapperResp.StatusCode != http.StatusCreated && mapperResp.StatusCode != http.StatusNoContent {
			body, _ := io.ReadAll(mapperResp.Body)
			return fmt.Errorf("failed to create email_verified protocol mapper: status %d, body: %s", mapperResp.StatusCode, string(body))
		}
	}

	return nil
}

func (r *DisconnectedPlatformReconciler) configureKeycloakRealmAndClient(ctx context.Context, keycloakHost, realmName, clientID string) error {
	// Get admin credentials
	adminSecret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace}, adminSecret); err != nil {
		return fmt.Errorf("failed to get admin secret: %w", err)
	}
	adminPassword := string(adminSecret.Data["password"])

	// Create HTTP client
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Timeout: 30 * time.Second,
	}

	// Get admin token
	tokenURL := fmt.Sprintf("https://%s/realms/master/protocol/openid-connect/token", keycloakHost)
	tokenData := url.Values{}
	tokenData.Set("username", "admin")
	tokenData.Set("password", adminPassword)
	tokenData.Set("grant_type", "password")
	tokenData.Set("client_id", "admin-cli")

	tokenResp, err := httpClient.PostForm(tokenURL, tokenData)
	if err != nil {
		return fmt.Errorf("failed to get admin token: %w", err)
	}
	defer tokenResp.Body.Close()

	tokenBody, _ := io.ReadAll(tokenResp.Body)
	if tokenResp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get admin token: status %d, body: %s", tokenResp.StatusCode, string(tokenBody))
	}

	var tokenResult map[string]interface{}
	if err := json.Unmarshal(tokenBody, &tokenResult); err != nil {
		return fmt.Errorf("failed to decode token response: %w", err)
	}

	adminToken, ok := tokenResult["access_token"].(string)
	if !ok {
		return fmt.Errorf("no access_token in response: %v", tokenResult)
	}

	// Create realm if it doesn't exist
	realmsURL := fmt.Sprintf("https://%s/admin/realms", keycloakHost)
	req, _ := http.NewRequest("GET", realmsURL, nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to list realms: %w", err)
	}
	defer resp.Body.Close()

	var realms []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&realms)

	realmExists := false
	for _, realm := range realms {
		if realm["realm"] == realmName {
			realmExists = true
			break
		}
	}

	if !realmExists {
		// Create realm
		realmConfig := map[string]interface{}{
			"realm":       realmName,
			"enabled":     true,
			"displayName": "Trusted Artifact Signer",
		}
		realmJSON, _ := json.Marshal(realmConfig)
		req, _ = http.NewRequest("POST", realmsURL, bytes.NewBuffer(realmJSON))
		req.Header.Set("Authorization", "Bearer "+adminToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err = httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to create realm: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to create realm: status %d, body: %s", resp.StatusCode, string(body))
		}
		log.FromContext(ctx).Info("Created Keycloak realm", "realm", realmName)
	}

	// Get client UUID or create client
	clientsURL := fmt.Sprintf("https://%s/admin/realms/%s/clients?clientId=%s", keycloakHost, realmName, clientID)
	req, _ = http.NewRequest("GET", clientsURL, nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)

	resp, err = httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to get client: %w", err)
	}
	defer resp.Body.Close()

	var clients []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&clients)

	var clientUUID string
	if len(clients) == 0 {
		// Create client
		clientConfig := map[string]interface{}{
			"clientId":                  clientID,
			"enabled":                   true,
			"publicClient":              false,
			"serviceAccountsEnabled":    true,
			"directAccessGrantsEnabled": true,
			"standardFlowEnabled":       false,
			"redirectUris":              []string{},
			"webOrigins":                []string{},
			"defaultClientScopes":       []string{"email", "profile"},
			"optionalClientScopes":      []string{},
		}
		clientJSON, _ := json.Marshal(clientConfig)

		createClientURL := fmt.Sprintf("https://%s/admin/realms/%s/clients", keycloakHost, realmName)
		req, _ = http.NewRequest("POST", createClientURL, bytes.NewBuffer(clientJSON))
		req.Header.Set("Authorization", "Bearer "+adminToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err = httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to create client: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to create client: status %d, body: %s", resp.StatusCode, string(body))
		}

		// Get the created client's UUID from Location header
		location := resp.Header.Get("Location")
		parts := bytes.Split([]byte(location), []byte("/"))
		clientUUID = string(parts[len(parts)-1])
		log.FromContext(ctx).Info("Created Keycloak client", "client", clientID, "uuid", clientUUID)
	} else {
		clientUUID = clients[0]["id"].(string)
	}

	// Configure protocol mappers
	mappersURL := fmt.Sprintf("https://%s/admin/realms/%s/clients/%s/protocol-mappers/models", keycloakHost, realmName, clientUUID)

	// Get existing mappers on the client
	req, _ = http.NewRequest("GET", mappersURL, nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)

	resp, err = httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to get protocol mappers: %w", err)
	}
	defer resp.Body.Close()

	var clientMappers []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&clientMappers)

	hasEmailMapper := false
	emailVerifiedMapperID := ""
	emailVerifiedMapperCorrect := false
	for _, mapper := range clientMappers {
		if name, ok := mapper["name"].(string); ok {
			if name == "email-mapper" {
				hasEmailMapper = true
			}
			if name == "email-verified-mapper" {
				if id, ok := mapper["id"].(string); ok {
					emailVerifiedMapperID = id
				}
				// Check if it has the correct config (jsonType.label must be "boolean")
				if config, ok := mapper["config"].(map[string]interface{}); ok {
					if jsonType, ok := config["jsonType.label"].(string); ok && jsonType == "boolean" {
						emailVerifiedMapperCorrect = true
					}
				}
			}
		}
	}

	// Add email mapper if missing
	if !hasEmailMapper {
		mapperConfig := map[string]interface{}{
			"name":            "email-mapper",
			"protocol":        "openid-connect",
			"protocolMapper":  "oidc-hardcoded-claim-mapper",
			"consentRequired": false,
			"config": map[string]interface{}{
				"claim.name":           "email",
				"claim.value":          fmt.Sprintf("service-account-%s@keycloak.local", clientID),
				"id.token.claim":       "true",
				"access.token.claim":   "true",
				"userinfo.token.claim": "true",
				"jsonType.label":       "String",
			},
		}
		mapperJSON, _ := json.Marshal(mapperConfig)
		req, _ = http.NewRequest("POST", mappersURL, bytes.NewBuffer(mapperJSON))
		req.Header.Set("Authorization", "Bearer "+adminToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err = httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to create email mapper: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to create email mapper: status %d, body: %s", resp.StatusCode, string(body))
		}
		log.FromContext(ctx).Info("Created email mapper", "client", clientID)
	}

	// Fix email_verified mapper if it exists with wrong config or create if missing
	if emailVerifiedMapperID != "" && !emailVerifiedMapperCorrect {
		// Delete the incorrect mapper
		deleteURL := fmt.Sprintf("https://%s/admin/realms/%s/clients/%s/protocol-mappers/models/%s",
			keycloakHost, realmName, clientUUID, emailVerifiedMapperID)
		req, _ = http.NewRequest("DELETE", deleteURL, nil)
		req.Header.Set("Authorization", "Bearer "+adminToken)

		resp, err = httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to delete incorrect email_verified mapper: %w", err)
		}
		defer resp.Body.Close()
		log.FromContext(ctx).Info("Deleted incorrect email_verified mapper", "client", clientID)
	}

	// Create email_verified mapper if it doesn't exist or was just deleted
	if emailVerifiedMapperID == "" || !emailVerifiedMapperCorrect {
		mapperConfig := map[string]interface{}{
			"name":           "email-verified-mapper",
			"protocol":       "openid-connect",
			"protocolMapper": "oidc-hardcoded-claim-mapper",
			"config": map[string]interface{}{
				"claim.name":           "email_verified",
				"claim.value":          "true",
				"jsonType.label":       "boolean",
				"id.token.claim":       "true",
				"access.token.claim":   "true",
				"userinfo.token.claim": "true",
			},
		}
		mapperJSON, _ := json.Marshal(mapperConfig)
		req, _ = http.NewRequest("POST", mappersURL, bytes.NewBuffer(mapperJSON))
		req.Header.Set("Authorization", "Bearer "+adminToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err = httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to create email_verified mapper: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to create email_verified mapper: status %d, body: %s", resp.StatusCode, string(body))
		}
		log.FromContext(ctx).Info("Created email_verified mapper on client", "client", clientID)
	}

	// Create a client scope for email_verified that works with service accounts
	if err := r.ensureEmailVerifiedClientScope(ctx, keycloakHost, clientID, realmName, clientUUID, adminToken, httpClient); err != nil {
		log.FromContext(ctx).Error(err, "Failed to ensure email_verified client scope")
		// Don't fail the whole reconciliation, just log it
	}

	// Retrieve and store the client secret
	return r.updateKeycloakClientSecret(ctx, keycloakHost, clientID, realmName)
}

func (r *DisconnectedPlatformReconciler) ensureEmailVerifiedClientScope(ctx context.Context, keycloakHost, clientID, realmName, clientUUID, adminToken string, httpClient *http.Client) error {
	// Create a dedicated client scope for email_verified claim that applies to service account tokens
	scopeName := "email-verified-scope"

	// Check if client scope exists
	clientScopesURL := fmt.Sprintf("https://%s/admin/realms/%s/client-scopes", keycloakHost, realmName)
	req, _ := http.NewRequest("GET", clientScopesURL, nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to get client scopes: %w", err)
	}
	defer resp.Body.Close()

	var clientScopes []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&clientScopes)

	scopeID := ""
	for _, scope := range clientScopes {
		if name, ok := scope["name"].(string); ok && name == scopeName {
			scopeID = scope["id"].(string)
			break
		}
	}

	// Create client scope if it doesn't exist
	if scopeID == "" {
		scopeConfig := map[string]interface{}{
			"name":        scopeName,
			"description": "Email verified claim for service account tokens",
			"protocol":    "openid-connect",
			"attributes": map[string]interface{}{
				"include.in.token.scope": "true",
			},
		}
		scopeJSON, _ := json.Marshal(scopeConfig)
		req, _ = http.NewRequest("POST", clientScopesURL, bytes.NewBuffer(scopeJSON))
		req.Header.Set("Authorization", "Bearer "+adminToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err = httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to create client scope: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to create client scope: status %d, body: %s", resp.StatusCode, string(body))
		}

		// Get the created scope ID from Location header
		location := resp.Header.Get("Location")
		parts := bytes.Split([]byte(location), []byte("/"))
		scopeID = string(parts[len(parts)-1])
		log.FromContext(ctx).Info("Created email_verified client scope", "scopeID", scopeID)
	}

	// Add email_verified mapper to the client scope
	scopeMappersURL := fmt.Sprintf("https://%s/admin/realms/%s/client-scopes/%s/protocol-mappers/models",
		keycloakHost, realmName, scopeID)

	// Check if mapper already exists
	req, _ = http.NewRequest("GET", scopeMappersURL, nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)

	resp, err = httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to get scope mappers: %w", err)
	}
	defer resp.Body.Close()

	var scopeMappers []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&scopeMappers)

	hasMapper := false
	for _, mapper := range scopeMappers {
		if name, ok := mapper["name"].(string); ok && name == "email-verified-scope-mapper" {
			hasMapper = true
			break
		}
	}

	if !hasMapper {
		mapperConfig := map[string]interface{}{
			"name":           "email-verified-scope-mapper",
			"protocol":       "openid-connect",
			"protocolMapper": "oidc-hardcoded-claim-mapper",
			"config": map[string]interface{}{
				"claim.name":           "email_verified",
				"claim.value":          "true",
				"jsonType.label":       "boolean",
				"id.token.claim":       "true",
				"access.token.claim":   "true",
				"userinfo.token.claim": "true",
			},
		}
		mapperJSON, _ := json.Marshal(mapperConfig)
		req, _ = http.NewRequest("POST", scopeMappersURL, bytes.NewBuffer(mapperJSON))
		req.Header.Set("Authorization", "Bearer "+adminToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err = httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to create scope mapper: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to create scope mapper: status %d, body: %s", resp.StatusCode, string(body))
		}
		log.FromContext(ctx).Info("Created email_verified mapper in client scope")
	}

	// Assign the client scope to the client as a default scope
	assignScopeURL := fmt.Sprintf("https://%s/admin/realms/%s/clients/%s/default-client-scopes/%s",
		keycloakHost, realmName, clientUUID, scopeID)
	req, _ = http.NewRequest("PUT", assignScopeURL, nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)

	resp, err = httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to assign client scope: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		// Don't fail if already assigned
		if resp.StatusCode != http.StatusConflict {
			return fmt.Errorf("failed to assign client scope: status %d, body: %s", resp.StatusCode, string(body))
		}
	}

	log.FromContext(ctx).Info("Assigned email_verified client scope to client", "client", clientID, "scope", scopeName)
	return nil
}

func (r *DisconnectedPlatformReconciler) updateKeycloakClientSecret(ctx context.Context, keycloakHost, clientID, realmName string) error {
	// Get admin credentials
	adminSecret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Name: "mirror-operator-keycloak-initial-admin", Namespace: architectNamespace}, adminSecret); err != nil {
		return fmt.Errorf("failed to get admin secret: %w", err)
	}
	adminPassword := string(adminSecret.Data["password"])

	// Create HTTP client that skips TLS verification
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Timeout: 30 * time.Second,
	}

	// Get admin access token
	tokenURL := fmt.Sprintf("https://%s/realms/master/protocol/openid-connect/token", keycloakHost)
	tokenData := url.Values{}
	tokenData.Set("client_id", "admin-cli")
	tokenData.Set("username", "admin")
	tokenData.Set("password", adminPassword)
	tokenData.Set("grant_type", "password")

	tokenResp, err := httpClient.PostForm(tokenURL, tokenData)
	if err != nil {
		return fmt.Errorf("failed to get admin token: %w", err)
	}
	defer tokenResp.Body.Close()

	var tokenResult map[string]interface{}
	if err := json.NewDecoder(tokenResp.Body).Decode(&tokenResult); err != nil {
		return fmt.Errorf("failed to decode token response: %w", err)
	}

	accessToken, ok := tokenResult["access_token"].(string)
	if !ok {
		return fmt.Errorf("no access token in response: %v", tokenResult)
	}

	// Find client UUID by clientId
	clientsURL := fmt.Sprintf("https://%s/admin/realms/%s/clients?clientId=%s", keycloakHost, realmName, clientID)
	req, _ := http.NewRequestWithContext(ctx, "GET", clientsURL, nil)
	req.Header.Set("Authorization", "Bearer "+accessToken)

	clientsResp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to list clients: %w", err)
	}
	defer clientsResp.Body.Close()

	var clients []map[string]interface{}
	if err := json.NewDecoder(clientsResp.Body).Decode(&clients); err != nil {
		return fmt.Errorf("failed to decode clients response: %w", err)
	}

	if len(clients) == 0 {
		return fmt.Errorf("client %s not found in realm %s", clientID, realmName)
	}

	clientUUID, ok := clients[0]["id"].(string)
	if !ok {
		return fmt.Errorf("client id not found in response")
	}

	// Get client secret
	secretURL := fmt.Sprintf("https://%s/admin/realms/%s/clients/%s/client-secret", keycloakHost, realmName, clientUUID)
	req, _ = http.NewRequestWithContext(ctx, "GET", secretURL, nil)
	req.Header.Set("Authorization", "Bearer "+accessToken)

	secretResp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to get client secret: %w", err)
	}
	defer secretResp.Body.Close()

	var secretResult map[string]interface{}
	if err := json.NewDecoder(secretResp.Body).Decode(&secretResult); err != nil {
		return fmt.Errorf("failed to decode secret response: %w", err)
	}

	clientSecret, ok := secretResult["value"].(string)
	if !ok {
		return fmt.Errorf("no secret value in response: %v", secretResult)
	}

	// If the secret is a placeholder, regenerate it
	if clientSecret == "will-be-replaced-by-controller" {
		// Regenerate client secret
		regenerateURL := fmt.Sprintf("https://%s/admin/realms/%s/clients/%s/client-secret", keycloakHost, realmName, clientUUID)
		req, _ = http.NewRequestWithContext(ctx, "POST", regenerateURL, nil)
		req.Header.Set("Authorization", "Bearer "+accessToken)

		regenerateResp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to regenerate client secret: %w", err)
		}
		defer regenerateResp.Body.Close()

		var newSecretResult map[string]interface{}
		if err := json.NewDecoder(regenerateResp.Body).Decode(&newSecretResult); err != nil {
			return fmt.Errorf("failed to decode regenerate response: %w", err)
		}

		clientSecret, ok = newSecretResult["value"].(string)
		if !ok {
			return fmt.Errorf("no secret value in regenerate response: %v", newSecretResult)
		}
		log.FromContext(ctx).Info("Regenerated Keycloak client secret", "clientID", clientID)
	}

	// Update the OIDC secret with the retrieved client secret
	oidcSecret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Name: "mirror-operator-keycloak-client-secret", Namespace: architectNamespace}, oidcSecret); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get OIDC secret: %w", err)
		}
		// Secret doesn't exist, create it
		oidcSecret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mirror-operator-keycloak-client-secret",
				Namespace: architectNamespace,
			},
			StringData: map[string]string{
				"clientSecret": clientSecret,
			},
			Type: corev1.SecretTypeOpaque,
		}
		if err := r.Create(ctx, oidcSecret); err != nil {
			return fmt.Errorf("failed to create OIDC secret: %w", err)
		}
		log.FromContext(ctx).Info("Created OIDC secret with retrieved client secret", "clientID", clientID)
	} else {
		// Update existing secret - must use Data field, not StringData
		if oidcSecret.Data == nil {
			oidcSecret.Data = make(map[string][]byte)
		}
		oidcSecret.Data["clientSecret"] = []byte(clientSecret)
		if err := r.Update(ctx, oidcSecret); err != nil {
			return fmt.Errorf("failed to update OIDC secret: %w", err)
		}
		log.FromContext(ctx).Info("Updated OIDC secret with retrieved client secret", "clientID", clientID)
	}

	return nil
}

func (r *DisconnectedPlatformReconciler) reconcileRHTASConfig(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	cfg := platform.Spec.Connected.RHTAS
	if cfg == nil || cfg.OIDC == nil {
		return nil
	}

	securesign := &unstructured.Unstructured{}
	securesign.SetGroupVersionKind(securesignGVK)
	securesign.SetName("mirror-operator-securesign")
	securesign.SetNamespace(architectNamespace)

	if err := r.Get(ctx, client.ObjectKeyFromObject(securesign), securesign); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	issuerURL := cfg.OIDC.Issuer
	clientID := cfg.OIDC.ClientID
	oidcType := cfg.OIDC.Type

	if cfg.OIDC.Managed != nil && cfg.OIDC.Managed.Enabled {
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

		realmName := cfg.OIDC.Managed.Realm
		if realmName == "" {
			realmName = "trusted-artifact-signer"
		}

		issuerURL = fmt.Sprintf("https://keycloak.%s/realms/%s", domain, realmName)
		clientID = "trusted-artifact-signer"
		if oidcType == "" {
			oidcType = "email"
		}
	}

	if issuerURL == "" || clientID == "" {
		return fmt.Errorf("OIDC issuer and clientID are required")
	}

	spec := map[string]interface{}{
		"fulcio": map[string]interface{}{
			"enabled": true,
			"certificate": map[string]interface{}{
				"organizationName":  "Red Hat",
				"organizationEmail": "rhtas@redhat.com",
			},
			"config": map[string]interface{}{
				"OIDCIssuers": []interface{}{
					map[string]interface{}{
						"ClientID":  clientID,
						"Issuer":    issuerURL,
						"IssuerURL": issuerURL,
						"Type":      oidcType,
					},
				},
			},
		},
		"rekor": map[string]interface{}{
			"enabled": true,
		},
		"ctlog": map[string]interface{}{
			"enabled": true,
		},
		"trillian": map[string]interface{}{
			"enabled": true,
		},
		"tuf": map[string]interface{}{
			"enabled": true,
		},
	}

	if cfg.Database != nil {
		dbConfig := map[string]interface{}{
			"host": cfg.Database.Host,
			"name": cfg.Database.Name,
		}
		if cfg.Database.Port > 0 {
			dbConfig["port"] = int64(cfg.Database.Port)
		}
		if cfg.Database.Username != "" {
			dbConfig["username"] = cfg.Database.Username
		}
		if cfg.Database.Password != "" {
			dbConfig["password"] = cfg.Database.Password
		}

		spec["trillian"].(map[string]interface{})["database"] = dbConfig
		spec["rekor"].(map[string]interface{})["database"] = dbConfig
	}

	securesign.Object["spec"] = spec

	return r.Create(ctx, securesign)
}

func (r *DisconnectedPlatformReconciler) deleteRHTASConfig(ctx context.Context) {
	securesign := &unstructured.Unstructured{}
	securesign.SetGroupVersionKind(securesignGVK)
	securesign.SetName("mirror-operator-securesign")
	securesign.SetNamespace(architectNamespace)
	if err := r.Delete(ctx, securesign); err != nil && !apierrors.IsNotFound(err) {
		log.FromContext(ctx).Error(err, "failed to delete RHTAS config")
	}

	cm := &corev1.ConfigMap{}
	cm.Name = "rhtas-trusted-root"
	cm.Namespace = architectNamespace
	if err := r.Delete(ctx, cm); err != nil && !apierrors.IsNotFound(err) {
		log.FromContext(ctx).Error(err, "failed to delete RHTAS trusted root ConfigMap")
	}
}

func (r *DisconnectedPlatformReconciler) ensureKeycloakTLS(ctx context.Context, platform *mirrorv1.DisconnectedPlatform, hostname, secretName string) error {
	cfg := platform.Spec.Connected.RHTAS

	// Require certIssuer to be explicitly specified
	if cfg.OIDC.Managed.CertIssuer == nil {
		return fmt.Errorf("certIssuer must be specified when not using custom tlsSecret")
	}

	issuerName := cfg.OIDC.Managed.CertIssuer.Name
	issuerKind := "ClusterIssuer" // Default to ClusterIssuer
	if cfg.OIDC.Managed.CertIssuer.Kind != "" {
		issuerKind = cfg.OIDC.Managed.CertIssuer.Kind
	}

	cert := &unstructured.Unstructured{}
	cert.SetGroupVersionKind(certificateGVK)
	cert.SetName("keycloak-certificate")
	cert.SetNamespace(architectNamespace)

	if err := r.Get(ctx, client.ObjectKeyFromObject(cert), cert); apierrors.IsNotFound(err) {
		cert.Object["spec"] = map[string]interface{}{
			"secretName": secretName,
			"issuerRef": map[string]interface{}{
				"name": issuerName,
				"kind": issuerKind,
			},
			"commonName": hostname,
			"dnsNames": []interface{}{
				hostname,
			},
			"duration":    "8760h",
			"renewBefore": "720h",
		}
		if err := r.Create(ctx, cert); err != nil {
			return fmt.Errorf("failed to create certificate: %w", err)
		}
		log.FromContext(ctx).Info("Created certificate for Keycloak", "issuer", issuerName, "kind", issuerKind)
	}

	return nil
}

func (r *DisconnectedPlatformReconciler) deleteManagedKeycloak(ctx context.Context) {
	realm := &unstructured.Unstructured{}
	realm.SetGroupVersionKind(keycloakRealmGVK)
	realm.SetName("mirror-operator-keycloak-realm")
	realm.SetNamespace(architectNamespace)
	if err := r.Delete(ctx, realm); err != nil && !apierrors.IsNotFound(err) {
		log.FromContext(ctx).Error(err, "failed to delete KeycloakRealmImport")
	}

	kc := &unstructured.Unstructured{}
	kc.SetGroupVersionKind(keycloakGVK)
	kc.SetName("mirror-operator-keycloak")
	kc.SetNamespace(architectNamespace)
	if err := r.Delete(ctx, kc); err != nil && !apierrors.IsNotFound(err) {
		log.FromContext(ctx).Error(err, "failed to delete Keycloak")
	}

	cert := &unstructured.Unstructured{}
	cert.SetGroupVersionKind(certificateGVK)
	cert.SetName("keycloak-certificate")
	cert.SetNamespace(architectNamespace)
	if err := r.Delete(ctx, cert); err != nil && !apierrors.IsNotFound(err) {
		log.FromContext(ctx).Error(err, "failed to delete Keycloak certificate")
	}
}

func (r *DisconnectedPlatformReconciler) extractRHTASRootKeys(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	securesign := &unstructured.Unstructured{}
	securesign.SetGroupVersionKind(securesignGVK)
	securesign.SetName("mirror-operator-securesign")
	securesign.SetNamespace(architectNamespace)

	if err := r.Get(ctx, client.ObjectKeyFromObject(securesign), securesign); err != nil {
		return err
	}

	status, found, err := unstructured.NestedMap(securesign.Object, "status")
	if err != nil || !found {
		return fmt.Errorf("securesign status not available yet")
	}

	// Check if Ready condition is true
	conditions, found, _ := unstructured.NestedSlice(status, "conditions")
	if !found {
		return fmt.Errorf("securesign conditions not available yet")
	}

	ready := false
	for _, cond := range conditions {
		condMap, ok := cond.(map[string]interface{})
		if !ok {
			continue
		}
		condType, _, _ := unstructured.NestedString(condMap, "type")
		condStatus, _, _ := unstructured.NestedString(condMap, "status")
		if condType == "Ready" && condStatus == "True" {
			ready = true
			break
		}
	}

	if !ready {
		return fmt.Errorf("securesign not ready yet")
	}

	tufURL, found, _ := unstructured.NestedString(status, "tuf", "url")
	if !found || tufURL == "" {
		return fmt.Errorf("TUF URL not found in Securesign status")
	}

	// Get TUF resource to find secret references
	tuf := &unstructured.Unstructured{}
	tuf.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "rhtas.redhat.com",
		Version: "v1alpha1",
		Kind:    "Tuf",
	})
	tuf.SetName("mirror-operator-securesign")
	tuf.SetNamespace(architectNamespace)

	if err := r.Get(ctx, client.ObjectKeyFromObject(tuf), tuf); err != nil {
		return fmt.Errorf("failed to get TUF resource: %w", err)
	}

	tufStatus, found, _ := unstructured.NestedMap(tuf.Object, "status")
	if !found {
		return fmt.Errorf("TUF status not available yet")
	}

	keys, found, _ := unstructured.NestedSlice(tufStatus, "keys")
	if !found {
		return fmt.Errorf("TUF keys not found in status")
	}

	var fulcioSecretName, fulcioSecretKey, rekorSecretName, rekorSecretKey string
	for _, key := range keys {
		keyMap, ok := key.(map[string]interface{})
		if !ok {
			continue
		}
		keyName, _, _ := unstructured.NestedString(keyMap, "name")
		secretRef, found, _ := unstructured.NestedMap(keyMap, "secretRef")
		if !found {
			continue
		}
		secretName, _, _ := unstructured.NestedString(secretRef, "name")
		secretKey, _, _ := unstructured.NestedString(secretRef, "key")

		if keyName == "fulcio_v1.crt.pem" {
			fulcioSecretName = secretName
			fulcioSecretKey = secretKey
		} else if keyName == "rekor.pub" {
			rekorSecretName = secretName
			rekorSecretKey = secretKey
		}
	}

	if fulcioSecretName == "" || rekorSecretName == "" {
		return fmt.Errorf("Fulcio or Rekor secret references not found in TUF status")
	}

	// Fetch Fulcio root CA from secret
	fulcioSecret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Name: fulcioSecretName, Namespace: architectNamespace}, fulcioSecret); err != nil {
		return fmt.Errorf("failed to get Fulcio secret: %w", err)
	}
	fulcioRootPEM := string(fulcioSecret.Data[fulcioSecretKey])
	if fulcioRootPEM == "" {
		return fmt.Errorf("Fulcio root CA is empty in secret %s", fulcioSecretName)
	}

	// Fetch Rekor public key from secret
	rekorSecret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Name: rekorSecretName, Namespace: architectNamespace}, rekorSecret); err != nil {
		return fmt.Errorf("failed to get Rekor secret: %w", err)
	}
	rekorPubKeyPEM := string(rekorSecret.Data[rekorSecretKey])
	if rekorPubKeyPEM == "" {
		return fmt.Errorf("Rekor public key is empty in secret %s", rekorSecretName)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rhtas-trusted-root",
			Namespace: architectNamespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "rhtas-trusted-root",
				"app.kubernetes.io/part-of":    "mirror-operator",
				"app.kubernetes.io/managed-by": "mirror-operator",
			},
		},
		Data: map[string]string{
			"fulcio-root.pem":      fulcioRootPEM,
			"rekor-public-key.pem": rekorPubKeyPEM,
			"tuf-repository-url":   tufURL,
			"extraction-timestamp": time.Now().Format(time.RFC3339),
		},
	}

	existing := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(cm), existing); err == nil {
		cm.ResourceVersion = existing.ResourceVersion
		if err := r.Update(ctx, cm); err != nil {
			return fmt.Errorf("failed to update RHTAS trusted root ConfigMap: %w", err)
		}
	} else if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, cm); err != nil {
			return fmt.Errorf("failed to create RHTAS trusted root ConfigMap: %w", err)
		}
	} else {
		return err
	}

	platform.Status.RHTASRootKeys = &mirrorv1.RHTASRootKeysInfo{
		ConfigMap:        "rhtas-trusted-root",
		FulcioRootHash:   hashString(fulcioRootPEM),
		RekorKeyHash:     hashString(rekorPubKeyPEM),
		LastUpdated:      metav1.Now(),
		TUFRepositoryURL: tufURL,
	}

	return nil
}

func hashString(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:8])
}

func (r *DisconnectedPlatformReconciler) configureCollectionPipelineSigning(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	cfg := platform.Spec.Connected.RHTAS
	if cfg == nil || cfg.OIDC == nil || cfg.OIDC.Managed == nil || !cfg.OIDC.Managed.Enabled {
		return nil
	}

	// Get Securesign status for service URLs
	securesign := &unstructured.Unstructured{}
	securesign.SetGroupVersionKind(securesignGVK)
	securesign.SetName("mirror-operator-securesign")
	securesign.SetNamespace(architectNamespace)

	if err := r.Get(ctx, client.ObjectKeyFromObject(securesign), securesign); err != nil {
		return fmt.Errorf("waiting for Securesign to be ready: %w", err)
	}

	status, found, err := unstructured.NestedMap(securesign.Object, "status")
	if err != nil || !found {
		return fmt.Errorf("Securesign status not available yet")
	}

	// Get service URLs from Securesign status
	fulcioURL, _, _ := unstructured.NestedString(status, "fulcio", "url")
	rekorURL, _, _ := unstructured.NestedString(status, "rekor", "url")
	tufURL, _, _ := unstructured.NestedString(status, "tuf", "url")

	if fulcioURL == "" || rekorURL == "" {
		return fmt.Errorf("Fulcio or Rekor URL not available in Securesign status")
	}

	// TUF URL defaults to service URL if not in status
	if tufURL == "" {
		tufURL = "http://tuf.mirror-operator-system.svc"
	}

	// Get Keycloak hostname for OIDC issuer
	kc := &unstructured.Unstructured{}
	kc.SetGroupVersionKind(keycloakGVK)
	kc.SetName("mirror-operator-keycloak")
	kc.SetNamespace(architectNamespace)

	if err := r.Get(ctx, client.ObjectKeyFromObject(kc), kc); err != nil {
		return fmt.Errorf("waiting for Keycloak: %w", err)
	}

	hostname, _, _ := unstructured.NestedString(kc.Object, "spec", "hostname", "hostname")
	if hostname == "" {
		return fmt.Errorf("Keycloak hostname not available")
	}

	oidcIssuer := fmt.Sprintf("https://%s/realms/%s", hostname, cfg.OIDC.Managed.Realm)
	clientID := "trusted-artifact-signer"

	// Retrieve the actual client secret from Keycloak
	// The KeycloakRealmImport created the client, now we need to get its generated secret
	if err := r.updateKeycloakClientSecret(ctx, hostname, clientID, cfg.OIDC.Managed.Realm); err != nil {
		return fmt.Errorf("failed to retrieve Keycloak client secret: %w", err)
	}

	// Configure email protocol mapper for service account tokens
	if err := r.configureKeycloakEmailMapper(ctx, hostname, clientID, cfg.OIDC.Managed.Realm); err != nil {
		return fmt.Errorf("failed to configure Keycloak email mapper: %w", err)
	}

	// Update all CollectionPipelines with keyless signing configuration
	pipelines := &mirrorv1.CollectionPipelineList{}
	if err := r.List(ctx, pipelines); err != nil {
		return fmt.Errorf("failed to list CollectionPipelines: %w", err)
	}

	for _, pipeline := range pipelines.Items {
		if pipeline.Spec.Signing == nil {
			pipeline.Spec.Signing = &mirrorv1.CosignSigningConfig{}
		}

		pipeline.Spec.Signing.Keyless = &mirrorv1.KeylessSigningConfig{
			FulcioURL:    fulcioURL,
			RekorURL:     rekorURL,
			TUFURL:       tufURL,
			OIDCIssuer:   oidcIssuer,
			OIDCClientID: clientID,
			OIDCClientSecret: &corev1.LocalObjectReference{
				Name: "mirror-operator-keycloak-client-secret",
			},
		}

		if err := r.Update(ctx, &pipeline); err != nil {
			log.FromContext(ctx).Error(err, "failed to update CollectionPipeline signing config", "pipeline", pipeline.Name)
		}
	}

	log.FromContext(ctx).Info("Configured keyless signing for CollectionPipelines", "fulcioURL", fulcioURL, "rekorURL", rekorURL, "tufURL", tufURL)
	return nil
}

func (r *DisconnectedPlatformReconciler) ensureKeycloakOIDCClient(ctx context.Context, hostname, realm, adminUser, adminPassword, clientID string) (string, error) {
	// This is a simplified implementation - in production you'd use Keycloak admin API
	// For now, we'll generate a client secret and assume the client is created via KeycloakRealmImport
	// The KeycloakRealmImport should include the sigstore client configuration

	// Check if we already have a client secret stored
	existing := &corev1.Secret{}
	err := r.Get(ctx, client.ObjectKey{Name: "sigstore-client-secret-cache", Namespace: architectNamespace}, existing)
	if err == nil {
		return string(existing.Data["secret"]), nil
	}

	// Generate new secret
	clientSecret := generateRandomString(32)

	// Cache it
	cache := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sigstore-client-secret-cache",
			Namespace: architectNamespace,
		},
		StringData: map[string]string{
			"secret": clientSecret,
		},
	}
	if err := r.Create(ctx, cache); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", err
	}

	return clientSecret, nil
}

func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[i%len(charset)]
	}
	return string(b)
}

func (r *DisconnectedPlatformReconciler) ensureOperatorGroup(ctx context.Context, op operatorDef) error {
	// Skip creating OperatorGroup for openshift-operators namespace
	// as it already has the global-operators OperatorGroup for all-namespace operators
	if op.ns == "openshift-operators" {
		return nil
	}

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

func (r *DisconnectedPlatformReconciler) ensureArchitectServiceAccount(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	saName := "airgap-architect-backend"

	// Create ServiceAccount
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: architectNamespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "airgap-architect",
				"app.kubernetes.io/component":  "backend",
				"app.kubernetes.io/part-of":    "mirror-operator",
				"app.kubernetes.io/managed-by": "mirror-operator",
			},
		},
	}

	existing := &corev1.ServiceAccount{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(sa), existing); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, sa); err != nil {
			return fmt.Errorf("failed to create ServiceAccount: %w", err)
		}
	}

	// Create Role with permissions for CollectionPipeline and related resources
	role := &unstructured.Unstructured{}
	role.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "rbac.authorization.k8s.io",
		Version: "v1",
		Kind:    "Role",
	})
	role.SetName(saName)
	role.SetNamespace(architectNamespace)
	role.SetLabels(sa.Labels)

	unstructured.SetNestedSlice(role.Object, []interface{}{
		map[string]interface{}{
			"apiGroups": []interface{}{"mirror.mirror.mathianasj.github.com"},
			"resources": []interface{}{"collectionpipelines", "mirrorimports", "disconnectedplatforms"},
			"verbs":     []interface{}{"get", "list", "watch", "create", "update", "patch", "delete"},
		},
		map[string]interface{}{
			"apiGroups": []interface{}{"mirror.mirror.mathianasj.github.com"},
			"resources": []interface{}{"collectionpipelines/status", "mirrorimports/status", "disconnectedplatforms/status"},
			"verbs":     []interface{}{"get", "list", "watch"},
		},
		map[string]interface{}{
			"apiGroups": []interface{}{""},
			"resources": []interface{}{"configmaps", "secrets"},
			"verbs":     []interface{}{"get", "list", "watch"},
		},
	}, "rules")

	existingRole := &unstructured.Unstructured{}
	existingRole.SetGroupVersionKind(role.GroupVersionKind())
	if err := r.Get(ctx, client.ObjectKeyFromObject(role), existingRole); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, role); err != nil {
			return fmt.Errorf("failed to create Role: %w", err)
		}
	} else if err == nil {
		// Update existing role
		role.SetResourceVersion(existingRole.GetResourceVersion())
		if err := r.Update(ctx, role); err != nil {
			return fmt.Errorf("failed to update Role: %w", err)
		}
	}

	// Create RoleBinding
	rb := &unstructured.Unstructured{}
	rb.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "rbac.authorization.k8s.io",
		Version: "v1",
		Kind:    "RoleBinding",
	})
	rb.SetName(saName)
	rb.SetNamespace(architectNamespace)
	rb.SetLabels(sa.Labels)

	unstructured.SetNestedSlice(rb.Object, []interface{}{
		map[string]interface{}{
			"kind":      "ServiceAccount",
			"name":      saName,
			"namespace": architectNamespace,
		},
	}, "subjects")

	unstructured.SetNestedMap(rb.Object, map[string]interface{}{
		"kind":     "Role",
		"name":     saName,
		"apiGroup": "rbac.authorization.k8s.io",
	}, "roleRef")

	existingRB := &unstructured.Unstructured{}
	existingRB.SetGroupVersionKind(rb.GroupVersionKind())
	if err := r.Get(ctx, client.ObjectKeyFromObject(rb), existingRB); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, rb); err != nil {
			return fmt.Errorf("failed to create RoleBinding: %w", err)
		}
	}

	return nil
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

	// Ensure ServiceAccount and RBAC for backend
	if err := r.ensureArchitectServiceAccount(ctx, platform); err != nil {
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

	podSpec := map[string]interface{}{
		"containers": []interface{}{
			buildContainer(name, image, labels),
		},
		"volumes": volumes,
	}

	// Add ServiceAccount for backend component
	if component == "backend" {
		podSpec["serviceAccountName"] = "airgap-architect-backend"
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
			"spec": podSpec,
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

// RHTAS Health Check and Self-Healing Functions

func (r *DisconnectedPlatformReconciler) updateStatusFromSecuresignHealth(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	// Read health status from Securesign (set by RHTASHealthCheck controller)
	securesign := &unstructured.Unstructured{}
	securesign.SetGroupVersionKind(securesignGVK)
	securesign.SetName("mirror-operator-securesign")
	securesign.SetNamespace(architectNamespace)

	if err := r.Get(ctx, client.ObjectKeyFromObject(securesign), securesign); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	conditions, found, err := unstructured.NestedSlice(securesign.Object, "status", "conditions")
	if err != nil || !found {
		return nil
	}

	// Look for HealthCheckPassed condition
	for _, cond := range conditions {
		if condMap, ok := cond.(map[string]interface{}); ok {
			if condMap["type"] == "HealthCheckPassed" {
				status, _ := condMap["status"].(string)
				message, _ := condMap["message"].(string)
				reason, _ := condMap["reason"].(string)

				if status == "False" {
					// Health check failed, update platform to Degraded
					r.updateDegradedCondition(ctx, platform, reason, message)
				} else {
					// Health check passed, clear degraded condition
					r.clearDegradedCondition(ctx, platform, "HealthCheckFailed")
					r.clearDegradedCondition(ctx, platform, "AllHealthChecksPassed")
				}
				break
			}
		}
	}

	return nil
}

func (r *DisconnectedPlatformReconciler) updateDegradedCondition(ctx context.Context, platform *mirrorv1.DisconnectedPlatform, reason, message string) {
	// Find and update or add Degraded condition
	now := metav1.Now()
	degradedCondition := metav1.Condition{
		Type:               "Degraded",
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
	}

	found := false
	for i := range platform.Status.Conditions {
		if platform.Status.Conditions[i].Type == "Degraded" {
			if platform.Status.Conditions[i].Status != metav1.ConditionTrue ||
				platform.Status.Conditions[i].Reason != reason {
				platform.Status.Conditions[i] = degradedCondition
			}
			found = true
			break
		}
	}

	if !found {
		platform.Status.Conditions = append(platform.Status.Conditions, degradedCondition)
	}

	if platform.Status.Phase != mirrorv1.PlatformPhaseError {
		platform.Status.Phase = "Degraded"
	}
}

func (r *DisconnectedPlatformReconciler) clearDegradedCondition(ctx context.Context, platform *mirrorv1.DisconnectedPlatform, reason string) {
	// Remove or update Degraded condition for this specific reason
	for i := range platform.Status.Conditions {
		if platform.Status.Conditions[i].Type == "Degraded" && platform.Status.Conditions[i].Reason == reason {
			platform.Status.Conditions[i].Status = metav1.ConditionFalse
			platform.Status.Conditions[i].LastTransitionTime = metav1.Now()
			platform.Status.Conditions[i].Message = "Health check passed"
			break
		}
	}

	// If all degraded conditions are cleared, set phase back to Ready
	allCleared := true
	for _, cond := range platform.Status.Conditions {
		if cond.Type == "Degraded" && cond.Status == metav1.ConditionTrue {
			allCleared = false
			break
		}
	}

	if allCleared && platform.Status.Phase == "Degraded" {
		platform.Status.Phase = mirrorv1.PlatformPhaseReady
	}
}

func (r *DisconnectedPlatformReconciler) performRHTASHealthChecks(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	cfg := platform.Spec.Connected.RHTAS
	if cfg == nil || cfg.OIDC == nil || cfg.OIDC.Managed == nil || !cfg.OIDC.Managed.Enabled {
		return nil
	}

	logger := log.FromContext(ctx)

	// Check 1: Verify TUF doesn't have tsa.certchain.pem key
	if err := r.checkAndFixTUFKeys(ctx); err != nil {
		logger.Error(err, "Failed to check/fix TUF keys")
	}

	// Check 2: Verify Fulcio can reach Keycloak
	if err := r.checkFulcioKeycloakConnectivity(ctx); err != nil {
		logger.Error(err, "Failed to verify Fulcio-Keycloak connectivity")
	}

	return nil
}

func (r *DisconnectedPlatformReconciler) checkAndFixTUFKeys(ctx context.Context) error {
	securesign := &unstructured.Unstructured{}
	securesign.SetGroupVersionKind(securesignGVK)
	securesign.SetName("mirror-operator-securesign")
	securesign.SetNamespace(architectNamespace)

	if err := r.Get(ctx, client.ObjectKeyFromObject(securesign), securesign); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // Securesign not created yet
		}
		return err
	}

	// Check TUF keys for tsa.certchain.pem
	keys, found, err := unstructured.NestedSlice(securesign.Object, "spec", "tuf", "keys")
	if err != nil || !found {
		return nil
	}

	hasTSAKey := false
	tsaKeyIndex := -1
	for i, key := range keys {
		if keyMap, ok := key.(map[string]interface{}); ok {
			if name, _ := keyMap["name"].(string); name == "tsa.certchain.pem" {
				hasTSAKey = true
				tsaKeyIndex = i
				break
			}
		}
	}

	if hasTSAKey {
		log.FromContext(ctx).Info("Detected tsa.certchain.pem in TUF keys, removing it to unblock TUF deployment")

		// Remove the TSA key
		newKeys := make([]interface{}, 0, len(keys)-1)
		for i, key := range keys {
			if i != tsaKeyIndex {
				newKeys = append(newKeys, key)
			}
		}

		if err := unstructured.SetNestedSlice(securesign.Object, newKeys, "spec", "tuf", "keys"); err != nil {
			return fmt.Errorf("failed to update TUF keys: %w", err)
		}

		if err := r.Update(ctx, securesign); err != nil {
			return fmt.Errorf("failed to patch Securesign to remove tsa.certchain.pem: %w", err)
		}

		log.FromContext(ctx).Info("Successfully removed tsa.certchain.pem from TUF keys")
	}

	return nil
}

func (r *DisconnectedPlatformReconciler) checkFulcioKeycloakConnectivity(ctx context.Context) error {
	// Check Securesign status to see if Fulcio is ready
	securesign := &unstructured.Unstructured{}
	securesign.SetGroupVersionKind(securesignGVK)
	securesign.SetName("mirror-operator-securesign")
	securesign.SetNamespace(architectNamespace)

	if err := r.Get(ctx, client.ObjectKeyFromObject(securesign), securesign); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // Securesign not created yet
		}
		return err
	}

	// Check if Fulcio pod has high restart count (indicating crash loop)
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(architectNamespace), client.MatchingLabels{"app": "fulcio-server"}); err != nil {
		return err
	}

	if len(podList.Items) == 0 {
		return nil
	}

	pod := podList.Items[0]

	// Check if pod has high restart count (likely failing to connect to Keycloak on startup)
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.RestartCount > 3 {
			// High restart count indicates the pod is crash looping
			// This is often due to Keycloak not being ready when Fulcio started
			log.FromContext(ctx).Info("Fulcio pod has high restart count, may indicate Keycloak connectivity issue during startup",
				"pod", pod.Name,
				"restartCount", containerStatus.RestartCount)

			// Check if Keycloak is ready now
			kc := &unstructured.Unstructured{}
			kc.SetGroupVersionKind(keycloakGVK)
			kc.SetName("mirror-operator-keycloak")
			kc.SetNamespace(architectNamespace)

			if err := r.Get(ctx, client.ObjectKeyFromObject(kc), kc); err == nil {
				conditions, _, _ := unstructured.NestedSlice(kc.Object, "status", "conditions")
				for _, cond := range conditions {
					condMap := cond.(map[string]interface{})
					if condMap["type"] == "Ready" && condMap["status"] == "True" {
						// Keycloak is ready now, restart Fulcio to retry connection
						log.FromContext(ctx).Info("Keycloak is ready, restarting Fulcio pod to establish connection", "pod", pod.Name)

						if err := r.Delete(ctx, &pod); err != nil {
							return fmt.Errorf("failed to restart Fulcio pod: %w", err)
						}

						log.FromContext(ctx).Info("Successfully restarted Fulcio pod")
						break
					}
				}
			}
		}
	}

	return nil
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

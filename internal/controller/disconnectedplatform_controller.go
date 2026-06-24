package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
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
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"

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
	Scheme                      *runtime.Scheme
	ArchitectFrontendImage      string
	ArchitectBackendImage       string
	ArchitectConsolePluginImage string
	ClientSet                   kubernetes.Interface
	RESTConfig                  *rest.Config
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
// +kubebuilder:rbac:groups=console.openshift.io,resources=consoleplugins,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.openshift.io,resources=consoles,verbs=get;list;watch;update;patch

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

	// Reconcile Airgap Architect UI early so it's not blocked by downstream components
	if err := r.reconcileArchitect(ctx, platform); err != nil {
		log.FromContext(ctx).Error(err, "failed to reconcile airgap-architect")
		platform.Status.Phase = mirrorv1.PlatformPhaseError
		r.setErrorCondition(platform, "ArchitectReconcileFailed", err.Error())
		return ctrl.Result{}, r.Status().Update(ctx, platform)
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
			platform.Status.Phase = mirrorv1.PlatformPhaseError
			r.setErrorCondition(platform, "ManagedKeycloakFailed", err.Error())
			return ctrl.Result{}, r.Status().Update(ctx, platform)
		}
		// Check if Keycloak instance is actually ready
		if err := r.checkKeycloakHealth(ctx, platform); err != nil {
			log.FromContext(ctx).Error(err, "Keycloak instance not healthy")
			platform.Status.Phase = mirrorv1.PlatformPhaseError
			r.setErrorCondition(platform, "KeycloakNotHealthy", err.Error())
			return ctrl.Result{RequeueAfter: 30 * time.Second}, r.Status().Update(ctx, platform)
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

	// Reconcile Quay for intermediate registry (only in connected mode)
	if platform.Spec.Mode == mirrorv1.PlatformModeConnected && platform.Spec.Connected != nil {
		if err := r.reconcileQuayConfig(ctx, platform); err != nil {
			log.FromContext(ctx).Error(err, "failed to reconcile Quay config")
		}
		// Ensure Quay credentials are in pull-secret for managed Quay
		if platform.Spec.Connected.Quay != nil && platform.Spec.Connected.Quay.Managed != nil && platform.Spec.Connected.Quay.Managed.Enabled {
			if err := r.ensureQuayCredentials(ctx, platform); err != nil {
				log.FromContext(ctx).Error(err, "failed to ensure Quay credentials in pull-secret")
			}
		}
		// Reconcile ObjectBucketClaim for artifacts storage
		if err := r.reconcileArtifactsBucket(ctx, platform); err != nil {
			log.FromContext(ctx).Error(err, "failed to reconcile artifacts bucket")
		}
		// Reconcile collection pipeline template
		if err := r.reconcileCollectionPipelineTemplate(ctx, platform); err != nil {
			log.FromContext(ctx).Error(err, "failed to reconcile collection pipeline template")
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

	// Set Ready condition to true if no errors
	r.setReadyCondition(platform, "ReconciliationSucceeded", "All components reconciled successfully")

	// Only update status if it changed
	if err := r.Status().Update(ctx, platform); err != nil {
		// Ignore conflict errors - another reconciliation already updated it
		if !apierrors.IsConflict(err) {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
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
	{
		name:      "quay-operator",
		pkg:       "quay-operator",
		channel:   "stable-3.13",
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
	if op.Keycloak != nil {
		overrides["rhbk-operator"] = op.Keycloak
	}
	if op.RHTAS != nil {
		overrides["trusted-artifact-signer"] = op.RHTAS
	}
	if op.RHTPA != nil {
		overrides["trusted-profile-analyzer"] = op.RHTPA
	}
	if op.QuayOperator != nil {
		overrides["quay-operator"] = op.QuayOperator
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

		// Enable console plugins for operators that provide them
		if compStatus == "Succeeded" {
			if op.name == "openshift-pipelines" {
				if err := r.enableConsolePluginInOperator(ctx, "pipelines-console-plugin"); err != nil {
					log.FromContext(ctx).Error(err, "failed to enable pipelines console plugin")
				}
			}
		}
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
	if cfg == nil || cfg.Storage == nil {
		return nil
	}

	existingTPA := &unstructured.Unstructured{}
	existingTPA.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "rhtpa.io",
		Version: "v1",
		Kind:    "TrustedProfileAnalyzer",
	})
	existingTPA.SetName("mirror-operator-trusted-profile-analyzer")
	existingTPA.SetNamespace(architectNamespace)

	if err := r.Get(ctx, client.ObjectKeyFromObject(existingTPA), existingTPA); err == nil {
		// Check if OIDC is configured
		oidc, found, _ := unstructured.NestedMap(existingTPA.Object, "spec", "oidc")
		if found && len(oidc) > 0 {
			// Check if modules.createDatabase and modules.migrateDatabase are enabled
			modules, found, _ := unstructured.NestedMap(existingTPA.Object, "spec", "modules")
			if found {
				createDB, found, _ := unstructured.NestedMap(modules, "createDatabase")
				migrateDB, found2, _ := unstructured.NestedMap(modules, "migrateDatabase")

				createDBEnabled, _, _ := unstructured.NestedBool(createDB, "enabled")
				migrateDBEnabled, _, _ := unstructured.NestedBool(migrateDB, "enabled")

				if found && found2 && createDBEnabled && migrateDBEnabled {
					// TPA already has OIDC and database modules configured
					// Update redirect URIs in case route hostname changed
					ingress := &unstructured.Unstructured{}
					ingress.SetGroupVersionKind(schema.GroupVersionKind{
						Group:   "config.openshift.io",
						Version: "v1",
						Kind:    "Ingress",
					})
					ingress.SetName("cluster")
					if err := r.Get(ctx, client.ObjectKeyFromObject(ingress), ingress); err == nil {
						domain, _, _ := unstructured.NestedString(ingress.Object, "spec", "domain")
						keycloakHost := "keycloak." + domain

						// Get Keycloak admin credentials to ensure read-only scope
						adminSecret := &corev1.Secret{}
						if err := r.Get(ctx, client.ObjectKey{
							Name:      "mirror-operator-keycloak-initial-admin",
							Namespace: architectNamespace,
						}, adminSecret); err == nil {
							adminUser := string(adminSecret.Data["username"])
							adminPassword := string(adminSecret.Data["password"])

							httpClient := &http.Client{
								Transport: &http.Transport{
									TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
								},
							}

							// Get admin token
							tokenURL := fmt.Sprintf("https://%s/realms/master/protocol/openid-connect/token", keycloakHost)
							tokenData := url.Values{}
							tokenData.Set("grant_type", "password")
							tokenData.Set("client_id", "admin-cli")
							tokenData.Set("username", adminUser)
							tokenData.Set("password", adminPassword)

							if tokenResp, err := httpClient.PostForm(tokenURL, tokenData); err == nil {
								defer tokenResp.Body.Close()
								var tokenResult map[string]interface{}
								if err := json.NewDecoder(tokenResp.Body).Decode(&tokenResult); err == nil {
									if adminToken, ok := tokenResult["access_token"].(string); ok && adminToken != "" {
										// Ensure read-only and create scopes exist
										if err := r.ensureTrustifyReadOnlyScope(ctx, keycloakHost, "trustify", adminToken, httpClient); err != nil {
											log.FromContext(ctx).Error(err, "Failed to ensure read-only scope")
										}
										if err := r.ensureTrustifyCreateScope(ctx, keycloakHost, "trustify", adminToken, httpClient); err != nil {
											log.FromContext(ctx).Error(err, "Failed to ensure create scope")
										}

										appDomain := "rhtpa." + domain

										// Ensure all OIDC clients exist (frontend, cli, sbom-uploader)
										if err := r.ensureTrustifyOIDCClient(ctx, keycloakHost, "trustify", "frontend", appDomain, adminToken, httpClient, true); err != nil {
											log.FromContext(ctx).Error(err, "Failed to ensure frontend OIDC client")
										}
										if err := r.ensureTrustifyOIDCClient(ctx, keycloakHost, "trustify", "cli", appDomain, adminToken, httpClient, false); err != nil {
											log.FromContext(ctx).Error(err, "Failed to ensure CLI OIDC client")
										}
										if err := r.ensureTrustifyOIDCClient(ctx, keycloakHost, "trustify", "sbom-uploader", appDomain, adminToken, httpClient, false); err != nil {
											log.FromContext(ctx).Error(err, "Failed to ensure SBOM uploader OIDC client")
										}

										// Assign scope to frontend, CLI, and sbom-uploader clients
										if err := r.assignScopeToTrustifyClients(ctx, keycloakHost, "trustify", adminToken, httpClient); err != nil {
											log.FromContext(ctx).Error(err, "Failed to assign scope to clients")
										}
									}
								}
							}
						}

						if err := r.updateTrustifyRedirectURIs(ctx, keycloakHost, "trustify", "frontend"); err != nil {
							log.FromContext(ctx).Error(err, "Failed to update Keycloak redirect URIs for RHTPA")
						}
					}
					return nil
				}
			}
		}
		// Delete existing TPA to recreate with OIDC and database modules
		log.FromContext(ctx).Info("Deleting existing TPA to update configuration")
		if err := r.Delete(ctx, existingTPA); err != nil {
			return fmt.Errorf("failed to delete existing TPA for update: %w", err)
		}
		// Wait a moment for deletion to complete
		time.Sleep(2 * time.Second)
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

	// Configure OIDC for TPA using managed Keycloak if OIDC not explicitly provided
	// Note: We'll update the redirect URIs after TPA creates its route
	var issuerURL string
	if cfg.OIDC != nil && cfg.OIDC.Issuer != "" {
		// Use explicitly provided OIDC configuration
		issuerURL = cfg.OIDC.Issuer
	} else {
		// Create trustify realm (OIDC client redirect URIs will be updated later)
		keycloakHost := "keycloak." + domain
		realmName := "trustify"

		if err := r.ensureTrustifyRealmAndOIDC(ctx, keycloakHost, realmName, appDomain); err != nil {
			return fmt.Errorf("failed to configure TPA OIDC in Keycloak: %w", err)
		}

		issuerURL = fmt.Sprintf("https://%s/realms/%s", keycloakHost, realmName)
	}

	// Configure database - use managed PostgreSQL if not explicitly provided
	var dbHost, dbName, dbUser, dbPassword string
	if cfg.Database != nil && cfg.Database.Host != "" && cfg.Database.Host != "postgres.example.com" {
		// Use explicitly provided database
		dbHost = cfg.Database.Host
		dbName = cfg.Database.Name
		dbUser = cfg.Database.Username
		dbPassword = cfg.Database.Password
	} else {
		// Create managed PostgreSQL for TPA
		var err error
		dbHost, dbName, dbUser, dbPassword, err = r.ensureRHTPAPostgreSQL(ctx)
		if err != nil {
			return fmt.Errorf("failed to create managed PostgreSQL for TPA: %w", err)
		}
		log.FromContext(ctx).Info("Using managed PostgreSQL for TPA", "host", dbHost)
	}

	spec := map[string]interface{}{
		"appDomain": appDomain,
		"openshift": map[string]interface{}{
			"useServiceCa": true,
		},
		"database": map[string]interface{}{
			"host":     dbHost,
			"name":     dbName,
			"username": dbUser,
			"password": dbPassword,
		},
		"createDatabase": map[string]interface{}{
			"enabled":  true,
			"image":    map[string]interface{}{},
			"name":     dbName,
			"username": "postgres",
			"password": "postgres",
		},
		"migrateDatabase": map[string]interface{}{
			"enabled": true,
			"image":   map[string]interface{}{},
		},
		"modules": map[string]interface{}{
			"createDatabase": map[string]interface{}{
				"enabled": true,
				"image":   map[string]interface{}{},
			},
			"migrateDatabase": map[string]interface{}{
				"enabled": true,
				"image":   map[string]interface{}{},
			},
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

	// Add OIDC configuration per TPA Helm chart schema
	if issuerURL != "" {
		spec["oidc"] = map[string]interface{}{
			"clients": map[string]interface{}{
				"frontend": map[string]interface{}{
					"clientId": "frontend",
					// frontend is a public client (no clientSecret field)
				},
				"cli": map[string]interface{}{
					"clientId": "cli",
					"clientSecret": map[string]interface{}{
						"valueFrom": map[string]interface{}{
							"secretKeyRef": map[string]interface{}{
								"name": "rhtpa-oidc-cli-secret",
								"key":  "clientSecret",
							},
						},
					},
				},
			},
			"insecure":  false,
			"loadUser":  true,
			"issuerUrl": issuerURL,
		}
	}

	st := map[string]interface{}{}

	// Map storage type: "local" -> "filesystem" for TPA Helm chart
	storageType := cfg.Storage.Type
	if storageType == "local" {
		storageType = "filesystem"
	}
	st["type"] = storageType

	if cfg.Storage.AccessKey != "" {
		// S3 storage configuration
		st["accessKey"] = cfg.Storage.AccessKey
		st["secretKey"] = cfg.Storage.SecretKey
		st["bucket"] = cfg.Storage.Bucket
		st["region"] = cfg.Storage.Region
	} else if cfg.Storage.Size != "" {
		// Filesystem storage configuration requires size
		st["size"] = cfg.Storage.Size
	}
	spec["storage"] = st

	newTPA := &unstructured.Unstructured{}
	newTPA.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "rhtpa.io",
		Version: "v1",
		Kind:    "TrustedProfileAnalyzer",
	})
	newTPA.SetName("mirror-operator-trusted-profile-analyzer")
	newTPA.SetNamespace(architectNamespace)
	newTPA.Object["spec"] = spec

	if err := r.Create(ctx, newTPA); err != nil {
		return err
	}

	// Update Keycloak frontend client redirect URIs with actual RHTPA route hostname
	// Only if using managed Keycloak
	if cfg.OIDC == nil || cfg.OIDC.Issuer == "" {
		keycloakHost := "keycloak." + domain
		if err := r.updateTrustifyRedirectURIs(ctx, keycloakHost, "trustify", "frontend"); err != nil {
			log.FromContext(ctx).Error(err, "Failed to update Keycloak redirect URIs for RHTPA, OAuth may not work")
		}
	}

	return nil
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

func (r *DisconnectedPlatformReconciler) updateTrustifyRedirectURIs(ctx context.Context, keycloakHost, realmName, clientID string) error {
	logger := log.FromContext(ctx)

	// Get the TPA resource to check ownership
	tpa := &unstructured.Unstructured{}
	tpa.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "rhtpa.io",
		Version: "v1",
		Kind:    "TrustedProfileAnalyzer",
	})
	tpa.SetName("mirror-operator-trusted-profile-analyzer")
	tpa.SetNamespace(architectNamespace)
	if err := r.Get(ctx, client.ObjectKeyFromObject(tpa), tpa); err != nil {
		return fmt.Errorf("failed to get TPA resource: %w", err)
	}
	tpaUID := tpa.GetUID()

	// Find Ingress resources owned by the TPA
	ingressList := &unstructured.UnstructuredList{}
	ingressList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "networking.k8s.io",
		Version: "v1",
		Kind:    "Ingress",
	})
	if err := r.List(ctx, ingressList, client.InNamespace(architectNamespace)); err != nil {
		return fmt.Errorf("failed to list ingresses: %w", err)
	}

	var rhtpaServerURL string
	for _, ingress := range ingressList.Items {
		// Check if this ingress is owned by the TPA
		ownerRefs := ingress.GetOwnerReferences()
		for _, owner := range ownerRefs {
			if owner.UID == tpaUID && owner.Kind == "TrustedProfileAnalyzer" {
				// This is an ingress owned by TPA - check if it's the server ingress
				ingressName := ingress.GetName()
				if strings.Contains(ingressName, "server") {
					// Get hostname from ingress status
					rules, found, _ := unstructured.NestedSlice(ingress.Object, "spec", "rules")
					if found && len(rules) > 0 {
						if ruleMap, ok := rules[0].(map[string]interface{}); ok {
							if host, found, _ := unstructured.NestedString(ruleMap, "host"); found && host != "" {
								rhtpaServerURL = "https://" + host
								logger.Info("Found RHTPA server hostname from Ingress", "hostname", host, "ingress", ingressName)
								break
							}
						}
					}
				}
			}
		}
		if rhtpaServerURL != "" {
			break
		}
	}

	if rhtpaServerURL == "" {
		return fmt.Errorf("RHTPA server ingress not found yet or hostname not set")
	}

	// Get Keycloak admin token
	adminSecret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{
		Name:      "mirror-operator-keycloak-initial-admin",
		Namespace: architectNamespace,
	}, adminSecret); err != nil {
		return fmt.Errorf("failed to get Keycloak admin credentials: %w", err)
	}
	adminPassword := string(adminSecret.Data["password"])

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Timeout: 30 * time.Second,
	}

	// Get admin token
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

	adminToken, ok := tokenResult["access_token"].(string)
	if !ok {
		return fmt.Errorf("no access token in response")
	}

	// Find the client by clientId
	clientsURL := fmt.Sprintf("https://%s/admin/realms/%s/clients?clientId=%s", keycloakHost, realmName, clientID)
	req, _ := http.NewRequest("GET", clientsURL, nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to check client: %w", err)
	}
	defer resp.Body.Close()

	var clients []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&clients); err != nil {
		return fmt.Errorf("failed to decode clients: %w", err)
	}

	if len(clients) == 0 {
		return fmt.Errorf("client %s not found in realm %s", clientID, realmName)
	}

	// Update redirect URIs
	clientUUID := clients[0]["id"].(string)
	updateURL := fmt.Sprintf("https://%s/admin/realms/%s/clients/%s", keycloakHost, realmName, clientUUID)
	updateData := map[string]interface{}{
		"redirectUris": []string{rhtpaServerURL + "/*"},
		"webOrigins":   []string{"*"},
	}
	updateJSON, _ := json.Marshal(updateData)
	req, _ = http.NewRequest("PUT", updateURL, bytes.NewReader(updateJSON))
	req.Header.Set("Authorization", "Bearer "+adminToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err = httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to update client: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to update client redirect URIs: status %d, body: %s", resp.StatusCode, string(body))
	}

	logger.Info("Updated OIDC client redirect URIs", "clientId", clientID, "rhtpaURL", rhtpaServerURL)
	return nil
}

func (r *DisconnectedPlatformReconciler) ensureTrustifyRealmAndOIDC(ctx context.Context, keycloakHost, realmName, appDomain string) error {
	logger := log.FromContext(ctx)

	// Get Keycloak admin credentials
	adminSecret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{
		Name:      "mirror-operator-keycloak-initial-admin",
		Namespace: architectNamespace,
	}, adminSecret); err != nil {
		return fmt.Errorf("failed to get Keycloak admin credentials: %w", err)
	}

	adminUser := string(adminSecret.Data["username"])
	adminPassword := string(adminSecret.Data["password"])

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	// Get admin access token
	tokenURL := fmt.Sprintf("https://%s/realms/master/protocol/openid-connect/token", keycloakHost)
	tokenData := url.Values{}
	tokenData.Set("grant_type", "password")
	tokenData.Set("client_id", "admin-cli")
	tokenData.Set("username", adminUser)
	tokenData.Set("password", adminPassword)

	tokenResp, err := httpClient.PostForm(tokenURL, tokenData)
	if err != nil {
		return fmt.Errorf("failed to get admin token: %w", err)
	}
	defer tokenResp.Body.Close()

	if tokenResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(tokenResp.Body)
		return fmt.Errorf("failed to get admin token: status %d, body: %s", tokenResp.StatusCode, string(body))
	}

	var tokenResult map[string]interface{}
	if err := json.NewDecoder(tokenResp.Body).Decode(&tokenResult); err != nil {
		return fmt.Errorf("failed to decode token response: %w", err)
	}

	adminToken, ok := tokenResult["access_token"].(string)
	if !ok || adminToken == "" {
		return fmt.Errorf("no access token in response")
	}

	// Check if trustify realm exists
	realmURL := fmt.Sprintf("https://%s/admin/realms/%s", keycloakHost, realmName)
	req, _ := http.NewRequest("GET", realmURL, nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to check realm: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// Create trustify realm
		logger.Info("Creating trustify realm in Keycloak")

		realmConfig := map[string]interface{}{
			"realm":       realmName,
			"enabled":     true,
			"displayName": "Trusted Profile Analyzer",
		}

		realmJSON, _ := json.Marshal(realmConfig)
		req, _ := http.NewRequest("POST", fmt.Sprintf("https://%s/admin/realms", keycloakHost), bytes.NewReader(realmJSON))
		req.Header.Set("Authorization", "Bearer "+adminToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to create realm: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to create realm: status %d, body: %s", resp.StatusCode, string(body))
		}

		logger.Info("Successfully created trustify realm")
	}

	// Create read-only and create client scopes for Trustify authorization
	if err := r.ensureTrustifyReadOnlyScope(ctx, keycloakHost, realmName, adminToken, httpClient); err != nil {
		logger.Error(err, "failed to create read-only client scope", "realm", realmName)
		// Continue even if scope creation fails - it might already exist
	}
	if err := r.ensureTrustifyCreateScope(ctx, keycloakHost, realmName, adminToken, httpClient); err != nil {
		logger.Error(err, "failed to create create:document client scope", "realm", realmName)
		// Continue even if scope creation fails - it might already exist
	}

	// Create frontend public OIDC client
	if err := r.ensureTrustifyOIDCClient(ctx, keycloakHost, realmName, "frontend", appDomain, adminToken, httpClient, true); err != nil {
		return fmt.Errorf("failed to create frontend OIDC client: %w", err)
	}

	// Create CLI confidential OIDC client
	if err := r.ensureTrustifyOIDCClient(ctx, keycloakHost, realmName, "cli", appDomain, adminToken, httpClient, false); err != nil {
		return fmt.Errorf("failed to create CLI OIDC client: %w", err)
	}

	// Create SBOM uploader confidential OIDC client for CollectionPipeline SBOM uploads
	if err := r.ensureTrustifyOIDCClient(ctx, keycloakHost, realmName, "sbom-uploader", appDomain, adminToken, httpClient, false); err != nil {
		return fmt.Errorf("failed to create SBOM uploader OIDC client: %w", err)
	}

	return nil
}

func (r *DisconnectedPlatformReconciler) ensureTrustifyReadOnlyScope(ctx context.Context, keycloakHost, realmName, adminToken string, httpClient *http.Client) error {
	logger := log.FromContext(ctx)

	// Create the read:document client scope for read-only access to Trustify APIs
	scopeName := "read:document"

	// Check if scope already exists
	scopesURL := fmt.Sprintf("https://%s/admin/realms/%s/client-scopes", keycloakHost, realmName)
	req, _ := http.NewRequest("GET", scopesURL, nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to list client scopes: %w", err)
	}
	defer resp.Body.Close()

	var scopes []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&scopes); err != nil {
		return fmt.Errorf("failed to decode client scopes: %w", err)
	}

	// Check if scope already exists
	scopeExists := false
	for _, scope := range scopes {
		if name, ok := scope["name"].(string); ok && name == scopeName {
			scopeExists = true
			logger.Info("Client scope already exists", "scope", scopeName, "realm", realmName)
			break
		}
	}

	if !scopeExists {
		// Create the client scope
		scopeConfig := map[string]interface{}{
			"name":        scopeName,
			"description": "Read-only access to Trustify documents (advisories, SBOMs, etc.)",
			"protocol":    "openid-connect",
			"attributes": map[string]interface{}{
				"include.in.token.scope":    "true",
				"display.on.consent.screen": "false",
			},
		}

		scopeJSON, _ := json.Marshal(scopeConfig)
		req, _ = http.NewRequest("POST", scopesURL, bytes.NewReader(scopeJSON))
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

		logger.Info("Created read-only client scope", "scope", scopeName, "realm", realmName)
	}

	return nil
}

func (r *DisconnectedPlatformReconciler) ensureTrustifyCreateScope(ctx context.Context, keycloakHost, realmName, adminToken string, httpClient *http.Client) error {
	logger := log.FromContext(ctx)

	// Create the create:document client scope for write access to Trustify APIs (SBOM uploads)
	scopeName := "create:document"

	// Check if scope already exists
	scopesURL := fmt.Sprintf("https://%s/admin/realms/%s/client-scopes", keycloakHost, realmName)
	req, _ := http.NewRequest("GET", scopesURL, nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to list client scopes: %w", err)
	}
	defer resp.Body.Close()

	var scopes []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&scopes); err != nil {
		return fmt.Errorf("failed to decode client scopes: %w", err)
	}

	// Check if scope already exists
	scopeExists := false
	for _, scope := range scopes {
		if name, ok := scope["name"].(string); ok && name == scopeName {
			scopeExists = true
			logger.Info("Client scope already exists", "scope", scopeName, "realm", realmName)
			break
		}
	}

	if !scopeExists {
		// Create the client scope
		scopeConfig := map[string]interface{}{
			"name":        scopeName,
			"description": "Write access to Trustify documents (create SBOMs, advisories, etc.)",
			"protocol":    "openid-connect",
			"attributes": map[string]interface{}{
				"include.in.token.scope":    "true",
				"display.on.consent.screen": "false",
			},
		}

		scopeJSON, _ := json.Marshal(scopeConfig)
		req, _ = http.NewRequest("POST", scopesURL, bytes.NewReader(scopeJSON))
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

		logger.Info("Created create:document client scope", "scope", scopeName, "realm", realmName)
	}

	return nil
}

func (r *DisconnectedPlatformReconciler) ensureTrustifyOIDCClient(ctx context.Context, keycloakHost, realmName, clientID, appDomain, adminToken string, httpClient *http.Client, isPublic bool) error {
	logger := log.FromContext(ctx)

	// Find RHTPA server hostname by looking for Ingress owned by TrustedProfileAnalyzer
	var actualAppDomain string
	tpaList := &unstructured.UnstructuredList{}
	tpaList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "charts.trustification.dev",
		Version: "v1alpha1",
		Kind:    "TrustedProfileAnalyzerList",
	})
	if err := r.List(ctx, tpaList); err == nil && len(tpaList.Items) > 0 {
		tpa := tpaList.Items[0]
		tpaUID := tpa.GetUID()

		// Find Ingresses owned by this TPA
		ingressList := &unstructured.UnstructuredList{}
		ingressList.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "networking.k8s.io",
			Version: "v1",
			Kind:    "IngressList",
		})
		if err := r.List(ctx, ingressList, client.InNamespace(tpa.GetNamespace())); err == nil {
			for _, ingress := range ingressList.Items {
				for _, owner := range ingress.GetOwnerReferences() {
					if owner.UID == tpaUID {
						// Found the Ingress owned by TPA
						if rules, found, _ := unstructured.NestedSlice(ingress.Object, "spec", "rules"); found && len(rules) > 0 {
							if rule, ok := rules[0].(map[string]interface{}); ok {
								if host, ok := rule["host"].(string); ok && host != "" {
									actualAppDomain = host
									logger.Info("Found RHTPA server hostname from Ingress", "hostname", actualAppDomain, "client", clientID)
									break
								}
							}
						}
					}
				}
				if actualAppDomain != "" {
					break
				}
			}
		}
	}

	// Fall back to the passed appDomain if we couldn't find the Ingress
	if actualAppDomain == "" {
		actualAppDomain = appDomain
		logger.Info("Using fallback appDomain for RHTPA", "appDomain", actualAppDomain, "client", clientID)
	}

	// Check if client exists
	clientsURL := fmt.Sprintf("https://%s/admin/realms/%s/clients?clientId=%s", keycloakHost, realmName, clientID)
	req, _ := http.NewRequest("GET", clientsURL, nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to check client: %w", err)
	}
	defer resp.Body.Close()

	var clients []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&clients); err != nil {
		return fmt.Errorf("failed to decode clients response: %w", err)
	}

	var clientUUID string
	var clientSecret string

	if len(clients) > 0 {
		// Client already exists - update its configuration
		logger.Info("OIDC client already exists, updating configuration", "clientId", clientID, "public", isPublic)
		clientUUID = clients[0]["id"].(string)

		// Update redirect URIs and client type
		updateURL := fmt.Sprintf("https://%s/admin/realms/%s/clients/%s", keycloakHost, realmName, clientUUID)
		updateData := map[string]interface{}{
			"redirectUris": []string{fmt.Sprintf("https://%s/*", actualAppDomain)},
			"webOrigins":   []string{"*"},
			"publicClient": isPublic,
		}
		updateJSON, _ := json.Marshal(updateData)
		req, _ = http.NewRequest("PUT", updateURL, bytes.NewReader(updateJSON))
		req.Header.Set("Authorization", "Bearer "+adminToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err = httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to update client: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			body, _ := io.ReadAll(resp.Body)
			logger.Error(fmt.Errorf("unexpected status"), "failed to update client configuration", "status", resp.StatusCode, "body", string(body))
		}

		// For confidential clients, get the client secret
		if !isPublic {
			secretURL := fmt.Sprintf("https://%s/admin/realms/%s/clients/%s/client-secret", keycloakHost, realmName, clientUUID)
			secretReq, _ := http.NewRequest("GET", secretURL, nil)
			secretReq.Header.Set("Authorization", "Bearer "+adminToken)

			secretResp, err := httpClient.Do(secretReq)
			if err == nil {
				defer secretResp.Body.Close()
				var secretData map[string]interface{}
				if json.NewDecoder(secretResp.Body).Decode(&secretData) == nil {
					if secret, ok := secretData["value"].(string); ok {
						clientSecret = secret
					}
				}
			}
		}
	} else {
		// Create OIDC client
		logger.Info("Creating TPA OIDC client", "clientId", clientID, "public", isPublic)

		clientConfig := map[string]interface{}{
			"clientId":                  clientID,
			"enabled":                   true,
			"protocol":                  "openid-connect",
			"publicClient":              isPublic,
			"standardFlowEnabled":       true,
			"directAccessGrantsEnabled": !isPublic, // Only confidential clients need this
			"serviceAccountsEnabled":    !isPublic, // Only confidential clients can be service accounts
			"redirectUris":              []string{fmt.Sprintf("https://%s/*", actualAppDomain)},
			"webOrigins":                []string{"*"},
		}

		// Generate secret for confidential clients
		if !isPublic {
			clientSecret = generateRandomString(32)
			clientConfig["secret"] = clientSecret
		}

		clientJSON, _ := json.Marshal(clientConfig)
		req, _ = http.NewRequest("POST", fmt.Sprintf("https://%s/admin/realms/%s/clients", keycloakHost, realmName), bytes.NewReader(clientJSON))
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

		if isPublic {
			logger.Info("Successfully created TPA OIDC public client", "clientId", clientID)
		} else {
			logger.Info("Successfully created TPA OIDC confidential client", "clientId", clientID)
		}

		// Get the client UUID for newly created client
		clientsURL := fmt.Sprintf("https://%s/admin/realms/%s/clients?clientId=%s", keycloakHost, realmName, clientID)
		req, _ = http.NewRequest("GET", clientsURL, nil)
		req.Header.Set("Authorization", "Bearer "+adminToken)

		resp, err = httpClient.Do(req)
		if err == nil {
			defer resp.Body.Close()
			var clients []map[string]interface{}
			if json.NewDecoder(resp.Body).Decode(&clients) == nil && len(clients) > 0 {
				clientUUID = clients[0]["id"].(string)
			}
		}
	}

	// Assign read:document scope to the client
	if clientUUID != "" {
		if err := r.assignReadScopeToClient(ctx, keycloakHost, realmName, clientUUID, adminToken, httpClient); err != nil {
			logger.Error(err, "failed to assign read scope to client", "clientId", clientID)
		}
	}

	// Store client secret if this is a confidential client (CLI or SBOM uploader)
	if !isPublic && clientSecret != "" {
		var secretName string
		switch clientID {
		case "cli":
			secretName = "rhtpa-oidc-cli-secret"
		case "sbom-uploader":
			secretName = "sbom-uploader-secret"
		default:
			// Don't store secrets for other clients
			return nil
		}

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: architectNamespace,
			},
			StringData: map[string]string{
				"clientSecret": clientSecret,
			},
		}

		existingSecret := &corev1.Secret{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(secret), existingSecret); apierrors.IsNotFound(err) {
			if err := r.Create(ctx, secret); err != nil {
				return fmt.Errorf("failed to create %s client secret: %w", clientID, err)
			}
			logger.Info("Created client secret", "clientId", clientID, "secretName", secret.Name)
		} else if err == nil {
			// Update existing secret
			existingSecret.StringData = secret.StringData
			if err := r.Update(ctx, existingSecret); err != nil {
				return fmt.Errorf("failed to update %s client secret: %w", clientID, err)
			}
			logger.Info("Updated client secret", "clientId", clientID, "secretName", secret.Name)
		}
	}

	return nil
}

func (r *DisconnectedPlatformReconciler) assignScopeToTrustifyClients(ctx context.Context, keycloakHost, realmName, adminToken string, httpClient *http.Client) error {
	logger := log.FromContext(ctx)

	// Assign read:document scope to frontend and CLI clients
	// Assign both read:document and create:document to sbom-uploader for pipeline uploads
	for _, clientID := range []string{"frontend", "cli", "sbom-uploader"} {
		// Get client UUID
		clientsURL := fmt.Sprintf("https://%s/admin/realms/%s/clients?clientId=%s", keycloakHost, realmName, clientID)
		req, _ := http.NewRequest("GET", clientsURL, nil)
		req.Header.Set("Authorization", "Bearer "+adminToken)

		resp, err := httpClient.Do(req)
		if err != nil {
			logger.Error(err, "failed to get client UUID", "clientId", clientID)
			continue
		}
		defer resp.Body.Close()

		var clients []map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&clients); err != nil {
			logger.Error(err, "failed to decode clients response", "clientId", clientID)
			continue
		}

		if len(clients) == 0 {
			logger.Info("Client not found", "clientId", clientID)
			continue
		}

		clientUUID := clients[0]["id"].(string)

		// Assign read:document scope to all clients
		if err := r.assignReadScopeToClient(ctx, keycloakHost, realmName, clientUUID, adminToken, httpClient); err != nil {
			logger.Error(err, "failed to assign read scope", "clientId", clientID)
		} else {
			logger.Info("Assigned read:document scope to client", "clientId", clientID)
		}

		// Additionally assign create:document scope to sbom-uploader for SBOM uploads
		if clientID == "sbom-uploader" {
			if err := r.assignScopeToClient(ctx, keycloakHost, realmName, clientUUID, "create:document", adminToken, httpClient); err != nil {
				logger.Error(err, "failed to assign create scope", "clientId", clientID)
			} else {
				logger.Info("Assigned create:document scope to client", "clientId", clientID)
			}
		}

		// For CLI client, assign trustify-manager role to its service account for create permissions
		if clientID == "cli" {
			serviceAccountUsername := "service-account-" + clientID
			if err := r.assignRoleToServiceAccount(ctx, keycloakHost, realmName, serviceAccountUsername, "trustify-manager", adminToken, httpClient); err != nil {
				logger.Error(err, "failed to assign trustify-manager role to service account", "clientId", clientID, "username", serviceAccountUsername)
			} else {
				logger.Info("Assigned trustify-manager role to service account", "clientId", clientID, "username", serviceAccountUsername)
			}
		}
	}

	return nil
}

func (r *DisconnectedPlatformReconciler) assignReadScopeToClient(ctx context.Context, keycloakHost, realmName, clientUUID, adminToken string, httpClient *http.Client) error {
	return r.assignScopeToClient(ctx, keycloakHost, realmName, clientUUID, "read:document", adminToken, httpClient)
}

func (r *DisconnectedPlatformReconciler) assignScopeToClient(ctx context.Context, keycloakHost, realmName, clientUUID, scopeName, adminToken string, httpClient *http.Client) error {
	logger := log.FromContext(ctx)

	// Get the scope UUID
	scopeListURL := fmt.Sprintf("https://%s/admin/realms/%s/client-scopes", keycloakHost, realmName)
	scopeReq, _ := http.NewRequest("GET", scopeListURL, nil)
	scopeReq.Header.Set("Authorization", "Bearer "+adminToken)
	scopeReq.Header.Set("Content-Type", "application/json")

	scopeResp, err := httpClient.Do(scopeReq)
	if err != nil {
		return fmt.Errorf("failed to list client scopes: %w", err)
	}
	defer scopeResp.Body.Close()

	if scopeResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(scopeResp.Body)
		return fmt.Errorf("failed to list client scopes: %s - %s", scopeResp.Status, string(body))
	}

	var scopes []map[string]interface{}
	if err := json.NewDecoder(scopeResp.Body).Decode(&scopes); err != nil {
		return fmt.Errorf("failed to decode client scopes response: %w", err)
	}

	// Find scope UUID
	var scopeUUID string
	for _, scope := range scopes {
		if scope["name"] == scopeName {
			scopeUUID = scope["id"].(string)
			break
		}
	}

	if scopeUUID == "" {
		return fmt.Errorf("%s scope not found", scopeName)
	}

	// Assign scope as default client scope
	assignURL := fmt.Sprintf("https://%s/admin/realms/%s/clients/%s/default-client-scopes/%s", keycloakHost, realmName, clientUUID, scopeUUID)
	assignReq, _ := http.NewRequest("PUT", assignURL, nil)
	assignReq.Header.Set("Authorization", "Bearer "+adminToken)
	assignReq.Header.Set("Content-Type", "application/json")

	assignResp, err := httpClient.Do(assignReq)
	if err != nil {
		return fmt.Errorf("failed to assign scope to client: %w", err)
	}
	defer assignResp.Body.Close()

	if assignResp.StatusCode != http.StatusNoContent && assignResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(assignResp.Body)
		return fmt.Errorf("failed to assign scope to client: %s - %s", assignResp.Status, string(body))
	}

	logger.Info("Assigned read:document scope to client", "clientUUID", clientUUID)
	return nil
}

func (r *DisconnectedPlatformReconciler) assignRoleToServiceAccount(ctx context.Context, keycloakHost, realmName, username, roleName, adminToken string, httpClient *http.Client) error {
	logger := log.FromContext(ctx)

	// Get the user UUID for the service account
	usersURL := fmt.Sprintf("https://%s/admin/realms/%s/users?username=%s&exact=true", keycloakHost, realmName, username)
	userReq, _ := http.NewRequest("GET", usersURL, nil)
	userReq.Header.Set("Authorization", "Bearer "+adminToken)

	userResp, err := httpClient.Do(userReq)
	if err != nil {
		return fmt.Errorf("failed to get service account user: %w", err)
	}
	defer userResp.Body.Close()

	var users []map[string]interface{}
	if err := json.NewDecoder(userResp.Body).Decode(&users); err != nil {
		return fmt.Errorf("failed to decode users response: %w", err)
	}

	if len(users) == 0 {
		return fmt.Errorf("service account user not found: %s", username)
	}

	userUUID := users[0]["id"].(string)

	// Get the role UUID
	rolesURL := fmt.Sprintf("https://%s/admin/realms/%s/roles/%s", keycloakHost, realmName, roleName)
	roleReq, _ := http.NewRequest("GET", rolesURL, nil)
	roleReq.Header.Set("Authorization", "Bearer "+adminToken)

	roleResp, err := httpClient.Do(roleReq)
	if err != nil {
		return fmt.Errorf("failed to get role: %w", err)
	}
	defer roleResp.Body.Close()

	if roleResp.StatusCode == 404 {
		return fmt.Errorf("role not found: %s", roleName)
	}

	var role map[string]interface{}
	if err := json.NewDecoder(roleResp.Body).Decode(&role); err != nil {
		return fmt.Errorf("failed to decode role response: %w", err)
	}

	roleUUID := role["id"].(string)

	// Assign the role to the user
	roleMapping := []map[string]interface{}{
		{
			"id":   roleUUID,
			"name": roleName,
		},
	}

	roleMappingJSON, _ := json.Marshal(roleMapping)
	assignURL := fmt.Sprintf("https://%s/admin/realms/%s/users/%s/role-mappings/realm", keycloakHost, realmName, userUUID)
	assignReq, _ := http.NewRequest("POST", assignURL, bytes.NewReader(roleMappingJSON))
	assignReq.Header.Set("Authorization", "Bearer "+adminToken)
	assignReq.Header.Set("Content-Type", "application/json")

	assignResp, err := httpClient.Do(assignReq)
	if err != nil {
		return fmt.Errorf("failed to assign role: %w", err)
	}
	defer assignResp.Body.Close()

	if assignResp.StatusCode != http.StatusNoContent && assignResp.StatusCode != http.StatusOK && assignResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(assignResp.Body)
		// If status is 409, the role is already assigned - that's fine
		if assignResp.StatusCode == 409 {
			logger.Info("Role already assigned to service account", "username", username, "role", roleName)
			return nil
		}
		return fmt.Errorf("failed to assign role: %s - %s", assignResp.Status, string(body))
	}

	logger.Info("Assigned role to service account", "username", username, "role", roleName)
	return nil
}

func (r *DisconnectedPlatformReconciler) ensureRHTPAPostgreSQL(ctx context.Context) (string, string, string, string, error) {
	// Create PostgreSQL StatefulSet for TPA persistence
	// Returns: host, dbname, username, password, error

	dbName := "rhtpadb"
	dbUser := "rhtpa"
	dbPassword := "rhtpa-db-password-" + generateRandomString(16)
	serviceName := "rhtpa-postgresql"
	pvcName := "rhtpa-postgresql-data"

	// Create database credentials secret
	dbSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rhtpa-db-credentials",
			Namespace: architectNamespace,
		},
		StringData: map[string]string{
			"username": dbUser,
			"password": dbPassword,
			"database": dbName,
		},
	}
	existingSecret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(dbSecret), existingSecret); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, dbSecret); err != nil {
			return "", "", "", "", fmt.Errorf("failed to create TPA database secret: %w", err)
		}
	} else if err == nil {
		// Secret exists, read password from it
		dbPassword = string(existingSecret.Data["password"])
	}

	// Create PVC
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: architectNamespace,
			Labels: map[string]string{
				"app": "rhtpa-postgresql",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("10Gi"),
				},
			},
		},
	}
	existingPVC := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pvc), existingPVC); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, pvc); err != nil {
			return "", "", "", "", fmt.Errorf("failed to create TPA PostgreSQL PVC: %w", err)
		}
	}

	// Create PostgreSQL StatefulSet
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rhtpa-postgresql",
			Namespace: architectNamespace,
			Labels: map[string]string{
				"app": "rhtpa-postgresql",
			},
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: serviceName,
			Replicas:    func() *int32 { r := int32(1); return &r }(),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "rhtpa-postgresql",
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": "rhtpa-postgresql",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "postgresql",
							Image: "registry.redhat.io/rhel9/postgresql-15:latest",
							Env: []corev1.EnvVar{
								{Name: "POSTGRESQL_USER", Value: dbUser},
								{Name: "POSTGRESQL_PASSWORD", Value: dbPassword},
								{Name: "POSTGRESQL_DATABASE", Value: dbName},
								{Name: "POSTGRESQL_ADMIN_PASSWORD", Value: "postgres"},
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
			return "", "", "", "", fmt.Errorf("failed to create TPA PostgreSQL StatefulSet: %w", err)
		}
		log.FromContext(ctx).Info("Created TPA PostgreSQL StatefulSet")
	}

	// Create PostgreSQL Service
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: architectNamespace,
			Labels: map[string]string{
				"app": "rhtpa-postgresql",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app": "rhtpa-postgresql",
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
			return "", "", "", "", fmt.Errorf("failed to create TPA PostgreSQL Service: %w", err)
		}
	}

	dbHost := fmt.Sprintf("%s.%s.svc", serviceName, architectNamespace)
	return dbHost, dbName, dbUser, dbPassword, nil
}

func (r *DisconnectedPlatformReconciler) checkKeycloakHealth(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	cfg := platform.Spec.Connected.RHTAS
	if cfg == nil || cfg.OIDC == nil || cfg.OIDC.Managed == nil || !cfg.OIDC.Managed.Enabled {
		return nil
	}

	// Check if Keycloak resource exists and is ready
	kc := &unstructured.Unstructured{}
	kc.SetGroupVersionKind(keycloakGVK)
	kc.SetName("mirror-operator-keycloak")
	kc.SetNamespace(architectNamespace)

	if err := r.Get(ctx, client.ObjectKeyFromObject(kc), kc); err != nil {
		return fmt.Errorf("Keycloak resource not found: %w", err)
	}

	// Check Keycloak status conditions
	status, found, _ := unstructured.NestedMap(kc.Object, "status")
	if !found {
		return fmt.Errorf("Keycloak status not available yet")
	}

	conditions, found, _ := unstructured.NestedSlice(status, "conditions")
	if !found {
		return fmt.Errorf("Keycloak conditions not available yet")
	}

	// Look for Ready condition
	for _, cond := range conditions {
		condMap, ok := cond.(map[string]interface{})
		if !ok {
			continue
		}
		condType, _, _ := unstructured.NestedString(condMap, "type")
		condStatus, _, _ := unstructured.NestedString(condMap, "status")
		if condType == "Ready" {
			if condStatus == "True" {
				return nil
			}
			message, _, _ := unstructured.NestedString(condMap, "message")
			return fmt.Errorf("Keycloak not ready: %s", message)
		}
	}

	return fmt.Errorf("Keycloak Ready condition not found")
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

	// Determine database configuration for Keycloak
	// Note: cfg.Database is for RHTAS/Securesign, not Keycloak
	// Keycloak always uses managed PostgreSQL unless a separate database config is added to the API
	var dbHost, dbName, dbUsername, dbPassword string
	var dbPort int

	// Deploy managed PostgreSQL StatefulSet for Keycloak
	if err := r.ensureManagedPostgreSQL(ctx, platform); err != nil {
		return fmt.Errorf("failed to ensure managed PostgreSQL: %w", err)
	}
	dbHost = "keycloak-postgresql.mirror-operator-system.svc"
	dbName = "keycloak"
	dbPort = 5432
	dbUsername = "keycloak"
	dbPassword = "keycloak-admin-password" // Generated password stored in secret

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
		// Update existing Keycloak to ensure correct database config
		kc.Object["spec"] = kcSpec
		if err := r.Update(ctx, kc); err != nil {
			return fmt.Errorf("failed to update Keycloak: %w", err)
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

	// Configure OpenShift OAuth as identity provider for all realms
	if err := r.configureOpenShiftOAuth(ctx, hostname); err != nil {
		log.FromContext(ctx).Error(err, "failed to configure OpenShift OAuth")
		// Don't fail reconciliation, just log the error
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
	hasAudienceMapper := false
	for _, mapper := range mappers {
		if name, ok := mapper["name"].(string); ok {
			if name == "email-mapper" {
				hasEmailMapper = true
			}
			if name == "email-verified-mapper" {
				hasEmailVerifiedMapper = true
			}
			if name == "audience-mapper" {
				hasAudienceMapper = true
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

	// Add audience protocol mapper if missing
	if !hasAudienceMapper {
		mapperConfig := map[string]interface{}{
			"name":            "audience-mapper",
			"protocol":        "openid-connect",
			"protocolMapper":  "oidc-audience-mapper",
			"consentRequired": false,
			"config": map[string]interface{}{
				"included.client.audience": "trusted-artifact-signer",
				"id.token.claim":           "false",
				"access.token.claim":       "true",
			},
		}

		mapperJSON, _ := json.Marshal(mapperConfig)
		req, _ = http.NewRequest("POST", mappersURL, bytes.NewBuffer(mapperJSON))
		req.Header.Set("Authorization", "Bearer "+adminToken)
		req.Header.Set("Content-Type", "application/json")

		mapperResp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to create audience protocol mapper: %w", err)
		}
		defer mapperResp.Body.Close()

		if mapperResp.StatusCode != http.StatusCreated && mapperResp.StatusCode != http.StatusNoContent {
			body, _ := io.ReadAll(mapperResp.Body)
			return fmt.Errorf("failed to create audience protocol mapper: status %d, body: %s", mapperResp.StatusCode, string(body))
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

// reconcileQuayConfig creates and manages a Quay registry for intermediate image storage
func (r *DisconnectedPlatformReconciler) reconcileQuayConfig(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)

	if platform.Spec.Connected == nil || platform.Spec.Connected.Quay == nil {
		return nil
	}

	quayConfig := platform.Spec.Connected.Quay

	// If using external Quay, just update mirrorRegistry field
	if quayConfig.ExternalURL != "" {
		if platform.Spec.Connected.MirrorRegistry != quayConfig.ExternalURL {
			platform.Spec.Connected.MirrorRegistry = quayConfig.ExternalURL
			logger.Info("Using external Quay registry", "url", quayConfig.ExternalURL)
		}
		return nil
	}

	// Deploy managed Quay instance
	if quayConfig.Managed != nil && quayConfig.Managed.Enabled {
		// Check if QuayRegistry CRD exists
		quayRegistry := &unstructured.Unstructured{}
		quayRegistry.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "quay.redhat.com",
			Version: "v1",
			Kind:    "QuayRegistry",
		})
		quayRegistry.SetName("mirror-operator-quay")
		quayRegistry.SetNamespace(architectNamespace)

		// Check if QuayRegistry already exists
		err := r.Get(ctx, client.ObjectKeyFromObject(quayRegistry), quayRegistry)
		if err == nil {
			// QuayRegistry exists, get its hostname and update mirrorRegistry
			hostname, err := r.getQuayHostname(ctx, quayRegistry)
			if err != nil {
				logger.Error(err, "failed to get Quay hostname")
				return err
			}

			if hostname != "" && platform.Spec.Connected.MirrorRegistry != hostname+"/mirror" {
				newRegistry := hostname + "/mirror"
				logger.Info("Updated mirrorRegistry from managed Quay", "registry", newRegistry)

				// Refetch to get latest version before updating
				latest := &mirrorv1.DisconnectedPlatform{}
				if err := r.Get(ctx, client.ObjectKeyFromObject(platform), latest); err != nil {
					return fmt.Errorf("failed to refetch platform for mirrorRegistry update: %w", err)
				}

				latest.Spec.Connected.MirrorRegistry = newRegistry
				if err := r.Update(ctx, latest); err != nil {
					return fmt.Errorf("failed to update mirrorRegistry: %w", err)
				}

				// Update our local copy too
				platform.Spec.Connected.MirrorRegistry = newRegistry
			}

			// Configure Clair VEX if needed for existing QuayRegistry
			if quayConfig.Managed.Clair != nil && quayConfig.Managed.Clair.UseRedHatVEXOnly {
				if err := r.configureClairVEX(ctx, quayRegistry); err != nil {
					logger.Error(err, "failed to configure Clair VEX")
					return err
				}
			}

			return nil
		} else if !apierrors.IsNotFound(err) {
			return err
		}

		// Create new QuayRegistry
		logger.Info("Creating managed Quay registry")

		orgName := quayConfig.Managed.OrganizationName
		if orgName == "" {
			orgName = "mirror"
		}

		// Determine if using S3 storage
		useS3Storage := quayConfig.Managed.Storage != nil && quayConfig.Managed.Storage.Type == "s3"

		// Build QuayRegistry spec
		// For filesystem storage: use managed objectstorage (requires ObjectBucketClaim CRD)
		// For S3 storage: use unmanaged objectstorage with config bundle secret
		objectStorageManaged := !useS3Storage

		components := []interface{}{
			map[string]interface{}{
				"kind":    "clair",
				"managed": true,
			},
			map[string]interface{}{
				"kind":    "postgres",
				"managed": true,
			},
			map[string]interface{}{
				"kind":    "objectstorage",
				"managed": objectStorageManaged,
			},
			map[string]interface{}{
				"kind":    "redis",
				"managed": true,
			},
			map[string]interface{}{
				"kind":    "horizontalpodautoscaler",
				"managed": true,
			},
			map[string]interface{}{
				"kind":    "route",
				"managed": true,
			},
			map[string]interface{}{
				"kind":    "mirror",
				"managed": true,
			},
			map[string]interface{}{
				"kind":    "tls",
				"managed": true,
			},
			map[string]interface{}{
				"kind":    "quay",
				"managed": true,
			},
		}

		if err := unstructured.SetNestedSlice(quayRegistry.Object, components, "spec", "components"); err != nil {
			return fmt.Errorf("failed to set QuayRegistry components: %w", err)
		}

		// If using S3 storage, set the configBundleSecret reference
		if useS3Storage {
			// Set the configBundleSecret reference
			if err := unstructured.SetNestedField(quayRegistry.Object, quayRegistry.GetName()+"-config-bundle", "spec", "configBundleSecret"); err != nil {
				return fmt.Errorf("failed to set configBundleSecret: %w", err)
			}
		}

		// Create QuayRegistry first
		if err := r.Create(ctx, quayRegistry); err != nil {
			return fmt.Errorf("failed to create QuayRegistry: %w", err)
		}

		logger.Info("Created managed Quay registry", "name", quayRegistry.GetName(), "s3Storage", useS3Storage)

		// If using S3 storage, create config bundle secret with S3 credentials after QuayRegistry is created
		if useS3Storage {
			if err := r.createQuayS3ConfigSecret(ctx, quayRegistry, quayConfig.Managed.Storage); err != nil {
				logger.Error(err, "failed to create Quay S3 config secret, will retry on next reconciliation")
				// Don't return error - let it retry on next reconciliation when QuayRegistry has UID
			}
		}

		// Configure Clair if requested
		if quayConfig.Managed.Clair != nil && quayConfig.Managed.Clair.UseRedHatVEXOnly {
			if err := r.configureClairVEX(ctx, quayRegistry); err != nil {
				logger.Error(err, "failed to configure Clair VEX, will retry on next reconciliation")
			}
		}

		// Wait for Quay to be ready and get hostname
		// Note: This will be updated in subsequent reconciliations
		return nil
	}

	return nil
}

// createQuayS3ConfigSecret creates a config bundle secret for Quay with S3 storage configuration
func (r *DisconnectedPlatformReconciler) createQuayS3ConfigSecret(ctx context.Context, quayRegistry *unstructured.Unstructured, storage *mirrorv1.QuayStorageConfig) error {
	logger := log.FromContext(ctx)

	// Parse endpoint to extract hostname and port
	endpoint := storage.S3Endpoint
	hostname := endpoint
	port := 443
	isSecure := true

	// Check if endpoint includes port (e.g., "minio.svc:9000")
	if strings.Contains(endpoint, ":") {
		parts := strings.Split(endpoint, ":")
		hostname = parts[0]
		if len(parts) == 2 {
			// Parse port
			if p, err := strconv.Atoi(parts[1]); err == nil {
				port = p
				// If port is not 443, assume not using TLS
				if port != 443 {
					isSecure = false
				}
			}
		}
	}

	// Quay config.yaml structure for S3 storage
	quayConfig := map[string]interface{}{
		"DISTRIBUTED_STORAGE_CONFIG": map[string]interface{}{
			"default": []interface{}{
				"RadosGWStorage",
				map[string]interface{}{
					"hostname":     hostname,
					"is_secure":    isSecure,
					"port":         port,
					"bucket_name":  storage.S3Bucket,
					"access_key":   storage.S3AccessKey,
					"secret_key":   storage.S3SecretKey,
					"storage_path": "/datastorage/registry",
				},
			},
		},
		"DISTRIBUTED_STORAGE_DEFAULT_LOCATIONS": []interface{}{},
		"DISTRIBUTED_STORAGE_PREFERENCE": []interface{}{
			"default",
		},
	}

	// Convert to YAML
	configYAML, err := yaml.Marshal(quayConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal Quay config: %w", err)
	}

	// Create secret
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      quayRegistry.GetName() + "-config-bundle",
			Namespace: quayRegistry.GetNamespace(),
		},
		Data: map[string][]byte{
			"config.yaml": configYAML,
		},
		Type: corev1.SecretTypeOpaque,
	}

	// Set owner reference if QuayRegistry has UID (already created)
	if quayRegistry.GetUID() != "" {
		secret.SetOwnerReferences([]metav1.OwnerReference{
			{
				APIVersion: quayRegistry.GetAPIVersion(),
				Kind:       quayRegistry.GetKind(),
				Name:       quayRegistry.GetName(),
				UID:        quayRegistry.GetUID(),
				Controller: func() *bool { b := true; return &b }(),
			},
		})
	}

	if err := r.Create(ctx, secret); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		logger.Info("Config bundle secret already exists", "secret", secret.Name)
	} else {
		logger.Info("Created Quay S3 config bundle secret", "secret", secret.Name)
	}

	return nil
}

// configureClairVEX configures Clair to use only Red Hat VEX data for vulnerability scanning
func (r *DisconnectedPlatformReconciler) configureClairVEX(ctx context.Context, quayRegistry *unstructured.Unstructured) error {
	logger := log.FromContext(ctx)

	// Find the Clair deployment
	deployment := &appsv1.Deployment{}
	deploymentName := quayRegistry.GetName() + "-clair-app"
	if err := r.Get(ctx, client.ObjectKey{Name: deploymentName, Namespace: quayRegistry.GetNamespace()}, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Clair deployment not found yet, will configure on next reconciliation", "deployment", deploymentName)
			return nil
		}
		return fmt.Errorf("failed to get Clair deployment: %w", err)
	}

	// Get the current Clair config secret name from the deployment
	var currentConfigSecret string
	for _, vol := range deployment.Spec.Template.Spec.Volumes {
		if vol.Name == "config" && vol.Secret != nil {
			currentConfigSecret = vol.Secret.SecretName
			break
		}
	}

	if currentConfigSecret == "" {
		return fmt.Errorf("could not find config volume in Clair deployment")
	}

	// Read the current config
	currentSecret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Name: currentConfigSecret, Namespace: quayRegistry.GetNamespace()}, currentSecret); err != nil {
		return fmt.Errorf("failed to get current Clair config secret: %w", err)
	}

	// Parse the current config to extract key values
	clairConfig := string(currentSecret.Data["config.yaml"])

	// Extract PSK key and connection strings
	pskKey := extractPSKKey(clairConfig)
	connString := extractValue(clairConfig, "connstring")
	webhookTarget := extractValue(clairConfig, "target")

	// Create VEX-only configuration
	vexConfig := fmt.Sprintf(`auth:
    psk:
        iss:
            - quay
            - clairctl
        key: %s
http_listen_addr: :8080
indexer:
    connstring: %s
    layer_scan_concurrency: 5
    migrations: true
    scanlock_retry: 10
log_level: info
matcher:
    connstring: %s
    migrations: true
metrics:
    name: prometheus
notifier:
    connstring: %s
    delivery_interval: 1m0s
    migrations: true
    poll_interval: 5m0s
    webhook:
        callback: http://mirror-operator-quay-clair-app/notifier/api/v1/notifications
        target: %s
updaters:
    sets:
        - rhcc
    config:
        rhcc:
            url: https://access.redhat.com/security/data/csaf/v2/vex/
            vex: true
`, pskKey, connString, connString, connString, webhookTarget)

	// Create or update the custom config secret
	customSecretName := quayRegistry.GetName() + "-clair-vex-config"
	customSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      customSecretName,
			Namespace: quayRegistry.GetNamespace(),
		},
		StringData: map[string]string{
			"config.yaml": vexConfig,
		},
	}

	// Set owner reference if QuayRegistry has UID
	if quayRegistry.GetUID() != "" {
		customSecret.SetOwnerReferences([]metav1.OwnerReference{
			{
				APIVersion: quayRegistry.GetAPIVersion(),
				Kind:       quayRegistry.GetKind(),
				Name:       quayRegistry.GetName(),
				UID:        quayRegistry.GetUID(),
				Controller: func() *bool { b := true; return &b }(),
			},
		})
	}

	if err := r.Create(ctx, customSecret); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Update existing secret
			existing := &corev1.Secret{}
			if err := r.Get(ctx, client.ObjectKey{Name: customSecretName, Namespace: quayRegistry.GetNamespace()}, existing); err != nil {
				return err
			}
			existing.StringData = customSecret.StringData
			if err := r.Update(ctx, existing); err != nil {
				return err
			}
			logger.Info("Updated Clair VEX config secret", "secret", customSecretName)
		} else {
			return err
		}
	} else {
		logger.Info("Created Clair VEX config secret", "secret", customSecretName)
	}

	// Update the deployment to use the custom config secret (if not already using it)
	if currentConfigSecret != customSecretName {
		for i, vol := range deployment.Spec.Template.Spec.Volumes {
			if vol.Name == "config" {
				deployment.Spec.Template.Spec.Volumes[i].Secret.SecretName = customSecretName
				break
			}
		}

		if err := r.Update(ctx, deployment); err != nil {
			return fmt.Errorf("failed to update Clair deployment with VEX config: %w", err)
		}
		logger.Info("Updated Clair deployment to use VEX-only configuration", "deployment", deploymentName)
	}

	return nil
}

// Helper functions to extract values from existing Clair config
func extractPSKKey(config string) string {
	lines := strings.Split(config, "\n")
	for _, line := range lines {
		if strings.Contains(line, "key:") {
			return strings.TrimSpace(strings.Split(line, "key:")[1])
		}
	}
	return ""
}

func extractValue(config, key string) string {
	lines := strings.Split(config, "\n")
	for _, line := range lines {
		if strings.Contains(line, key+":") {
			parts := strings.Split(line, key+":")
			if len(parts) >= 2 {
				return strings.TrimSpace(parts[1])
			}
		}
	}
	return ""
}

// getQuayHostname extracts the hostname from a QuayRegistry resource
func (r *DisconnectedPlatformReconciler) getQuayHostname(ctx context.Context, quayRegistry *unstructured.Unstructured) (string, error) {
	// Get hostname from QuayRegistry status
	hostname, found, err := unstructured.NestedString(quayRegistry.Object, "status", "registryEndpoint")
	if err != nil {
		return "", err
	}
	if !found || hostname == "" {
		// Try getting from route
		route := &unstructured.Unstructured{}
		route.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "route.openshift.io",
			Version: "v1",
			Kind:    "Route",
		})
		route.SetName(quayRegistry.GetName() + "-quay")
		route.SetNamespace(quayRegistry.GetNamespace())

		if err := r.Get(ctx, client.ObjectKeyFromObject(route), route); err != nil {
			return "", err
		}

		hostname, _, _ = unstructured.NestedString(route.Object, "spec", "host")
	}

	// Remove protocol if present
	hostname = strings.TrimPrefix(hostname, "https://")
	hostname = strings.TrimPrefix(hostname, "http://")

	return hostname, nil
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

	// Only update RHTASRootKeys if the keys changed
	newFulcioHash := hashString(fulcioRootPEM)
	newRekorHash := hashString(rekorPubKeyPEM)

	// Check if keys changed
	keysChanged := platform.Status.RHTASRootKeys == nil ||
		platform.Status.RHTASRootKeys.FulcioRootHash != newFulcioHash ||
		platform.Status.RHTASRootKeys.RekorKeyHash != newRekorHash ||
		platform.Status.RHTASRootKeys.TUFRepositoryURL != tufURL

	if keysChanged {
		platform.Status.RHTASRootKeys = &mirrorv1.RHTASRootKeysInfo{
			ConfigMap:        "rhtas-trusted-root",
			FulcioRootHash:   newFulcioHash,
			RekorKeyHash:     newRekorHash,
			LastUpdated:      metav1.Now(),
			TUFRepositoryURL: tufURL,
		}
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
		// Secret already exists, merge with existing to preserve Quay credentials
		merged, changed, err := r.mergePullSecrets(existing.Data[pullSecretKey], sourceSecret.Data[pullSecretKey])
		if err != nil {
			return fmt.Errorf("failed to merge pull secrets: %w", err)
		}

		// After merging, also ensure Quay credentials are added if managed Quay is enabled
		mergedWithQuay, quayChanged, err := r.addQuayCredentialsIfNeeded(ctx, merged)
		if err != nil {
			log.FromContext(ctx).Error(err, "failed to add Quay credentials to pull-secret")
			// Don't fail the whole reconcile, just log
		} else {
			merged = mergedWithQuay
			changed = changed || quayChanged
		}

		if changed {
			targetSecret.ResourceVersion = existing.ResourceVersion
			targetSecret.Data[pullSecretKey] = merged
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

	// Creating new secret - also add Quay credentials if needed
	quayEnhanced, _, err := r.addQuayCredentialsIfNeeded(ctx, targetSecret.Data[pullSecretKey])
	if err != nil {
		log.FromContext(ctx).Error(err, "failed to add Quay credentials during create")
	} else {
		targetSecret.Data[pullSecretKey] = quayEnhanced
	}

	return r.Create(ctx, targetSecret)
}

// addQuayCredentialsIfNeeded adds Quay robot credentials to dockerconfig if managed Quay is deployed
func (r *DisconnectedPlatformReconciler) addQuayCredentialsIfNeeded(ctx context.Context, dockerconfigJSON []byte) ([]byte, bool, error) {
	logger := log.FromContext(ctx)

	// Check if there's a managed Quay instance
	quayRegistry := &unstructured.Unstructured{}
	quayRegistry.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "quay.redhat.com",
		Version: "v1",
		Kind:    "QuayRegistry",
	})
	quayRegistry.SetName("mirror-operator-quay")
	quayRegistry.SetNamespace(architectNamespace)

	if err := r.Get(ctx, client.ObjectKeyFromObject(quayRegistry), quayRegistry); err != nil {
		// No Quay registry, return unchanged
		return dockerconfigJSON, false, nil
	}

	// Get Quay hostname
	hostname, err := r.getQuayHostname(ctx, quayRegistry)
	if err != nil || hostname == "" {
		// Quay not ready yet
		return dockerconfigJSON, false, nil
	}

	// Parse dockerconfig
	var dockerConfig map[string]interface{}
	if err := json.Unmarshal(dockerconfigJSON, &dockerConfig); err != nil {
		return nil, false, fmt.Errorf("failed to parse dockerconfig: %w", err)
	}

	auths, ok := dockerConfig["auths"].(map[string]interface{})
	if !ok {
		auths = make(map[string]interface{})
		dockerConfig["auths"] = auths
	}

	// Check if Quay credentials already exist
	if _, hasQuay := auths[hostname]; hasQuay {
		// Already have credentials, return unchanged
		return dockerconfigJSON, false, nil
	}

	// Get robot credentials from database
	robot, token, err := r.getQuayRobotCredentials(ctx)
	if err != nil {
		return dockerconfigJSON, false, fmt.Errorf("failed to get Quay robot credentials: %w", err)
	}

	// Add Quay credentials (base64 encode username:token)
	authString := base64.StdEncoding.EncodeToString([]byte(robot + ":" + token))
	auths[hostname] = map[string]interface{}{
		"auth": authString,
	}

	logger.Info("Adding Quay credentials to pull-secret", "registry", hostname, "robot", robot)

	// Marshal back
	updated, err := json.Marshal(dockerConfig)
	if err != nil {
		return nil, false, fmt.Errorf("failed to marshal updated dockerconfig: %w", err)
	}

	return updated, true, nil
}

// getQuayRobotCredentials retrieves robot account credentials from Quay
// It first checks the cached secret, then queries the Quay database if needed
func (r *DisconnectedPlatformReconciler) getQuayRobotCredentials(ctx context.Context) (string, string, error) {
	logger := log.FromContext(ctx)
	robot := "mirror+mirroroperator"

	// Check if we have cached credentials in a secret
	robotSecret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: "quay-robot-credentials", Namespace: architectNamespace}, robotSecret); err == nil {
		token := string(robotSecret.Data["token"])
		if token != "" {
			logger.V(1).Info("Retrieved robot credentials from cached secret")
			return robot, token, nil
		}
	}

	// Secret not found or empty - query Quay database directly
	logger.Info("Cached robot credentials not found, querying Quay database")
	token, err := r.queryQuayRobotToken(ctx, "mirror", "mirroroperator")
	if err != nil {
		return "", "", fmt.Errorf("failed to query robot token from Quay: %w", err)
	}

	// Cache the retrieved token for future use
	if err := r.saveQuayRobotCredentials(ctx, robot, token); err != nil {
		logger.Error(err, "failed to cache robot credentials to secret")
		// Non-fatal - we still have the token
	}

	return robot, token, nil
}

// queryQuayRobotToken queries the Quay database directly for a robot account's token
func (r *DisconnectedPlatformReconciler) queryQuayRobotToken(ctx context.Context, orgName, robotShortName string) (string, error) {
	logger := log.FromContext(ctx)

	// Python script to query robot token from Quay database
	pythonScript := `
import sys
from app import app
from data import model
from data.database import configure

# Initialize database
configure(app.config)

org_name = "` + orgName + `"
robot_short_name = "` + robotShortName + `"
robot_full_name = org_name + "+" + robot_short_name

try:
    # Look up the robot account
    robot = model.user.lookup_robot(robot_full_name)
    if not robot:
        print(f"Robot {robot_full_name} not found", file=sys.stderr)
        sys.exit(1)

    # Retrieve the robot's token
    robot_token = model.user.retrieve_robot_token(robot)
    if not robot_token:
        print(f"Robot {robot_full_name} has no token", file=sys.stderr)
        sys.exit(1)

    print(robot_token)

except Exception as e:
    print(f"Error querying robot token: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)
`

	// Execute Python script in Quay pod
	output, err := r.execInQuayPod(ctx, architectNamespace, pythonScript)
	if err != nil {
		return "", fmt.Errorf("failed to execute token query script: %w", err)
	}

	token := strings.TrimSpace(output)
	if token == "" {
		return "", fmt.Errorf("token query script returned empty token")
	}

	logger.Info("Successfully retrieved robot token from Quay database", "org", orgName, "robot", robotShortName)
	return token, nil
}

// mergePullSecrets merges source dockerconfig into existing, preserving Quay credentials
// Returns merged dockerconfig bytes, whether it changed, and any error
func (r *DisconnectedPlatformReconciler) mergePullSecrets(existing, source []byte) ([]byte, bool, error) {
	var existingConfig, sourceConfig map[string]interface{}

	// Parse existing config
	if err := json.Unmarshal(existing, &existingConfig); err != nil {
		return nil, false, fmt.Errorf("failed to parse existing dockerconfig: %w", err)
	}

	// Parse source config
	if err := json.Unmarshal(source, &sourceConfig); err != nil {
		return nil, false, fmt.Errorf("failed to parse source dockerconfig: %w", err)
	}

	existingAuths, _ := existingConfig["auths"].(map[string]interface{})
	sourceAuths, _ := sourceConfig["auths"].(map[string]interface{})

	if existingAuths == nil {
		existingAuths = make(map[string]interface{})
	}
	if sourceAuths == nil {
		sourceAuths = make(map[string]interface{})
	}

	// Preserve Quay credentials from existing
	quayHosts := []string{}
	for host := range existingAuths {
		if strings.Contains(host, "mirror-operator-quay") {
			quayHosts = append(quayHosts, host)
		}
	}

	// Merge: source overwrites existing, except for Quay registries
	changed := false
	for host, auth := range sourceAuths {
		if existingAuth, exists := existingAuths[host]; !exists || fmt.Sprintf("%v", existingAuth) != fmt.Sprintf("%v", auth) {
			existingAuths[host] = auth
			changed = true
		}
	}

	// Quay credentials are already preserved in existingAuths
	// No need to set changed=true since they were never removed

	existingConfig["auths"] = existingAuths

	merged, err := json.Marshal(existingConfig)
	if err != nil {
		return nil, false, fmt.Errorf("failed to marshal merged dockerconfig: %w", err)
	}

	return merged, changed, nil
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
	// Kubernetes resource names must be <= 63 characters
	// Use a shorter prefix and hash long platform names
	prefix := "mo" // mirror-operator shortened
	maxLen := 63

	baseName := prefix + "-" + platform.Name + "-" + component
	if len(baseName) <= maxLen {
		return baseName
	}

	// Name is too long, use hash of platform name
	h := sha256.Sum256([]byte(platform.Name))
	hash := hex.EncodeToString(h[:4]) // 8 character hash
	return prefix + "-" + hash + "-" + component
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

func (r *DisconnectedPlatformReconciler) getRouteHostname(route *unstructured.Unstructured) string {
	// First try spec.host (custom hostname)
	if host, found, _ := unstructured.NestedString(route.Object, "spec", "host"); found && host != "" {
		return host
	}

	// Fall back to status.ingress[0].host (auto-generated hostname)
	ingress, found, _ := unstructured.NestedSlice(route.Object, "status", "ingress")
	if found && len(ingress) > 0 {
		if ingressMap, ok := ingress[0].(map[string]interface{}); ok {
			if host, found, _ := unstructured.NestedString(ingressMap, "host"); found && host != "" {
				return host
			}
		}
	}

	return ""
}

func (r *DisconnectedPlatformReconciler) reconcileArchitect(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)
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
	// Always create routes with default edge termination if not explicitly disabled
	var frontendRouteHostname, backendRouteHostname string

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
		if cfg.Route != nil && cfg.Route.Host != "" {
			frontendRouteHostname = cfg.Route.Host
		} else {
			// Get auto-generated hostname from route status
			frontendRouteHostname = r.getRouteHostname(frontendRoute)
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
		if cfg.Route != nil && cfg.Route.Host != "" {
			// If custom host specified, use subdomain pattern
			backendRouteHostname = "api-" + cfg.Route.Host
		} else {
			// Get auto-generated hostname from route status
			backendRouteHostname = r.getRouteHostname(backendRoute)
		}
	}

	// Frontend resources (after routes to get hostnames)
	// Deploy if frontendImage is explicitly set, or if console plugin is not enabled (default mode)
	deployFrontend := frontendImage != "" && (cfg.ConsolePlugin == nil || !cfg.ConsolePlugin.Enabled || cfg.FrontendImage != "")

	if deployFrontend {
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
		)
	} else if cfg.ConsolePlugin != nil && cfg.ConsolePlugin.Enabled && cfg.FrontendImage == "" {
		// Only delete frontend if console plugin is enabled AND frontendImage is not explicitly set
		// This allows hybrid mode (both frontend and plugin) when frontendImage is specified
		if err := r.deleteArchitectFrontendResources(ctx, platform); err != nil {
			logger.Error(err, "failed to delete frontend resources")
		}
	}

	// Console Plugin resources (if enabled)
	if cfg.ConsolePlugin != nil && cfg.ConsolePlugin.Enabled {
		consolePluginImage := cfg.ConsolePlugin.Image
		if consolePluginImage == "" {
			consolePluginImage = r.ArchitectConsolePluginImage
		}
		pluginName := "airgap-architect-plugin"
		pluginLabels := architectComponentLabels("console-plugin")
		if err := r.ensureConsolePlugin(ctx, platform, pluginName, consolePluginImage, replicas, pluginLabels, pullSecretName, pullSecretNamespace); err != nil {
			return err
		}
		if err := r.ensureArchitectService(ctx, platform, pluginName, int32(9001), pluginLabels); err != nil {
			return err
		}
		if err := r.ensureConsolePluginCR(ctx, platform, pluginName); err != nil {
			return err
		}
		if err := r.enableConsolePluginInOperator(ctx, pluginName); err != nil {
			logger.Error(err, "failed to enable console plugin in operator")
		}
		platform.Status.Components = append(platform.Status.Components,
			mirrorv1.ComponentStatus{Name: "airgap-architect-console-plugin", Status: "Running"},
		)
		logger.Info("Console plugin enabled - UI accessible via OpenShift Console: Administrator → Airgap Architect")
	} else {
		// Delete console plugin resources if disabled or not configured
		if err := r.deleteConsolePluginResources(ctx, platform); err != nil {
			logger.Error(err, "failed to delete console plugin resources")
		}
	}

	platform.Status.Components = append(platform.Status.Components,
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

	// Get GitHub token secret name if configured
	githubTokenSecretName := ""
	if platform.Spec.Architect != nil && platform.Spec.Architect.GitHubTokenSecret != nil {
		githubTokenSecretName = platform.Spec.Architect.GitHubTokenSecret.Name
	}

	// Create container builder with GitHub token secret
	containerBuilder := makeBackendContainerBuilder(githubTokenSecretName)

	if err := r.Get(ctx, client.ObjectKeyFromObject(dep), dep); err == nil {
		return r.updateArchitectDeployment(ctx, platform, name, image, replicas, labels, pullSecretName, pullSecretNamespace, containerBuilder)
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	newDep := architectBackendDeployment(name, image, replicas, labels, pullSecretName, pullSecretNamespace, containerBuilder)
	setOwnerReference(newDep, platform)
	return r.Create(ctx, newDep)
}

func architectBackendDeployment(name, image string, replicas int32, labels map[string]string, pullSecretName, pullSecretNamespace string, buildContainer containerBuilder) *unstructured.Unstructured {
	dep := &unstructured.Unstructured{}
	dep.SetGroupVersionKind(deploymentGVK)
	dep.SetName(name)
	dep.SetNamespace(architectNamespace)
	dep.SetLabels(labels)
	dep.Object["spec"] = architectDeploymentSpec(name, image, replicas, labels, pullSecretName, pullSecretNamespace, buildContainer)
	return dep
}

// makeBackendContainerBuilder creates a container builder function with GitHub token secret support
func makeBackendContainerBuilder(githubTokenSecretName string) containerBuilder {
	return func(name, image string, labels map[string]string) map[string]interface{} {
		env := []interface{}{
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
			map[string]interface{}{
				"name":  "LOG_LEVEL",
				"value": "debug",
			},
		}

		volumeMounts := []interface{}{
			map[string]interface{}{
				"name":      "data",
				"mountPath": "/data",
			},
			map[string]interface{}{
				"name":      pullSecretVolumeName,
				"mountPath": pullSecretMountPath,
				"readOnly":  true,
			},
			map[string]interface{}{
				"name":      "tls-cert",
				"mountPath": "/var/serving-cert",
				"readOnly":  true,
			},
		}

		// Add TLS environment variables
		env = append(env, map[string]interface{}{
			"name":  "TLS_CERT_PATH",
			"value": "/var/serving-cert/tls.crt",
		})
		env = append(env, map[string]interface{}{
			"name":  "TLS_KEY_PATH",
			"value": "/var/serving-cert/tls.key",
		})

		// Add GitHub token if secret is configured
		if githubTokenSecretName != "" {
			env = append(env, map[string]interface{}{
				"name": "GITHUB_TOKEN",
				"valueFrom": map[string]interface{}{
					"secretKeyRef": map[string]interface{}{
						"name": githubTokenSecretName,
						"key":  "token",
					},
				},
			})
		}

		return map[string]interface{}{
			"name":            "airgap-architect-backend",
			"image":           image,
			"imagePullPolicy": "Always",
			"ports": []interface{}{
				map[string]interface{}{
					"containerPort": int64(architectPort),
					"protocol":      "TCP",
				},
			},
			"env":          env,
			"volumeMounts": volumeMounts,
			"readinessProbe": map[string]interface{}{
				"tcpSocket": map[string]interface{}{
					"port": int64(architectPort),
				},
				"initialDelaySeconds": int64(5),
				"periodSeconds":       int64(10),
				"failureThreshold":    int64(3),
				"successThreshold":    int64(1),
				"timeoutSeconds":      int64(1),
			},
			"livenessProbe": map[string]interface{}{
				"tcpSocket": map[string]interface{}{
					"port": int64(architectPort),
				},
				"initialDelaySeconds": int64(15),
				"periodSeconds":       int64(20),
				"failureThreshold":    int64(3),
				"successThreshold":    int64(1),
				"timeoutSeconds":      int64(1),
			},
		}
	}
}

func backendContainer(name, image string, labels map[string]string) map[string]interface{} {
	// Default backend container without GitHub token (deprecated - use makeBackendContainerBuilder)
	return makeBackendContainerBuilder("")(name, image, labels)
}

func (r *DisconnectedPlatformReconciler) ensureArchitectService(ctx context.Context, platform *mirrorv1.DisconnectedPlatform, name string, port int32, labels map[string]string) error {
	// Check if this is the console plugin or backend service (needs TLS)
	isConsolePlugin := name == "airgap-architect-plugin"
	isBackend := strings.Contains(name, "airgap-architect-backend")
	enableTLS := isConsolePlugin || isBackend

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(serviceGVK)
	existing.SetName(name)
	existing.SetNamespace(architectNamespace)
	if err := r.Get(ctx, client.ObjectKeyFromObject(existing), existing); err == nil {
		// Update existing service
		svc := architectService(name, port, labels, enableTLS)
		svc.SetResourceVersion(existing.GetResourceVersion())
		setOwnerReference(svc, platform)
		return r.Update(ctx, svc)
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	svc := architectService(name, port, labels, enableTLS)
	setOwnerReference(svc, platform)
	return r.Create(ctx, svc)
}

func architectService(name string, port int32, labels map[string]string, enableTLS bool) *unstructured.Unstructured {
	svc := &unstructured.Unstructured{}
	svc.SetGroupVersionKind(serviceGVK)
	svc.SetName(name)
	svc.SetNamespace(architectNamespace)
	svc.SetLabels(labels)

	// Add serving cert annotation for console plugin to enable HTTPS
	if enableTLS {
		annotations := map[string]interface{}{
			"service.beta.openshift.io/serving-cert-secret-name": name + "-cert",
		}
		svc.SetAnnotations(map[string]string{
			"service.beta.openshift.io/serving-cert-secret-name": name + "-cert",
		})
		unstructured.SetNestedMap(svc.Object, annotations, "metadata", "annotations")
	}

	unstructured.SetNestedField(svc.Object, "ClusterIP", "spec", "type")

	// Port name should be "https" for TLS-enabled services
	portName := "http"
	if enableTLS {
		portName = "https"
	}

	unstructured.SetNestedSlice(svc.Object, []interface{}{
		map[string]interface{}{
			"name":       portName,
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

// Console Plugin

func (r *DisconnectedPlatformReconciler) ensureConsolePlugin(ctx context.Context, platform *mirrorv1.DisconnectedPlatform, name, image string, replicas int32, labels map[string]string, pullSecretName, pullSecretNamespace string) error {
	dep := &unstructured.Unstructured{}
	dep.SetGroupVersionKind(deploymentGVK)
	dep.SetName(name)
	dep.SetNamespace(architectNamespace)
	if err := r.Get(ctx, client.ObjectKeyFromObject(dep), dep); err == nil {
		return r.updateArchitectDeployment(ctx, platform, name, image, replicas, labels, pullSecretName, pullSecretNamespace, func(name, image string, labels map[string]string) map[string]interface{} {
			return consolePluginContainer(name, image, labels)
		})
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	newDep := consolePluginDeployment(name, image, replicas, labels, pullSecretName, pullSecretNamespace)
	setOwnerReference(newDep, platform)
	return r.Create(ctx, newDep)
}

func consolePluginDeployment(name, image string, replicas int32, labels map[string]string, pullSecretName, pullSecretNamespace string) *unstructured.Unstructured {
	dep := &unstructured.Unstructured{}
	dep.SetGroupVersionKind(deploymentGVK)
	dep.SetName(name)
	dep.SetNamespace(architectNamespace)
	dep.SetLabels(labels)
	dep.Object["spec"] = architectDeploymentSpec(name, image, replicas, labels, pullSecretName, pullSecretNamespace, func(name, image string, labels map[string]string) map[string]interface{} {
		return consolePluginContainer(name, image, labels)
	})
	return dep
}

func consolePluginContainer(name, image string, labels map[string]string) map[string]interface{} {
	return map[string]interface{}{
		"name":            "plugin",
		"image":           image,
		"imagePullPolicy": "Always",
		"ports": []interface{}{
			map[string]interface{}{
				"containerPort": int64(9001),
				"name":          "https",
				"protocol":      "TCP",
			},
		},
		"volumeMounts": []interface{}{
			map[string]interface{}{
				"name":      "serving-cert",
				"mountPath": "/var/serving-cert",
				"readOnly":  true,
			},
		},
		"resources": map[string]interface{}{
			"requests": map[string]interface{}{
				"cpu":    "100m",
				"memory": "128Mi",
			},
			"limits": map[string]interface{}{
				"cpu":    "500m",
				"memory": "256Mi",
			},
		},
		"livenessProbe": map[string]interface{}{
			"httpGet": map[string]interface{}{
				"path":   "/plugin-manifest.json",
				"port":   int64(9001),
				"scheme": "HTTPS",
			},
			"initialDelaySeconds": int64(5),
			"periodSeconds":       int64(10),
		},
		"readinessProbe": map[string]interface{}{
			"httpGet": map[string]interface{}{
				"path":   "/plugin-manifest.json",
				"port":   int64(9001),
				"scheme": "HTTPS",
			},
			"initialDelaySeconds": int64(5),
			"periodSeconds":       int64(10),
		},
	}
}

func (r *DisconnectedPlatformReconciler) ensureConsolePluginCR(ctx context.Context, platform *mirrorv1.DisconnectedPlatform, pluginName string) error {
	logger := log.FromContext(ctx)

	consolePlugin := &unstructured.Unstructured{}
	consolePlugin.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "console.openshift.io",
		Version: "v1",
		Kind:    "ConsolePlugin",
	})
	consolePlugin.SetName(pluginName)

	if err := r.Get(ctx, client.ObjectKeyFromObject(consolePlugin), consolePlugin); err == nil {
		// Already exists, verify spec
		return r.updateConsolePluginCR(ctx, platform, consolePlugin, pluginName)
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	// Create new ConsolePlugin CR
	backendServiceName := architectResourceName(platform, "airgap-architect-backend")
	consolePlugin.Object["spec"] = map[string]interface{}{
		"displayName": "Airgap Architect",
		"backend": map[string]interface{}{
			"type": "Service",
			"service": map[string]interface{}{
				"name":      pluginName,
				"namespace": architectNamespace,
				"port":      int64(9001),
				"basePath":  "/",
			},
		},
		"proxy": []interface{}{
			map[string]interface{}{
				"alias":         "backend",
				"authorization": "None",
				"endpoint": map[string]interface{}{
					"type": "Service",
					"service": map[string]interface{}{
						"name":      backendServiceName,
						"namespace": architectNamespace,
						"port":      int64(4000),
					},
				},
			},
		},
	}

	logger.Info("Creating ConsolePlugin CR", "name", pluginName)
	return r.Create(ctx, consolePlugin)
}

func (r *DisconnectedPlatformReconciler) updateConsolePluginCR(ctx context.Context, platform *mirrorv1.DisconnectedPlatform, consolePlugin *unstructured.Unstructured, pluginName string) error {
	logger := log.FromContext(ctx)

	logger.Info("Updating ConsolePlugin CR - fetching service-ca certificate", "pluginName", pluginName)

	// Get the service-ca certificate
	caCertContent, err := r.getServiceCACert(ctx)
	if err != nil {
		logger.Error(err, "failed to get service-ca certificate, plugin may not load properly")
		caCertContent = "" // Continue without cert - will use insecure connection
	} else {
		logger.Info("Successfully retrieved service-ca certificate", "certLength", len(caCertContent))
	}

	backendServiceName := architectResourceName(platform, "airgap-architect-backend")
	desiredSpec := map[string]interface{}{
		"displayName": "Airgap Architect",
		"backend": map[string]interface{}{
			"type": "Service",
			"service": map[string]interface{}{
				"name":      pluginName,
				"namespace": architectNamespace,
				"port":      int64(9001),
				"basePath":  "/",
			},
		},
		"proxy": []interface{}{
			map[string]interface{}{
				"alias":         "backend",
				"authorization": "None",
				"endpoint": map[string]interface{}{
					"type": "Service",
					"service": map[string]interface{}{
						"name":      backendServiceName,
						"namespace": architectNamespace,
						"port":      int64(4000),
					},
				},
			},
		},
	}

	// Add caCertificate if we have one
	if caCertContent != "" {
		backendMap := desiredSpec["backend"].(map[string]interface{})
		serviceMap := backendMap["service"].(map[string]interface{})
		serviceMap["caCertificate"] = caCertContent
		logger.Info("Adding CA certificate to ConsolePlugin spec", "certLength", len(caCertContent))
	}

	currentSpec, _, _ := unstructured.NestedMap(consolePlugin.Object, "spec")

	// Check if caCertificate field changed
	currentCert, _, _ := unstructured.NestedString(consolePlugin.Object, "spec", "backend", "service", "caCertificate")
	needsUpdate := currentCert != caCertContent

	if !needsUpdate && fmt.Sprintf("%v", currentSpec) == fmt.Sprintf("%v", desiredSpec) {
		return nil
	}

	consolePlugin.Object["spec"] = desiredSpec
	logger.Info("Updating ConsolePlugin CR", "name", pluginName, "reason", "adding CA certificate")
	return r.Update(ctx, consolePlugin)
}

// enableConsolePluginInOperator patches the console operator to enable a plugin
func (r *DisconnectedPlatformReconciler) enableConsolePluginInOperator(ctx context.Context, pluginName string) error {
	logger := log.FromContext(ctx)

	console := &unstructured.Unstructured{}
	console.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "operator.openshift.io",
		Version: "v1",
		Kind:    "Console",
	})
	console.SetName("cluster")

	if err := r.Get(ctx, client.ObjectKeyFromObject(console), console); err != nil {
		return fmt.Errorf("failed to get console operator: %w", err)
	}

	// Get current plugins list
	plugins, found, err := unstructured.NestedStringSlice(console.Object, "spec", "plugins")
	if err != nil {
		return fmt.Errorf("failed to get plugins list: %w", err)
	}
	if !found {
		plugins = []string{}
	}

	// Check if plugin is already enabled
	for _, p := range plugins {
		if p == pluginName {
			logger.V(1).Info("Console plugin already enabled", "plugin", pluginName)
			return nil
		}
	}

	// Add plugin to list
	plugins = append(plugins, pluginName)
	if err := unstructured.SetNestedStringSlice(console.Object, plugins, "spec", "plugins"); err != nil {
		return fmt.Errorf("failed to set plugins list: %w", err)
	}

	logger.Info("Enabling console plugin in operator", "plugin", pluginName)
	return r.Update(ctx, console)
}

// getServiceCACert retrieves the service-ca certificate bundle
func (r *DisconnectedPlatformReconciler) getServiceCACert(ctx context.Context) (string, error) {
	// The service-ca-operator maintains a signing CA bundle
	configMap := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      "signing-cabundle",
		Namespace: "openshift-service-ca",
	}, configMap)

	if err != nil {
		return "", fmt.Errorf("failed to get signing-cabundle ConfigMap: %w", err)
	}

	caCert, ok := configMap.Data["ca-bundle.crt"]
	if !ok {
		return "", fmt.Errorf("ca-bundle.crt not found in ConfigMap")
	}

	return caCert, nil
}

func (r *DisconnectedPlatformReconciler) deleteConsolePluginResources(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)
	pluginName := "airgap-architect-plugin"

	// Delete ConsolePlugin CR
	consolePlugin := &unstructured.Unstructured{}
	consolePlugin.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "console.openshift.io",
		Version: "v1",
		Kind:    "ConsolePlugin",
	})
	consolePlugin.SetName(pluginName)
	if err := r.Delete(ctx, consolePlugin); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to delete ConsolePlugin CR")
	}

	// Delete Service
	svc := &unstructured.Unstructured{}
	svc.SetGroupVersionKind(serviceGVK)
	svc.SetName(pluginName)
	svc.SetNamespace(architectNamespace)
	if err := r.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to delete console plugin service")
	}

	// Delete Deployment
	dep := &unstructured.Unstructured{}
	dep.SetGroupVersionKind(deploymentGVK)
	dep.SetName(pluginName)
	dep.SetNamespace(architectNamespace)
	if err := r.Delete(ctx, dep); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to delete console plugin deployment")
	}

	return nil
}

func (r *DisconnectedPlatformReconciler) deleteArchitectFrontendResources(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)
	frontendName := architectResourceName(platform, "airgap-architect-frontend")

	// Delete Service
	svc := &unstructured.Unstructured{}
	svc.SetGroupVersionKind(serviceGVK)
	svc.SetName(frontendName)
	svc.SetNamespace(architectNamespace)
	if err := r.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to delete frontend service")
	}

	// Delete Deployment
	dep := &unstructured.Unstructured{}
	dep.SetGroupVersionKind(deploymentGVK)
	dep.SetName(frontendName)
	dep.SetNamespace(architectNamespace)
	if err := r.Delete(ctx, dep); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to delete frontend deployment")
	}

	// Delete Route
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(routeGVK)
	route.SetName("airgap-architect")
	route.SetNamespace(architectNamespace)
	if err := r.Delete(ctx, route); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to delete frontend route")
	}

	return nil
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

	// Add TLS cert volume for backend component
	if component == "backend" {
		volumes = append(volumes, map[string]interface{}{
			"name": "tls-cert",
			"secret": map[string]interface{}{
				"secretName": name + "-cert",
			},
		})
	}

	// Add serving cert volume for console plugin component
	if component == "console-plugin" {
		volumes = append(volumes, map[string]interface{}{
			"name": "serving-cert",
			"secret": map[string]interface{}{
				"secretName": "airgap-architect-plugin-cert",
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
	logger := log.FromContext(ctx)
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(deploymentGVK)
	existing.SetName(name)
	existing.SetNamespace(architectNamespace)
	if err := r.Get(ctx, client.ObjectKeyFromObject(existing), existing); err != nil {
		return err
	}

	// Build desired spec
	desiredSpec := architectDeploymentSpec(name, image, replicas, labels, pullSecretName, pullSecretNamespace, buildContainer)

	// Compare with existing spec to avoid unnecessary updates
	existingSpec, found, err := unstructured.NestedMap(existing.Object, "spec")
	if err != nil {
		return fmt.Errorf("failed to get existing spec: %w", err)
	}
	if found {
		// Compare critical fields that would require an update
		equal, reason := deploymentsEqual(existingSpec, desiredSpec)
		if equal {
			logger.V(1).Info("Deployment spec unchanged, skipping update", "name", name)
			return nil
		}
		logger.Info("Deployment specs differ, will update", "name", name, "reason", reason)
	}

	// Specs differ, perform update
	dep := &unstructured.Unstructured{}
	dep.SetGroupVersionKind(deploymentGVK)
	dep.SetName(name)
	dep.SetNamespace(architectNamespace)
	dep.SetLabels(labels)
	dep.SetResourceVersion(existing.GetResourceVersion())
	setOwnerReference(dep, platform)
	dep.Object["spec"] = desiredSpec

	logger.Info("Updating deployment due to spec changes", "name", name)
	return r.Update(ctx, dep)
}

// deploymentsEqual compares deployment specs, ignoring fields that don't affect pod template
func deploymentsEqual(existing, desired map[string]interface{}) (bool, string) {
	// Compare replicas
	existingReplicas, _, _ := unstructured.NestedInt64(existing, "replicas")
	desiredReplicas, _, _ := unstructured.NestedInt64(desired, "replicas")
	if existingReplicas != desiredReplicas {
		return false, "replicas differ"
	}

	// Get container specs for comparison (most important for changes)
	existingContainers, _, _ := unstructured.NestedSlice(existing, "template", "spec", "containers")
	desiredContainers, _, _ := unstructured.NestedSlice(desired, "template", "spec", "containers")

	if len(existingContainers) != len(desiredContainers) {
		return false, "container count differs"
	}

	// Compare first container (we only have one)
	if len(existingContainers) > 0 && len(desiredContainers) > 0 {
		existingContainer, _ := existingContainers[0].(map[string]interface{})
		desiredContainer, _ := desiredContainers[0].(map[string]interface{})

		// Compare critical fields: image, env, volumeMounts
		if existingContainer["image"] != desiredContainer["image"] {
			return false, fmt.Sprintf("image differs: existing=%s desired=%s", existingContainer["image"], desiredContainer["image"])
		}

		// Compare env vars
		existingEnv, _ := json.Marshal(existingContainer["env"])
		desiredEnv, _ := json.Marshal(desiredContainer["env"])
		if string(existingEnv) != string(desiredEnv) {
			return false, "env differs"
		}

		// Compare volume mounts
		existingMounts, _ := json.Marshal(existingContainer["volumeMounts"])
		desiredMounts, _ := json.Marshal(desiredContainer["volumeMounts"])
		if string(existingMounts) != string(desiredMounts) {
			return false, "volumeMounts differ"
		}

		// Compare readiness probe
		existingReadiness, _ := json.Marshal(existingContainer["readinessProbe"])
		desiredReadiness, _ := json.Marshal(desiredContainer["readinessProbe"])
		if string(existingReadiness) != string(desiredReadiness) {
			return false, fmt.Sprintf("readinessProbe differs: existing=%s desired=%s", string(existingReadiness), string(desiredReadiness))
		}

		// Compare liveness probe
		existingLiveness, _ := json.Marshal(existingContainer["livenessProbe"])
		desiredLiveness, _ := json.Marshal(desiredContainer["livenessProbe"])
		if string(existingLiveness) != string(desiredLiveness) {
			return false, fmt.Sprintf("livenessProbe differs: existing=%s desired=%s", string(existingLiveness), string(desiredLiveness))
		}
	}

	// Specs are equivalent
	return true, ""
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

	// Handle custom host if specified
	if routeCfg != nil && routeCfg.Host != "" {
		unstructured.SetNestedField(route.Object, routeCfg.Host, "spec", "host")
	}

	// Set TLS termination (default to edge if not specified)
	termination := "edge"
	if routeCfg != nil && routeCfg.TLS != nil && routeCfg.TLS.Termination != "" {
		termination = routeCfg.TLS.Termination
	}
	unstructured.SetNestedField(route.Object, termination, "spec", "tls", "termination")
	unstructured.SetNestedField(route.Object, "Redirect", "spec", "tls", "insecureEdgeTerminationPolicy")

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

// collectionPipelineEventHandler triggers DisconnectedPlatform reconciliation when CollectionPipelines complete
type collectionPipelineEventHandler struct {
	client client.Client
}

func (h *collectionPipelineEventHandler) Create(ctx context.Context, e event.TypedCreateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	// Trigger on create to set up initial file server
	h.triggerPlatformReconcile(ctx, q)
}

func (h *collectionPipelineEventHandler) Update(ctx context.Context, e event.TypedUpdateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	oldPipeline, oldOk := e.ObjectOld.(*mirrorv1.CollectionPipeline)
	newPipeline, newOk := e.ObjectNew.(*mirrorv1.CollectionPipeline)

	if !oldOk || !newOk {
		return
	}

	// Only trigger if phase changed to Complete or Succeeded
	if (newPipeline.Status.Phase == "Complete" || newPipeline.Status.Phase == "Succeeded") &&
		oldPipeline.Status.Phase != newPipeline.Status.Phase {
		h.triggerPlatformReconcile(ctx, q)
	}
}

func (h *collectionPipelineEventHandler) Delete(ctx context.Context, e event.TypedDeleteEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	// Trigger on delete to remove from file server
	h.triggerPlatformReconcile(ctx, q)
}

func (h *collectionPipelineEventHandler) Generic(ctx context.Context, e event.TypedGenericEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	// Not needed
}

func (h *collectionPipelineEventHandler) triggerPlatformReconcile(ctx context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
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

func (r *DisconnectedPlatformReconciler) setErrorCondition(platform *mirrorv1.DisconnectedPlatform, reason, message string) {
	// Check if condition already exists with same status
	for i, c := range platform.Status.Conditions {
		if c.Type == "Ready" {
			// Condition exists - only update if status, reason, or message changed
			if c.Status == metav1.ConditionFalse && c.Reason == reason && c.Message == message {
				// No change needed
				return
			}
			// Update with new values, preserve LastTransitionTime if status didn't change
			lastTransition := c.LastTransitionTime
			if c.Status != metav1.ConditionFalse {
				lastTransition = metav1.Now()
			}
			platform.Status.Conditions[i] = metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				ObservedGeneration: platform.Generation,
				LastTransitionTime: lastTransition,
				Reason:             reason,
				Message:            message,
			}
			return
		}
	}

	// Condition doesn't exist, add it
	platform.Status.Conditions = append(platform.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		ObservedGeneration: platform.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	})
}

func (r *DisconnectedPlatformReconciler) setReadyCondition(platform *mirrorv1.DisconnectedPlatform, reason, message string) {
	// Check if condition already exists with same status
	for i, c := range platform.Status.Conditions {
		if c.Type == "Ready" {
			// Condition exists - only update if status, reason, or message changed
			if c.Status == metav1.ConditionTrue && c.Reason == reason && c.Message == message {
				// No change needed
				return
			}
			// Update with new values, preserve LastTransitionTime if status didn't change
			lastTransition := c.LastTransitionTime
			if c.Status != metav1.ConditionTrue {
				lastTransition = metav1.Now()
			}
			platform.Status.Conditions[i] = metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				ObservedGeneration: platform.Generation,
				LastTransitionTime: lastTransition,
				Reason:             reason,
				Message:            message,
			}
			return
		}
	}

	// Condition doesn't exist, add it
	platform.Status.Conditions = append(platform.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		ObservedGeneration: platform.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	})
}

func (r *DisconnectedPlatformReconciler) configureOpenShiftOAuth(ctx context.Context, keycloakHost string) error {
	logger := log.FromContext(ctx)

	// Get Keycloak admin credentials
	adminSecret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{
		Name:      "mirror-operator-keycloak-initial-admin",
		Namespace: architectNamespace,
	}, adminSecret); err != nil {
		return fmt.Errorf("failed to get Keycloak admin credentials: %w", err)
	}

	adminUser := string(adminSecret.Data["username"])
	adminPassword := string(adminSecret.Data["password"])

	// Get OpenShift API server URL for Keycloak openshift-v4 provider baseUrl
	infrastructure := &unstructured.Unstructured{}
	infrastructure.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "Infrastructure",
	})
	infrastructure.SetName("cluster")
	if err := r.Get(ctx, client.ObjectKeyFromObject(infrastructure), infrastructure); err != nil {
		return fmt.Errorf("failed to get cluster infrastructure: %w", err)
	}
	apiServerURL, _, _ := unstructured.NestedString(infrastructure.Object, "status", "apiServerURL")
	if apiServerURL == "" {
		return fmt.Errorf("apiServerURL not found in cluster infrastructure")
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	// Get admin access token
	tokenURL := fmt.Sprintf("https://%s/realms/master/protocol/openid-connect/token", keycloakHost)
	tokenData := url.Values{}
	tokenData.Set("grant_type", "password")
	tokenData.Set("client_id", "admin-cli")
	tokenData.Set("username", adminUser)
	tokenData.Set("password", adminPassword)

	tokenResp, err := httpClient.PostForm(tokenURL, tokenData)
	if err != nil {
		return fmt.Errorf("failed to get admin token: %w", err)
	}
	defer tokenResp.Body.Close()

	if tokenResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(tokenResp.Body)
		return fmt.Errorf("failed to get admin token: status %d, body: %s", tokenResp.StatusCode, string(body))
	}

	var tokenResult map[string]interface{}
	if err := json.NewDecoder(tokenResp.Body).Decode(&tokenResult); err != nil {
		return fmt.Errorf("failed to decode token response: %w", err)
	}

	adminToken, ok := tokenResult["access_token"].(string)
	if !ok || adminToken == "" {
		return fmt.Errorf("no access token in response")
	}

	// Configure OpenShift OAuth for both realms
	realms := []string{"trusted-artifact-signer", "trustify"}
	for _, realmName := range realms {
		if err := r.addOpenShiftIdentityProvider(ctx, keycloakHost, realmName, apiServerURL, adminToken, httpClient); err != nil {
			logger.Error(err, "failed to add OpenShift IdP to realm", "realm", realmName)
			// Continue with other realms
		} else {
			logger.Info("Configured OpenShift OAuth for realm", "realm", realmName)
		}
	}

	return nil
}

func (r *DisconnectedPlatformReconciler) addOpenShiftIdentityProvider(ctx context.Context, keycloakHost, realmName, apiServerURL, adminToken string, httpClient *http.Client) error {
	logger := log.FromContext(ctx)

	// Check if OpenShift identity provider already exists
	idpURL := fmt.Sprintf("https://%s/admin/realms/%s/identity-provider/instances", keycloakHost, realmName)
	req, _ := http.NewRequest("GET", idpURL, nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to list identity providers: %w", err)
	}
	defer resp.Body.Close()

	var idps []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&idps); err != nil {
		return fmt.Errorf("failed to decode identity providers: %w", err)
	}

	// Check if OpenShift IdP already exists
	idpExists := false
	for _, idp := range idps {
		if alias, ok := idp["alias"].(string); ok && alias == "openshift" {
			idpExists = true
			break
		}
	}

	// Get or create OpenShift OAuth client in OpenShift
	oauthClient := &unstructured.Unstructured{}
	oauthClient.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "oauth.openshift.io",
		Version: "v1",
		Kind:    "OAuthClient",
	})
	oauthClient.SetName("keycloak-" + realmName)

	redirectURL := fmt.Sprintf("https://%s/realms/%s/broker/openshift/endpoint", keycloakHost, realmName)

	if err := r.Get(ctx, client.ObjectKeyFromObject(oauthClient), oauthClient); apierrors.IsNotFound(err) {
		// Generate a random client secret
		clientSecret := generateRandomString(32)

		unstructured.SetNestedField(oauthClient.Object, clientSecret, "secret")
		unstructured.SetNestedField(oauthClient.Object, "prompt", "grantMethod")
		unstructured.SetNestedStringSlice(oauthClient.Object, []string{redirectURL}, "redirectURIs")

		if err := r.Create(ctx, oauthClient); err != nil {
			return fmt.Errorf("failed to create OAuthClient: %w", err)
		}
		logger.Info("Created OpenShift OAuthClient", "name", oauthClient.GetName())
	} else if err != nil {
		return fmt.Errorf("failed to check OAuthClient: %w", err)
	}

	// Get the client secret
	clientSecret, _, _ := unstructured.NestedString(oauthClient.Object, "secret")
	if clientSecret == "" {
		return fmt.Errorf("OAuthClient secret is empty")
	}

	// If IdP already exists, update its client secret
	if idpExists {
		// Get the full IdP configuration first
		getURL := fmt.Sprintf("https://%s/admin/realms/%s/identity-provider/instances/openshift", keycloakHost, realmName)
		req, _ = http.NewRequest("GET", getURL, nil)
		req.Header.Set("Authorization", "Bearer "+adminToken)

		resp, err = httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to get identity provider: %w", err)
		}
		defer resp.Body.Close()

		var existingIdP map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&existingIdP); err != nil {
			return fmt.Errorf("failed to decode identity provider: %w", err)
		}

		// Update the client secret and profile settings in the config
		if config, ok := existingIdP["config"].(map[string]interface{}); ok {
			config["clientSecret"] = clientSecret
			config["baseUrl"] = apiServerURL
		}

		// Set updateProfileFirstLoginMode to "off" to prevent profile update prompts
		// Set username template to use preferred_username from OpenShift
		existingIdP["updateProfileFirstLoginMode"] = "off"
		existingIdP["config"].(map[string]interface{})["userInfoUrl"] = apiServerURL + "/apis/user.openshift.io/v1/users/~"

		// PUT the updated IdP configuration
		updateJSON, _ := json.Marshal(existingIdP)
		req, _ = http.NewRequest("PUT", getURL, bytes.NewReader(updateJSON))
		req.Header.Set("Authorization", "Bearer "+adminToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err = httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to update identity provider: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to update identity provider secret: status %d, body: %s", resp.StatusCode, string(body))
		}

		logger.Info("Updated OpenShift identity provider client secret and profile settings", "realm", realmName)

		// Configure realm to skip profile review for federated users
		if err := r.configureRealmProfileSettings(ctx, keycloakHost, realmName, adminToken, httpClient); err != nil {
			logger.Error(err, "failed to configure realm profile settings", "realm", realmName)
		}

		return nil
	}

	// Create OpenShift identity provider in Keycloak using openshift-v4 provider
	idpConfig := map[string]interface{}{
		"alias":                       "openshift",
		"displayName":                 "OpenShift",
		"providerId":                  "openshift-v4",
		"enabled":                     true,
		"trustEmail":                  true,
		"firstBrokerLoginFlowAlias":   "first broker login",
		"updateProfileFirstLoginMode": "off", // Don't prompt for profile updates
		"config": map[string]interface{}{
			"baseUrl":      apiServerURL,
			"clientId":     "keycloak-" + realmName,
			"clientSecret": clientSecret,
			"defaultScope": "user:full",
			"userInfoUrl":  apiServerURL + "/apis/user.openshift.io/v1/users/~",
		},
	}

	idpJSON, _ := json.Marshal(idpConfig)
	req, _ = http.NewRequest("POST", idpURL, bytes.NewBuffer(idpJSON))
	req.Header.Set("Authorization", "Bearer "+adminToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err = httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create identity provider: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create identity provider: status %d, body: %s", resp.StatusCode, string(body))
	}

	logger.Info("Successfully added OpenShift identity provider", "realm", realmName)

	// Configure realm to skip profile review for federated users
	if err := r.configureRealmProfileSettings(ctx, keycloakHost, realmName, adminToken, httpClient); err != nil {
		logger.Error(err, "failed to configure realm profile settings", "realm", realmName)
	}

	return nil
}

func (r *DisconnectedPlatformReconciler) configureRealmProfileSettings(ctx context.Context, keycloakHost, realmName, adminToken string, httpClient *http.Client) error {
	logger := log.FromContext(ctx)

	// Get the "first broker login" authentication flow
	flowURL := fmt.Sprintf("https://%s/admin/realms/%s/authentication/flows/first%%20broker%%20login/executions", keycloakHost, realmName)
	req, _ := http.NewRequest("GET", flowURL, nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		logger.Error(err, "failed to get authentication flow", "realm", realmName)
		return fmt.Errorf("failed to get authentication flow: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		logger.Error(fmt.Errorf("unexpected status"), "failed to get authentication flow", "status", resp.StatusCode, "body", string(body))
		return fmt.Errorf("failed to get authentication flow: status %d", resp.StatusCode)
	}

	var executions []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&executions); err != nil {
		return fmt.Errorf("failed to decode executions: %w", err)
	}

	logger.Info("Found authentication flow executions", "realm", realmName, "count", len(executions))

	// Find the "Review Profile" execution and set its requirement to DISABLED
	for i, execution := range executions {
		if displayName, ok := execution["displayName"].(string); ok && displayName == "Review Profile" {
			logger.Info("Found Review Profile execution", "realm", realmName)

			// Check current requirement
			currentReq, _ := execution["requirement"].(string)
			if currentReq == "DISABLED" {
				logger.Info("Review Profile already disabled", "realm", realmName)
				continue
			}

			// Update the execution requirement to DISABLED
			execution["requirement"] = "DISABLED"
			executions[i] = execution

			// Update via the flow executions endpoint
			updateURL := fmt.Sprintf("https://%s/admin/realms/%s/authentication/flows/first%%20broker%%20login/executions", keycloakHost, realmName)

			// PUT the updated execution back
			execJSON, _ := json.Marshal(execution)
			updateReq, _ := http.NewRequest("PUT", updateURL, bytes.NewReader(execJSON))
			updateReq.Header.Set("Authorization", "Bearer "+adminToken)
			updateReq.Header.Set("Content-Type", "application/json")

			updateResp, err := httpClient.Do(updateReq)
			if err != nil {
				logger.Error(err, "failed to disable Review Profile execution", "realm", realmName)
			} else {
				defer updateResp.Body.Close()
				if updateResp.StatusCode == http.StatusNoContent || updateResp.StatusCode == http.StatusAccepted {
					logger.Info("Disabled Review Profile execution to skip profile updates", "realm", realmName)
				} else {
					body, _ := io.ReadAll(updateResp.Body)
					logger.Error(fmt.Errorf("unexpected status"), "failed to disable Review Profile",
						"realm", realmName, "status", updateResp.StatusCode, "body", string(body))
				}
			}
			break
		}
	}

	return nil
}

func (r *DisconnectedPlatformReconciler) addOpenShiftAttributeMappers(ctx context.Context, keycloakHost, realmName, adminToken string, httpClient *http.Client) error {
	logger := log.FromContext(ctx)

	// Add username mapper to handle usernames with special characters like "kube:admin"
	// Note: We don't need hardcoded attribute mappers for email/firstName/lastName
	// because we set updateProfileFirstLoginMode to "off" which skips the profile update screen
	mappers := []map[string]interface{}{
		{
			"name":                   "username-mapper",
			"identityProviderAlias":  "openshift",
			"identityProviderMapper": "oidc-username-idp-mapper",
			"config": map[string]interface{}{
				"template": "${CLAIM.preferred_username}",
			},
		},
	}

	for _, mapper := range mappers {
		mapperJSON, _ := json.Marshal(mapper)
		mapperURL := fmt.Sprintf("https://%s/admin/realms/%s/identity-provider/instances/openshift/mappers", keycloakHost, realmName)
		req, _ := http.NewRequest("POST", mapperURL, bytes.NewReader(mapperJSON))
		req.Header.Set("Authorization", "Bearer "+adminToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err := httpClient.Do(req)
		if err != nil {
			logger.Error(err, "failed to create attribute mapper", "mapper", mapper["name"])
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
			body, _ := io.ReadAll(resp.Body)
			logger.Error(fmt.Errorf("unexpected status"), "failed to create attribute mapper",
				"mapper", mapper["name"], "status", resp.StatusCode, "body", string(body))
		} else {
			logger.Info("Added attribute mapper", "mapper", mapper["name"], "realm", realmName)
		}
	}

	return nil
}

// ensureQuayCredentials creates a robot account in managed Quay and merges credentials into pull-secret
func (r *DisconnectedPlatformReconciler) ensureQuayCredentials(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)

	// Get Quay hostname
	quayRegistry := &unstructured.Unstructured{}
	quayRegistry.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "quay.redhat.com",
		Version: "v1",
		Kind:    "QuayRegistry",
	})
	quayRegistry.SetName("mirror-operator-quay")
	quayRegistry.SetNamespace(architectNamespace)

	if err := r.Get(ctx, client.ObjectKeyFromObject(quayRegistry), quayRegistry); err != nil {
		return fmt.Errorf("failed to get QuayRegistry: %w", err)
	}

	hostname, err := r.getQuayHostname(ctx, quayRegistry)
	if err != nil || hostname == "" {
		logger.Info("Quay hostname not yet available, skipping credential setup")
		return nil
	}

	// Create or get robot account credentials using Python API (REST API requires CSRF tokens)
	// This runs Python code inside the Quay pod via kubectl exec
	robotShortName := "mirroroperator"
	robotFullName := "mirror+" + robotShortName
	robotToken, err := r.ensureQuayRobotViaPython(ctx, "mirror", robotShortName)
	if err != nil {
		return fmt.Errorf("failed to ensure Quay robot account: %w", err)
	}

	// Save robot credentials to secret for reuse by addQuayCredentialsIfNeeded
	if err := r.saveQuayRobotCredentials(ctx, robotFullName, robotToken); err != nil {
		logger.Error(err, "failed to save robot credentials")
	}

	logger.Info("Successfully configured Quay credentials", "registry", hostname, "robot", robotFullName)
	return nil
}

// reconcileArtifactsBucket creates an ObjectBucketClaim for storing collection artifacts
func (r *DisconnectedPlatformReconciler) reconcileArtifactsBucket(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)
	namespace := architectNamespace

	// Define ObjectBucketClaim
	obc := &unstructured.Unstructured{}
	obc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "objectbucket.io",
		Version: "v1alpha1",
		Kind:    "ObjectBucketClaim",
	})
	obc.SetName("collection-artifacts")
	obc.SetNamespace(namespace)

	// Check if OBC already exists
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(obc.GroupVersionKind())
	err := r.Get(ctx, client.ObjectKeyFromObject(obc), existing)
	if err == nil {
		// OBC exists, nothing to do
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get ObjectBucketClaim: %w", err)
	}

	// Create OBC
	spec := map[string]interface{}{
		"generateBucketName": "collection-artifacts",
		"storageClassName":   "openshift-storage.noobaa.io",
	}
	obc.Object["spec"] = spec

	if err := ctrl.SetControllerReference(platform, obc, r.Scheme); err != nil {
		return fmt.Errorf("failed to set owner reference on OBC: %w", err)
	}

	if err := r.Create(ctx, obc); err != nil {
		return fmt.Errorf("failed to create ObjectBucketClaim: %w", err)
	}

	logger.Info("Created ObjectBucketClaim for collection artifacts")
	return nil
}

// ensureQuayRobotAccount creates or retrieves a robot account in Quay
func (r *DisconnectedPlatformReconciler) ensureQuayRobotAccount(ctx context.Context, quayURL, hostname, orgName, robotShortName string) (string, error) {
	logger := log.FromContext(ctx)

	// First, ensure there's a user to create the robot account
	// Check if we have saved admin credentials in a secret
	adminSecret := &corev1.Secret{}
	adminSecretName := "quay-admin-credentials"
	adminUser := "admin"
	adminPassword := ""

	err := r.Get(ctx, types.NamespacedName{Name: adminSecretName, Namespace: architectNamespace}, adminSecret)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return "", err
		}

		// Admin secret doesn't exist - try to initialize Quay with admin user
		logger.Info("Initializing Quay with admin user")
		adminPassword = generateRandomPassword(16)

		// Try user initialization endpoint
		initData := map[string]interface{}{
			"username":     adminUser,
			"password":     adminPassword,
			"email":        "admin@example.com",
			"access_token": true,
		}

		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: 10 * time.Second,
		}

		initBody, _ := json.Marshal(initData)
		resp, err := client.Post(quayURL+"/api/v1/user/initialize", "application/json", bytes.NewReader(initBody))
		if err != nil {
			logger.Info("Failed to initialize Quay user (may already exist)", "error", err.Error())
		} else {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				logger.Info("Successfully initialized Quay admin user")

				// Save admin credentials for future use
				adminSecret = &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      adminSecretName,
						Namespace: architectNamespace,
					},
					StringData: map[string]string{
						"username": adminUser,
						"password": adminPassword,
					},
					Type: corev1.SecretTypeOpaque,
				}
				if err := r.Create(ctx, adminSecret); err != nil {
					logger.Error(err, "failed to save admin credentials")
				}
			} else {
				bodyBytes, _ := io.ReadAll(resp.Body)
				logger.Info("Quay initialization response", "status", resp.StatusCode, "body", string(bodyBytes))
			}
		}
	} else {
		// Load saved admin credentials
		adminPassword = string(adminSecret.Data["password"])
		if savedUser := string(adminSecret.Data["username"]); savedUser != "" {
			adminUser = savedUser
		}
	}

	// If we still don't have admin password, generate one and try to create user directly
	if adminPassword == "" {
		logger.Info("No admin credentials available, attempting to create user via API")
		adminPassword = generateRandomPassword(16)
	}

	// Create organization if it doesn't exist
	if err := r.ensureQuayOrganization(ctx, quayURL, adminUser, adminPassword, orgName); err != nil {
		logger.Error(err, "failed to ensure Quay organization", "org", orgName)
	}

	// Create or get robot account
	robotFullName := orgName + "+" + robotShortName
	robotToken, err := r.createQuayRobotAccount(ctx, quayURL, adminUser, adminPassword, orgName, robotShortName)
	if err != nil {
		return "", fmt.Errorf("failed to create robot account: %w", err)
	}

	// Save robot credentials to a secret for reuse
	robotSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "quay-robot-credentials",
			Namespace: architectNamespace,
		},
		StringData: map[string]string{
			"username": robotFullName,
			"token":    robotToken,
		},
		Type: corev1.SecretTypeOpaque,
	}

	existing := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(robotSecret), existing); err == nil {
		// Update existing
		robotSecret.ResourceVersion = existing.ResourceVersion
		if err := r.Update(ctx, robotSecret); err != nil {
			logger.Error(err, "failed to update robot credentials secret")
		}
	} else if apierrors.IsNotFound(err) {
		// Create new
		if err := r.Create(ctx, robotSecret); err != nil {
			logger.Error(err, "failed to create robot credentials secret")
		}
	}

	logger.Info("Robot account ready", "robot", robotFullName)
	return robotToken, nil
}

// ensureQuayOrganization creates an organization in Quay if it doesn't exist
func (r *DisconnectedPlatformReconciler) ensureQuayOrganization(ctx context.Context, quayURL, username, password, orgName string) error {
	logger := log.FromContext(ctx)

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Timeout: 10 * time.Second,
	}

	// Check if organization exists
	req, _ := http.NewRequest("GET", quayURL+"/api/v1/organization/"+orgName, nil)
	req.SetBasicAuth(username, password)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		logger.Info("Quay organization already exists", "org", orgName)
		return nil
	}

	// Create organization
	logger.Info("Creating Quay organization", "org", orgName)
	orgData := map[string]interface{}{
		"name":  orgName,
		"email": "mirror@example.com",
	}

	orgBody, _ := json.Marshal(orgData)
	req, _ = http.NewRequest("POST", quayURL+"/api/v1/organization/", bytes.NewReader(orgBody))
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/json")

	resp, err = client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create organization: %s (status %d)", string(bodyBytes), resp.StatusCode)
	}

	logger.Info("Successfully created Quay organization", "org", orgName)
	return nil
}

// createQuayRobotAccount creates a robot account in Quay and returns its token
func (r *DisconnectedPlatformReconciler) createQuayRobotAccount(ctx context.Context, quayURL, username, password, orgName, robotName string) (string, error) {
	logger := log.FromContext(ctx)

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Timeout: 10 * time.Second,
	}

	// Check if robot already exists
	robotFullName := orgName + "+" + robotName
	req, _ := http.NewRequest("GET", quayURL+"/api/v1/organization/"+orgName+"/robots/"+robotName, nil)
	req.SetBasicAuth(username, password)

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		// Robot exists, get its token
		var robotData map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&robotData); err != nil {
			return "", err
		}

		if token, ok := robotData["token"].(string); ok && token != "" {
			logger.Info("Using existing robot account", "robot", robotFullName)
			return token, nil
		}
	}

	// Create new robot account
	logger.Info("Creating robot account", "robot", robotFullName)
	robotData := map[string]interface{}{
		"description": "Mirror operator robot account for image mirroring",
	}

	robotBody, _ := json.Marshal(robotData)
	req, _ = http.NewRequest("PUT", quayURL+"/api/v1/organization/"+orgName+"/robots/"+robotName, bytes.NewReader(robotBody))
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/json")

	resp, err = client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("failed to create robot account: %s (status %d)", string(bodyBytes), resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	token, ok := result["token"].(string)
	if !ok || token == "" {
		return "", fmt.Errorf("robot account created but no token returned")
	}

	logger.Info("Successfully created robot account", "robot", robotFullName)
	return token, nil
}

// mergeQuayCredentialsIntoPullSecret adds Quay robot credentials to the pull-secret
func (r *DisconnectedPlatformReconciler) mergeQuayCredentialsIntoPullSecret(ctx context.Context, hostname, username, token string) error {
	logger := log.FromContext(ctx)

	pullSecret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: "pull-secret", Namespace: architectNamespace}, pullSecret); err != nil {
		return fmt.Errorf("failed to get pull-secret: %w", err)
	}

	// Parse existing dockerconfigjson
	var dockerConfig map[string]interface{}
	if pullSecret.Data[".dockerconfigjson"] != nil {
		if err := json.Unmarshal(pullSecret.Data[".dockerconfigjson"], &dockerConfig); err != nil {
			return fmt.Errorf("failed to parse dockerconfigjson: %w", err)
		}
	} else {
		dockerConfig = map[string]interface{}{
			"auths": make(map[string]interface{}),
		}
	}

	// Add Quay credentials
	auths, ok := dockerConfig["auths"].(map[string]interface{})
	if !ok {
		auths = make(map[string]interface{})
		dockerConfig["auths"] = auths
	}

	// Create auth string: base64(username:token)
	authString := username + ":" + token
	authEncoded := map[string]interface{}{
		"auth": authString,
	}

	auths[hostname] = authEncoded
	logger.Info("Adding Quay registry to pull-secret", "registry", hostname)

	// Marshal back to JSON
	updatedConfig, err := json.Marshal(dockerConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal updated dockerconfig: %w", err)
	}

	pullSecret.Data[".dockerconfigjson"] = updatedConfig

	if err := r.Update(ctx, pullSecret); err != nil {
		return fmt.Errorf("failed to update pull-secret: %w", err)
	}

	logger.Info("Successfully merged Quay credentials into pull-secret", "registry", hostname)
	return nil
}

// generateRandomPassword generates a random password
func generateRandomPassword(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	password := make([]byte, length)
	for i := range password {
		password[i] = charset[sha256.Sum256([]byte(fmt.Sprintf("%d%d", time.Now().UnixNano(), i)))[0]%byte(len(charset))]
	}
	return string(password)
}

// ensureQuayAPIToken ensures we have a Quay API token, bootstrapping if needed
func (r *DisconnectedPlatformReconciler) ensureQuayAPIToken(ctx context.Context) (string, error) {
	logger := log.FromContext(ctx)

	// Check if we already have an API token saved
	tokenSecret := &corev1.Secret{}
	tokenSecret.SetName("quay-api-token")
	tokenSecret.SetNamespace(architectNamespace)

	if err := r.Get(ctx, client.ObjectKeyFromObject(tokenSecret), tokenSecret); err == nil {
		// Token exists, return it
		if token, ok := tokenSecret.Data["token"]; ok && len(token) > 0 {
			logger.Info("Found existing Quay API token")
			return string(token), nil
		}
	}

	// No token found, need to bootstrap
	logger.Info("No Quay API token found, bootstrapping")
	token, err := r.bootstrapQuayAPIToken(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to bootstrap Quay API token: %w", err)
	}

	// Save token to secret for future use
	tokenSecret.Data = map[string][]byte{
		"token": []byte(token),
	}
	if err := r.Create(ctx, tokenSecret); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("failed to save API token: %w", err)
	}

	logger.Info("Successfully bootstrapped Quay API token")
	return token, nil
}

// bootstrapQuayAPIToken ensures admin user exists in Quay (deprecated - no longer used)
// Kept for backwards compatibility but we now use Python API directly for all operations
func (r *DisconnectedPlatformReconciler) bootstrapQuayAPIToken(ctx context.Context) (string, error) {
	// No longer needed - we use Python API directly for robot creation
	// Just return a dummy token to satisfy the interface
	return "unused", nil
}

// execInQuayPod executes a Python script in the Quay pod using proper client-go implementation
func (r *DisconnectedPlatformReconciler) execInQuayPod(ctx context.Context, namespace, pythonScript string) (string, error) {
	logger := log.FromContext(ctx)

	// Find Quay pod
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(namespace), client.MatchingLabels{"quay-component": "quay-app"}); err != nil {
		return "", fmt.Errorf("failed to list Quay pods: %w", err)
	}

	if len(podList.Items) == 0 {
		return "", fmt.Errorf("no Quay pods found")
	}

	pod := &podList.Items[0]
	podName := pod.Name

	// Find a ready container
	containerName := ""
	for _, container := range pod.Spec.Containers {
		containerName = container.Name
		break
	}
	if containerName == "" {
		return "", fmt.Errorf("no containers found in pod %s", podName)
	}

	logger.Info("Executing Python script in Quay pod", "pod", podName, "container", containerName)

	// Step 1: Copy script to pod using tar/exec (similar to kubectl cp)
	// We'll write the script directly via stdin with a command
	writeScriptCommand := []string{"sh", "-c", "cat > /tmp/bootstrap.py"}
	if err := r.execCommandInPod(ctx, namespace, podName, containerName, writeScriptCommand, strings.NewReader(pythonScript), io.Discard, io.Discard); err != nil {
		return "", fmt.Errorf("failed to copy script to pod: %w", err)
	}

	// Step 2: Execute the Python script
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	execCommand := []string{"python", "/tmp/bootstrap.py"}
	if err := r.execCommandInPod(ctx, namespace, podName, containerName, execCommand, nil, stdout, stderr); err != nil {
		return "", fmt.Errorf("failed to execute script: %w (stderr: %s)", err, stderr.String())
	}

	// Extract the last line which should be the token (previous lines are stderr messages)
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) == 0 {
		return "", fmt.Errorf("script returned no output")
	}
	token := strings.TrimSpace(lines[len(lines)-1])

	return token, nil
}

// execCommandInPod executes a command in a pod using the Kubernetes exec API
func (r *DisconnectedPlatformReconciler) execCommandInPod(ctx context.Context, namespace, podName, containerName string, command []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if r.ClientSet == nil || r.RESTConfig == nil {
		return fmt.Errorf("ClientSet or RESTConfig not initialized")
	}

	req := r.ClientSet.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   command,
			Stdin:     stdin != nil,
			Stdout:    stdout != nil,
			Stderr:    stderr != nil,
			TTY:       false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(r.RESTConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("failed to create executor: %w", err)
	}

	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
		Tty:    false,
	})
	if err != nil {
		return fmt.Errorf("failed to execute command: %w", err)
	}

	return nil
}

// ensureQuayOrganizationViaAPI creates organization using REST API
func (r *DisconnectedPlatformReconciler) ensureQuayOrganizationViaAPI(ctx context.Context, quayURL, apiToken, orgName string) error {
	logger := log.FromContext(ctx)

	// Check if organization exists
	checkURL := quayURL + "/api/v1/organization/" + orgName
	req, err := http.NewRequestWithContext(ctx, "GET", checkURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to check organization: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		logger.Info("Organization already exists", "org", orgName)
		return nil
	}

	// Create organization
	createURL := quayURL + "/api/v1/organization/"
	payload := map[string]interface{}{
		"name": orgName,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err = http.NewRequestWithContext(ctx, "POST", createURL, bytes.NewReader(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err = client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create organization: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create organization (status %d): %s", resp.StatusCode, string(body))
	}

	logger.Info("Successfully created organization", "org", orgName)
	return nil
}

// ensureQuayRobotViaAPI creates or retrieves a robot account using REST API
func (r *DisconnectedPlatformReconciler) ensureQuayRobotViaAPI(ctx context.Context, quayURL, apiToken, orgName, robotShortName string) (string, error) {
	logger := log.FromContext(ctx)

	// Create or update robot account
	robotURL := quayURL + "/api/v1/organization/" + orgName + "/robots/" + robotShortName
	payload := map[string]interface{}{
		"description": "Mirror operator robot account",
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", robotURL, bytes.NewReader(payloadBytes))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to create robot: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("failed to create robot (status %d): %s", resp.StatusCode, string(body))
	}

	// Parse response to get robot token
	var robotResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&robotResp); err != nil {
		return "", fmt.Errorf("failed to parse robot response: %w", err)
	}

	token, ok := robotResp["token"].(string)
	if !ok || token == "" {
		return "", fmt.Errorf("robot response missing token field")
	}

	logger.Info("Successfully created/updated robot account", "org", orgName, "robot", robotShortName)

	// Add robot to owners team for admin permissions
	if err := r.addRobotToOwnersTeam(ctx, quayURL, apiToken, orgName, robotShortName); err != nil {
		logger.Error(err, "failed to add robot to owners team, continuing anyway")
	}

	return token, nil
}

// addRobotToOwnersTeam adds robot to the owners team for admin permissions
func (r *DisconnectedPlatformReconciler) addRobotToOwnersTeam(ctx context.Context, quayURL, apiToken, orgName, robotShortName string) error {
	logger := log.FromContext(ctx)
	robotFullName := orgName + "+" + robotShortName

	teamURL := quayURL + "/api/v1/organization/" + orgName + "/team/owners/members/" + robotFullName
	req, err := http.NewRequestWithContext(ctx, "PUT", teamURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to add robot to team: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to add robot to team (status %d): %s", resp.StatusCode, string(body))
	}

	logger.Info("Successfully added robot to owners team", "robot", robotFullName)
	return nil
}

// saveQuayRobotCredentials saves robot credentials to a secret for reuse
func (r *DisconnectedPlatformReconciler) saveQuayRobotCredentials(ctx context.Context, robotName, robotToken string) error {
	secret := &corev1.Secret{}
	secret.SetName("quay-robot-credentials")
	secret.SetNamespace(architectNamespace)

	secret.Data = map[string][]byte{
		"username": []byte(robotName),
		"token":    []byte(robotToken),
	}

	// Create or update
	existing := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(secret), existing); err == nil {
		// Update existing
		secret.SetResourceVersion(existing.GetResourceVersion())
		return r.Update(ctx, secret)
	} else if apierrors.IsNotFound(err) {
		// Create new
		return r.Create(ctx, secret)
	} else {
		return err
	}
}

// ensureQuayRobotViaPython creates or retrieves a robot account using Python API in Quay pod
func (r *DisconnectedPlatformReconciler) ensureQuayRobotViaPython(ctx context.Context, orgName, robotShortName string) (string, error) {
	logger := log.FromContext(ctx)

	// Python script to create/get robot account and add to owners team
	pythonScript := `
import os
import sys
from app import app
from data import model
from data.database import configure

# Initialize database
configure(app.config)

org_name = "` + orgName + `"
robot_short_name = "` + robotShortName + `"
robot_full_name = org_name + "+" + robot_short_name

try:
    # Ensure initialization user exists
    init_username = "mirroroperator-init"
    init_email = "mirroroperator@example.com"
    try:
        init_user = model.user.get_user(init_username)
        print(f"Init user {init_username} already exists", file=sys.stderr)
    except:
        print(f"Creating init user {init_username}", file=sys.stderr)
        # Create initialization user (will be the org owner)
        init_user = model.user.create_user(init_username, init_email, "", auto_verify=True)
        init_user.verified = True
        init_user.save()
        print(f"Init user {init_username} created and verified", file=sys.stderr)

    # Get or create organization
    try:
        org = model.organization.get_organization(org_name)
        print(f"Organization {org_name} already exists", file=sys.stderr)
    except:
        print(f"Organization {org_name} not found, creating it", file=sys.stderr)
        # Create organization directly in database to avoid team member constraint issues
        from data.database import User
        org = User.create(username=org_name, email=init_email, verified=True, organization=True)
        print(f"Organization {org_name} created", file=sys.stderr)

        # Create owners team for the organization
        try:
            owners_team = model.team.create_team("owners", org, "admin", "Owners of the organization")
            print(f"Created owners team for organization", file=sys.stderr)
        except Exception as e:
            print(f"Warning: Could not create owners team: {e}", file=sys.stderr)

    # Check if robot already exists
    try:
        existing_robot = model.user.lookup_robot(robot_full_name)
        print(f"Robot {robot_full_name} already exists", file=sys.stderr)
        # Get the robot's token
        robot_token = model.user.retrieve_robot_token(existing_robot)
        print(robot_token)
    except:
        print(f"Creating robot {robot_full_name}", file=sys.stderr)
        # Create robot account - returns (robot, token) tuple
        robot_result = model.user.create_robot(robot_short_name, org, "Mirror operator robot account")
        # Extract robot and token from tuple
        if isinstance(robot_result, tuple):
            robot, robot_token = robot_result
        else:
            robot = robot_result
            robot_token = model.user.retrieve_robot_token(robot)

        # Grant robot admin access to organization - two step process
        try:
            owners_team = model.team.get_organization_team(org_name, "owners")

            # Step 1: Create team member entry
            from data.database import TeamMember
            team_member = TeamMember.create(user=robot, team=owners_team)
            print(f"Created team member entry for robot", file=sys.stderr)

            # Step 2: Set role to admin
            from data.database import TeamRole
            team_member.role = TeamRole.admin
            team_member.save()
            print(f"Set robot role to admin in owners team", file=sys.stderr)
        except Exception as e:
            print(f"Warning: Could not add robot to owners team: {e}", file=sys.stderr)

        print(robot_token)

except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)
`

	// Execute Python script in Quay pod
	output, err := r.execInQuayPod(ctx, architectNamespace, pythonScript)
	if err != nil {
		return "", fmt.Errorf("failed to execute robot creation script: %w", err)
	}

	token := strings.TrimSpace(output)
	if token == "" {
		return "", fmt.Errorf("robot creation script returned empty token")
	}

	logger.Info("Successfully ensured Quay robot account via Python", "org", orgName, "robot", robotShortName)
	return token, nil
}

// reconcileArtifactFileServer creates a file server deployment/service/route to serve collection bundles
// reconcileCollectionPipelineTemplate creates a reusable Pipeline template for collection workflows
func (r *DisconnectedPlatformReconciler) reconcileCollectionPipelineTemplate(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)

	// Only reconcile in connected mode
	if platform.Spec.Mode != mirrorv1.PlatformModeConnected {
		return nil
	}

	namespace := architectNamespace
	pipelineName := "collection-pipeline-template"

	// Import pipelinev1 types
	pipeline := &unstructured.Unstructured{}
	pipeline.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "tekton.dev",
		Version: "v1",
		Kind:    "Pipeline",
	})
	pipeline.SetName(pipelineName)
	pipeline.SetNamespace(namespace)

	// Define pipeline parameters
	params := []map[string]interface{}{
		{"name": "config-map-name", "type": "string", "description": "ConfigMap containing ImageSetConfiguration"},
		{"name": "mirror-image", "type": "string", "default": "quay.io/mathianasj/oc-mirror:v2"},
		{"name": "working-pvc-name", "type": "string", "description": "PVC for working directory/cache"},
		{"name": "intermediate-registry", "type": "string", "default": "", "description": "Intermediate registry for m2m workflow (empty = local cache)"},
		{"name": "has-keyless-signing", "type": "string", "default": "false", "description": "Enable keyless signing"},
		{"name": "fulcio-url", "type": "string", "default": ""},
		{"name": "rekor-url", "type": "string", "default": ""},
		{"name": "tuf-url", "type": "string", "default": ""},
		{"name": "oidc-issuer", "type": "string", "default": ""},
		{"name": "oidc-client-id", "type": "string", "default": ""},
		{"name": "has-tpa", "type": "string", "default": "false"},
		{"name": "tpa-host", "type": "string", "default": ""},
		{"name": "tpa-oidc-issuer", "type": "string", "default": ""},
		{"name": "tpa-oidc-client-id", "type": "string", "default": ""},
		{"name": "has-s3", "type": "string", "default": "false", "description": "Enable S3 storage output"},
		{"name": "s3-bucket", "type": "string", "default": ""},
		{"name": "s3-endpoint", "type": "string", "default": ""},
		{"name": "s3-region", "type": "string", "default": ""},
		{"name": "s3-secret-name", "type": "string", "default": ""},
	}

	// Define workspaces
	workspaces := []map[string]interface{}{
		{"name": "config", "description": "ImageSetConfiguration ConfigMap"},
		{"name": "pull-secret", "description": "Registry pull secret"},
		{"name": "output", "description": "Working PVC for cache and temp files"},
		{"name": "oidc-secret", "description": "OIDC client secret for keyless signing", "optional": true},
		{"name": "tpa-oidc-secret", "description": "TPA OIDC secret for SBOM upload", "optional": true},
		{"name": "cosign-key", "description": "Cosign private key", "optional": true},
	}

	// Define tasks - I'll create a simplified version first, then we can expand
	tasks := r.buildPipelineTasks()

	pipelineSpec := map[string]interface{}{
		"params":     params,
		"workspaces": workspaces,
		"tasks":      tasks,
	}

	// Marshal to JSON and back to convert []map[string]interface{} into a format unstructured can handle
	specJSON, err := json.Marshal(pipelineSpec)
	if err != nil {
		return fmt.Errorf("failed to marshal pipeline spec: %w", err)
	}

	var specMap map[string]interface{}
	if err := json.Unmarshal(specJSON, &specMap); err != nil {
		return fmt.Errorf("failed to unmarshal pipeline spec: %w", err)
	}

	pipeline.Object["spec"] = specMap

	if err := ctrl.SetControllerReference(platform, pipeline, r.Scheme); err != nil {
		return fmt.Errorf("failed to set owner reference on pipeline: %w", err)
	}

	// Create or update Pipeline
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(pipeline.GroupVersionKind())
	if err := r.Get(ctx, client.ObjectKeyFromObject(pipeline), existing); err == nil {
		pipeline.SetResourceVersion(existing.GetResourceVersion())
		if err := r.Update(ctx, pipeline); err != nil {
			return fmt.Errorf("failed to update collection pipeline template: %w", err)
		}
		logger.Info("Updated collection pipeline template")
	} else if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, pipeline); err != nil {
			return fmt.Errorf("failed to create collection pipeline template: %w", err)
		}
		logger.Info("Created collection pipeline template")
	} else {
		return err
	}

	return nil
}

// buildPipelineTasks constructs the task definitions for the collection pipeline
// All tasks are defined with 'when' expressions - Tekton will skip tasks based on params
func (r *DisconnectedPlatformReconciler) buildPipelineTasks() []map[string]interface{} {
	return []map[string]interface{}{
		// Task 1: dry-run (only for m2m workflow)
		{
			"name": "dry-run",
			"when": []map[string]interface{}{
				{"input": "$(params.intermediate-registry)", "operator": "notin", "values": []string{""}},
			},
			"taskSpec": map[string]interface{}{
				"steps": []map[string]interface{}{
					{
						"name":    "dry-run",
						"image":   "$(params.mirror-image)",
						"command": []string{"/bin/sh", "-c"},
						"args": []string{`
set -e
oc-mirror \
  --v2 \
  --config=/workspace/config/imageset-config.yaml \
  --authfile=/workspace/pull-secret/.dockerconfigjson \
  --dry-run \
  --workspace=file:///workspace/output \
  docker://$(params.intermediate-registry)
`},
					},
				},
			},
			"workspaces": []map[string]interface{}{
				{"name": "config"},
				{"name": "pull-secret"},
				{"name": "output"},
			},
		},

		// Task 2: mirror-to-intermediate (only for m2m workflow)
		{
			"name":     "mirror-to-intermediate",
			"runAfter": []string{"dry-run"},
			"when": []map[string]interface{}{
				{"input": "$(params.intermediate-registry)", "operator": "notin", "values": []string{""}},
			},
			"taskSpec": map[string]interface{}{
				"steps": []map[string]interface{}{
					{
						"name":    "mirror-to-intermediate",
						"image":   "$(params.mirror-image)",
						"command": []string{"/bin/sh", "-c"},
						"args": []string{`
set -e

MAX_RETRIES=3
RETRY_COUNT=0
SUCCESS=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ] && [ $SUCCESS -eq 0 ]; do
  if [ $RETRY_COUNT -gt 0 ]; then
    echo "=== Retry attempt $RETRY_COUNT of $((MAX_RETRIES - 1)) ==="
    sleep 10
  fi

  OUTPUT_FILE="/tmp/oc-mirror-output-$$.log"
  oc-mirror \
    --v2 \
    --config=/workspace/config/imageset-config.yaml \
    --authfile=/workspace/pull-secret/.dockerconfigjson \
    --cache-dir=/workspace/output/.cache \
    --workspace=file:///workspace/output \
    docker://$(params.intermediate-registry) 2>&1 | tee "$OUTPUT_FILE" || true

  if grep -q "✓.*release images mirrored successfully" "$OUTPUT_FILE" || \
     grep -q "✓.*operator images mirrored successfully" "$OUTPUT_FILE" || \
     grep -q "✓.*additional images mirrored successfully" "$OUTPUT_FILE"; then
    echo "=== Mirror completed successfully (detected success message) ==="
    SUCCESS=1
    break
  else
    echo "=== Mirror may have failed, no success indicator found ==="
    RETRY_COUNT=$((RETRY_COUNT + 1))
  fi

  rm -f "$OUTPUT_FILE"
done

if [ $SUCCESS -eq 0 ]; then
  echo "=== Mirror failed after $MAX_RETRIES attempts ==="
  exit 1
fi
`},
					},
				},
			},
			"workspaces": []map[string]interface{}{
				{"name": "config"},
				{"name": "pull-secret"},
				{"name": "output"},
			},
		},

		// Task 3: syft-sbom (runs after mirror-to-intermediate for m2m, or oc-mirror for local)
		{
			"name":     "syft-sbom",
			"runAfter": []string{"mirror-to-intermediate", "oc-mirror"},
			"when": []map[string]interface{}{
				{"input": "$(params.has-tpa)", "operator": "in", "values": []string{"true"}},
			},
			"taskSpec": map[string]interface{}{
				"steps": []map[string]interface{}{
					{
						"name":    "generate-sbom",
						"image":   "$(params.mirror-image)",
						"command": []string{"/bin/sh", "-c"},
						"args": []string{`
set -e
echo "=== Generating SBOM with Syft ==="

# Set up registry authentication
export HOME=/tmp
mkdir -p $HOME/.docker
cp /workspace/pull-secret/.dockerconfigjson $HOME/.docker/config.json
export DOCKER_CONFIG=$HOME/.docker

MAPPING_FILE="/workspace/output/working-dir/dry-run/mapping.txt"
if [ ! -f "$MAPPING_FILE" ]; then
  MAPPING_FILE=$(find /workspace/output/working-dir -name "mapping.txt" -type f 2>/dev/null | head -1)
fi

if [ -z "$MAPPING_FILE" ] || [ ! -f "$MAPPING_FILE" ]; then
  echo "No mapping.txt found, skipping SBOM generation"
  mkdir -p /workspace/output/sboms
  exit 0
fi

echo "Found mapping file: $MAPPING_FILE"
image_count=$(grep -c -v '^#' "$MAPPING_FILE" || echo 0)
echo "Total images: $image_count"

# Create SBOM cache and output directories
SBOM_CACHE_DIR="/workspace/output/sbom-cache"
mkdir -p "$SBOM_CACHE_DIR"
mkdir -p /workspace/output/sboms

# Determine if scanning from intermediate registry or local cache
if [ -n "$(params.intermediate-registry)" ]; then
  echo "Scanning from intermediate registry: $(params.intermediate-registry)"
  SCAN_FROM_REGISTRY="$(params.intermediate-registry)"
else
  echo "Scanning from local cache"
  SCAN_FROM_REGISTRY=""
fi

# Generate SBOM for each image with caching
current=0
cached=0
scanned=0
while IFS='=' read -r source dest; do
  [ -z "$source" ] || [[ "$source" =~ ^# ]] && continue

  current=$((current + 1))
  dest_no_proto="${dest#docker://}"

  # Extract digest for cache lookup
  DIGEST=$(echo "$dest" | grep -oP 'sha256:[a-f0-9]+' || echo "")
  if [ -z "$DIGEST" ]; then
    DIGEST=$(echo "$source" | grep -oP 'sha256:[a-f0-9]+' || echo "")
  fi

  SAFE_NAME=$(echo "$dest_no_proto" | tr '/:@' '_')
  OUTPUT_FILE="/workspace/output/sboms/${SAFE_NAME}.spdx.json"

  # Check cache first if we have a digest
  if [ -n "$DIGEST" ]; then
    CACHE_FILE="$SBOM_CACHE_DIR/$(echo "$DIGEST" | tr ':' '_').json"
    if [ -f "$CACHE_FILE" ]; then
      cp "$CACHE_FILE" "$OUTPUT_FILE"
      pkg_count=$(jq '.packages | length // 0' "$CACHE_FILE" 2>/dev/null || echo 0)
      echo "  [$current/$image_count] ✓ Cached: $dest_no_proto ($pkg_count packages)"
      cached=$((cached + 1))
      continue
    fi
  fi

  # Scan image
  if [ -n "$SCAN_FROM_REGISTRY" ]; then
    IMAGE="registry:${dest_no_proto//localhost:55000/$SCAN_FROM_REGISTRY}"
  else
    if [ -z "$DIGEST" ]; then
      echo "  [$current/$image_count] Skipping (no digest): $source"
      continue
    fi
    IMAGE="/workspace/output/.cache/blobs/sha256/${DIGEST#sha256:}"
  fi

  echo "  [$current/$image_count] Scanning: $dest_no_proto"
  if syft "$IMAGE" -o spdx-json="$OUTPUT_FILE" 2>/dev/null; then
    scanned=$((scanned + 1))
    # Cache the SBOM by digest for future runs
    if [ -n "$DIGEST" ]; then
      cp "$OUTPUT_FILE" "$SBOM_CACHE_DIR/$(echo "$DIGEST" | tr ':' '_').json"
    fi
  else
    echo "    Failed to scan"
  fi
done < "$MAPPING_FILE"

echo "=== SBOM generation complete ==="
echo "Total: $image_count | Cached: $cached | Scanned: $scanned"
ls -lh /workspace/output/sboms/ | head -20
`},
						"env": []map[string]interface{}{
							{"name": "INTERMEDIATE_REGISTRY", "value": "$(params.intermediate-registry)"},
							{"name": "SYFT_CACHE_DIR", "value": "/workspace/output/syft-cache"},
						},
					},
				},
			},
			"workspaces": []map[string]interface{}{
				{"name": "output"},
				{"name": "pull-secret"},
			},
		},

		// Task 4: sign-images (only for keyless signing in m2m workflow)
		{
			"name":     "sign-images",
			"runAfter": []string{"syft-sbom"},
			"when": []map[string]interface{}{
				{"input": "$(params.has-keyless-signing)", "operator": "in", "values": []string{"true"}},
				{"input": "$(params.intermediate-registry)", "operator": "notin", "values": []string{""}},
			},
			"taskSpec": map[string]interface{}{
				"steps": []map[string]interface{}{
					{
						"name":    "sign-images",
						"image":   "$(params.mirror-image)",
						"command": []string{"/bin/sh", "-c"},
						"args": []string{`
set -e

# Set up registry authentication
export HOME=/tmp
mkdir -p $HOME/.docker
cp /workspace/pull-secret/.dockerconfigjson $HOME/.docker/config.json
export DOCKER_CONFIG=$HOME/.docker

echo "=== Initializing TUF root ==="
cosign initialize --mirror="$(params.tuf-url)" --root="$(params.tuf-url)/root.json"

echo "=== Signing images with cosign keyless ==="

MAPPING_FILE="/workspace/output/working-dir/dry-run/mapping.txt"
if [ ! -f "$MAPPING_FILE" ]; then
  MAPPING_FILE=$(find /workspace/output/working-dir -name "mapping.txt" -type f 2>/dev/null | head -1)
fi

if [ -z "$MAPPING_FILE" ] || [ ! -f "$MAPPING_FILE" ]; then
  echo "No mapping.txt found, skipping image signing"
  exit 0
fi

# Get OIDC token
OIDC_TOKEN=$(curl -s -X POST "$(params.oidc-issuer)/protocol/openid-connect/token" \
  -d "client_id=$(params.oidc-client-id)" \
  -d "client_secret=$(cat /workspace/oidc-secret/clientSecret)" \
  -d "grant_type=client_credentials" \
  -d "audience=trusted-artifact-signer" | jq -r '.access_token')

if [ -z "$OIDC_TOKEN" ] || [ "$OIDC_TOKEN" = "null" ]; then
  echo "ERROR: Failed to get OIDC token"
  exit 1
fi

export COSIGN_EXPERIMENTAL=1

# Sign each image in intermediate registry
signed_count=0
total_images=$(grep -c -v '^#' "$MAPPING_FILE" || echo 0)
echo "Processing $total_images images..."

while IFS='=' read -r source dest; do
  [ -z "$source" ] || [[ "$source" =~ ^# ]] && continue

  # Replace localhost:55000 with intermediate registry in dest path
  dest_no_proto="${dest#docker://}"
  image_ref="${dest_no_proto//localhost:55000/\$(params.intermediate-registry)}"

  echo "Signing $image_ref"
  if cosign sign \
    --fulcio-url=$(params.fulcio-url) \
    --rekor-url=$(params.rekor-url) \
    --oidc-issuer=$(params.oidc-issuer) \
    --identity-token="$OIDC_TOKEN" \
    --yes \
    "$image_ref"; then
    signed_count=$((signed_count + 1))
  else
    echo "Failed to sign $image_ref"
  fi
done < "$MAPPING_FILE"

echo "=== Image signing complete ==="
echo "Signed $signed_count of $total_images images"
`},
						"env": []map[string]interface{}{
							{"name": "COSIGN_EXPERIMENTAL", "value": "1"},
						},
					},
				},
			},
			"workspaces": []map[string]interface{}{
				{"name": "output"},
				{"name": "oidc-secret"},
				{"name": "pull-secret"},
			},
		},

		// Task 5: oc-mirror (local cache workflow - no intermediate registry)
		{
			"name": "oc-mirror",
			"when": []map[string]interface{}{
				{"input": "$(params.intermediate-registry)", "operator": "in", "values": []string{""}},
			},
			"taskSpec": map[string]interface{}{
				"steps": []map[string]interface{}{
					{
						"name":    "oc-mirror",
						"image":   "$(params.mirror-image)",
						"command": []string{"/bin/sh", "-c"},
						"args": []string{`
set -e
oc-mirror \
  --v2 \
  --config=/workspace/config/imageset-config.yaml \
  --authfile=/workspace/pull-secret/.dockerconfigjson \
  --workspace=file:///workspace/output
`},
					},
				},
			},
			"workspaces": []map[string]interface{}{
				{"name": "config"},
				{"name": "pull-secret"},
				{"name": "output"},
			},
		},

		// Task 6: mirror-from-intermediate (pull from intermediate to disk with signatures)
		{
			"name":     "mirror-from-intermediate",
			"runAfter": []string{"sign-images"},
			"when": []map[string]interface{}{
				{"input": "$(params.intermediate-registry)", "operator": "notin", "values": []string{""}},
			},
			"taskSpec": map[string]interface{}{
				"steps": []map[string]interface{}{
					{
						"name":    "mirror-from-intermediate",
						"image":   "$(params.mirror-image)",
						"command": []string{"/bin/sh", "-c"},
						"args": []string{`
set -e
echo "=== Creating ImageSetConfiguration to mirror FROM intermediate registry ==="

MAPPING_FILE="/workspace/output/working-dir/dry-run/mapping.txt"
if [ ! -f "$MAPPING_FILE" ]; then
  MAPPING_FILE=$(find /workspace/output/working-dir -name "mapping.txt" -type f 2>/dev/null | head -1)
fi

if [ -z "$MAPPING_FILE" ] || [ ! -f "$MAPPING_FILE" ]; then
  echo "ERROR: mapping.txt not found from dry-run"
  exit 1
fi

cat > /tmp/imageset-from-intermediate.yaml <<EOF
apiVersion: mirror.openshift.io/v1alpha2
kind: ImageSetConfiguration
mirror:
  platform:
    graph: false
    architectures:
      - amd64
  additionalImages:
EOF

while IFS='=' read -r source dest; do
  [ -z "$source" ] || [[ "$source" =~ ^# ]] && continue

  # Remove docker:// prefix from dest and replace localhost:55000 with intermediate registry
  dest_no_proto="${dest#docker://}"
  intermediate_ref="${dest_no_proto//localhost:55000/\$(params.intermediate-registry)}"

  echo "    - name: $intermediate_ref" >> /tmp/imageset-from-intermediate.yaml
done < "$MAPPING_FILE"

echo "=== Generated ImageSetConfiguration ==="
cat /tmp/imageset-from-intermediate.yaml

echo "=== Mirroring FROM intermediate registry TO disk ==="
oc-mirror \
  --v2 \
  --config=/tmp/imageset-from-intermediate.yaml \
  --authfile=/workspace/pull-secret/.dockerconfigjson \
  file:///workspace/output
`},
					},
				},
			},
			"workspaces": []map[string]interface{}{
				{"name": "output"},
				{"name": "pull-secret"},
			},
		},

		// Task 7: sign-bundles (sign the tar file)
		{
			"name":     "sign-bundles",
			"runAfter": []string{"mirror-from-intermediate", "oc-mirror"},
			"when": []map[string]interface{}{
				{"input": "$(params.has-keyless-signing)", "operator": "in", "values": []string{"true"}},
			},
			"taskSpec": map[string]interface{}{
				"steps": []map[string]interface{}{
					{
						"name":    "sign-bundles",
						"image":   "$(params.mirror-image)",
						"command": []string{"/bin/sh", "-c"},
						"args": []string{`
set -e
echo "=== Initializing TUF root ==="
cosign initialize --mirror="$(params.tuf-url)" --root="$(params.tuf-url)/root.json"

echo "=== Signing tar bundles with cosign ==="

# Get OIDC token
OIDC_TOKEN=$(curl -s -X POST "$(params.oidc-issuer)/protocol/openid-connect/token" \
  -d "client_id=$(params.oidc-client-id)" \
  -d "client_secret=$(cat /workspace/oidc-secret/clientSecret)" \
  -d "grant_type=client_credentials" \
  -d "audience=trusted-artifact-signer" | jq -r '.access_token')

export COSIGN_EXPERIMENTAL=1

# Find and sign all tar files
find /workspace/output -name "*.tar" | while read tarfile; do
  echo "Signing $tarfile"
  cosign sign-blob \
    --fulcio-url=$(params.fulcio-url) \
    --rekor-url=$(params.rekor-url) \
    --oidc-issuer=$(params.oidc-issuer) \
    --identity-token="$OIDC_TOKEN" \
    --yes \
    --bundle="${tarfile}.bundle" \
    "$tarfile" > "${tarfile}.sig" || echo "Failed to sign $tarfile"
done

echo "=== Bundle signing complete ==="
`},
					},
				},
			},
			"workspaces": []map[string]interface{}{
				{"name": "output"},
				{"name": "oidc-secret"},
			},
		},

		// Task 8: upload-sbom (upload SBOMs to TPA)
		{
			"name":     "upload-sbom",
			"runAfter": []string{"sign-bundles"},
			"when": []map[string]interface{}{
				{"input": "$(params.has-tpa)", "operator": "in", "values": []string{"true"}},
			},
			"taskSpec": map[string]interface{}{
				"steps": []map[string]interface{}{
					{
						"name":    "upload-sbom",
						"image":   "$(params.mirror-image)",
						"command": []string{"/bin/sh", "-c"},
						"args": []string{`
set -e
echo "=== Uploading SBOMs to TPA ==="

# Get TPA OIDC token
TPA_TOKEN=$(curl -s -X POST "$(params.tpa-oidc-issuer)/protocol/openid-connect/token" \
  -d "client_id=$(params.tpa-oidc-client-id)" \
  -d "client_secret=$(cat /workspace/tpa-oidc-secret/clientSecret)" \
  -d "grant_type=client_credentials" | jq -r '.access_token')

# Upload each SBOM
find /workspace/output/sboms -name "*.spdx.json" | while read sbom; do
  echo "Uploading $sbom"
  curl -X POST \
    -H "Authorization: Bearer $TPA_TOKEN" \
    -H "Content-Type: application/json" \
    -d @"$sbom" \
    "https://$(params.tpa-host)/api/v1/sbom" || echo "Failed to upload $sbom"
done

echo "=== SBOM upload complete ==="
`},
					},
				},
			},
			"workspaces": []map[string]interface{}{
				{"name": "output"},
				{"name": "tpa-oidc-secret"},
			},
		},

		// Task 9: upload-to-s3 (upload artifacts to S3 bucket)
		{
			"name":     "upload-to-s3",
			"runAfter": []string{"upload-sbom", "sign-bundles"},
			"taskSpec": map[string]interface{}{
				"steps": []map[string]interface{}{
					{
						"name":    "upload-artifacts",
						"image":   "amazon/aws-cli:latest",
						"command": []string{"/bin/sh", "-c"},
						"args": []string{`
set -ex
echo "Uploading artifacts to S3..."

# Collection name from working-pvc-name (format: collection-storage-<name>)
COLLECTION_NAME=$(echo "$(params.working-pvc-name)" | sed 's/collection-storage-//')

# Upload all .tar, .sig, and .bundle files
find /workspace/output -maxdepth 1 \( -name "*.tar" -o -name "*.sig" -o -name "*.bundle" \) | while read file; do
  FILENAME=$(basename "$file")
  echo "Uploading $FILENAME to s3://$(params.s3-bucket)/$COLLECTION_NAME/"
  aws s3 cp "$file" "s3://$(params.s3-bucket)/$COLLECTION_NAME/$FILENAME" \
    --endpoint-url="$(params.s3-endpoint)" \
    --region="$(params.s3-region)"
done

echo "=== Upload complete ==="
aws s3 ls "s3://$(params.s3-bucket)/$COLLECTION_NAME/" --endpoint-url="$(params.s3-endpoint)" --region="$(params.s3-region)"
`},
						"env": []map[string]interface{}{
							{
								"name": "AWS_ACCESS_KEY_ID",
								"valueFrom": map[string]interface{}{
									"secretKeyRef": map[string]interface{}{
										"name": "$(params.s3-secret-name)",
										"key":  "AWS_ACCESS_KEY_ID",
									},
								},
							},
							{
								"name": "AWS_SECRET_ACCESS_KEY",
								"valueFrom": map[string]interface{}{
									"secretKeyRef": map[string]interface{}{
										"name": "$(params.s3-secret-name)",
										"key":  "AWS_SECRET_ACCESS_KEY",
									},
								},
							},
						},
					},
				},
			},
			"workspaces": []map[string]interface{}{
				{"name": "output"},
			},
		},
	}
}

// reconcileArtifactFileServer creates a single file server that mounts artifacts PVCs from completed CollectionPipelines
func (r *DisconnectedPlatformReconciler) reconcileArtifactFileServer(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) error {
	logger := log.FromContext(ctx)

	namespace := architectNamespace
	name := "artifact-fileserver"

	// Get all CollectionPipelines to find completed ones
	pipelines := &mirrorv1.CollectionPipelineList{}
	if err := r.List(ctx, pipelines, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("failed to list CollectionPipelines: %w", err)
	}

	// Build volume mounts only for completed pipelines with artifacts PVCs
	var volumes []corev1.Volume
	var volumeMounts []corev1.VolumeMount

	for _, pipeline := range pipelines.Items {
		// Only mount if pipeline is Complete or Succeeded
		if pipeline.Status.Phase != "Complete" && pipeline.Status.Phase != "Succeeded" {
			continue
		}

		// Determine artifacts PVC name
		var artifactsPVCName string
		if pipeline.Spec.Incremental && pipeline.Spec.BaseVersion != "" {
			artifactsPVCName = fmt.Sprintf("collection-artifacts-%s", pipeline.Spec.BaseVersion)
		} else {
			artifactsPVCName = fmt.Sprintf("collection-artifacts-%s", pipeline.Name)
		}

		// Check if PVC exists and is Bound
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.Get(ctx, types.NamespacedName{Name: artifactsPVCName, Namespace: namespace}, pvc); err != nil {
			logger.Info("Skipping PVC (not found)", "pvc", artifactsPVCName, "pipeline", pipeline.Name)
			continue
		}

		if pvc.Status.Phase != corev1.ClaimBound {
			logger.Info("Skipping PVC (not bound)", "pvc", artifactsPVCName, "phase", pvc.Status.Phase)
			continue
		}

		// Check if any pods are currently using this PVC (to avoid conflicts with running tasks)
		pods := &corev1.PodList{}
		if err := r.List(ctx, pods, client.InNamespace(namespace)); err == nil {
			pvcInUse := false
			for _, pod := range pods.Items {
				if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending {
					for _, vol := range pod.Spec.Volumes {
						if vol.PersistentVolumeClaim != nil && vol.PersistentVolumeClaim.ClaimName == artifactsPVCName {
							logger.Info("Skipping PVC (in use by pod)", "pvc", artifactsPVCName, "pod", pod.Name)
							pvcInUse = true
							break
						}
					}
					if pvcInUse {
						break
					}
				}
			}
			if pvcInUse {
				continue
			}
		}

		// Mount this PVC at /opt/app-root/src/<pipeline-name>
		volumeName := fmt.Sprintf("artifacts-%s", pipeline.Name)
		volumes = append(volumes, corev1.Volume{
			Name: volumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: artifactsPVCName,
					ReadOnly:  true,
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      volumeName,
			MountPath: fmt.Sprintf("/opt/app-root/src/%s", pipeline.Name),
			ReadOnly:  true,
		})
	}

	if len(volumes) == 0 {
		logger.Info("No completed collections with bound artifacts PVCs, skipping file server")
		// TODO: Delete existing file server deployment if it exists
		return nil
	}

	// Create nginx deployment
	replicas := int32(1)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": name},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": name},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "nginx",
							Image: "registry.access.redhat.com/ubi9/nginx-122:latest",
							Ports: []corev1.ContainerPort{
								{ContainerPort: 8080, Protocol: corev1.ProtocolTCP},
							},
							VolumeMounts: volumeMounts,
						},
					},
					Volumes: volumes,
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(platform, deployment, r.Scheme); err != nil {
		return fmt.Errorf("failed to set owner reference on deployment: %w", err)
	}

	// Create or update deployment
	existing := &appsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(deployment), existing); err == nil {
		deployment.SetResourceVersion(existing.GetResourceVersion())
		if err := r.Update(ctx, deployment); err != nil {
			return fmt.Errorf("failed to update artifact fileserver deployment: %w", err)
		}
		logger.Info("Updated artifact fileserver deployment", "volumes", len(volumes))
	} else if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, deployment); err != nil {
			return fmt.Errorf("failed to create artifact fileserver deployment: %w", err)
		}
		logger.Info("Created artifact fileserver deployment", "volumes", len(volumes))
	} else {
		return err
	}

	// Create Service
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": name},
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
		return fmt.Errorf("failed to set owner reference on service: %w", err)
	}

	existingSvc := &corev1.Service{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(service), existingSvc); err == nil {
		service.SetResourceVersion(existingSvc.GetResourceVersion())
		service.Spec.ClusterIP = existingSvc.Spec.ClusterIP
		if err := r.Update(ctx, service); err != nil {
			return fmt.Errorf("failed to update artifact fileserver service: %w", err)
		}
	} else if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, service); err != nil {
			return fmt.Errorf("failed to create artifact fileserver service: %w", err)
		}
	} else {
		return err
	}

	// Create Route
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "route.openshift.io",
		Version: "v1",
		Kind:    "Route",
	})
	route.SetName(name)
	route.SetNamespace(namespace)

	routeSpec := map[string]interface{}{
		"to": map[string]interface{}{
			"kind": "Service",
			"name": name,
		},
		"port": map[string]interface{}{
			"targetPort": "http",
		},
		"tls": map[string]interface{}{
			"termination": "edge",
		},
	}

	if err := unstructured.SetNestedMap(route.Object, routeSpec, "spec"); err != nil {
		return fmt.Errorf("failed to set route spec: %w", err)
	}

	if err := ctrl.SetControllerReference(platform, route, r.Scheme); err != nil {
		return fmt.Errorf("failed to set owner reference on route: %w", err)
	}

	existingRoute := &unstructured.Unstructured{}
	existingRoute.SetGroupVersionKind(route.GroupVersionKind())
	if err := r.Get(ctx, client.ObjectKeyFromObject(route), existingRoute); err == nil {
		route.SetResourceVersion(existingRoute.GetResourceVersion())
		if err := r.Update(ctx, route); err != nil {
			return fmt.Errorf("failed to update artifact fileserver route: %w", err)
		}
	} else if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, route); err != nil {
			return fmt.Errorf("failed to create artifact fileserver route: %w", err)
		}
	} else {
		return err
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

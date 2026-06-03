package controller

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

// RHTASHealthCheckReconciler performs periodic health checks on RHTAS components
type RHTASHealthCheckReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=rhtas.redhat.com,resources=securesigns,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=k8s.keycloak.org,resources=keycloaks,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch

func (r *RHTASHealthCheckReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Get the Securesign resource
	securesign := &unstructured.Unstructured{}
	securesign.SetGroupVersionKind(securesignGVK)
	if err := r.Get(ctx, req.NamespacedName, securesign); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.Info("Running periodic RHTAS health checks")

	healthIssues := []string{}

	// Health Check 1: TUF keys validation
	if err := r.validateTUFKeys(ctx, securesign); err != nil {
		logger.Error(err, "TUF keys validation failed")
		healthIssues = append(healthIssues, "TUF keys contain problematic entries")
	}

	// Health Check 2: Fulcio connectivity to Keycloak
	if err := r.validateFulcioKeycloakConnection(ctx); err != nil {
		logger.Error(err, "Fulcio-Keycloak connectivity check failed")
		healthIssues = append(healthIssues, "Fulcio cannot reach Keycloak OIDC endpoint")
	}

	// Health Check 3: Component readiness
	if err := r.validateComponentReadiness(ctx, securesign); err != nil {
		logger.Error(err, "Component readiness check failed")
		healthIssues = append(healthIssues, "RHTAS components not all ready")
	}

	// Health Check 4: Email verified claim validation
	if err := r.validateEmailVerifiedClaim(ctx); err != nil {
		logger.Error(err, "Email verified claim validation failed")
		healthIssues = append(healthIssues, "OIDC token missing email_verified claim or claim is false")
	}

	// Update Securesign status with health check results instead of DisconnectedPlatform
	// This avoids race conditions with DisconnectedPlatform controller
	if len(healthIssues) > 0 {
		logger.Info("Health issues detected", "issues", healthIssues)
		r.updateSecuresignHealthStatus(ctx, securesign, false, strings.Join(healthIssues, "; "))
	} else {
		logger.Info("All RHTAS health checks passed")
		r.updateSecuresignHealthStatus(ctx, securesign, true, "All RHTAS components healthy")
	}

	// Requeue after 10 minutes for next health check
	return ctrl.Result{RequeueAfter: 10 * time.Minute}, nil
}

func (r *RHTASHealthCheckReconciler) validateTUFKeys(ctx context.Context, securesign *unstructured.Unstructured) error {
	keys, found, err := unstructured.NestedSlice(securesign.Object, "spec", "tuf", "keys")
	if err != nil || !found {
		return nil
	}

	for _, key := range keys {
		if keyMap, ok := key.(map[string]interface{}); ok {
			if name, _ := keyMap["name"].(string); name == "tsa.certchain.pem" {
				// Found problematic key, auto-fix it
				log.FromContext(ctx).Info("Auto-fixing TUF keys: removing tsa.certchain.pem")

				newKeys := []interface{}{}
				for _, k := range keys {
					if km, ok := k.(map[string]interface{}); ok {
						if n, _ := km["name"].(string); n != "tsa.certchain.pem" {
							newKeys = append(newKeys, k)
						}
					}
				}

				if err := unstructured.SetNestedSlice(securesign.Object, newKeys, "spec", "tuf", "keys"); err != nil {
					return err
				}

				if err := r.Update(ctx, securesign); err != nil {
					return err
				}

				log.FromContext(ctx).Info("Successfully removed tsa.certchain.pem from TUF keys")
				return nil
			}
		}
	}

	return nil
}

func (r *RHTASHealthCheckReconciler) validateFulcioKeycloakConnection(ctx context.Context) error {
	// Check if Fulcio pods have crash loop due to Keycloak connectivity
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(architectNamespace),
		client.MatchingLabels{"app": "fulcio-server"}); err != nil {
		return err
	}

	if len(podList.Items) == 0 {
		return nil
	}

	pod := podList.Items[0]

	// Check for excessive restarts
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.RestartCount > 3 {
			log.FromContext(ctx).Info("Fulcio pod has high restart count, checking Keycloak readiness",
				"pod", pod.Name,
				"restartCount", containerStatus.RestartCount)

			// Verify Keycloak is ready
			kc := &unstructured.Unstructured{}
			kc.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "k8s.keycloak.org",
				Version: "v2alpha1",
				Kind:    "Keycloak",
			})
			kc.SetName("mirror-operator-keycloak")
			kc.SetNamespace(architectNamespace)

			if err := r.Get(ctx, client.ObjectKeyFromObject(kc), kc); err == nil {
				conditions, _, _ := unstructured.NestedSlice(kc.Object, "status", "conditions")
				for _, cond := range conditions {
					condMap := cond.(map[string]interface{})
					if condMap["type"] == "Ready" && condMap["status"] == "True" {
						// Keycloak ready, restart Fulcio
						log.FromContext(ctx).Info("Restarting Fulcio pod to reconnect to Keycloak", "pod", pod.Name)

						if err := r.Delete(ctx, &pod); err != nil {
							return err
						}

						log.FromContext(ctx).Info("Successfully restarted Fulcio pod")
						return nil
					}
				}
			}
		}
	}

	return nil
}

func (r *RHTASHealthCheckReconciler) validateComponentReadiness(ctx context.Context, securesign *unstructured.Unstructured) error {
	// Check Securesign overall status
	conditions, found, err := unstructured.NestedSlice(securesign.Object, "status", "conditions")
	if err != nil || !found {
		return nil
	}

	for _, cond := range conditions {
		if condMap, ok := cond.(map[string]interface{}); ok {
			if condMap["type"] == "Ready" {
				if condMap["status"] != "True" {
					log.FromContext(ctx).Info("Securesign not ready",
						"reason", condMap["reason"],
						"message", condMap["message"])
					// Don't return error - this is informational
				}
				break
			}
		}
	}

	return nil
}

func (r *RHTASHealthCheckReconciler) validateEmailVerifiedClaim(ctx context.Context) error {
	// Get the OIDC client secret
	secret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{
		Name:      "mirror-operator-keycloak-client-secret",
		Namespace: architectNamespace,
	}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // Secret not created yet
		}
		return err
	}

	clientSecret := string(secret.Data["clientSecret"])
	if clientSecret == "" {
		return nil
	}

	// Get Keycloak hostname from DisconnectedPlatform
	platformList := &mirrorv1.DisconnectedPlatformList{}
	if err := r.List(ctx, platformList); err != nil {
		return err
	}

	if len(platformList.Items) == 0 {
		return nil
	}

	platform := &platformList.Items[0]
	if platform.Spec.Connected == nil || platform.Spec.Connected.RHTAS == nil ||
		platform.Spec.Connected.RHTAS.OIDC == nil || platform.Spec.Connected.RHTAS.OIDC.Managed == nil {
		return nil
	}

	realmName := platform.Spec.Connected.RHTAS.OIDC.Managed.Realm
	if realmName == "" {
		realmName = "trusted-artifact-signer"
	}

	// Get cluster ingress domain
	ingress := &unstructured.Unstructured{}
	ingress.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "Ingress",
	})
	ingress.SetName("cluster")
	if err := r.Get(ctx, client.ObjectKey{Name: "cluster"}, ingress); err != nil {
		return err
	}

	domain, _, _ := unstructured.NestedString(ingress.Object, "spec", "domain")
	tokenURL := fmt.Sprintf("https://keycloak.%s/realms/%s/protocol/openid-connect/token", domain, realmName)

	// Get access token using client credentials
	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", "trusted-artifact-signer")
	data.Set("client_secret", clientSecret)

	resp, err := http.PostForm(tokenURL, data)
	if err != nil {
		return fmt.Errorf("failed to get access token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("token request failed with status %d", resp.StatusCode)
	}

	var tokenResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return fmt.Errorf("failed to decode token response: %w", err)
	}

	accessToken, ok := tokenResp["access_token"].(string)
	if !ok || accessToken == "" {
		return fmt.Errorf("no access token in response")
	}

	// Decode the JWT to check email_verified claim
	parts := strings.Split(accessToken, ".")
	if len(parts) != 3 {
		return fmt.Errorf("invalid JWT format")
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return fmt.Errorf("failed to decode JWT payload: %w", err)
	}

	var claims map[string]interface{}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return fmt.Errorf("failed to unmarshal JWT claims: %w", err)
	}

	emailVerified, exists := claims["email_verified"]
	if !exists {
		log.FromContext(ctx).Info("email_verified claim missing from token", "claims", claims)
		return fmt.Errorf("email_verified claim is missing from OIDC token")
	}

	verified, ok := emailVerified.(bool)
	if !ok || !verified {
		log.FromContext(ctx).Info("email_verified claim is false", "value", emailVerified)
		return fmt.Errorf("email_verified claim is false (value: %v)", emailVerified)
	}

	log.FromContext(ctx).Info("email_verified claim validation passed", "value", true)
	return nil
}

func (r *RHTASHealthCheckReconciler) updateSecuresignHealthStatus(ctx context.Context, securesign *unstructured.Unstructured, healthy bool, message string) {
	// Update Securesign status conditions with health check results
	// DisconnectedPlatform controller will read this and update its own status

	conditions, found, err := unstructured.NestedSlice(securesign.Object, "status", "conditions")
	if err != nil || !found {
		conditions = []interface{}{}
	}

	now := metav1.Now()
	healthCondition := map[string]interface{}{
		"type":               "HealthCheckPassed",
		"status":             "True",
		"reason":             "AllHealthChecksPassed",
		"message":            message,
		"lastTransitionTime": now.Format(time.RFC3339),
	}

	if !healthy {
		healthCondition["status"] = "False"
		healthCondition["reason"] = "HealthCheckFailed"
	}

	// Find and update existing health condition or add new one
	conditionFound := false
	for i, cond := range conditions {
		if condMap, ok := cond.(map[string]interface{}); ok {
			if condMap["type"] == "HealthCheckPassed" {
				conditions[i] = healthCondition
				conditionFound = true
				break
			}
		}
	}

	if !conditionFound {
		conditions = append(conditions, healthCondition)
	}

	if err := unstructured.SetNestedSlice(securesign.Object, conditions, "status", "conditions"); err != nil {
		log.FromContext(ctx).Error(err, "Failed to set Securesign health condition")
		return
	}

	if err := r.Status().Update(ctx, securesign); err != nil {
		log.FromContext(ctx).Error(err, "Failed to update Securesign health status")
	}
}

func (r *RHTASHealthCheckReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "rhtas.redhat.com/v1alpha1",
			"kind":       "Securesign",
		}}).
		Complete(r)
}

package controller

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

var _ = Describe("Integration CrossOp: Quay, Keycloak, Securesign", func() {
	var reconciler *DisconnectedPlatformReconciler

	BeforeEach(func() {
		reconciler = &DisconnectedPlatformReconciler{
			Client: k8sClient,
			Scheme: scheme.Scheme,
		}
		createNamespace(architectNamespace)
	})

	cleanupPlatform := func(name string) {
		p := &mirrorv1.DisconnectedPlatform{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name}, p); err == nil {
			p.SetFinalizers(nil)
			_ = k8sClient.Update(ctx, p)
			_ = k8sClient.Delete(ctx, p)
		}
	}

	deleteIfExists := func(obj client.Object) {
		_ = k8sClient.Delete(ctx, obj)
	}

	connectedPlatformWithRHTAS := func(name string) *mirrorv1.DisconnectedPlatform {
		return &mirrorv1.DisconnectedPlatform{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: mirrorv1.DisconnectedPlatformSpec{
				Mode: mirrorv1.PlatformModeConnected,
				Connected: &mirrorv1.ConnectedConfig{
					ArtifactStorage: mirrorv1.ArtifactStorageConfig{Size: "10Gi"},
					RHTAS: &mirrorv1.RHTASInstallerConfig{
						OIDC: &mirrorv1.RHTASOIDCConfig{
							Managed: &mirrorv1.ManagedKeycloakConfig{
								Enabled: true,
								Realm:   "sigstore",
							},
						},
					},
				},
			},
		}
	}

	Describe("QuayRegistry → Hostname Resolution", func() {
		It("extracts hostname from status.registryEndpoint and strips protocol", func() {
			staleQR := newUnstructuredObj("quay.redhat.com", "v1", "QuayRegistry", "mirror-operator-quay", architectNamespace)
			_ = k8sClient.Delete(ctx, staleQR)

			qr := newUnstructuredObj("quay.redhat.com", "v1", "QuayRegistry", "mirror-operator-quay", architectNamespace)
			qr.Object["spec"] = map[string]interface{}{}
			Expect(k8sClient.Create(ctx, qr)).To(Succeed())
			defer deleteIfExists(qr)

			setNestedStatus(qr, map[string]interface{}{
				"registryEndpoint": "https://quay.example.com",
			})
			Expect(updateUnstructuredStatus(ctx, k8sClient, qr)).To(Succeed())

			hostname, err := reconciler.getQuayHostname(ctx, qr)
			Expect(err).NotTo(HaveOccurred())
			Expect(hostname).To(Equal("quay.example.com"))
		})

		It("falls back to Route when status.registryEndpoint is empty", func() {
			qr := newUnstructuredObj("quay.redhat.com", "v1", "QuayRegistry", "mirror-operator-quay-fb", architectNamespace)
			qr.Object["spec"] = map[string]interface{}{}
			Expect(k8sClient.Create(ctx, qr)).To(Succeed())
			defer deleteIfExists(qr)

			route := newUnstructuredObj("route.openshift.io", "v1", "Route", "mirror-operator-quay-fb-quay", architectNamespace)
			route.Object["spec"] = map[string]interface{}{
				"host": "quay-route.apps.example.com",
			}
			Expect(k8sClient.Create(ctx, route)).To(Succeed())
			defer deleteIfExists(route)

			hostname, err := reconciler.getQuayHostname(ctx, qr)
			Expect(err).NotTo(HaveOccurred())
			Expect(hostname).To(Equal("quay-route.apps.example.com"))
		})

		It("returns empty string when neither status nor route exists", func() {
			qr := newUnstructuredObj("quay.redhat.com", "v1", "QuayRegistry", "mirror-operator-quay-none", architectNamespace)
			qr.Object["spec"] = map[string]interface{}{}
			Expect(k8sClient.Create(ctx, qr)).To(Succeed())
			defer deleteIfExists(qr)

			hostname, err := reconciler.getQuayHostname(ctx, qr)
			Expect(err).NotTo(HaveOccurred())
			Expect(hostname).To(BeEmpty())
		})
	})

	Describe("Keycloak Health Check", func() {
		It("returns nil when Keycloak is Ready", func() {
			name := uniqueNamespace("dp-kc-healthy")
			platform := connectedPlatformWithRHTAS(name)
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			kc := newUnstructuredObj("k8s.keycloak.org", "v2alpha1", "Keycloak", "mirror-operator-keycloak", architectNamespace)
			kc.Object["spec"] = map[string]interface{}{}
			Expect(k8sClient.Create(ctx, kc)).To(Succeed())
			defer deleteIfExists(kc)

			setNestedStatus(kc, map[string]interface{}{
				"conditions": []interface{}{
					map[string]interface{}{
						"type":   "Ready",
						"status": "True",
					},
				},
			})
			Expect(updateUnstructuredStatus(ctx, k8sClient, kc)).To(Succeed())

			err := reconciler.checkKeycloakHealth(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns error when Keycloak is not Ready", func() {
			name := uniqueNamespace("dp-kc-unhealthy")
			platform := connectedPlatformWithRHTAS(name)
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			kc := newUnstructuredObj("k8s.keycloak.org", "v2alpha1", "Keycloak", "mirror-operator-keycloak", architectNamespace)
			kc.Object["spec"] = map[string]interface{}{}
			// Delete any leftover from prior test
			_ = k8sClient.Delete(ctx, kc)
			Expect(k8sClient.Create(ctx, kc)).To(Succeed())
			defer deleteIfExists(kc)

			setNestedStatus(kc, map[string]interface{}{
				"conditions": []interface{}{
					map[string]interface{}{
						"type":    "Ready",
						"status":  "False",
						"message": "Database connection failed",
					},
				},
			})
			Expect(updateUnstructuredStatus(ctx, k8sClient, kc)).To(Succeed())

			err := reconciler.checkKeycloakHealth(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Database connection failed"))
		})

		It("returns error when Keycloak has no conditions", func() {
			name := uniqueNamespace("dp-kc-nocond")
			platform := connectedPlatformWithRHTAS(name)
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			kc := newUnstructuredObj("k8s.keycloak.org", "v2alpha1", "Keycloak", "mirror-operator-keycloak", architectNamespace)
			kc.Object["spec"] = map[string]interface{}{}
			_ = k8sClient.Delete(ctx, kc)
			Expect(k8sClient.Create(ctx, kc)).To(Succeed())
			defer deleteIfExists(kc)

			setNestedStatus(kc, map[string]interface{}{
				"ready": false,
			})
			Expect(updateUnstructuredStatus(ctx, k8sClient, kc)).To(Succeed())

			err := reconciler.checkKeycloakHealth(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("conditions not available"))
		})

		It("is a no-op when RHTAS config is nil", func() {
			name := uniqueNamespace("dp-kc-noop")
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						ArtifactStorage: mirrorv1.ArtifactStorageConfig{Size: "10Gi"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			err := reconciler.checkKeycloakHealth(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Securesign → Root Keys Extraction", func() {
		setupSecuresignChain := func(platformName string) (*mirrorv1.DisconnectedPlatform, func()) {
			platform := connectedPlatformWithRHTAS(platformName)
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())

			// Create Securesign with Ready status and TUF URL
			ss := newUnstructuredObj("rhtas.redhat.com", "v1alpha1", "Securesign", "mirror-operator-securesign", architectNamespace)
			ss.Object["spec"] = map[string]interface{}{}
			_ = k8sClient.Delete(ctx, ss)
			Expect(k8sClient.Create(ctx, ss)).To(Succeed())

			setNestedStatus(ss, map[string]interface{}{
				"conditions": []interface{}{
					map[string]interface{}{
						"type":   "Ready",
						"status": "True",
					},
				},
				"tuf": map[string]interface{}{
					"url": "http://tuf.mirror-operator-system.svc",
				},
			})
			Expect(updateUnstructuredStatus(ctx, k8sClient, ss)).To(Succeed())

			// Create TUF resource with key references
			tuf := newUnstructuredObj("rhtas.redhat.com", "v1alpha1", "Tuf", "mirror-operator-securesign", architectNamespace)
			tuf.Object["spec"] = map[string]interface{}{}
			_ = k8sClient.Delete(ctx, tuf)
			Expect(k8sClient.Create(ctx, tuf)).To(Succeed())

			setNestedStatus(tuf, map[string]interface{}{
				"keys": []interface{}{
					map[string]interface{}{
						"name": "fulcio_v1.crt.pem",
						"secretRef": map[string]interface{}{
							"name": fmt.Sprintf("fulcio-cert-%s", platformName),
							"key":  "cert",
						},
					},
					map[string]interface{}{
						"name": "rekor.pub",
						"secretRef": map[string]interface{}{
							"name": fmt.Sprintf("rekor-key-%s", platformName),
							"key":  "public",
						},
					},
				},
			})
			Expect(updateUnstructuredStatus(ctx, k8sClient, tuf)).To(Succeed())

			// Create the actual secrets
			fulcioSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("fulcio-cert-%s", platformName),
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"cert": []byte("-----BEGIN CERTIFICATE-----\nFULCIO_ROOT_CERT\n-----END CERTIFICATE-----"),
				},
			}
			Expect(k8sClient.Create(ctx, fulcioSecret)).To(Succeed())

			rekorSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("rekor-key-%s", platformName),
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"public": []byte("-----BEGIN PUBLIC KEY-----\nREKOR_PUBLIC_KEY\n-----END PUBLIC KEY-----"),
				},
			}
			Expect(k8sClient.Create(ctx, rekorSecret)).To(Succeed())

			cleanup := func() {
				cleanupPlatform(platformName)
				deleteIfExists(ss)
				deleteIfExists(tuf)
				deleteIfExists(fulcioSecret)
				deleteIfExists(rekorSecret)
				cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "rhtas-trusted-root", Namespace: architectNamespace}}
				_ = k8sClient.Delete(ctx, cm)
			}

			return platform, cleanup
		}

		It("creates rhtas-trusted-root ConfigMap with correct key data", func() {
			name := uniqueNamespace("dp-rootkeys")
			platform, cleanup := setupSecuresignChain(name)
			defer cleanup()

			err := reconciler.extractRHTASRootKeys(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      "rhtas-trusted-root",
				Namespace: architectNamespace,
			}, cm)).To(Succeed())

			Expect(cm.Data["fulcio-root.pem"]).To(ContainSubstring("FULCIO_ROOT_CERT"))
			Expect(cm.Data["rekor-public-key.pem"]).To(ContainSubstring("REKOR_PUBLIC_KEY"))
			Expect(cm.Data["tuf-repository-url"]).To(Equal("http://tuf.mirror-operator-system.svc"))
			Expect(cm.Data["extraction-timestamp"]).NotTo(BeEmpty())
		})

		It("populates platform.Status.RHTASRootKeys", func() {
			name := uniqueNamespace("dp-rootkeys-status")
			platform, cleanup := setupSecuresignChain(name)
			defer cleanup()

			err := reconciler.extractRHTASRootKeys(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			Expect(platform.Status.RHTASRootKeys).NotTo(BeNil())
			Expect(platform.Status.RHTASRootKeys.ConfigMap).To(Equal("rhtas-trusted-root"))
			Expect(platform.Status.RHTASRootKeys.FulcioRootHash).NotTo(BeEmpty())
			Expect(platform.Status.RHTASRootKeys.RekorKeyHash).NotTo(BeEmpty())
			Expect(platform.Status.RHTASRootKeys.TUFRepositoryURL).To(Equal("http://tuf.mirror-operator-system.svc"))
		})

		It("returns error when Securesign is not ready", func() {
			name := uniqueNamespace("dp-rootkeys-notready")
			platform := connectedPlatformWithRHTAS(name)
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			ss := newUnstructuredObj("rhtas.redhat.com", "v1alpha1", "Securesign", "mirror-operator-securesign", architectNamespace)
			ss.Object["spec"] = map[string]interface{}{}
			_ = k8sClient.Delete(ctx, ss)
			Expect(k8sClient.Create(ctx, ss)).To(Succeed())
			defer deleteIfExists(ss)

			setNestedStatus(ss, map[string]interface{}{
				"conditions": []interface{}{
					map[string]interface{}{
						"type":   "Ready",
						"status": "False",
					},
				},
			})
			Expect(updateUnstructuredStatus(ctx, k8sClient, ss)).To(Succeed())

			err := reconciler.extractRHTASRootKeys(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not ready"))
		})

		It("returns error when TUF URL is missing", func() {
			name := uniqueNamespace("dp-rootkeys-notuf")
			platform := connectedPlatformWithRHTAS(name)
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			ss := newUnstructuredObj("rhtas.redhat.com", "v1alpha1", "Securesign", "mirror-operator-securesign", architectNamespace)
			ss.Object["spec"] = map[string]interface{}{}
			_ = k8sClient.Delete(ctx, ss)
			Expect(k8sClient.Create(ctx, ss)).To(Succeed())
			defer deleteIfExists(ss)

			setNestedStatus(ss, map[string]interface{}{
				"conditions": []interface{}{
					map[string]interface{}{
						"type":   "Ready",
						"status": "True",
					},
				},
			})
			Expect(updateUnstructuredStatus(ctx, k8sClient, ss)).To(Succeed())

			err := reconciler.extractRHTASRootKeys(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("TUF URL not found"))
		})
	})

	Describe("RHTAS Health Status Propagation", func() {
		It("sets Degraded condition when HealthCheckPassed is False", func() {
			name := uniqueNamespace("dp-rhtas-degraded")
			platform := connectedPlatformWithRHTAS(name)
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			ss := newUnstructuredObj("rhtas.redhat.com", "v1alpha1", "Securesign", "mirror-operator-securesign", architectNamespace)
			ss.Object["spec"] = map[string]interface{}{}
			_ = k8sClient.Delete(ctx, ss)
			Expect(k8sClient.Create(ctx, ss)).To(Succeed())
			defer deleteIfExists(ss)

			setNestedStatus(ss, map[string]interface{}{
				"conditions": []interface{}{
					map[string]interface{}{
						"type":    "HealthCheckPassed",
						"status":  "False",
						"reason":  "FulcioUnhealthy",
						"message": "Cannot reach OIDC provider",
					},
				},
			})
			Expect(updateUnstructuredStatus(ctx, k8sClient, ss)).To(Succeed())

			err := reconciler.updateStatusFromSecuresignHealth(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			found := false
			for _, cond := range platform.Status.Conditions {
				if cond.Type == "Degraded" && cond.Status == metav1.ConditionTrue {
					Expect(cond.Reason).To(Equal("FulcioUnhealthy"))
					Expect(cond.Message).To(Equal("Cannot reach OIDC provider"))
					found = true
				}
			}
			Expect(found).To(BeTrue(), "expected Degraded condition to be set")
			Expect(platform.Status.Phase).To(Equal(mirrorv1.PlatformPhase("Degraded")))
		})

		It("clears Degraded condition when HealthCheckPassed is True", func() {
			name := uniqueNamespace("dp-rhtas-healthy")
			platform := connectedPlatformWithRHTAS(name)
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			// Set up initial degraded state
			platform.Status.Phase = "Degraded"
			platform.Status.Conditions = []metav1.Condition{
				{
					Type:               "Degraded",
					Status:             metav1.ConditionTrue,
					Reason:             "HealthCheckFailed",
					Message:            "Previous failure",
					LastTransitionTime: metav1.Now(),
				},
			}

			ss := newUnstructuredObj("rhtas.redhat.com", "v1alpha1", "Securesign", "mirror-operator-securesign", architectNamespace)
			ss.Object["spec"] = map[string]interface{}{}
			_ = k8sClient.Delete(ctx, ss)
			Expect(k8sClient.Create(ctx, ss)).To(Succeed())
			defer deleteIfExists(ss)

			setNestedStatus(ss, map[string]interface{}{
				"conditions": []interface{}{
					map[string]interface{}{
						"type":   "HealthCheckPassed",
						"status": "True",
					},
				},
			})
			Expect(updateUnstructuredStatus(ctx, k8sClient, ss)).To(Succeed())

			err := reconciler.updateStatusFromSecuresignHealth(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			for _, cond := range platform.Status.Conditions {
				if cond.Type == "Degraded" {
					Expect(cond.Status).To(Equal(metav1.ConditionFalse))
				}
			}
			Expect(platform.Status.Phase).To(Equal(mirrorv1.PlatformPhaseReady))
		})

		It("is a no-op when Securesign does not exist", func() {
			name := uniqueNamespace("dp-rhtas-noss")
			platform := connectedPlatformWithRHTAS(name)
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			// Ensure no Securesign exists
			ss := newUnstructuredObj("rhtas.redhat.com", "v1alpha1", "Securesign", "mirror-operator-securesign", architectNamespace)
			_ = k8sClient.Delete(ctx, ss)

			err := reconciler.updateStatusFromSecuresignHealth(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("UpdateService Creation from QuayRegistry", func() {
		It("creates UpdateService with correct image paths from QuayRegistry hostname", func() {
			name := uniqueNamespace("dp-us")
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						ArtifactStorage: mirrorv1.ArtifactStorageConfig{Size: "10Gi"},
						Quay: &mirrorv1.QuayInstallerConfig{
							Managed: &mirrorv1.ManagedQuayConfig{
								OrganizationName: "myorg",
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			qr := newUnstructuredObj("quay.redhat.com", "v1", "QuayRegistry", "mirror-operator-quay", architectNamespace)
			qr.Object["spec"] = map[string]interface{}{}
			_ = k8sClient.Delete(ctx, qr)
			Expect(k8sClient.Create(ctx, qr)).To(Succeed())
			defer deleteIfExists(qr)

			setNestedStatus(qr, map[string]interface{}{
				"registryEndpoint": "https://quay.apps.cluster.example.com",
			})
			Expect(updateUnstructuredStatus(ctx, k8sClient, qr)).To(Succeed())

			createNamespace("openshift-update-service")

			err := reconciler.ensureUpdateService(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			us := newUnstructuredObj("updateservice.operator.openshift.io", "v1", "UpdateService", "update-service-oc-mirror", "openshift-update-service")
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(us), us)).To(Succeed())

			spec, _, _ := unstructured.NestedMap(us.Object, "spec")
			Expect(spec["graphDataImage"]).To(Equal("quay.apps.cluster.example.com/myorg/openshift/graph-image:latest"))
			Expect(spec["releases"]).To(Equal("quay.apps.cluster.example.com/myorg/openshift/release-images"))
			Expect(spec["replicas"]).To(Equal(int64(2)))

			defer func() {
				_ = k8sClient.Delete(ctx, us)
			}()
		})

		It("is idempotent — does not recreate UpdateService on second call", func() {
			name := uniqueNamespace("dp-us-idem")
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						ArtifactStorage: mirrorv1.ArtifactStorageConfig{Size: "10Gi"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			qr := newUnstructuredObj("quay.redhat.com", "v1", "QuayRegistry", "mirror-operator-quay", architectNamespace)
			qr.Object["spec"] = map[string]interface{}{}
			_ = k8sClient.Delete(ctx, qr)
			Expect(k8sClient.Create(ctx, qr)).To(Succeed())
			defer deleteIfExists(qr)

			setNestedStatus(qr, map[string]interface{}{
				"registryEndpoint": "https://quay2.example.com",
			})
			Expect(updateUnstructuredStatus(ctx, k8sClient, qr)).To(Succeed())

			createNamespace("openshift-update-service")

			Expect(reconciler.ensureUpdateService(ctx, platform)).To(Succeed())
			Expect(reconciler.ensureUpdateService(ctx, platform)).To(Succeed())

			us := newUnstructuredObj("updateservice.operator.openshift.io", "v1", "UpdateService", "update-service-oc-mirror", "openshift-update-service")
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(us), us)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, us) }()
		})

		It("skips when QuayRegistry not found", func() {
			name := uniqueNamespace("dp-us-noqr")
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						ArtifactStorage: mirrorv1.ArtifactStorageConfig{Size: "10Gi"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			// Ensure no QuayRegistry exists
			qr := newUnstructuredObj("quay.redhat.com", "v1", "QuayRegistry", "mirror-operator-quay", architectNamespace)
			_ = k8sClient.Delete(ctx, qr)

			err := reconciler.ensureUpdateService(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("OBC Creation", func() {
		It("creates ObjectBucketClaim with correct spec", func() {
			name := uniqueNamespace("dp-obc")

			// Clean up any OBC left by prior tests
			staleOBC := newUnstructuredObj("objectbucket.io", "v1alpha1", "ObjectBucketClaim", "collection-artifacts", architectNamespace)
			_ = k8sClient.Delete(ctx, staleOBC)

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						ArtifactStorage: mirrorv1.ArtifactStorageConfig{Size: "10Gi"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			err := reconciler.reconcileArtifactsBucket(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			obc := newUnstructuredObj("objectbucket.io", "v1alpha1", "ObjectBucketClaim", "collection-artifacts", architectNamespace)
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(obc), obc)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, obc) }()

			spec, _, _ := unstructured.NestedMap(obc.Object, "spec")
			Expect(spec["generateBucketName"]).To(Equal("collection-artifacts"))
			Expect(spec["storageClassName"]).To(Equal("openshift-storage.noobaa.io"))
		})

		It("is idempotent — does not create duplicate OBC", func() {
			name := uniqueNamespace("dp-obc-idem")
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						ArtifactStorage: mirrorv1.ArtifactStorageConfig{Size: "10Gi"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			Expect(reconciler.reconcileArtifactsBucket(ctx, platform)).To(Succeed())
			Expect(reconciler.reconcileArtifactsBucket(ctx, platform)).To(Succeed())

			obc := newUnstructuredObj("objectbucket.io", "v1alpha1", "ObjectBucketClaim", "collection-artifacts", architectNamespace)
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(obc), obc)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, obc) }()
		})
	})

	Describe("S3 Config Sync", func() {
		It("merges S3 config from OBC ConfigMap into Secret", func() {
			name := uniqueNamespace("dp-s3sync")
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						ArtifactStorage: mirrorv1.ArtifactStorageConfig{Size: "10Gi"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			// Clean up stale resources from prior tests
			staleCM := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts", Namespace: architectNamespace}}
			_ = k8sClient.Delete(ctx, staleCM)
			staleSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts", Namespace: architectNamespace}}
			_ = k8sClient.Delete(ctx, staleSecret)

			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "collection-artifacts",
					Namespace: architectNamespace,
				},
				Data: map[string]string{
					"BUCKET_NAME":   "my-bucket-123",
					"BUCKET_HOST":   "some-other-host.svc",
					"BUCKET_REGION": "us-west-2",
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			defer deleteIfExists(cm)

			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "collection-artifacts",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"AWS_ACCESS_KEY_ID":     []byte("AKIA123"),
					"AWS_SECRET_ACCESS_KEY": []byte("secret123"),
				},
			}
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())
			defer deleteIfExists(secret)

			err := reconciler.syncS3ConfigToSecret(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			updated := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      "collection-artifacts",
				Namespace: architectNamespace,
			}, updated)).To(Succeed())

			Expect(string(updated.Data["S3_BUCKET"])).To(Equal("my-bucket-123"))
			Expect(string(updated.Data["AWS_REGION"])).To(Equal("us-west-2"))
			// Original keys should still be present
			Expect(string(updated.Data["AWS_ACCESS_KEY_ID"])).To(Equal("AKIA123"))
		})

		It("defaults region to us-east-1 when not specified", func() {
			name := uniqueNamespace("dp-s3sync-region")
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						ArtifactStorage: mirrorv1.ArtifactStorageConfig{Size: "10Gi"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "collection-artifacts",
					Namespace: architectNamespace,
				},
				Data: map[string]string{
					"BUCKET_NAME": "bucket-no-region",
				},
			}
			_ = k8sClient.Delete(ctx, cm)
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			defer deleteIfExists(cm)

			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "collection-artifacts",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{},
			}
			_ = k8sClient.Delete(ctx, secret)
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())
			defer deleteIfExists(secret)

			err := reconciler.syncS3ConfigToSecret(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			updated := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      "collection-artifacts",
				Namespace: architectNamespace,
			}, updated)).To(Succeed())
			Expect(string(updated.Data["AWS_REGION"])).To(Equal("us-east-1"))
		})

		It("is a no-op when OBC ConfigMap does not exist", func() {
			name := uniqueNamespace("dp-s3sync-nocm")
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						ArtifactStorage: mirrorv1.ArtifactStorageConfig{Size: "10Gi"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			// Ensure no ConfigMap named "collection-artifacts" exists
			cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts", Namespace: architectNamespace}}
			_ = k8sClient.Delete(ctx, cm)

			err := reconciler.syncS3ConfigToSecret(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("is a no-op when OBC Secret does not exist", func() {
			name := uniqueNamespace("dp-s3sync-nosec")
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						ArtifactStorage: mirrorv1.ArtifactStorageConfig{Size: "10Gi"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			// Create ConfigMap but not the Secret
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "collection-artifacts",
					Namespace: architectNamespace,
				},
				Data: map[string]string{"BUCKET_NAME": "test"},
			}
			_ = k8sClient.Delete(ctx, cm)
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			defer deleteIfExists(cm)

			// Ensure no Secret
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts", Namespace: architectNamespace}}
			_ = k8sClient.Delete(ctx, secret)

			err := reconciler.syncS3ConfigToSecret(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("resolves external S3 route when BUCKET_HOST is s3.openshift-storage.svc", func() {
			name := uniqueNamespace("dp-s3sync-route")
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					Connected: &mirrorv1.ConnectedConfig{
						ArtifactStorage: mirrorv1.ArtifactStorageConfig{Size: "10Gi"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())
			defer cleanupPlatform(name)

			createNamespace("openshift-storage")

			// Clean up stale resources from prior tests
			staleCM := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts", Namespace: architectNamespace}}
			_ = k8sClient.Delete(ctx, staleCM)
			staleSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "collection-artifacts", Namespace: architectNamespace}}
			_ = k8sClient.Delete(ctx, staleSecret)
			staleRoute := newUnstructuredObj("route.openshift.io", "v1", "Route", "s3", "openshift-storage")
			_ = k8sClient.Delete(ctx, staleRoute)

			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "collection-artifacts",
					Namespace: architectNamespace,
				},
				Data: map[string]string{
					"BUCKET_NAME": "my-bucket",
					"BUCKET_HOST": "s3.openshift-storage.svc",
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			defer deleteIfExists(cm)

			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "collection-artifacts",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{},
			}
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())
			defer deleteIfExists(secret)

			s3Route := newUnstructuredObj("route.openshift.io", "v1", "Route", "s3", "openshift-storage")
			s3Route.Object["spec"] = map[string]interface{}{
				"host": "s3-openshift-storage.apps.cluster.example.com",
			}
			Expect(k8sClient.Create(ctx, s3Route)).To(Succeed())
			defer deleteIfExists(s3Route)

			err := reconciler.syncS3ConfigToSecret(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			updated := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      "collection-artifacts",
				Namespace: architectNamespace,
			}, updated)).To(Succeed())
			Expect(string(updated.Data["S3_ENDPOINT"])).To(Equal("https://s3-openshift-storage.apps.cluster.example.com"))
		})
	})
})

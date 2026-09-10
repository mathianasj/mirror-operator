package controller

import (
	"context"
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

var _ = Describe("DisconnectedPlatform Airgapped", func() {
	var (
		ctx        context.Context
		testScheme *runtime.Scheme
	)

	BeforeEach(func() {
		ctx = context.Background()
		testScheme = runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(testScheme)).To(Succeed())
		Expect(mirrorv1.AddToScheme(testScheme)).To(Succeed())
		Expect(rbacv1.AddToScheme(testScheme)).To(Succeed())
	})

	Describe("toStringInterfaceMap", func() {
		It("converts string map to interface map", func() {
			input := map[string]string{"key1": "val1", "key2": "val2"}
			result := toStringInterfaceMap(input)
			Expect(result).To(HaveLen(2))
			Expect(result["key1"]).To(Equal("val1"))
			Expect(result["key2"]).To(Equal("val2"))
		})

		It("returns empty map for empty input", func() {
			result := toStringInterfaceMap(map[string]string{})
			Expect(result).To(HaveLen(0))
		})

		It("returns empty map for nil input", func() {
			result := toStringInterfaceMap(nil)
			Expect(result).To(HaveLen(0))
		})
	})

	Describe("ensureImportScannerRBAC", func() {
		It("creates ServiceAccount, Role, and RoleBinding", func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: architectNamespace},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-platform",
					UID:  "test-uid",
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ns, platform).Build(),
				Scheme: testScheme,
			}

			err := r.ensureImportScannerRBAC(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			sa := &corev1.ServiceAccount{}
			err = r.Get(ctx, client.ObjectKey{Name: "import-bundle-scanner", Namespace: architectNamespace}, sa)
			Expect(err).NotTo(HaveOccurred())

			role := &rbacv1.Role{}
			err = r.Get(ctx, client.ObjectKey{Name: "import-bundle-scanner", Namespace: architectNamespace}, role)
			Expect(err).NotTo(HaveOccurred())
			Expect(role.Rules).To(HaveLen(1))
			Expect(role.Rules[0].Resources).To(ContainElement("mirrorimports"))

			rb := &rbacv1.RoleBinding{}
			err = r.Get(ctx, client.ObjectKey{Name: "import-bundle-scanner", Namespace: architectNamespace}, rb)
			Expect(err).NotTo(HaveOccurred())
			Expect(rb.RoleRef.Name).To(Equal("import-bundle-scanner"))
		})

		It("is idempotent — second call does not error", func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: architectNamespace},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-platform",
					UID:  "test-uid",
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ns, platform).Build(),
				Scheme: testScheme,
			}

			Expect(r.ensureImportScannerRBAC(ctx, platform)).To(Succeed())
			Expect(r.ensureImportScannerRBAC(ctx, platform)).To(Succeed())
		})
	})

	Describe("ensureImportScannerScript", func() {
		It("creates the scanner script ConfigMap", func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: architectNamespace},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-platform",
					UID:  "test-uid",
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ns, platform).Build(),
				Scheme: testScheme,
			}

			err := r.ensureImportScannerScript(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			cm := &corev1.ConfigMap{}
			err = r.Get(ctx, client.ObjectKey{Name: "import-scanner-script", Namespace: architectNamespace}, cm)
			Expect(err).NotTo(HaveOccurred())
			Expect(cm.Data).To(HaveKey("scan-imports.sh"))
			Expect(cm.Data["scan-imports.sh"]).To(ContainSubstring("Scanning"))
		})

		It("is idempotent — returns nil on second call without error", func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: architectNamespace},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-platform",
					UID:  "test-uid",
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ns, platform).Build(),
				Scheme: testScheme,
			}

			Expect(r.ensureImportScannerScript(ctx, platform)).To(Succeed())
			Expect(r.ensureImportScannerScript(ctx, platform)).To(Succeed())
		})
	})

	Describe("ensureImportJobRBAC", func() {
		It("creates ServiceAccount, ClusterRole, and ClusterRoleBinding for import jobs", func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: architectNamespace},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-platform",
					UID:  "test-uid",
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ns, platform).Build(),
				Scheme: testScheme,
			}

			err := r.ensureImportJobRBAC(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			sa := &corev1.ServiceAccount{}
			err = r.Get(ctx, client.ObjectKey{Name: "mirror-import-job", Namespace: architectNamespace}, sa)
			Expect(err).NotTo(HaveOccurred())

			cr := &rbacv1.ClusterRole{}
			err = r.Get(ctx, client.ObjectKey{Name: "mirror-import-job"}, cr)
			Expect(err).NotTo(HaveOccurred())
			Expect(cr.Rules).To(HaveLen(2))

			crb := &rbacv1.ClusterRoleBinding{}
			err = r.Get(ctx, client.ObjectKey{Name: "mirror-import-job"}, crb)
			Expect(err).NotTo(HaveOccurred())
			Expect(crb.RoleRef.Name).To(Equal("mirror-import-job"))
		})
	})

	Describe("ensureAirgappedUpdateService", func() {
		It("creates an UpdateService CR with correct graph and releases references", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			err := r.ensureAirgappedUpdateService(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			us := &unstructured.Unstructured{}
			us.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "updateservice.operator.openshift.io", Version: "v1", Kind: "UpdateService",
			})
			err = r.Get(ctx, client.ObjectKey{Name: "update-service-oc-mirror", Namespace: "openshift-update-service"}, us)
			Expect(err).NotTo(HaveOccurred())

			graphImage, _, _ := unstructured.NestedString(us.Object, "spec", "graphDataImage")
			Expect(graphImage).To(Equal("quay.airgap.local/mirror/openshift/graph-image:latest"))

			releases, _, _ := unstructured.NestedString(us.Object, "spec", "releases")
			Expect(releases).To(Equal("quay.airgap.local/mirror/openshift/release-images"))
		})

		It("uses custom organization name when configured", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/myorg",
						Quay: &mirrorv1.AirgappedQuayConfig{
							OrganizationName: "myorg",
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			err := r.ensureAirgappedUpdateService(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			us := &unstructured.Unstructured{}
			us.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "updateservice.operator.openshift.io", Version: "v1", Kind: "UpdateService",
			})
			err = r.Get(ctx, client.ObjectKey{Name: "update-service-oc-mirror", Namespace: "openshift-update-service"}, us)
			Expect(err).NotTo(HaveOccurred())

			graphImage, _, _ := unstructured.NestedString(us.Object, "spec", "graphDataImage")
			Expect(graphImage).To(ContainSubstring("/myorg/"))
		})

		It("returns nil when mirror registry is empty", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "",
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			err := r.ensureAirgappedUpdateService(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("is idempotent — updates existing UpdateService without error", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			Expect(r.ensureAirgappedUpdateService(ctx, platform)).To(Succeed())
			Expect(r.ensureAirgappedUpdateService(ctx, platform)).To(Succeed())
		})
	})

	Describe("createAirgappedQuayConfigSecret", func() {
		It("creates a config bundle secret with LocalStorage backend", func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: architectNamespace},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-platform",
					UID:  "test-uid",
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ns, platform).Build(),
				Scheme: testScheme,
			}

			err := r.createAirgappedQuayConfigSecret(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			secret := &corev1.Secret{}
			err = r.Get(ctx, client.ObjectKey{Name: "mirror-operator-quay-config-bundle", Namespace: architectNamespace}, secret)
			Expect(err).NotTo(HaveOccurred())
			Expect(secret.Data).To(HaveKey("config.yaml"))
			configYAML := string(secret.Data["config.yaml"])
			Expect(configYAML).To(ContainSubstring("LocalStorage"))
		})
	})

	Describe("reconcileAirgappedQuay", func() {
		It("returns nil when Quay config is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			err := r.reconcileAirgappedQuay(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when Quay is disabled", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
						Quay: &mirrorv1.AirgappedQuayConfig{
							Enabled: false,
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			err := r.reconcileAirgappedQuay(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("creates QuayRegistry CR with correct spec when enabled", func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: architectNamespace},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-platform",
					UID:  "test-uid",
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
						Quay: &mirrorv1.AirgappedQuayConfig{
							Enabled: true,
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ns, platform).Build(),
				Scheme: testScheme,
			}

			err := r.reconcileAirgappedQuay(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			qr := &unstructured.Unstructured{}
			qr.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "quay.redhat.com", Version: "v1", Kind: "QuayRegistry",
			})
			err = r.Get(ctx, client.ObjectKey{Name: "mirror-operator-quay", Namespace: architectNamespace}, qr)
			Expect(err).NotTo(HaveOccurred())

			configBundle, _, _ := unstructured.NestedString(qr.Object, "spec", "configBundleSecret")
			Expect(configBundle).To(Equal("mirror-operator-quay-config-bundle"))

			components, _, _ := unstructured.NestedSlice(qr.Object, "spec", "components")
			Expect(components).NotTo(BeEmpty())
		})
	})

	Describe("ensureAirgappedRegistryCredentials", func() {
		It("returns nil when user-provided credentials are set", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						RegistryCredentials: &corev1.LocalObjectReference{Name: "my-creds"},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			err := r.ensureAirgappedRegistryCredentials(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when Quay is not enabled and no user credentials", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			err := r.ensureAirgappedRegistryCredentials(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("skips credential setup when QuayRegistry not yet created", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
						Quay: &mirrorv1.AirgappedQuayConfig{
							Enabled: true,
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			err := r.ensureAirgappedRegistryCredentials(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("reconcileImportScanner", func() {
		It("creates CronJob with correct schedule and script mount", func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: architectNamespace},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-platform",
					UID:  "test-uid",
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
						ImportPath:     "/mnt/import",
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ns, platform).Build(),
				Scheme: testScheme,
			}

			err := r.reconcileImportScanner(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			cronJob := &batchv1.CronJob{}
			err = r.Get(ctx, client.ObjectKey{Name: "import-bundle-scanner", Namespace: architectNamespace}, cronJob)
			Expect(err).NotTo(HaveOccurred())
			Expect(cronJob.Spec.Schedule).To(Equal("*/30 * * * *"))

			containers := cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers
			Expect(containers).To(HaveLen(1))
			Expect(containers[0].Name).To(Equal("scanner"))

			foundScriptMount := false
			for _, vm := range containers[0].VolumeMounts {
				if vm.Name == "scanner-script" && vm.MountPath == "/scripts" {
					foundScriptMount = true
				}
			}
			Expect(foundScriptMount).To(BeTrue())

			foundImportEnv := false
			for _, env := range containers[0].Env {
				if env.Name == "IMPORT_PATH" && env.Value == "/mnt/import" {
					foundImportEnv = true
				}
			}
			Expect(foundImportEnv).To(BeTrue())
		})

		It("uses custom schedule when configured", func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: architectNamespace},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-platform",
					UID:  "test-uid",
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry:     "quay.airgap.local/mirror",
						ImportPath:         "/mnt/import",
						ImportScanSchedule: "*/5 * * * *",
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ns, platform).Build(),
				Scheme: testScheme,
			}

			err := r.reconcileImportScanner(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			cronJob := &batchv1.CronJob{}
			err = r.Get(ctx, client.ObjectKey{Name: "import-bundle-scanner", Namespace: architectNamespace}, cronJob)
			Expect(err).NotTo(HaveOccurred())
			Expect(cronJob.Spec.Schedule).To(Equal("*/5 * * * *"))
		})

		It("is idempotent — second call does not error", func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: architectNamespace},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-platform",
					UID:  "test-uid",
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
						ImportPath:     "/mnt/import",
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ns, platform).Build(),
				Scheme: testScheme,
			}

			Expect(r.reconcileImportScanner(ctx, platform)).To(Succeed())
			Expect(r.reconcileImportScanner(ctx, platform)).To(Succeed())
		})
	})

	Describe("reconcileRHCOSServer", func() {
		It("returns nil when mirror registry is empty", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "",
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			err := r.reconcileRHCOSServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when ACM host inventory is not enabled", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			err := r.reconcileRHCOSServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("creates Deployment and Service with correct image", func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: architectNamespace},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-platform",
					UID:  "test-uid",
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
						ACM: &mirrorv1.AirgappedACMConfig{
							Enabled: true,
							HostInventory: &mirrorv1.HostInventoryConfig{
								Enabled: true,
								Versions: []mirrorv1.HostInventoryVersion{
									{OpenshiftVersion: "4.18.12"},
								},
							},
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ns, platform).Build(),
				Scheme: testScheme,
			}

			err := r.reconcileRHCOSServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			err = r.Get(ctx, client.ObjectKey{Name: "rhcos-server", Namespace: architectNamespace}, deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Template.Spec.Containers[0].Image).To(Equal("quay.airgap.local/mirror/rhcos-server:4.18.12"))

			svc := &corev1.Service{}
			err = r.Get(ctx, client.ObjectKey{Name: "rhcos-server", Namespace: architectNamespace}, svc)
			Expect(err).NotTo(HaveOccurred())
			Expect(svc.Spec.Ports[0].Port).To(Equal(int32(8080)))
		})

		It("uses custom RHCOS image when configured", func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: architectNamespace},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-platform",
					UID:  "test-uid",
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
						ACM: &mirrorv1.AirgappedACMConfig{
							Enabled: true,
							HostInventory: &mirrorv1.HostInventoryConfig{
								Enabled:    true,
								RHCOSImage: "my-registry:8443/rhcos-server:custom",
								Versions: []mirrorv1.HostInventoryVersion{
									{OpenshiftVersion: "4.18.12"},
								},
							},
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(ns, platform).Build(),
				Scheme: testScheme,
			}

			err := r.reconcileRHCOSServer(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			err = r.Get(ctx, client.ObjectKey{Name: "rhcos-server", Namespace: architectNamespace}, deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Template.Spec.Containers[0].Image).To(Equal("my-registry:8443/rhcos-server:custom"))
		})
	})

	Describe("getMirrorRegistryCA", func() {
		It("returns CA from Quay config bundle secret", func() {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-quay-config-bundle",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"ssl.cert": []byte("-----BEGIN CERTIFICATE-----\ntest-ca\n-----END CERTIFICATE-----"),
				},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						Quay: &mirrorv1.AirgappedQuayConfig{Enabled: true},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(secret).Build(),
				Scheme: testScheme,
			}

			ca := r.getMirrorRegistryCA(ctx, platform)
			Expect(ca).To(ContainSubstring("test-ca"))
		})

		It("returns CA from Quay-generated TLS secret when config bundle has no cert", func() {
			configSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-quay-config-bundle",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{},
			}
			tlsSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-operator-quay-quay-ssl",
					Namespace: architectNamespace,
				},
				Data: map[string][]byte{
					"tls.crt": []byte("-----BEGIN CERTIFICATE-----\nquay-tls-ca\n-----END CERTIFICATE-----"),
				},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						Quay: &mirrorv1.AirgappedQuayConfig{Enabled: true},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(configSecret, tlsSecret).Build(),
				Scheme: testScheme,
			}

			ca := r.getMirrorRegistryCA(ctx, platform)
			Expect(ca).To(ContainSubstring("quay-tls-ca"))
		})

		It("returns empty string when no CA found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:      mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			ca := r.getMirrorRegistryCA(ctx, platform)
			Expect(ca).To(Equal(""))
		})
	})

	Describe("ensureClusterImageSets", func() {
		It("returns nil when ACM host inventory is not enabled", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			err := r.ensureClusterImageSets(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("creates ClusterImageSet CRs for each version", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
						ACM: &mirrorv1.AirgappedACMConfig{
							Enabled: true,
							HostInventory: &mirrorv1.HostInventoryConfig{
								Enabled: true,
								Versions: []mirrorv1.HostInventoryVersion{
									{OpenshiftVersion: "4.18.12"},
									{OpenshiftVersion: "4.17.8", CpuArchitecture: "aarch64"},
								},
							},
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			err := r.ensureClusterImageSets(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			cis1 := &unstructured.Unstructured{}
			cis1.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "hive.openshift.io", Version: "v1", Kind: "ClusterImageSet",
			})
			err = r.Get(ctx, client.ObjectKey{Name: "openshift-4-18-12"}, cis1)
			Expect(err).NotTo(HaveOccurred())
			releaseImage1, _, _ := unstructured.NestedString(cis1.Object, "spec", "releaseImage")
			Expect(releaseImage1).To(Equal("quay.airgap.local/openshift/release-images:4.18.12-x86_64"))

			cis2 := &unstructured.Unstructured{}
			cis2.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "hive.openshift.io", Version: "v1", Kind: "ClusterImageSet",
			})
			err = r.Get(ctx, client.ObjectKey{Name: "openshift-4-17-8"}, cis2)
			Expect(err).NotTo(HaveOccurred())
			releaseImage2, _, _ := unstructured.NestedString(cis2.Object, "spec", "releaseImage")
			Expect(releaseImage2).To(Equal("quay.airgap.local/openshift/release-images:4.17.8-aarch64"))
		})

		It("is idempotent — skips existing ClusterImageSets", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
						ACM: &mirrorv1.AirgappedACMConfig{
							Enabled: true,
							HostInventory: &mirrorv1.HostInventoryConfig{
								Enabled: true,
								Versions: []mirrorv1.HostInventoryVersion{
									{OpenshiftVersion: "4.18.12"},
								},
							},
						},
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			Expect(r.ensureClusterImageSets(ctx, platform)).To(Succeed())
			Expect(r.ensureClusterImageSets(ctx, platform)).To(Succeed())
		})
	})

	Describe("ensureAssistedInstallerMirrorConfig", func() {
		It("returns nil when mirror registry is empty", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "",
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			err := r.ensureAssistedInstallerMirrorConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("creates ConfigMap with registries.conf in multicluster-engine namespace", func() {
			mceNs := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: "multicluster-engine"},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(mceNs, platform).Build(),
				Scheme: testScheme,
			}

			err := r.ensureAssistedInstallerMirrorConfig(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			cm := &corev1.ConfigMap{}
			err = r.Get(ctx, client.ObjectKey{Name: "assisted-installer-mirror-config", Namespace: "multicluster-engine"}, cm)
			Expect(err).NotTo(HaveOccurred())
			Expect(cm.Data).To(HaveKey("registries.conf"))
			Expect(cm.Data).To(HaveKey("ca-bundle.crt"))
			Expect(cm.Data["registries.conf"]).To(ContainSubstring("unqualified-search-registries"))
		})
	})

	Describe("buildIgnitionConfigOverride", func() {
		It("returns empty string when no ClusterImagePolicy resources", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			result := r.buildIgnitionConfigOverride(ctx)
			Expect(result).To(Equal(""))
		})

		It("returns valid ignition JSON when ClusterImagePolicy exists", func() {
			cip := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "config.openshift.io/v1",
				"kind":       "ClusterImagePolicy",
				"metadata":   map[string]interface{}{"name": "test-policy"},
				"spec": map[string]interface{}{
					"scopes": []interface{}{"quay.io/myorg"},
					"policy": map[string]interface{}{
						"rootOfTrust": map[string]interface{}{
							"publicKey": map[string]interface{}{
								"keyData": "LS0tLS1CRUdJTi...",
							},
						},
					},
				},
			}}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cip).Build(),
				Scheme: testScheme,
			}

			result := r.buildIgnitionConfigOverride(ctx)
			Expect(result).NotTo(BeEmpty())

			var ignition map[string]interface{}
			err := json.Unmarshal([]byte(result), &ignition)
			Expect(err).NotTo(HaveOccurred())

			version, _, _ := unstructured.NestedString(ignition, "ignition", "version")
			Expect(version).To(Equal("3.2.0"))

			files, _, _ := unstructured.NestedSlice(ignition, "storage", "files")
			Expect(files).To(HaveLen(2))

			file0 := files[0].(map[string]interface{})
			Expect(file0["path"]).To(Equal("/etc/containers/policy.json"))

			file1 := files[1].(map[string]interface{})
			Expect(file1["path"]).To(Equal("/etc/containers/registries.d/sigstore-registries.yaml"))
		})
	})

	Describe("deleteHostInventoryResources", func() {
		It("does not error when resources do not exist", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}

			Expect(func() { r.deleteHostInventoryResources(ctx) }).NotTo(Panic())
		})

		It("deletes existing RHCOS server deployment and service", func() {
			deploy := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: "rhcos-server", Namespace: architectNamespace},
			}
			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "rhcos-server", Namespace: architectNamespace},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(deploy, svc).Build(),
				Scheme: testScheme,
			}

			r.deleteHostInventoryResources(ctx)

			err := r.Get(ctx, client.ObjectKey{Name: "rhcos-server", Namespace: architectNamespace}, &appsv1.Deployment{})
			Expect(err).To(HaveOccurred())

			err = r.Get(ctx, client.ObjectKey{Name: "rhcos-server", Namespace: architectNamespace}, &corev1.Service{})
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("reconcileImportPipelineTemplate", func() {
		It("returns nil when not in airgapped mode", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test-platform"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			err := r.reconcileImportPipelineTemplate(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("creates Pipeline with correct name and params", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-platform",
					UID:  "test-uid",
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client:      fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme:      testScheme,
				MirrorImage: "quay.io/test/oc-mirror:latest",
			}

			err := r.reconcileImportPipelineTemplate(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			pipeline := &unstructured.Unstructured{}
			pipeline.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "tekton.dev", Version: "v1", Kind: "Pipeline",
			})
			err = r.Get(ctx, client.ObjectKey{Name: "import-pipeline-template", Namespace: architectNamespace}, pipeline)
			Expect(err).NotTo(HaveOccurred())

			params, _, _ := unstructured.NestedSlice(pipeline.Object, "spec", "params")
			Expect(params).NotTo(BeEmpty())

			paramNames := []string{}
			for _, p := range params {
				pm := p.(map[string]interface{})
				paramNames = append(paramNames, pm["name"].(string))
			}
			Expect(paramNames).To(ContainElement("bundle-filename"))
			Expect(paramNames).To(ContainElement("target-registry"))
			Expect(paramNames).To(ContainElement("mirror-image"))
			Expect(paramNames).To(ContainElement("verify-enabled"))

			workspaces, _, _ := unstructured.NestedSlice(pipeline.Object, "spec", "workspaces")
			Expect(workspaces).NotTo(BeEmpty())

			tasks, _, _ := unstructured.NestedSlice(pipeline.Object, "spec", "tasks")
			Expect(tasks).NotTo(BeEmpty())
		})

		It("is idempotent — updates existing pipeline", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-platform",
					UID:  "test-uid",
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "quay.airgap.local/mirror",
					},
				},
			}

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build(),
				Scheme: testScheme,
			}

			Expect(r.reconcileImportPipelineTemplate(ctx, platform)).To(Succeed())
			Expect(r.reconcileImportPipelineTemplate(ctx, platform)).To(Succeed())
		})
	})

	Describe("buildImportPipelineTasks", func() {
		It("returns task list with verify-bundle, extract-bundle, mirror-content, apply-manifests, and cleanup-workspace tasks", func() {
			r := &DisconnectedPlatformReconciler{}
			tasks := r.buildImportPipelineTasks()
			Expect(tasks).NotTo(BeEmpty())

			taskNames := []string{}
			for _, t := range tasks {
				taskNames = append(taskNames, t["name"].(string))
			}
			Expect(taskNames).To(ContainElement("verify-bundle"))
			Expect(taskNames).To(ContainElement("extract-bundle"))
			Expect(taskNames).To(ContainElement("mirror-content"))
			Expect(taskNames).To(ContainElement("apply-manifests"))
			Expect(taskNames).To(ContainElement("cleanup-workspace"))
		})

		It("verify-bundle task has when condition on verify-enabled param", func() {
			r := &DisconnectedPlatformReconciler{}
			tasks := r.buildImportPipelineTasks()

			var verifyTask map[string]interface{}
			for _, t := range tasks {
				if t["name"] == "verify-bundle" {
					verifyTask = t
					break
				}
			}
			Expect(verifyTask).NotTo(BeNil())

			when, ok := verifyTask["when"].([]map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(when).To(HaveLen(1))
			Expect(when[0]["input"]).To(Equal("$(params.verify-enabled)"))
		})
	})

	Describe("ensureACMPullSecret", func() {
		It("copies pull secret from operator namespace to open-cluster-management", func() {
			source := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: architectNamespace},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(source).Build(),
				Scheme: testScheme,
			}
			err := r.ensureACMPullSecret(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			target := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: "open-cluster-management"}, target)).To(Succeed())
			Expect(target.Type).To(Equal(corev1.SecretTypeDockerConfigJson))
		})

		It("updates existing secret when data differs", func() {
			source := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: architectNamespace},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{"new":"data"}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			existing := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "open-cluster-management"},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{"old":"data"}}`)},
				Type:       corev1.SecretTypeDockerConfigJson,
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(source, existing).Build(),
				Scheme: testScheme,
			}
			err := r.ensureACMPullSecret(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			updated := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "pull-secret", Namespace: "open-cluster-management"}, updated)).To(Succeed())
			Expect(string(updated.Data[".dockerconfigjson"])).To(ContainSubstring("new"))
		})

		It("returns error when source secret not found", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureACMPullSecret(ctx, platform)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get pull-secret"))
		})
	})

	Describe("ensureMultiClusterHub", func() {
		It("returns nil when MultiClusterHub already exists", func() {
			mch := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "operator.open-cluster-management.io/v1",
				"kind":       "MultiClusterHub",
				"metadata":   map[string]interface{}{"name": "multiclusterhub", "namespace": "open-cluster-management"},
				"status":     map[string]interface{}{"phase": "Running"},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "airgapped",
					Airgapped: &mirrorv1.AirgappedConfig{
						ACM: &mirrorv1.AirgappedACMConfig{Enabled: true},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(mch).Build(),
				Scheme: testScheme,
			}
			err := r.ensureMultiClusterHub(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			found := false
			for _, c := range platform.Status.Components {
				if c.Name == "multiclusterhub" && c.Status == "Running" {
					found = true
				}
			}
			Expect(found).To(BeTrue())
		})

		It("creates MultiClusterHub with default pull secret", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "airgapped",
					Airgapped: &mirrorv1.AirgappedConfig{
						ACM: &mirrorv1.AirgappedACMConfig{Enabled: true},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureMultiClusterHub(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			mch := &unstructured.Unstructured{Object: map[string]interface{}{}}
			mch.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "operator.open-cluster-management.io", Version: "v1", Kind: "MultiClusterHub",
			})
			Expect(r.Get(ctx, client.ObjectKey{Name: "multiclusterhub", Namespace: "open-cluster-management"}, mch)).To(Succeed())
			pullSecret, _, _ := unstructured.NestedString(mch.Object, "spec", "imagePullSecret")
			Expect(pullSecret).To(Equal("pull-secret"))
		})

		It("creates MultiClusterHub with custom pull secret and CA", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "airgapped",
					Airgapped: &mirrorv1.AirgappedConfig{
						ACM: &mirrorv1.AirgappedACMConfig{
							Enabled: true,
							MultiClusterHub: &mirrorv1.MultiClusterHubConfig{
								ImagePullSecret:          &corev1.LocalObjectReference{Name: "custom-secret"},
								CustomCAConfigMap:        "custom-ca",
								DisableHubSelfManagement: true,
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureMultiClusterHub(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			mch := &unstructured.Unstructured{Object: map[string]interface{}{}}
			mch.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "operator.open-cluster-management.io", Version: "v1", Kind: "MultiClusterHub",
			})
			Expect(r.Get(ctx, client.ObjectKey{Name: "multiclusterhub", Namespace: "open-cluster-management"}, mch)).To(Succeed())
			pullSecret, _, _ := unstructured.NestedString(mch.Object, "spec", "imagePullSecret")
			Expect(pullSecret).To(Equal("custom-secret"))
			ca, _, _ := unstructured.NestedString(mch.Object, "spec", "customCAConfigmap")
			Expect(ca).To(Equal("custom-ca"))
			disabled, _, _ := unstructured.NestedBool(mch.Object, "spec", "disableHubSelfManagement")
			Expect(disabled).To(BeTrue())
		})
	})

	Describe("ensureProvisioningConfiguration", func() {
		It("creates Provisioning CR when not found", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureProvisioningConfiguration(ctx)
			Expect(err).NotTo(HaveOccurred())

			prov := &unstructured.Unstructured{Object: map[string]interface{}{}}
			prov.SetGroupVersionKind(schema.GroupVersionKind{Group: "metal3.io", Version: "v1alpha1", Kind: "Provisioning"})
			Expect(r.Get(ctx, client.ObjectKey{Name: "provisioning-configuration"}, prov)).To(Succeed())
			net, _, _ := unstructured.NestedString(prov.Object, "spec", "provisioningNetwork")
			Expect(net).To(Equal("Disabled"))
			watchAll, _, _ := unstructured.NestedBool(prov.Object, "spec", "watchAllNamespaces")
			Expect(watchAll).To(BeTrue())
		})

		It("updates existing Provisioning CR when settings differ", func() {
			existing := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "metal3.io/v1alpha1",
				"kind":       "Provisioning",
				"metadata":   map[string]interface{}{"name": "provisioning-configuration"},
				"spec": map[string]interface{}{
					"provisioningNetwork": "Managed",
					"watchAllNamespaces":  false,
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			err := r.ensureProvisioningConfiguration(ctx)
			Expect(err).NotTo(HaveOccurred())

			updated := &unstructured.Unstructured{Object: map[string]interface{}{}}
			updated.SetGroupVersionKind(schema.GroupVersionKind{Group: "metal3.io", Version: "v1alpha1", Kind: "Provisioning"})
			Expect(r.Get(ctx, client.ObjectKey{Name: "provisioning-configuration"}, updated)).To(Succeed())
			net, _, _ := unstructured.NestedString(updated.Object, "spec", "provisioningNetwork")
			Expect(net).To(Equal("Disabled"))
		})

		It("is idempotent when already configured correctly", func() {
			existing := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "metal3.io/v1alpha1",
				"kind":       "Provisioning",
				"metadata":   map[string]interface{}{"name": "provisioning-configuration"},
				"spec": map[string]interface{}{
					"provisioningNetwork": "Disabled",
					"watchAllNamespaces":  true,
				},
			}}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			err := r.ensureProvisioningConfiguration(ctx)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureAgentServiceConfig", func() {
		It("creates AgentServiceConfig with correct storage and OS images", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "airgapped",
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "mirror.example.com:8443",
						ACM: &mirrorv1.AirgappedACMConfig{
							Enabled: true,
							HostInventory: &mirrorv1.HostInventoryConfig{
								Enabled:             true,
								DatabaseStorageSize: "100Gi",
								StorageClass:        "fast-storage",
								Versions: []mirrorv1.HostInventoryVersion{
									{OpenshiftVersion: "4.18.12", CpuArchitecture: "x86_64"},
								},
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureAgentServiceConfig(ctx, platform, "http://rhcos-server.mirror-operator-system.svc:8080")
			Expect(err).NotTo(HaveOccurred())

			asc := &unstructured.Unstructured{Object: map[string]interface{}{}}
			asc.SetGroupVersionKind(schema.GroupVersionKind{Group: "agent-install.openshift.io", Version: "v1beta1", Kind: "AgentServiceConfig"})
			Expect(r.Get(ctx, client.ObjectKey{Name: "agent"}, asc)).To(Succeed())

			osImages, _, _ := unstructured.NestedSlice(asc.Object, "spec", "osImages")
			Expect(len(osImages)).To(Equal(1))
			osImage := osImages[0].(map[string]interface{})
			Expect(osImage["openshiftVersion"]).To(Equal("4.18.12"))
		})

		It("is idempotent — returns nil when AgentServiceConfig already exists", func() {
			existing := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "agent-install.openshift.io/v1beta1",
				"kind":       "AgentServiceConfig",
				"metadata":   map[string]interface{}{"name": "agent"},
				"spec":       map[string]interface{}{},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "airgapped",
					Airgapped: &mirrorv1.AirgappedConfig{
						ACM: &mirrorv1.AirgappedACMConfig{
							Enabled: true,
							HostInventory: &mirrorv1.HostInventoryConfig{
								Enabled: true,
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			err := r.ensureAgentServiceConfig(ctx, platform, "http://rhcos.svc:8080")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureInfraEnv", func() {
		It("returns nil when InfraEnv config is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "airgapped",
					Airgapped: &mirrorv1.AirgappedConfig{
						ACM: &mirrorv1.AirgappedACMConfig{
							Enabled: true,
							HostInventory: &mirrorv1.HostInventoryConfig{
								Enabled: true,
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureInfraEnv(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when InfraEnv is not enabled", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "airgapped",
					Airgapped: &mirrorv1.AirgappedConfig{
						ACM: &mirrorv1.AirgappedACMConfig{
							Enabled: true,
							HostInventory: &mirrorv1.HostInventoryConfig{
								Enabled:  true,
								InfraEnv: &mirrorv1.InfraEnvConfig{Enabled: false},
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureInfraEnv(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("is idempotent — returns nil when InfraEnv already exists", func() {
			existing := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "agent-install.openshift.io/v1beta1",
				"kind":       "InfraEnv",
				"metadata":   map[string]interface{}{"name": "mirror-operator-infraenv", "namespace": architectNamespace},
				"spec":       map[string]interface{}{},
			}}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "airgapped",
					Airgapped: &mirrorv1.AirgappedConfig{
						ACM: &mirrorv1.AirgappedACMConfig{
							Enabled: true,
							HostInventory: &mirrorv1.HostInventoryConfig{
								Enabled:  true,
								InfraEnv: &mirrorv1.InfraEnvConfig{Enabled: true},
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			err := r.ensureInfraEnv(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ensureACMCredential", func() {
		It("returns nil when credential config is nil", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "airgapped",
					Airgapped: &mirrorv1.AirgappedConfig{
						ACM: &mirrorv1.AirgappedACMConfig{
							Enabled: true,
							HostInventory: &mirrorv1.HostInventoryConfig{Enabled: true},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureACMCredential(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns nil when credential is not enabled", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "airgapped",
					Airgapped: &mirrorv1.AirgappedConfig{
						ACM: &mirrorv1.AirgappedACMConfig{
							Enabled: true,
							HostInventory: &mirrorv1.HostInventoryConfig{
								Enabled:    true,
								Credential: &mirrorv1.CredentialConfig{Enabled: false},
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			err := r.ensureACMCredential(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})

		It("creates credential secret with pull secret data", func() {
			pullSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: architectNamespace},
				Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "airgapped",
					Airgapped: &mirrorv1.AirgappedConfig{
						ACM: &mirrorv1.AirgappedACMConfig{
							Enabled: true,
							HostInventory: &mirrorv1.HostInventoryConfig{
								Enabled: true,
								Credential: &mirrorv1.CredentialConfig{
									Enabled:    true,
									BaseDomain: "example.com",
								},
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pullSecret).Build(),
				Scheme: testScheme,
			}
			err := r.ensureACMCredential(ctx, platform)
			Expect(err).NotTo(HaveOccurred())

			cred := &corev1.Secret{}
			Expect(r.Get(ctx, client.ObjectKey{Name: "mirror-operator-credential", Namespace: "multicluster-engine"}, cred)).To(Succeed())
			pullSecretValue := cred.StringData["pullSecret"]
			if pullSecretValue == "" {
				pullSecretValue = string(cred.Data["pullSecret"])
			}
		})

		It("is idempotent — returns nil when credential already exists", func() {
			existing := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "mirror-operator-credential", Namespace: "multicluster-engine"},
			}
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "airgapped",
					Airgapped: &mirrorv1.AirgappedConfig{
						ACM: &mirrorv1.AirgappedACMConfig{
							Enabled: true,
							HostInventory: &mirrorv1.HostInventoryConfig{
								Enabled: true,
								Credential: &mirrorv1.CredentialConfig{
									Enabled: true,
								},
							},
						},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(existing).Build(),
				Scheme: testScheme,
			}
			err := r.ensureACMCredential(ctx, platform)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

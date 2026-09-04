package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

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
			err = r.Get(ctx, client.ObjectKey{Name: "mirror-operator-update-service", Namespace: architectNamespace}, us)
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
			err = r.Get(ctx, client.ObjectKey{Name: "mirror-operator-update-service", Namespace: architectNamespace}, us)
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
})

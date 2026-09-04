package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

var _ = Describe("helpers", func() {
	Describe("envOrDefault", func() {
		It("returns environment variable when set", func() {
			GinkgoT().Setenv("TEST_HELPER_VAR", "from-env")
			Expect(envOrDefault("TEST_HELPER_VAR", "fallback")).To(Equal("from-env"))
		})

		It("returns fallback when env var is not set", func() {
			Expect(envOrDefault("NONEXISTENT_HELPER_VAR_12345", "fallback")).To(Equal("fallback"))
		})

		It("returns fallback when env var is empty", func() {
			GinkgoT().Setenv("TEST_EMPTY_VAR", "")
			Expect(envOrDefault("TEST_EMPTY_VAR", "default")).To(Equal("default"))
		})
	})

	Describe("containsString", func() {
		It("returns true when string is present", func() {
			Expect(containsString([]string{"a", "b", "c"}, "b")).To(BeTrue())
		})

		It("returns false when string is absent", func() {
			Expect(containsString([]string{"a", "b", "c"}, "d")).To(BeFalse())
		})

		It("returns false on empty slice", func() {
			Expect(containsString([]string{}, "a")).To(BeFalse())
		})

		It("returns false on nil slice", func() {
			Expect(containsString(nil, "a")).To(BeFalse())
		})
	})

	Describe("removeString", func() {
		It("removes the target string", func() {
			Expect(removeString([]string{"a", "b", "c"}, "b")).To(Equal([]string{"a", "c"}))
		})

		It("returns nil when removing the only element", func() {
			Expect(removeString([]string{"a"}, "a")).To(BeNil())
		})

		It("returns all elements when target is absent", func() {
			Expect(removeString([]string{"a", "b"}, "c")).To(Equal([]string{"a", "b"}))
		})

		It("removes all occurrences", func() {
			Expect(removeString([]string{"a", "b", "a"}, "a")).To(Equal([]string{"b"}))
		})

		It("handles nil slice", func() {
			Expect(removeString(nil, "a")).To(BeNil())
		})
	})

	Describe("buildQuayComponents", func() {
		It("returns all required components without replica override", func() {
			components := buildQuayComponents(nil, true, true, true)
			Expect(components).To(HaveLen(9))

			kinds := []string{}
			for _, c := range components {
				m := c.(map[string]interface{})
				kinds = append(kinds, m["kind"].(string))
			}
			Expect(kinds).To(ContainElements("clair", "postgres", "objectstorage", "redis", "route", "mirror", "tls", "quay"))
			Expect(kinds).To(ContainElement("horizontalpodautoscaler"))
		})

		It("includes replica overrides for quay, clair, mirror when set", func() {
			replicas := int32(2)
			components := buildQuayComponents(&replicas, true, true, true)

			for _, c := range components {
				m := c.(map[string]interface{})
				kind := m["kind"].(string)
				if kind == "quay" || kind == "clair" || kind == "mirror" {
					overrides, ok := m["overrides"].(map[string]interface{})
					Expect(ok).To(BeTrue(), "expected overrides on "+kind)
					Expect(overrides["replicas"]).To(Equal(int64(2)))
				}
			}
		})

		It("disables HPA when replica override is set", func() {
			replicas := int32(1)
			components := buildQuayComponents(&replicas, true, true, true)

			for _, c := range components {
				m := c.(map[string]interface{})
				if m["kind"] == "horizontalpodautoscaler" {
					Expect(m["managed"]).To(BeFalse())
				}
			}
		})

		It("enables HPA when no replica override", func() {
			components := buildQuayComponents(nil, true, true, true)

			for _, c := range components {
				m := c.(map[string]interface{})
				if m["kind"] == "horizontalpodautoscaler" {
					Expect(m["managed"]).To(BeTrue())
				}
			}
		})

		It("sets objectstorage managed flag correctly", func() {
			components := buildQuayComponents(nil, false, true, true)
			for _, c := range components {
				m := c.(map[string]interface{})
				if m["kind"] == "objectstorage" {
					Expect(m["managed"]).To(BeFalse())
				}
			}
		})

		It("sets route managed flag correctly", func() {
			components := buildQuayComponents(nil, true, false, true)
			for _, c := range components {
				m := c.(map[string]interface{})
				if m["kind"] == "route" {
					Expect(m["managed"]).To(BeFalse())
				}
			}
		})

		It("sets tls managed flag correctly", func() {
			components := buildQuayComponents(nil, true, true, false)
			for _, c := range components {
				m := c.(map[string]interface{})
				if m["kind"] == "tls" {
					Expect(m["managed"]).To(BeFalse())
				}
			}
		})

		It("always adds GUNICORN_CMD_ARGS env to quay component", func() {
			components := buildQuayComponents(nil, true, true, true)
			for _, c := range components {
				m := c.(map[string]interface{})
				if m["kind"] == "quay" {
					overrides := m["overrides"].(map[string]interface{})
					envList := overrides["env"].([]interface{})
					Expect(envList).To(HaveLen(1))
					env := envList[0].(map[string]interface{})
					Expect(env["name"]).To(Equal("GUNICORN_CMD_ARGS"))
					Expect(env["value"]).To(Equal("--timeout 300"))
				}
			}
		})
	})

	Describe("isSingleNodeOpenShift", func() {
		var (
			ctx        context.Context
			testScheme *runtime.Scheme
		)

		BeforeEach(func() {
			ctx = context.Background()
			testScheme = runtime.NewScheme()
			Expect(clientgoscheme.AddToScheme(testScheme)).To(Succeed())
			Expect(mirrorv1.AddToScheme(testScheme)).To(Succeed())
		})

		It("returns true when topology is SingleReplica", func() {
			infra := &unstructured.Unstructured{}
			infra.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "config.openshift.io", Version: "v1", Kind: "Infrastructure",
			})
			infra.SetName("cluster")
			unstructured.SetNestedField(infra.Object, "SingleReplica", "status", "controlPlaneTopology")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(infra).Build(),
			}
			Expect(r.isSingleNodeOpenShift(ctx)).To(BeTrue())
		})

		It("returns false when topology is HighlyAvailable", func() {
			infra := &unstructured.Unstructured{}
			infra.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "config.openshift.io", Version: "v1", Kind: "Infrastructure",
			})
			infra.SetName("cluster")
			unstructured.SetNestedField(infra.Object, "HighlyAvailable", "status", "controlPlaneTopology")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(infra).Build(),
			}
			Expect(r.isSingleNodeOpenShift(ctx)).To(BeFalse())
		})

		It("returns false when Infrastructure CR is missing", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
			}
			Expect(r.isSingleNodeOpenShift(ctx)).To(BeFalse())
		})
	})

	Describe("resolveQuayReplicaOverride", func() {
		var (
			ctx        context.Context
			testScheme *runtime.Scheme
		)

		BeforeEach(func() {
			ctx = context.Background()
			testScheme = runtime.NewScheme()
			Expect(clientgoscheme.AddToScheme(testScheme)).To(Succeed())
			Expect(mirrorv1.AddToScheme(testScheme)).To(Succeed())
		})

		It("returns explicit override when provided", func() {
			override := int32(3)
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
			}
			result := r.resolveQuayReplicaOverride(ctx, &override)
			Expect(result).NotTo(BeNil())
			Expect(*result).To(Equal(int32(3)))
		})

		It("returns 1 on SNO when no explicit override", func() {
			infra := &unstructured.Unstructured{}
			infra.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "config.openshift.io", Version: "v1", Kind: "Infrastructure",
			})
			infra.SetName("cluster")
			unstructured.SetNestedField(infra.Object, "SingleReplica", "status", "controlPlaneTopology")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(infra).Build(),
			}
			result := r.resolveQuayReplicaOverride(ctx, nil)
			Expect(result).NotTo(BeNil())
			Expect(*result).To(Equal(int32(1)))
		})

		It("returns nil on multi-node when no explicit override", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
			}
			result := r.resolveQuayReplicaOverride(ctx, nil)
			Expect(result).To(BeNil())
		})
	})

	Describe("getClusterVersion", func() {
		var (
			ctx        context.Context
			testScheme *runtime.Scheme
		)

		BeforeEach(func() {
			ctx = context.Background()
			testScheme = runtime.NewScheme()
			Expect(clientgoscheme.AddToScheme(testScheme)).To(Succeed())
			Expect(mirrorv1.AddToScheme(testScheme)).To(Succeed())
		})

		It("returns the cluster version", func() {
			cv := &unstructured.Unstructured{}
			cv.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "config.openshift.io", Version: "v1", Kind: "ClusterVersion",
			})
			cv.SetName("version")
			unstructured.SetNestedField(cv.Object, "4.17.3", "status", "desired", "version")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cv).Build(),
			}
			Expect(r.getClusterVersion(ctx)).To(Equal("4.17.3"))
		})

		It("returns empty string when CR is missing", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
			}
			Expect(r.getClusterVersion(ctx)).To(BeEmpty())
		})
	})

	Describe("getClusterProxy", func() {
		var (
			ctx        context.Context
			testScheme *runtime.Scheme
		)

		BeforeEach(func() {
			ctx = context.Background()
			testScheme = runtime.NewScheme()
			Expect(clientgoscheme.AddToScheme(testScheme)).To(Succeed())
			Expect(mirrorv1.AddToScheme(testScheme)).To(Succeed())
		})

		It("returns proxy configuration", func() {
			proxy := &unstructured.Unstructured{}
			proxy.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "config.openshift.io", Version: "v1", Kind: "Proxy",
			})
			proxy.SetName("cluster")
			unstructured.SetNestedField(proxy.Object, "http://proxy:8080", "spec", "httpProxy")
			unstructured.SetNestedField(proxy.Object, "https://proxy:8443", "spec", "httpsProxy")
			unstructured.SetNestedField(proxy.Object, ".cluster.local", "spec", "noProxy")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(proxy).Build(),
			}
			httpP, httpsP, noP := r.getClusterProxy(ctx)
			Expect(httpP).To(Equal("http://proxy:8080"))
			Expect(httpsP).To(Equal("https://proxy:8443"))
			Expect(noP).To(Equal(".cluster.local"))
		})

		It("returns empty strings when Proxy CR is missing", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
			}
			httpP, httpsP, noP := r.getClusterProxy(ctx)
			Expect(httpP).To(BeEmpty())
			Expect(httpsP).To(BeEmpty())
			Expect(noP).To(BeEmpty())
		})
	})

	Describe("getClusterSSHKey", func() {
		var (
			ctx        context.Context
			testScheme *runtime.Scheme
		)

		BeforeEach(func() {
			ctx = context.Background()
			testScheme = runtime.NewScheme()
			Expect(clientgoscheme.AddToScheme(testScheme)).To(Succeed())
			Expect(mirrorv1.AddToScheme(testScheme)).To(Succeed())
		})

		It("returns the first SSH key from MachineConfig", func() {
			mc := &unstructured.Unstructured{}
			mc.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "machineconfiguration.openshift.io", Version: "v1", Kind: "MachineConfig",
			})
			mc.SetName("99-master-ssh")
			unstructured.SetNestedSlice(mc.Object, []interface{}{
				map[string]interface{}{
					"name":              "core",
					"sshAuthorizedKeys": []interface{}{"ssh-rsa AAAA..."},
				},
			}, "spec", "config", "passwd", "users")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(mc).Build(),
			}
			Expect(r.getClusterSSHKey(ctx)).To(Equal("ssh-rsa AAAA..."))
		})

		It("returns empty string when MachineConfig is missing", func() {
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
			}
			Expect(r.getClusterSSHKey(ctx)).To(BeEmpty())
		})

		It("returns empty string when no users exist", func() {
			mc := &unstructured.Unstructured{}
			mc.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "machineconfiguration.openshift.io", Version: "v1", Kind: "MachineConfig",
			})
			mc.SetName("99-master-ssh")
			unstructured.SetNestedSlice(mc.Object, []interface{}{}, "spec", "config", "passwd", "users")

			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(mc).Build(),
			}
			Expect(r.getClusterSSHKey(ctx)).To(BeEmpty())
		})
	})
})

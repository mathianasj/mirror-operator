package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

var _ = Describe("Integration: DisconnectedPlatform", func() {
	var reconciler *DisconnectedPlatformReconciler

	BeforeEach(func() {
		reconciler = &DisconnectedPlatformReconciler{
			Client: k8sClient,
			Scheme: scheme.Scheme,
		}
	})

	reconcilePlatform := func(name string) (ctrl.Result, error) {
		return reconciler.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Name: name},
		})
	}

	fetchPlatform := func(name string) *mirrorv1.DisconnectedPlatform {
		p := &mirrorv1.DisconnectedPlatform{}
		ExpectWithOffset(1, k8sClient.Get(ctx, types.NamespacedName{Name: name}, p)).To(Succeed())
		return p
	}

	cleanupPlatform := func(name string) {
		p := &mirrorv1.DisconnectedPlatform{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name}, p); err == nil {
			p.SetFinalizers(nil)
			_ = k8sClient.Update(ctx, p)
			_ = k8sClient.Delete(ctx, p)
		}
	}

	Describe("Connected Mode - Basic Lifecycle", func() {
		It("adds finalizer on first reconcile", func() {
			name := uniqueNamespace("dp-basic")
			defer cleanupPlatform(name)

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

			result, err := reconcilePlatform(name)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			p := fetchPlatform(name)
			Expect(p.GetFinalizers()).To(ContainElement(platformFinalizer))
		})
	})

	Describe("Connected Mode - Status After Reconcile", func() {
		It("sets status phase after finalizer is in place", func() {
			name := uniqueNamespace("dp-status")
			defer cleanupPlatform(name)

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

			_, err := reconcilePlatform(name)
			Expect(err).NotTo(HaveOccurred())

			_, err = reconcilePlatform(name)
			if err != nil {
				Expect(err.Error()).NotTo(ContainSubstring("panic"))
			}

			p := fetchPlatform(name)
			Expect(p.Status.Phase).NotTo(BeEmpty())
		})
	})

	Describe("Airgapped Mode - Basic Lifecycle", func() {
		It("adds finalizer and handles airgapped path without crashing", func() {
			name := uniqueNamespace("dp-air")
			defer cleanupPlatform(name)

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeAirgapped,
					Airgapped: &mirrorv1.AirgappedConfig{
						MirrorRegistry: "registry.airgapped.local:5000",
					},
				},
			}
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())

			result, err := reconcilePlatform(name)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			p := fetchPlatform(name)
			Expect(p.GetFinalizers()).To(ContainElement(platformFinalizer))

			_, err = reconcilePlatform(name)
			if err != nil {
				Expect(err.Error()).NotTo(ContainSubstring("panic"))
			}

			p = fetchPlatform(name)
			Expect(p.Status.Phase).NotTo(BeEmpty())
		})
	})

	Describe("Deletion Flow", func() {
		It("removes finalizer and cleans up on deletion", func() {
			name := uniqueNamespace("dp-del")

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

			_, err := reconcilePlatform(name)
			Expect(err).NotTo(HaveOccurred())

			p := fetchPlatform(name)
			Expect(p.GetFinalizers()).To(ContainElement(platformFinalizer))

			Expect(k8sClient.Delete(ctx, p)).To(Succeed())

			_, err = reconcilePlatform(name)
			Expect(err).NotTo(HaveOccurred())

			remaining := &mirrorv1.DisconnectedPlatform{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: name}, remaining)
			if err == nil {
				Expect(remaining.GetFinalizers()).NotTo(ContainElement(platformFinalizer))
			}
		})
	})

	Describe("Idempotency", func() {
		It("produces stable status after multiple reconciles", func() {
			name := uniqueNamespace("dp-idem")
			defer cleanupPlatform(name)

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

			_, err := reconcilePlatform(name)
			Expect(err).NotTo(HaveOccurred())

			for i := 0; i < 3; i++ {
				_, err = reconcilePlatform(name)
				if err != nil {
					Expect(err.Error()).NotTo(ContainSubstring("panic"))
				}
			}

			p := fetchPlatform(name)
			phase := p.Status.Phase
			componentCount := len(p.Status.Components)
			conditionCount := len(p.Status.Conditions)

			_, err = reconcilePlatform(name)
			if err != nil {
				Expect(err.Error()).NotTo(ContainSubstring("panic"))
			}

			p2 := fetchPlatform(name)
			Expect(p2.Status.Phase).To(Equal(phase))
			Expect(len(p2.Status.Components)).To(Equal(componentCount))
			Expect(len(p2.Status.Conditions)).To(Equal(conditionCount))
		})
	})

	Describe("Not Found Handling", func() {
		It("returns no error when the platform does not exist", func() {
			result, err := reconcilePlatform("nonexistent-platform")
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))
		})
	})

	Describe("Nil Connected Config Safety", func() {
		It("handles connected mode with nil connected config without panic", func() {
			name := uniqueNamespace("dp-nilconn")
			defer cleanupPlatform(name)

			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
				},
			}
			Expect(k8sClient.Create(ctx, platform)).To(Succeed())

			_, err := reconcilePlatform(name)
			Expect(err).NotTo(HaveOccurred())

			_, err = reconcilePlatform(name)
			if err != nil {
				Expect(err.Error()).NotTo(ContainSubstring("panic"))
			}
		})
	})
})

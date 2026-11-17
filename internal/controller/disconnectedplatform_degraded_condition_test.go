package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

var _ = Describe("clearDegradedCondition (issue #130)", func() {
	var (
		ctx      context.Context
		scheme   *runtime.Scheme
		r        *DisconnectedPlatformReconciler
		platform *mirrorv1.DisconnectedPlatform
	)

	BeforeEach(func() {
		ctx = context.Background()
		scheme = runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
		Expect(mirrorv1.AddToScheme(scheme)).To(Succeed())
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		r = &DisconnectedPlatformReconciler{Client: c, Scheme: scheme}

		platform = &mirrorv1.DisconnectedPlatform{}
		platform.Status.Conditions = []metav1.Condition{
			{
				Type:               "Degraded",
				Status:             metav1.ConditionTrue,
				Reason:             "RHTASHealthCheckFailed",
				Message:            "email_verified claim not set",
				LastTransitionTime: metav1.Now(),
			},
		}
	})

	It("updates Reason to a passing value when clearing a Degraded condition", func() {
		r.clearDegradedCondition(ctx, platform, "RHTASHealthCheckFailed")

		var cond *metav1.Condition
		for i := range platform.Status.Conditions {
			if platform.Status.Conditions[i].Type == "Degraded" {
				cond = &platform.Status.Conditions[i]
				break
			}
		}

		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Message).To(Equal("Health check passed"))
		Expect(cond.Reason).NotTo(Equal("RHTASHealthCheckFailed"),
			"Reason must not retain the old failure reason after the condition is cleared")
		Expect(cond.Reason).NotTo(BeEmpty(),
			"Reason must be set to a non-empty passing value")
	})

	It("does not re-update when the condition is already cleared with the correct Reason", func() {
		// Pre-clear
		r.clearDegradedCondition(ctx, platform, "RHTASHealthCheckFailed")
		clearedTime := platform.Status.Conditions[0].LastTransitionTime

		// Clear again — should be a no-op (no LastTransitionTime change)
		r.clearDegradedCondition(ctx, platform, "RHTASHealthCheckFailed")

		Expect(platform.Status.Conditions[0].LastTransitionTime).To(Equal(clearedTime),
			"a second clear call must not bump LastTransitionTime when already in passing state")
	})

	It("also clears the HealthCheckFailed reason used by the cross-operator health checker", func() {
		platform.Status.Conditions[0].Reason = "HealthCheckFailed"
		platform.Status.Conditions[0].Message = "component not ready"

		r.clearDegradedCondition(ctx, platform, "HealthCheckFailed")

		cond := &platform.Status.Conditions[0]
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).NotTo(Equal("HealthCheckFailed"),
			"HealthCheckFailed Reason must be replaced after recovery")
	})
})

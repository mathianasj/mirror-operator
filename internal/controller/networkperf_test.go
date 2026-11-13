package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

var _ = Describe("Network Performance", func() {
	Describe("MeasureThroughput", func() {
		It("measures throughput from an HTTP endpoint", func() {
			payload := strings.Repeat("x", 10240) // 10 KiB
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, payload)
			}))
			defer server.Close()

			result := MeasureThroughput(context.Background(), server.URL, server.Client())
			Expect(result.Error).NotTo(HaveOccurred())
			Expect(result.BytesPerSec).To(BeNumerically(">", 0))
			Expect(result.Endpoint).To(Equal(server.URL))
		})

		It("returns an error for unreachable endpoints", func() {
			result := MeasureThroughput(context.Background(), "http://192.0.2.1:1/unreachable", &http.Client{Timeout: 500 * time.Millisecond})
			Expect(result.Error).To(HaveOccurred())
		})

		It("caps download at downloadTestMaxBytes", func() {
			payload := strings.Repeat("x", 2*downloadTestMaxBytes)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, payload)
			}))
			defer server.Close()

			result := MeasureThroughput(context.Background(), server.URL, server.Client())
			Expect(result.Error).NotTo(HaveOccurred())
			Expect(result.BytesPerSec).To(BeNumerically(">", 0))
		})
	})

	Describe("RecommendTuning", func() {
		It("recommends aggressive settings for fast connections (>=50 MB/s)", func() {
			bps := int64(50 * 1024 * 1024)
			maxPR, retries, timeout := RecommendTuning(bps)
			Expect(maxPR).To(Equal(int32(8)))
			Expect(retries).To(Equal(int32(2)))
			Expect(timeout).To(BeNumerically(">=", 2*time.Minute))
		})

		It("recommends moderate settings for medium connections (10-50 MB/s)", func() {
			bps := int64(20 * 1024 * 1024)
			maxPR, retries, timeout := RecommendTuning(bps)
			Expect(maxPR).To(Equal(int32(6)))
			Expect(retries).To(Equal(int32(3)))
			Expect(timeout).To(BeNumerically(">=", 2*time.Minute))
		})

		It("recommends conservative settings for slow connections (1-10 MB/s)", func() {
			bps := int64(5 * 1024 * 1024)
			maxPR, retries, timeout := RecommendTuning(bps)
			Expect(maxPR).To(Equal(int32(4)))
			Expect(retries).To(Equal(int32(4)))
			Expect(timeout).To(BeNumerically(">=", 2*time.Minute))
		})

		It("recommends very conservative settings for very slow connections (<1 MB/s)", func() {
			bps := int64(512 * 1024)
			maxPR, retries, timeout := RecommendTuning(bps)
			Expect(maxPR).To(Equal(int32(2)))
			Expect(retries).To(Equal(int32(5)))
			Expect(timeout).To(BeNumerically(">", 2*time.Minute))
		})

		It("caps the timeout at 30 minutes for very slow connections", func() {
			bps := int64(1024) // 1 KB/s
			_, _, timeout := RecommendTuning(bps)
			Expect(timeout).To(Equal(30 * time.Minute))
		})

		It("sets minimum timeout of 2 minutes for very fast connections", func() {
			bps := int64(1024 * 1024 * 1024) // 1 GB/s
			_, _, timeout := RecommendTuning(bps)
			Expect(timeout).To(Equal(2 * time.Minute))
		})
	})

	Describe("ResolveTuning", func() {
		It("returns defaults when no spec or recommendations", func() {
			maxPR, retries, timeout := ResolveTuning(nil, nil)
			Expect(maxPR).To(Equal(defaultMaxPerRegistry))
			Expect(retries).To(Equal(defaultRetries))
			Expect(timeout).To(Equal(time.Duration(defaultRequestTimeoutS) * time.Second))
		})

		It("uses recommendations when no user overrides", func() {
			recommended := &mirrorv1.NetworkPerformanceStatus{
				RecommendedMaxPerRegistry: 8,
				RecommendedRetries:        2,
				RecommendedRequestTimeout: &metav1.Duration{Duration: 3 * time.Minute},
			}
			spec := &mirrorv1.MirrorTuningConfig{AutoTune: true}
			maxPR, retries, timeout := ResolveTuning(spec, recommended)
			Expect(maxPR).To(Equal(int32(8)))
			Expect(retries).To(Equal(int32(2)))
			Expect(timeout).To(Equal(3 * time.Minute))
		})

		It("user overrides take precedence over recommendations", func() {
			recommended := &mirrorv1.NetworkPerformanceStatus{
				RecommendedMaxPerRegistry: 8,
				RecommendedRetries:        2,
				RecommendedRequestTimeout: &metav1.Duration{Duration: 3 * time.Minute},
			}
			userMax := int32(4)
			userRetries := int32(5)
			userTimeout := metav1.Duration{Duration: 10 * time.Minute}
			spec := &mirrorv1.MirrorTuningConfig{
				MaxPerRegistry: &userMax,
				Retries:        &userRetries,
				RequestTimeout: &userTimeout,
			}
			maxPR, retries, timeout := ResolveTuning(spec, recommended)
			Expect(maxPR).To(Equal(int32(4)))
			Expect(retries).To(Equal(int32(5)))
			Expect(timeout).To(Equal(10 * time.Minute))
		})

		It("partial user overrides mix with recommendations", func() {
			recommended := &mirrorv1.NetworkPerformanceStatus{
				RecommendedMaxPerRegistry: 8,
				RecommendedRetries:        2,
				RecommendedRequestTimeout: &metav1.Duration{Duration: 3 * time.Minute},
			}
			userRetries := int32(7)
			spec := &mirrorv1.MirrorTuningConfig{
				Retries: &userRetries,
			}
			maxPR, retries, timeout := ResolveTuning(spec, recommended)
			Expect(maxPR).To(Equal(int32(8)))
			Expect(retries).To(Equal(int32(7)))
			Expect(timeout).To(Equal(3 * time.Minute))
		})
	})

	Describe("FormatThroughput", func() {
		It("formats GB/s", func() {
			Expect(FormatThroughput(2 * 1024 * 1024 * 1024)).To(Equal("2.0 GB/s"))
		})

		It("formats MB/s", func() {
			Expect(FormatThroughput(50 * 1024 * 1024)).To(Equal("50.0 MB/s"))
		})

		It("formats KB/s", func() {
			Expect(FormatThroughput(512 * 1024)).To(Equal("512.0 KB/s"))
		})

		It("formats B/s", func() {
			Expect(FormatThroughput(500)).To(Equal("500 B/s"))
		})
	})

	Describe("reconcileNetworkPerformance", func() {
		var testScheme *runtime.Scheme

		BeforeEach(func() {
			testScheme = runtime.NewScheme()
			Expect(clientgoscheme.AddToScheme(testScheme)).To(Succeed())
			Expect(mirrorv1.AddToScheme(testScheme)).To(Succeed())
		})

		It("sets effective values from defaults when no mirrorTuning is specified", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec:       mirrorv1.DisconnectedPlatformSpec{Mode: mirrorv1.PlatformModeConnected},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			r.reconcileNetworkPerformance(context.Background(), platform)

			Expect(platform.Status.NetworkPerformance).NotTo(BeNil())
			Expect(platform.Status.NetworkPerformance.EffectiveMaxPerRegistry).To(Equal(defaultMaxPerRegistry))
			Expect(platform.Status.NetworkPerformance.EffectiveRetries).To(Equal(defaultRetries))
			Expect(platform.Status.NetworkPerformance.EffectiveRequestTimeout).NotTo(BeNil())
		})

		It("applies user overrides without running performance test when autoTune is false", func() {
			userMax := int32(10)
			userRetries := int32(1)
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: mirrorv1.PlatformModeConnected,
					MirrorTuning: &mirrorv1.MirrorTuningConfig{
						AutoTune:       false,
						MaxPerRegistry: &userMax,
						Retries:        &userRetries,
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			r.reconcileNetworkPerformance(context.Background(), platform)

			Expect(platform.Status.NetworkPerformance).NotTo(BeNil())
			Expect(platform.Status.NetworkPerformance.EffectiveMaxPerRegistry).To(Equal(int32(10)))
			Expect(platform.Status.NetworkPerformance.EffectiveRetries).To(Equal(int32(1)))
			// No throughput test should have run
			Expect(platform.Status.NetworkPerformance.ThroughputBytesPerSec).To(Equal(int64(0)))
		})

		It("skips performance test on airgapped clusters", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:         mirrorv1.PlatformModeAirgapped,
					MirrorTuning: &mirrorv1.MirrorTuningConfig{AutoTune: true},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			r.reconcileNetworkPerformance(context.Background(), platform)

			Expect(platform.Status.NetworkPerformance).NotTo(BeNil())
			Expect(platform.Status.NetworkPerformance.ThroughputBytesPerSec).To(Equal(int64(0)))
			Expect(platform.Status.NetworkPerformance.EffectiveMaxPerRegistry).To(Equal(defaultMaxPerRegistry))
		})

		It("skips performance test when recent result exists", func() {
			recent := metav1.NewTime(time.Now().Add(-10 * time.Minute))
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode:         mirrorv1.PlatformModeConnected,
					MirrorTuning: &mirrorv1.MirrorTuningConfig{AutoTune: true},
				},
				Status: mirrorv1.DisconnectedPlatformStatus{
					NetworkPerformance: &mirrorv1.NetworkPerformanceStatus{
						LastTestTime:              &recent,
						ThroughputBytesPerSec:     50 * 1024 * 1024,
						RecommendedMaxPerRegistry: 8,
						RecommendedRetries:        2,
						RecommendedRequestTimeout: &metav1.Duration{Duration: 3 * time.Minute},
					},
				},
			}
			r := &DisconnectedPlatformReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme).Build(),
				Scheme: testScheme,
			}
			r.reconcileNetworkPerformance(context.Background(), platform)

			// Should use existing recommendations, not re-test
			Expect(platform.Status.NetworkPerformance.EffectiveMaxPerRegistry).To(Equal(int32(8)))
			Expect(platform.Status.NetworkPerformance.EffectiveRetries).To(Equal(int32(2)))
		})
	})
})

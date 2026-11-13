package controller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

const (
	defaultTestEndpoint     = "https://quay.io/v2/"
	perfTestInterval        = 1 * time.Hour
	defaultMaxPerRegistry   = int32(6)
	defaultRetries          = int32(3)
	defaultRequestTimeoutS  = 300
	downloadTestMaxBytes    = 1 << 20 // 1 MiB cap for throughput probe
	downloadTestTimeout     = 30 * time.Second
	typicalLayerSizeMB      = 200
)

// ThroughputResult holds the outcome of a single network throughput measurement.
type ThroughputResult struct {
	BytesPerSec int64
	Endpoint    string
	Error       error
}

// MeasureThroughput downloads up to downloadTestMaxBytes from endpoint and
// returns the measured throughput. The caller may substitute the http.Client
// for testing.
func MeasureThroughput(ctx context.Context, endpoint string, httpClient *http.Client) ThroughputResult {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: downloadTestTimeout}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return ThroughputResult{Endpoint: endpoint, Error: fmt.Errorf("creating request: %w", err)}
	}

	start := time.Now()
	resp, err := httpClient.Do(req)
	if err != nil {
		return ThroughputResult{Endpoint: endpoint, Error: fmt.Errorf("performing request: %w", err)}
	}
	defer resp.Body.Close()

	n, _ := io.Copy(io.Discard, io.LimitReader(resp.Body, downloadTestMaxBytes))
	elapsed := time.Since(start)

	if n == 0 || elapsed == 0 {
		return ThroughputResult{Endpoint: endpoint, Error: fmt.Errorf("no data transferred")}
	}

	bps := int64(float64(n) / elapsed.Seconds())
	return ThroughputResult{BytesPerSec: bps, Endpoint: endpoint}
}

// RecommendTuning derives oc-mirror tuning parameters from a measured throughput.
func RecommendTuning(bps int64) (maxPerRegistry, retries int32, requestTimeout time.Duration) {
	mbps := float64(bps) / (1024 * 1024)

	switch {
	case mbps >= 50:
		maxPerRegistry = 8
		retries = 2
	case mbps >= 10:
		maxPerRegistry = 6
		retries = 3
	case mbps >= 1:
		maxPerRegistry = 4
		retries = 4
	default:
		maxPerRegistry = 2
		retries = 5
	}

	if bps > 0 {
		layerBytes := int64(typicalLayerSizeMB) * 1024 * 1024
		seconds := float64(layerBytes) / float64(bps)
		// Add 50% headroom, minimum 2 minutes
		timeout := time.Duration(seconds*1.5) * time.Second
		if timeout < 2*time.Minute {
			timeout = 2 * time.Minute
		}
		if timeout > 30*time.Minute {
			timeout = 30 * time.Minute
		}
		requestTimeout = timeout
	} else {
		requestTimeout = time.Duration(defaultRequestTimeoutS) * time.Second
	}

	return maxPerRegistry, retries, requestTimeout
}

// ResolveTuning merges user overrides with auto-tuned recommendations. User
// overrides always take precedence.
func ResolveTuning(spec *mirrorv1.MirrorTuningConfig, recommended *mirrorv1.NetworkPerformanceStatus) (maxPerRegistry, retries int32, requestTimeout time.Duration) {
	// Start from defaults
	maxPerRegistry = defaultMaxPerRegistry
	retries = defaultRetries
	requestTimeout = time.Duration(defaultRequestTimeoutS) * time.Second

	// Apply auto-tuned recommendations if available
	if recommended != nil {
		if recommended.RecommendedMaxPerRegistry > 0 {
			maxPerRegistry = recommended.RecommendedMaxPerRegistry
		}
		if recommended.RecommendedRetries > 0 {
			retries = recommended.RecommendedRetries
		}
		if recommended.RecommendedRequestTimeout != nil {
			requestTimeout = recommended.RecommendedRequestTimeout.Duration
		}
	}

	// User overrides always win
	if spec != nil {
		if spec.MaxPerRegistry != nil {
			maxPerRegistry = *spec.MaxPerRegistry
		}
		if spec.Retries != nil {
			retries = *spec.Retries
		}
		if spec.RequestTimeout != nil {
			requestTimeout = spec.RequestTimeout.Duration
		}
	}

	return maxPerRegistry, retries, requestTimeout
}

// FormatThroughput produces a human-friendly throughput string.
func FormatThroughput(bps int64) string {
	switch {
	case bps >= 1024*1024*1024:
		return fmt.Sprintf("%.1f GB/s", float64(bps)/(1024*1024*1024))
	case bps >= 1024*1024:
		return fmt.Sprintf("%.1f MB/s", float64(bps)/(1024*1024))
	case bps >= 1024:
		return fmt.Sprintf("%.1f KB/s", float64(bps)/1024)
	default:
		return fmt.Sprintf("%d B/s", bps)
	}
}

// reconcileNetworkPerformance runs a network throughput test (if due) and updates
// status with recommendations and effective tuning values.
func (r *DisconnectedPlatformReconciler) reconcileNetworkPerformance(ctx context.Context, platform *mirrorv1.DisconnectedPlatform) {
	logger := log.FromContext(ctx)

	tuning := platform.Spec.MirrorTuning
	autoTune := tuning != nil && tuning.AutoTune

	// Run a throughput test if autoTune is enabled and enough time has passed
	if autoTune && platform.Spec.Mode == mirrorv1.PlatformModeConnected {
		needsTest := true
		if platform.Status.NetworkPerformance != nil && platform.Status.NetworkPerformance.LastTestTime != nil {
			if time.Since(platform.Status.NetworkPerformance.LastTestTime.Time) < perfTestInterval {
				needsTest = false
			}
		}

		if needsTest {
			result := MeasureThroughput(ctx, defaultTestEndpoint, nil)
			if result.Error != nil {
				logger.Error(result.Error, "network performance test failed", "endpoint", defaultTestEndpoint)
			} else {
				recMax, recRetries, recTimeout := RecommendTuning(result.BytesPerSec)
				now := metav1.Now()
				if platform.Status.NetworkPerformance == nil {
					platform.Status.NetworkPerformance = &mirrorv1.NetworkPerformanceStatus{}
				}
				platform.Status.NetworkPerformance.ThroughputBytesPerSec = result.BytesPerSec
				platform.Status.NetworkPerformance.ThroughputHuman = FormatThroughput(result.BytesPerSec)
				platform.Status.NetworkPerformance.LastTestTime = &now
				platform.Status.NetworkPerformance.TestEndpoint = result.Endpoint
				platform.Status.NetworkPerformance.RecommendedMaxPerRegistry = recMax
				platform.Status.NetworkPerformance.RecommendedRetries = recRetries
				platform.Status.NetworkPerformance.RecommendedRequestTimeout = &metav1.Duration{Duration: recTimeout}
				logger.Info("network performance test completed",
					"throughput", FormatThroughput(result.BytesPerSec),
					"recommendedMaxPerRegistry", recMax,
					"recommendedRetries", recRetries,
					"recommendedRequestTimeout", recTimeout)
			}
		}
	}

	// Always resolve effective values (even without autoTune, user overrides apply)
	effMax, effRetries, effTimeout := ResolveTuning(tuning, platform.Status.NetworkPerformance)
	if platform.Status.NetworkPerformance == nil {
		platform.Status.NetworkPerformance = &mirrorv1.NetworkPerformanceStatus{}
	}
	platform.Status.NetworkPerformance.EffectiveMaxPerRegistry = effMax
	platform.Status.NetworkPerformance.EffectiveRetries = effRetries
	platform.Status.NetworkPerformance.EffectiveRequestTimeout = &metav1.Duration{Duration: effTimeout}
}

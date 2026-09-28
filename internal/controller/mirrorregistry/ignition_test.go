package mirrorregistry

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestBuildQuayConfig(t *testing.T) {
	cfg := &Config{
		Port:     8443,
		DataPath: "/opt/quay",
	}

	result := buildQuayConfig(cfg)

	checks := []struct {
		name     string
		contains string
	}{
		{"server hostname", "SERVER_HOSTNAME: localhost:8443"},
		{"sqlite db uri", "DB_URI: sqlite:////sqlite/quay_sqlite.db"},
		{"anonymous access", "FEATURE_ANONYMOUS_ACCESS: true"},
		{"https scheme", "PREFERRED_URL_SCHEME: https"},
		{"create namespace on push", "CREATE_NAMESPACE_ON_PUSH: true"},
		{"redis host", "host: localhost"},
		{"redis password", "password: mirror-registry-redis-pass"},
		{"setup complete", "SETUP_COMPLETE: true"},
		{"local storage", "LocalStorage"},
		{"datastorage path", "storage_path: /datastorage"},
	}

	for _, tc := range checks {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(result, tc.contains) {
				t.Errorf("expected config to contain %q", tc.contains)
			}
		})
	}
}

func TestBuildQuayConfigCustomPort(t *testing.T) {
	cfg := &Config{Port: 9443}
	result := buildQuayConfig(cfg)
	if !strings.Contains(result, "SERVER_HOSTNAME: localhost:9443") {
		t.Error("expected custom port in SERVER_HOSTNAME")
	}
}

func TestBuildInitScript(t *testing.T) {
	cfg := &Config{DataPath: "/mnt/registry"}
	result := buildInitScript(cfg)

	checks := []struct {
		name     string
		contains string
	}{
		{"config dir", `QUAY_CONFIG_DIR="/mnt/registry/quay-config"`},
		{"ca dir", `CA_DIR="/mnt/registry/ca"`},
		{"mkdir", "mkdir -p"},
		{"cert check", "ssl.cert"},
		{"chmod 644", "chmod 644"},
		{"ca trust", "update-ca-trust"},
		{"ca signing", "-CA \"${CA_DIR}/ca.crt\""},
		{"ca key", "-CAkey \"${CA_DIR}/ca.key\""},
		{"csr generation", "openssl req -new"},
		{"podman volumes", "podman volume create sqlite-storage"},
		{"redis secret", "podman secret create redis_pass"},
	}

	for _, tc := range checks {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(result, tc.contains) {
				t.Errorf("expected init script to contain %q", tc.contains)
			}
		})
	}
}

func TestBuildSystemdUnits(t *testing.T) {
	cfg := &Config{
		DataPath:       "/opt/quay",
		Port:           8443,
		QuayImage:      "registry.example.com/quay:v1",
		RedisImage:     "registry.example.com/redis:v1",
		PauseImage:     "registry.example.com/pause:v1",
		PullSecretJSON: `{"auths":{}}`,
	}

	units := buildSystemdUnits(cfg)
	if len(units) != 5 {
		t.Fatalf("expected 5 systemd units, got %d", len(units))
	}

	names := []string{
		"mirror-registry-init.service",
		"mirror-registry-pod.service",
		"mirror-registry-redis.service",
		"mirror-registry-app.service",
		"mirror-registry-admin-init.service",
	}
	for i, name := range names {
		u := units[i].(map[string]interface{})
		if u["name"] != name {
			t.Errorf("unit %d: expected name %q, got %q", i, name, u["name"])
		}
		if u["enabled"] != true {
			t.Errorf("unit %d: expected enabled=true", i)
		}
	}
}

func TestBuildPodUnit(t *testing.T) {
	cfg := &Config{
		Port:       8443,
		PauseImage: "registry.example.com/pause:v1",
	}
	result := buildPodUnit(cfg, " --authfile /opt/quay/auth.json")

	if !strings.Contains(result, "--authfile /opt/quay/auth.json") {
		t.Error("expected authfile argument")
	}
	if !strings.Contains(result, "registry.example.com/pause:v1") {
		t.Error("expected pause image")
	}
	if !strings.Contains(result, "--publish 8443:8443") {
		t.Error("expected port publish")
	}
	if !strings.Contains(result, "Before=kubelet.service crio.service") {
		t.Error("expected Before kubelet/crio ordering")
	}
}

func TestBuildAppUnit(t *testing.T) {
	cfg := &Config{
		DataPath:  "/mnt/data",
		QuayImage: "registry.example.com/quay:v1",
	}
	result := buildAppUnit(cfg, "")

	if !strings.Contains(result, "/mnt/data/quay-config:/quay-registry/conf/stack:Z") {
		t.Error("expected config bind mount with custom data path and :Z SELinux label")
	}
	if !strings.Contains(result, "registry.example.com/quay:v1 registry") {
		t.Error("expected quay image with 'registry' command")
	}
	if !strings.Contains(result, "sqlite-storage:/sqlite:Z,U") {
		t.Error("expected sqlite volume with :Z,U flags")
	}
}

func TestBuildAppUnitNoAuthfile(t *testing.T) {
	cfg := &Config{
		DataPath:  "/opt/quay",
		QuayImage: "quay.io/test/quay:v1",
	}
	result := buildAppUnit(cfg, "")
	if strings.Contains(result, "--authfile") {
		t.Error("expected no --authfile when authfileArg is empty")
	}
}

func TestBuildIgnitionFiles(t *testing.T) {
	cfg := &Config{
		DataPath:       "/opt/quay",
		Port:           8443,
		PullSecretJSON: `{"auths":{}}`,
		CACertPEM:      "---CERT---",
		CAKeyPEM:       "---KEY---",
	}

	files := buildIgnitionFiles(cfg)
	if len(files) != 6 {
		t.Fatalf("expected 6 files (config + init + admin-init + ca.crt + ca.key + auth), got %d", len(files))
	}

	paths := make(map[string]bool)
	for _, f := range files {
		fm := f.(map[string]interface{})
		paths[fm["path"].(string)] = true
	}

	expected := []string{
		"/opt/quay/quay-config/config.yaml",
		"/usr/local/bin/mirror-registry-init.sh",
		"/usr/local/bin/mirror-registry-admin-init.sh",
		"/opt/quay/ca/ca.crt",
		"/opt/quay/ca/ca.key",
		"/opt/quay/auth.json",
	}
	for _, p := range expected {
		if !paths[p] {
			t.Errorf("expected file path %q in ignition files", p)
		}
	}
}

func TestBuildIgnitionFilesNoPullSecret(t *testing.T) {
	cfg := &Config{
		DataPath:  "/opt/quay",
		Port:      8443,
		CACertPEM: "---CERT---",
		CAKeyPEM:  "---KEY---",
	}

	files := buildIgnitionFiles(cfg)
	if len(files) != 5 {
		t.Fatalf("expected 5 files (no auth.json without pull secret), got %d", len(files))
	}
}

func TestBuildIgnitionFilesNoCA(t *testing.T) {
	cfg := &Config{
		DataPath: "/opt/quay",
		Port:     8443,
	}

	files := buildIgnitionFiles(cfg)
	if len(files) != 3 {
		t.Fatalf("expected 3 files (no CA, no pull secret), got %d", len(files))
	}
}

func TestIgnitionFile(t *testing.T) {
	content := "test content"
	f := ignitionFile("/test/path", 0755, content)

	if f["path"] != "/test/path" {
		t.Error("wrong path")
	}
	if f["mode"] != int64(0755) {
		t.Error("wrong mode")
	}
	if f["overwrite"] != true {
		t.Error("expected overwrite=true")
	}

	src := f["contents"].(map[string]interface{})["source"].(string)
	prefix := "data:text/plain;base64,"
	if !strings.HasPrefix(src, prefix) {
		t.Fatal("expected base64 data URI")
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(src, prefix))
	if err != nil {
		t.Fatalf("failed to decode base64: %v", err)
	}
	if string(decoded) != content {
		t.Errorf("decoded content %q != expected %q", string(decoded), content)
	}
}

func TestBuildInitUnitConditionPath(t *testing.T) {
	result := buildInitUnit("/mnt/registry")
	if !strings.Contains(result, "ConditionPathExists=!/mnt/registry/quay-config/ssl.cert") {
		t.Error("expected ConditionPathExists with custom data path")
	}
}

func TestBuildAdminInitScript(t *testing.T) {
	cfg := &Config{Port: 8443}
	result := buildAdminInitScript(cfg)

	checks := []struct {
		name     string
		contains string
	}{
		{"health check url", "https://localhost:8443/api/v1/discovery"},
		{"user initialize endpoint", "https://localhost:8443/api/v1/user/initialize"},
		{"admin username", `"username": "admin"`},
		{"admin password", `"password": "mirror-registry-admin"`},
		{"max wait", "MAX_WAIT=300"},
	}
	for _, tc := range checks {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(result, tc.contains) {
				t.Errorf("expected admin init script to contain %q", tc.contains)
			}
		})
	}
}

func TestBuildAdminInitScriptCustomPort(t *testing.T) {
	cfg := &Config{Port: 9443}
	result := buildAdminInitScript(cfg)

	if !strings.Contains(result, "https://localhost:9443/api/v1/discovery") {
		t.Error("expected custom port in health check URL")
	}
	if !strings.Contains(result, "https://localhost:9443/api/v1/user/initialize") {
		t.Error("expected custom port in initialize URL")
	}
}

func TestBuildAdminInitUnit(t *testing.T) {
	result := buildAdminInitUnit()

	if !strings.Contains(result, "After=mirror-registry-app.service") {
		t.Error("expected After mirror-registry-app")
	}
	if !strings.Contains(result, "Requires=mirror-registry-app.service") {
		t.Error("expected Requires mirror-registry-app")
	}
	if !strings.Contains(result, "Type=oneshot") {
		t.Error("expected oneshot type")
	}
	if !strings.Contains(result, "mirror-registry-admin-init.sh") {
		t.Error("expected admin init script path")
	}
}

func TestBuildRedisUnit(t *testing.T) {
	cfg := &Config{
		RedisImage: "registry.example.com/redis:v1",
	}
	result := buildRedisUnit(cfg, "")

	if !strings.Contains(result, "registry.example.com/redis:v1") {
		t.Error("expected redis image")
	}
	if !strings.Contains(result, "--secret=redis_pass,type=env,target=REDIS_PASSWORD") {
		t.Error("expected redis password secret")
	}
}
